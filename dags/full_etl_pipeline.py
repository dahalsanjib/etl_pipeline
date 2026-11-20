import re
import shutil
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

import pendulum
import yaml
from airflow.sdk import DAG, Variable, get_current_context, task

TZ = "Asia/Kathmandu"
SETTING_KEYS = (
    "source_dir",
    "archive_dir",
    "sql_dir",
    "conn_id",
    "database",
    "stg_schema",
    "stage",
    "file_format",
    "feeds_config",
)


def load_settings():
    settings = {key: Variable.get(key) for key in SETTING_KEYS}
    config = yaml.safe_load(Path(settings["feeds_config"]).read_text())
    return settings, config


def run_dates():
    logical = pendulum.instance(get_current_context()["logical_date"]).in_timezone(TZ)
    return logical.strftime("%Y%m%d"), logical.strftime("%Y-%m-%d")


def file_path(settings, feed, stamp):
    return Path(settings["source_dir"]) / f"{feed['file_prefix']}_{stamp}.csv"


def get_hook(settings):
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=settings["conn_id"])


def read_sql(settings, script, file_date, batch_id):
    text = (Path(settings["sql_dir"]) / script).read_text(encoding="utf-8")
    lines = [
        line
        for line in text.splitlines()
        if not re.match(r"\s*(USE DATABASE|SET fd|SET batch)\b", line, re.I)
    ]
    text = "\n".join(lines)
    text = re.sub(r"\$fd\b", lambda m: f"'{file_date}'", text)
    text = re.sub(r"\$batch\b", lambda m: "'" + batch_id.replace("'", "''") + "'", text)
    return [s.strip() for s in text.split(";") if s.strip()]


def run_script(hook, settings, script, file_date, batch_id):
    hook.run(read_sql(settings, script, file_date, batch_id))


def write_log(hook, settings, config, batch_id, step, source, target, file_date, status, stats, start, reason):
    end = pendulum.now(TZ)
    table = f"{settings['database']}.{config['control']['batch_log']}"
    hook.run(
        f"INSERT INTO {table} (BATCH_ID, STEP_NAME, SOURCE_NAME, TARGET_NAME, FILE_DATE, STATUS, "
        "ROWS_READ, ROWS_LOADED, ROWS_REJECTED, START_TS, END_TS, ELAPSED_SEC, FAILURE_REASON) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        parameters=(
            batch_id, step, source, target, file_date, status,
            stats.get("read"), stats.get("loaded"), stats.get("rejected"),
            start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"),
            round((end - start).total_seconds(), 2), reason,
        ),
    )


@contextmanager
def logged_step(hook, settings, config, step, source, target, file_date):
    batch_id = get_current_context()["run_id"]
    start = pendulum.now(TZ)
    stats = {"status": "SUCCESS", "note": None}
    try:
        yield stats
    except Exception as exc:
        write_log(hook, settings, config, batch_id, step, source, target, file_date, "FAILED", stats, start, str(exc)[:900])
        raise
    write_log(hook, settings, config, batch_id, step, source, target, file_date, stats["status"], stats, start, stats["note"])


with DAG(
    dag_id="full_etl_pipeline",
    start_date=pendulum.datetime(2026, 11, 13, tz=TZ),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["etl", "snowflake", "pipeline"],
) as dag:

    @task.sensor(poke_interval=30, timeout=600, mode="poke")
    def wait_for_files():
        settings, config = load_settings()
        stamp, _ = run_dates()
        missing = [
            file_path(settings, feed, stamp).name
            for feed in config["feeds"].values()
            if feed["required_daily"] and not file_path(settings, feed, stamp).exists()
        ]
        if missing:
            print("Still waiting for:", missing)
            return False
        print("All required files present for", stamp)
        return True

    @task
    def build_load_list():
        settings, config = load_settings()
        stamp, _ = run_dates()
        to_load = []
        for name, feed in config["feeds"].items():
            path = file_path(settings, feed, stamp)
            if path.exists():
                to_load.append(
                    {
                        "name": name,
                        "prefix": feed["file_prefix"],
                        "path": str(path),
                        "table": feed["table"],
                        "columns": feed["columns"],
                        "stage": feed["stage"],
                        "silver_script": feed["silver_script"],
                        "silver_table": feed["silver_table"],
                    }
                )
            else:
                print("No file for", name, "on", stamp, "- skipping")
        print("Will load:", [f["name"] for f in to_load])
        return to_load

    @task
    def register_batch():
        settings, config = load_settings()
        _, file_date = run_dates()
        context = get_current_context()
        batch_id = context["run_id"]
        run_type = context["dag_run"].run_type
        run_type = str(getattr(run_type, "value", run_type)).upper()
        hook = get_hook(settings)
        table = f"{settings['database']}.{config['control']['param']}"
        hook.run(f"DELETE FROM {table} WHERE BATCH_ID = %s", parameters=(batch_id,))
        hook.run(
            f"INSERT INTO {table} (BATCH_ID, BUSINESS_DATE, RUN_TYPE) VALUES (%s, %s, %s)",
            parameters=(batch_id, file_date, run_type),
        )
        print("Registered batch", batch_id, file_date, run_type)

    @task
    def load_bronze(feed):
        settings, config = load_settings()
        stamp, file_date = run_dates()
        batch_id = get_current_context()["run_id"]
        file_name = Path(feed["path"]).name
        hook = get_hook(settings)

        db, schema = settings["database"], settings["stg_schema"]
        table = f"{db}.{schema}.{feed['table']}"
        stage_path = f"@{db}.{schema}.{settings['stage']}/{feed['prefix']}/{stamp}/"
        file_format = f"{db}.{schema}.{settings['file_format']}"
        columns = feed["columns"]
        source_cols = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
        target_cols = ", ".join(columns + ["SRC_FILE_NM", "FILE_DATE", "SRC_ROW_NUM", "BATCH_ID"])

        with logged_step(hook, settings, config, "BRONZE_LOAD", file_name, feed["table"], file_date) as stats:
            with open(feed["path"], encoding="utf-8") as handle:
                file_rows = sum(1 for _ in handle) - 1
            if file_rows < 1:
                raise ValueError(f"R1 failed: {feed['name']} file {file_name} has no data rows")

            hook.run(f"PUT file://{feed['path']} {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
            hook.run(f"DELETE FROM {table} WHERE FILE_DATE = '{file_date}'")
            hook.run(
                f"""
                COPY INTO {table} ({target_cols})
                FROM (
                    SELECT {source_cols},
                           METADATA$FILENAME,
                           TO_DATE('{file_date}'),
                           METADATA$FILE_ROW_NUMBER,
                           '{batch_id}'
                    FROM {stage_path}
                )
                FILE_FORMAT = (FORMAT_NAME = '{file_format}')
                FORCE = TRUE
                """
            )
            loaded = hook.get_first(f"SELECT COUNT(*) FROM {table} WHERE FILE_DATE = '{file_date}'")[0]
            print(feed["name"], "expected", file_rows, "loaded", loaded)
            if loaded != file_rows:
                raise ValueError(f"{feed['name']}: file has {file_rows} rows but table has {loaded}")

            archive_folder = Path(settings["archive_dir"]) / stamp
            archive_folder.mkdir(parents=True, exist_ok=True)
            shutil.copy2(feed["path"], archive_folder / file_name)
            stats.update(read=file_rows, loaded=loaded, rejected=0)
        return {"feed": feed["name"], "rows": loaded}

    @task
    def run_silver(feeds):
        settings, config = load_settings()
        _, file_date = run_dates()
        batch_id = get_current_context()["run_id"]
        db = settings["database"]
        rej = f"{db}.{config['control']['rej_table']}"
        hook = get_hook(settings)

        for feed in sorted(feeds, key=lambda f: f["stage"]):
            name = feed["name"]
            stg_table = f"{db}.{settings['stg_schema']}.{feed['table']}"
            silver_table = f"{db}.{feed['silver_table']}"
            with logged_step(hook, settings, config, f"SILVER_{name.upper()}", feed["table"], feed["silver_table"], file_date) as stats:
                run_script(hook, settings, feed["silver_script"], file_date, batch_id)
                bronze = hook.get_first(f"SELECT COUNT(*) FROM {stg_table} WHERE FILE_DATE = '{file_date}'")[0]
                clean = hook.get_first(f"SELECT COUNT(*) FROM {silver_table} WHERE FILE_DATE = '{file_date}'")[0]
                rejected = hook.get_first(
                    f"SELECT COUNT(*) FROM {rej} WHERE FEED_NAME = '{name}' AND FILE_DATE = '{file_date}'"
                )[0]
                alerts = hook.get_first(
                    f"SELECT COUNT(*) FROM {rej} WHERE FEED_NAME = '{name}' AND FILE_DATE = '{file_date}' AND RULE_ID = 'R4'"
                )[0]
                stats.update(read=bronze, loaded=clean, rejected=rejected)
                print(name, "bronze", bronze, "clean", clean, "rejected", rejected)
                if bronze != clean + rejected:
                    raise ValueError(f"R7 failed for {name}: bronze {bronze} != clean {clean} + rejected {rejected}")
                if alerts:
                    stats["status"] = "WARNING"
                    stats["note"] = f"{alerts} rows rejected by R4 (item or store not in master)"
                    print("WARNING:", name, stats["note"])

    def run_gold(key):
        settings, config = load_settings()
        _, file_date = run_dates()
        batch_id = get_current_context()["run_id"]
        db = settings["database"]
        hook = get_hook(settings)
        for step in config[key]:
            target = f"{db}.{step['target']}"
            with logged_step(hook, settings, config, step["script"], "TMP", step["target"], file_date) as stats:
                last = hook.get_first(
                    f"SELECT MAX(LAST_LOAD_DATE) FROM {db}.{config['control']['load_dates']} WHERE TARGET_NAME = %s",
                    parameters=(step["target"],),
                )[0]
                if last is not None and str(last) > file_date:
                    raise ValueError(
                        f"Out-of-order load blocked: {step['target']} already loaded to {last}, run date is {file_date}. "
                        "Truncate Gold and replay from the first date."
                    )
                run_script(hook, settings, step["script"], file_date, batch_id)
                count = hook.get_first(step["count_sql"].format(target=target, fd=file_date))[0]
                stats.update(loaded=count)
                print(step["target"], "rows", count)

    @task
    def load_gold_dims():
        run_gold("gold_dims")

    @task
    def load_gold_facts():
        run_gold("gold_facts")

    @task
    def reconcile(feeds):
        settings, config = load_settings()
        _, file_date = run_dates()
        batch_id = get_current_context()["run_id"]
        db = settings["database"]
        hook = get_hook(settings)

        with logged_step(hook, settings, config, "RECONCILE", "TMP/TGT", "TGT", file_date) as stats:
            sql = read_sql(settings, config["reconcile_script"], file_date, batch_id)[0]
            rows = hook.get_records(sql)
            for row in rows:
                print(row)
            failed = [row for row in rows if row[3] != "PASS"]
            stats.update(read=len(rows), loaded=len(rows) - len(failed))
            if failed:
                raise ValueError(f"R8 failed: {failed}")

        targets = [f"{settings['stg_schema']}.{f['table']}" for f in feeds]
        targets += [f["silver_table"] for f in feeds]
        targets += [s["target"] for s in config["gold_dims"] + config["gold_facts"]]
        table = f"{db}.{config['control']['load_dates']}"
        for target in targets:
            hook.run(
                f"MERGE INTO {table} t USING (SELECT %s AS TARGET_NAME, TO_DATE(%s) AS LAST_LOAD_DATE, %s AS BATCH_ID) s "
                "ON t.TARGET_NAME = s.TARGET_NAME "
                "WHEN MATCHED THEN UPDATE SET LAST_LOAD_DATE = GREATEST(t.LAST_LOAD_DATE, s.LAST_LOAD_DATE), "
                "BATCH_ID = s.BATCH_ID, UPDATED_TS = CURRENT_TIMESTAMP() "
                "WHEN NOT MATCHED THEN INSERT (TARGET_NAME, LAST_LOAD_DATE, BATCH_ID) "
                "VALUES (s.TARGET_NAME, s.LAST_LOAD_DATE, s.BATCH_ID)",
                parameters=(target, file_date, batch_id),
            )

    wait = wait_for_files()
    plan = build_load_list()
    register = register_batch()
    bronze = load_bronze.expand(feed=plan)
    silver = run_silver(plan)
    dims = load_gold_dims()
    facts = load_gold_facts()
    recon = reconcile(plan)

    wait >> plan >> register >> bronze >> silver >> dims >> facts >> recon