from datetime import timedelta
from pathlib import Path
import shutil

import pendulum
import yaml
from airflow.sdk import DAG, task, Variable, get_current_context

TZ = "Asia/Kathmandu"
SETTING_KEYS = ("source_dir", "archive_dir", "conn_id", "database", "stg_schema", "stage", "file_format", "feeds_config")

def load_settings():
    settings = {key: Variable.get(key) for key in SETTING_KEYS}
    feeds = yaml.safe_load(Path(settings["feeds_config"]).read_text())["feeds"]
    return settings, feeds

def run_dates():
    logical = pendulum.instance(get_current_context()["logical_date"]).in_timezone(TZ)
    return logical.strftime("%Y%m%d"), logical.strftime("%Y-%m-%d")

def file_path(settings, feed, stamp):
    return Path(settings["source_dir"]) / f"{feed['file_prefix']}_{stamp}.csv"


with DAG(
    dag_id="bronze_pipeline",
    start_date=pendulum.datetime(2026, 11, 13, tz=TZ),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "snowflake"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    @task.sensor(poke_interval=30, timeout=600, mode="poke")
    def wait_for_files():
        settings, feeds = load_settings()
        stamp, _ = run_dates()
        missing = [
            file_path(settings, feed, stamp).name
            for feed in feeds.values()
            if feed["required_daily"] and not file_path(settings, feed, stamp).exists()
        ]
        if missing:
            print("Still waiting for files: ", missing)
            return False
        print("All required files are present for ", stamp)
        return True

    @task
    def build_load_list():
        settings, feeds = load_settings()
        stamp, _ = run_dates()
        to_load = []
        for name, feed in feeds.items():
            path = file_path(settings, feed, stamp)
            if path.exists():
                to_load.append({
                        "name": name,
                        "prefix": feed["file_prefix"],
                        "path": str(path),
                        "table": feed["table"],
                        "columns": feed["columns"],
                    })
            else:
                print(f"File {path} does not exist, skipping")

        print("Will load:", [f["name"] for f in to_load])
        return to_load

    @task
    def load_feed(feed):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        settings, _ = load_settings()
        stamp, file_date = run_dates()
        batch_id = get_current_context()["run_id"]
        file_name = Path(feed["path"]).name

        with open(feed["path"], encoding="utf-8") as handle:
            file_rows = sum(1 for _ in handle) - 1
        if file_rows < 1:
            raise ValueError(f"R1 failed: {feed['name']} file {file_name} has no data rows")

        db, schema = settings["database"], settings["stg_schema"]
        table = f"{db}.{schema}.{feed['table']}"
        stage_path = f"@{db}.{schema}.{settings['stage']}/{feed['prefix']}/{stamp}/"
        file_format = f"{db}.{schema}.{settings['file_format']}"
        columns = feed["columns"]
        source_cols = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
        target_cols = ", ".join(columns + ["SRC_FILE_NM", "FILE_DATE", "SRC_ROW_NUM", "BATCH_ID"])

        hook = SnowflakeHook(snowflake_conn_id=settings["conn_id"])
        hook.run(f"PUT file://{feed['path']} {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        hook.run(f"DELETE FROM {table} WHERE FILE_DATE = '{file_date}'")
        hook.run(f"""
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
        """)

        loaded = hook.get_first(f"SELECT COUNT(*) FROM {table} WHERE FILE_DATE = '{file_date}'")[0]
        print(feed["name"], "expected", file_rows, "loaded", loaded)
        if loaded != file_rows:
            raise ValueError(f"{feed['name']}: file has {file_rows} rows but table has {loaded}")

        archive_folder = Path(settings["archive_dir"]) / stamp
        archive_folder.mkdir(parents=True, exist_ok=True)
        shutil.copy2(feed["path"], archive_folder / file_name)
        print("Archived", file_name, "to", archive_folder)
        return {"feed": feed["name"], "rows": loaded}

    wait_for_files() >> (plan := build_load_list()) >> load_feed.expand(feed=plan)