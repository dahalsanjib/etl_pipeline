"""Pipeline tests (pytest). Run inside the Airflow container, see README.

  1. the DAG imports and has the expected tasks, order, retries and settings
  2. feeds.yaml agrees with the SQL files and the source files
  3. the SQL files turn into valid statements after the DAG fills in the parameters
"""
import os
import re
import sys
from datetime import timedelta
from pathlib import Path

import pytest


def find_root():
    env = os.environ.get("PROJECT_ROOT")
    if env:
        return Path(env)
    for candidate in (Path("/opt/airflow"), Path(__file__).resolve().parents[1]):
        if (candidate / "include" / "config" / "feeds.yaml").exists():
            return candidate
    return Path(__file__).resolve().parents[1]


ROOT = find_root()
SQL_DIR = ROOT / "include" / "sql"
DAG_ID = "full_etl_pipeline"
DATES = ["20261113", "20261114", "20261115", "20261116", "20261117"]
EXPECTED_TASKS = [
    "wait_for_files", "build_load_list", "register_batch", "load_bronze",
    "run_silver", "load_gold_dims", "load_gold_facts", "reconcile",
]
DIRECT_EDGES = list(zip(EXPECTED_TASKS, EXPECTED_TASKS[1:]))  # (parent, child)


def read_config():
    import yaml
    return yaml.safe_load((ROOT / "include" / "config" / "feeds.yaml").read_text())


def script_names(config):
    names = [f["silver_script"] for f in config["feeds"].values()]
    names += [s["script"] for s in config["gold_dims"] + config["gold_facts"]]
    names.append(config["reconcile_script"])
    return names


def task_setting(task, name):
    value = getattr(task, name, None)
    if value is None:  # mapped tasks keep these in partial_kwargs
        value = getattr(task, "partial_kwargs", {}).get(name)
    return value


# ---------------------------------------------------------------- fixtures
@pytest.fixture(scope="module")
def dag_module():
    sys.path.insert(0, str(ROOT / "dags"))
    import full_etl_pipeline  # importing runs the DAG file, so an import error fails here
    return full_etl_pipeline


@pytest.fixture(scope="module")
def dag(dag_module):
    return dag_module.dag


@pytest.fixture(scope="module")
def config():
    return read_config()


# ---------------------------------------------------------------- DAG structure
def test_dag_id(dag):
    assert dag.dag_id == DAG_ID


def test_has_expected_tasks(dag):
    assert sorted(dag.task_dict) == sorted(EXPECTED_TASKS)


def test_dag_settings(dag):
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    schedule = str(getattr(dag, "schedule", None) or getattr(getattr(dag, "timetable", None), "summary", ""))
    assert schedule in ("@daily", "0 0 * * *")


@pytest.mark.parametrize("task_id", EXPECTED_TASKS)
def test_retries(dag, task_id):
    task = dag.task_dict[task_id]
    assert task_setting(task, "retries") == 2
    assert task_setting(task, "retry_delay") == timedelta(minutes=5)


@pytest.mark.parametrize("parent,child", DIRECT_EDGES)
def test_direct_dependency(dag, parent, child):
    assert parent in dag.task_dict[child].upstream_task_ids


def test_no_cycles_and_correct_order(dag):
    remaining = {t: set(task.upstream_task_ids) for t, task in dag.task_dict.items()}
    ready = [t for t, ups in remaining.items() if not ups]
    order = []
    while ready:
        current = ready.pop()
        order.append(current)
        for t, ups in remaining.items():
            if current in ups:
                ups.discard(current)
                if not ups and t not in order and t not in ready:
                    ready.append(t)
    assert len(order) == len(remaining), "cycle found in the DAG"
    position = {t: i for i, t in enumerate(order)}
    for before, after in DIRECT_EDGES:
        assert position[before] < position[after], f"{before} must run before {after}"


# ---------------------------------------------------------------- config and files
def test_every_configured_script_exists(config):
    for name in script_names(config):
        assert (SQL_DIR / name).is_file(), f"missing {name}"


def test_every_feed_has_required_fields(config):
    required = ("file_prefix", "table", "required_daily", "stage", "silver_script", "silver_table", "columns")
    for name, feed in config["feeds"].items():
        for key in required:
            assert key in feed, f"{name} is missing {key}"


def test_masters_run_before_sales_and_inventory(config):
    feeds = config["feeds"]
    assert feeds["item_master"]["stage"] < feeds["pos_sales"]["stage"]
    assert feeds["store_master"]["stage"] < feeds["inventory_snapshot"]["stage"]


@pytest.mark.parametrize("stamp", DATES)
def test_daily_source_files_exist(config, stamp):
    for feed in config["feeds"].values():
        if feed["required_daily"]:
            assert (ROOT / "data" / "source" / f"{feed['file_prefix']}_{stamp}.csv").is_file()


@pytest.mark.parametrize("stamp", ["20261113", "20261115"])
def test_store_master_files_exist(stamp):
    assert (ROOT / "data" / "source" / f"store_master_{stamp}.csv").is_file()


# ---------------------------------------------------------------- SQL
def all_scripts():
    return script_names(read_config())


@pytest.mark.parametrize("name", all_scripts())
def test_sql_turns_into_clean_statements(dag_module, name):
    statements = dag_module.read_sql({"sql_dir": str(SQL_DIR)}, name, "2026-11-15", "BATCH_X")
    assert statements
    for statement in statements:
        assert "$fd" not in statement
        assert "$batch" not in statement
        assert not re.match(r"\s*(USE DATABASE|SET )", statement, re.I)
        assert re.sub(r"--[^\n]*", "", statement).strip(), f"{name} has an empty statement"


def test_reconcile_is_one_statement(dag_module, config):
    statements = dag_module.read_sql({"sql_dir": str(SQL_DIR)}, config["reconcile_script"], "2026-11-15", "B")
    assert len(statements) == 1


def test_date_and_batch_are_substituted(dag_module):
    text = " ".join(dag_module.read_sql({"sql_dir": str(SQL_DIR)}, "tgt_fct_sales.sql", "2026-11-15", "BATCH_X"))
    assert "'2026-11-15'" in text
    assert "'BATCH_X'" in text


def test_silver_scripts_write_rejects_with_rule_ids(config):
    for feed in config["feeds"].values():
        text = (SQL_DIR / feed["silver_script"]).read_text()
        assert "CTL.C_REJ_TABLE" in text
        assert "RULE_ID" in text


def test_out_of_order_guard_is_in_the_dag():
    assert "Out-of-order load blocked" in (ROOT / "dags" / "full_etl_pipeline.py").read_text()