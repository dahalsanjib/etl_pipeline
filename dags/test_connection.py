from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG, task

SOURCE_DIR = "/opt/airflow/data/source"

with DAG(
    dag_id='test_connection',
    start_date=datetime(2026, 11, 18),
    catchup=False,
    tags=['test', 'snowflake']
) as dag:

    @task
    def check_files():
        files = sorted(p.name for p in Path(SOURCE_DIR).glob("*.csv"))
        print(f"Found {len(files)} files in {SOURCE_DIR}")
        for name in files:
            print(name)
        if not files:
            raise FileNotFoundError("No CSV files in SOURCE")

    @task
    def check_snowflake():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        row = hook.get_first(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()"
        )

        print("user, role, warehouse, database = ", row)

    check_files()
    check_snowflake()