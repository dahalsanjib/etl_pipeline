from datetime import datetime

from airflow.sdk import DAG, dag, task

STAGE = "RETAIL_DW.STG.STG_SOURCE_FILES"
FILE_FORMAT = "RETAIL_DW.STG.FF_SOURCE_CSV"
TABLE = "RETAIL_DW.STG.STG_ITEM_MASTER"
LOCAL_FILE = "/opt/airflow/data/source/item_master_20261113.csv"
FILE_DATE = "2026-11-13"
STAGE_PATH = f"@{STAGE}/item_master/20261113/"

with DAG(
    dag_id="test_load_item_master",
    start_date=datetime(2026, 11, 13),
    schedule=None,
    catchup=False,
    tags=["test", "snowflake"]
) as dag:

    @task
    def load_one_file():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id = "snowflake_conn")

        hook.run(f"PUT file://{LOCAL_FILE} {STAGE_PATH} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        hook.run(f"DELETE FROM {TABLE} WHERE FILE_DATE = '{FILE_DATE}'")

        hook.run(f"""
            COPY INTO {TABLE}
                (ITEM_ID, ITEM_NAME, SUBCLASS_ID, SUBCLASS_NAME, BRAND, UNIT_COST, UNIT_PRICE, STATUS,
                 FILE_NAME, FILE_DATE, SRC_ROW_NO, BATCH_ID)
                 FROM (
                    SELECT $1, $2, $3, $4, $5, $6, $7, $8,
                       METADATA$FILENAME,
                       TO_DATE('{FILE_DATE}'),
                       METADATA$FILE_ROW_NUMBER,
                       'MANUAL_TEST'
                    FROM {STAGE_PATH}
                 )
                 FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
                 FORCE = TRUE
        """)

        count = hook.get_first(f"SELECT COUNT(*) FROM {TABLE} WHERE FILE_DATE = '{FILE_DATE}'")
        print("ROWS loaded for ", FILE_DATE, "=", count)

    load_one_file()