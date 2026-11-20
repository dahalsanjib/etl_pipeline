from airflow.sdk import dag, task
from datetime import datetime
from airflow.timetables.trigger import CronTriggerTimetable
#0 16 * * MON-TUE"
@dag(
    dag_id = "simple_etl",    
    start_date= datetime(2026,11,5),
    schedule= "@daily",
    # schedule= CronTriggerTimetable("0 16 * * MON-FRI", timezone = "America/Los_Angeles"),
    catchup= False,
    tags = "bootcamp"
)
def simple_etl():
    @task.python
    def extract():
        print("Extracting the data")
        
    @task.python
    def transform():
        print("Transforming the data")

    @task.python
    def load():
        print("Loading the data")
    ext = extract()
    trf = transform()
    ld = load()
    ext>>trf>>ld
simple_etl()
