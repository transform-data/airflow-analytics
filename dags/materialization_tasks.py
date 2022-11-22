import os
import requests
import datetime
import pendulum
os.environ["GIT_PYTHON_REFRESH"] = "quiet"
from transform import MQLClient
from airflow.decorators import dag, task
from airflow.models import Variable

TRANSFORM_API_KEY = Variable.get('TRANSFORM_API_KEY')
@dag(
 dag_id="materialize_sales_metrics",
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
#todo: figure out how to run start and end date for materializations based of most recent timestamp for each materialization.
def RunMaterialization():
    @task
    def run_materialization():
        mql = MQLClient(TRANSFORM_API_KEY)
        mats = mql.list_materializations()
        success =0
        fail = 0
        for mat in mats:
            try:
                materialize_response= mql.materialize(mat.name, timeout=3600, start_time='2018-01-01')
                print(f"Successfully created table: {materialize_response.schema}.{materialize_response.table} ")
                success +=1
            except RuntimeError as err:
                print("Runtime error",err)
                fail +=1
            except Exception as err:
                print(f"Unexpected {err}, {type(err)}")
                fail += 1
        print(f"Successful materialization: {success}. Failed materialization: {fail} ")
    run_materialization()
dag = RunMaterialization()






