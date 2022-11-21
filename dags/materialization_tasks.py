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
#todo: Figure out how to run start and end date for materializations based of most recent timestamp for each materialization.
def RunMaterialization():
    @task
    def run_materialization():
        mql = MQLClient(TRANSFORM_API_KEY)
        mats = mql.list_materializations()
        for mat in mats:
            mql.materialize(mat.name, timeout=3600)
    run_materialization()
dag = RunMaterialization()






