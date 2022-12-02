import os
import requests
import datetime
import pendulum
os.environ["GIT_PYTHON_REFRESH"] = "quiet"
from transform import MQLClient
from airflow.decorators import dag, task
from airflow import DAG
from airflow.models import Variable
from transform_airflow.operators import MaterializeOperator

##for local testing
#TRANSFORM_API_KEY = os.environ["TRANSFORM_API_KEY"]

TRANSFORM_API_KEY = Variable.get('TRANSFORM_API_KEY')
mql = MQLClient(TRANSFORM_API_KEY)
materialization_list = mql.list_materializations()
with DAG(
    dag_id="run_materializations",
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    ) as dag:
        for materialization in materialization_list:
            mat_operator = MaterializeOperator(
                task_id= f"{materialization.name}",
                dag=dag,
                materialization_name=materialization.name,
                start_time="2018-01-01",
                creds={
                "TRANSFORM_API_KEY": TRANSFORM_API_KEY,
                "MQL_QUERY_URL": "<your mql server url>"
                }
            )






