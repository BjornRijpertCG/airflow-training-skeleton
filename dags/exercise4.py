
#september 20th

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator

from datetime import datetime

from decimal import Decimal
import time

import sys


from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator


args = {"owner": "Bjorn_Rijpert", "start_date": "2019-09-20"}

dag = DAG( 
  dag_id="exercise4", 
   default_args=args, 
   start_date=datetime.today(),
   schedule_interval="@daily",
)

start = DummyOperator(
  task_id="start", 
  dag=dag,

)

get_data = PostgresToGoogleCloudStorageOperator(
  task_id="get_data",
  sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
  bucket="airflow-training-data2",
  filename="{{ ds }}/properties_{}.json", postgres_conn_id="airflow_exercise4",
  dag=dag,
)


start >> get_data
