
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

from airflow.contrib.operators.http_to_gcs_operator import HttpToGcsOperator


args = {"owner": "Bjorn_Rijpert", "start_date": datetime(2019,9,20)}

dag = DAG( 
  dag_id="exercise4", 
   default_args=args,
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

get_exchange_rate = HttpToGcsOperator (
  task_id = "get_exchange",
  endpoint = "/history?start_at={{yesterday_ds}}&end_at={{ds}}&symbols=EUR&base=GBP",
  gcs_bucket="airflow-training-data2",
  gcs_path="currency/{{ ds }}-" + currency + ".json",  
  http_conn_id="airflow-training-currency-http",
  gcs_conn_id = "google_cloud_default",
  *args,
  **kwargs
  
)



start >> get_data 
start >> get_exchange_rate
