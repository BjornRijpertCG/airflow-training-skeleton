
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
from airflow.contrib.operators.sql_to_gcs import BaseSQLToGoogleCloudStorageOperator

args = {"owner": "Bjorn_Rijpert", "start_date": "2019-09-20"}

dag = DAG( 
  dag_id="exercise3", 
   default_args=args, 
   start_date=datetime.today(),
   schedule_interval="@daily",
)

