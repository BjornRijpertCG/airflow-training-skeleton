import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator

from datetime import datetime

args = {"owner": "Bjorn_Rijpert", "start_date": airflow.utils.dates.days_ago(5)}

dag = DAG( 
  dag_id="exercise2", 
   default_args=args, 
   start_date=datetime.today(),
   schedule_interval="@daily",
)

def _current_date():
    print(date.today())

t1 = PythonOperator( 
  task_id="print_date", 
  python_callable=_current_date, 
  dag=dag,
)

t2 = BashOperator( 
  task_id="sleep_a_bit_1", 
  bash_command="sleep 1", 
  dag=dag,
)

t3 = BashOperator( 
  task_id="sleep_a_bit_5", 
  bash_command="sleep 5", 
  dag=dag,
)

t4 = BashOperator( 
  task_id="sleep_a_bit_10", 
  bash_command="sleep 10", 
  dag=dag,
)

t5 = DummyOperator(
  task_id="end", 
  dag=dag,

)

t1 >> [t2,t3,t4] >> t5
