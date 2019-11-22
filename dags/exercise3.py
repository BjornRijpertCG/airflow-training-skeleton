
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator

from datetime import datetime

args = {"owner": "Bjorn_Rijpert", "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG( 
  dag_id="exercise3", 
   default_args=args, 
   start_date=datetime.today(),
   schedule_interval="@daily",
)

def _weekday():
   print(datetime.today().weekday())
    

printweekday = PythonOperator( 
  task_id="print_weekday", 
  python_callable=_weekday,
  dag=dag,
) 

weekday_person_to_email = {
      "Mon": "Bob",   # Monday
      "Tue": "Joe",   # Tuesday
      "Wed": "Alice", # Wednesday
      "Thu": "Joe",   # Thursday
      "Fri": "Alice", # Friday 
      "Sat": "Alice", # Saturday 
      "Sun": "Alice", # Sunday
  } for name in set(weekday_person_to_email.values()):
         branching >> DummyOperator(task_id=name, dag=dag) 

branching = BranchPythonOperator(
  task_id="branching", 
  python_callable=_get_weekday, 
  provide_context=True, 
  dag=dag
)

dummyTask = DummyOperator(
  task_id="end", 
  dag=dag

)

printweekday >> branching >> dummyTask
