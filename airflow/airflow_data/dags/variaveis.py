from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

def print_variable(**kwargs):
    minha_var = Variable.get('minhavar')
    print(f'===================={minha_var}===============')

with DAG('variaveis', description='variaveis',
          schedule_interval=None, start_date=datetime(2024,9,30),
          catchup=False) as dag:
    task1 = PythonOperator(task_id='task1', python_callable=print_variable)


task1