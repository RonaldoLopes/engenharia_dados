from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def task_write(**kwargs):
    kwargs['ti'].xcom_push(key='valorxcom1', value=10200)

def task_read(**kwargs):
    valor = kwargs['ti'].xcom_pull(key='valorxcom1')
    print(f'==========valor=====: {valor}')

with DAG('xcom_dag', description='xcom_dag',
          schedule_interval=None, start_date=datetime(2024,9,30),
          catchup=False) as dag:

    task1 = PythonOperator(task_id='task1', python_callable=task_write)
    task2 = PythonOperator(task_id='task2', python_callable=task_read)


task1 >> task2