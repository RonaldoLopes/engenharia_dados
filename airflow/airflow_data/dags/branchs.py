from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime
import random

def gera_numero_aleatorio():
    return random.randint(1, 100)

def avalia_numero_aleatorio(**kwargs):
    number = kwargs['task_instance'].xcom_pull(task_ids='gera_numero_aleatorio_task')
    if number % 2 == 0:
        return 'par_task'
    else:
        return 'impar_task'


with DAG('branchteste', description='branchteste',
          schedule_interval=None, start_date=datetime(2024, 9, 30),
          catchup=False) as dag:

    gera_numero_aleatorio_task = PythonOperator(
        task_id='gera_numero_aleatorio_task',
        python_callable = gera_numero_aleatorio
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=avalia_numero_aleatorio,
        provide_context=True
    )

    par_task = BashOperator(task_id="par_task", bash_command='echo "Numero par"')
    impar_task = BashOperator(task_id="impar_task", bash_command='echo "Numero impar"')

    gera_numero_aleatorio_task >> branch_task
    branch_task >> par_task
    branch_task >> impar_task