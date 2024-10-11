from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

with DAG('dag_run_dag_1', description='dag_run_dag_1',
          schedule_interval=None, start_date=datetime(2024,9,30),
          catchup=False) as dag:

    task1 = BashOperator(task_id='task1', bash_command='sleep 5')
    task2 = TriggerDagRunOperator(task_id='task2', trigger_dag_id='dag_run_dag_2')

    task1 >> task2

