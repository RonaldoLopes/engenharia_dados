from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('pool', description='pool',
          schedule_interval=None, start_date=datetime(2024, 9, 30),
          catchup=False) as dag:

    task1 = BashOperator(task_id="task1", bash_command="sleep 10", pool='meupool')
    task2 = BashOperator(task_id="task2", bash_command="sleep 10", pool='meupool', priority_weight=5)
    task3 = BashOperator(task_id="task3", bash_command="sleep 10", pool='meupool')
    task4 = BashOperator(task_id="task4", bash_command="sleep 10", pool='meupool', priority_weight=10)
