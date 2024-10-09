from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

dag = DAG('trigger_dag2', description="trigger_dag2",
          schedule_interval=None, start_date=datetime(2024,9,30),
          catchup=False)

task1 = BashOperator(task_id='task1', bash_command='exit 1', dag=dag)
task2 = BashOperator(task_id='task2', bash_command='sleep 5', dag=dag)
task3 = BashOperator(
    task_id='task3',
    bash_command='sleep 5',
    dag=dag,
    trigger_rule='one_failed'
    )

[task1, task2] >> task3