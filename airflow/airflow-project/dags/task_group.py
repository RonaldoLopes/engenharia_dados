from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

# Define os argumentos padrão
default_args = {
    'start_date': datetime(2024, 9, 21),
    'catchup': False
}

with DAG(
    'dag_with_task_groups',
    default_args=default_args,
    description='Exemplo de DAG com Task Groups',
    schedule_interval='@daily',
) as dag:

    start = DummyOperator(task_id='start')

    # Task Group 1
    with TaskGroup(group_id='task_group_1') as tg1:
        task_1 = DummyOperator(task_id='task_1')
        task_2 = DummyOperator(task_id='task_2')
        task_3 = DummyOperator(task_id='task_3')

        task_1 >> [task_2, task_3]  # task_2 e task_3 são paralelas

    # Task Group 2
    with TaskGroup(group_id='task_group_2') as tg2:
        task_4 = DummyOperator(task_id='task_4')
        task_5 = DummyOperator(task_id='task_5')
        task_6 = DummyOperator(task_id='task_6')

        task_4 >> task_5 >> task_6  # task_4 -> task_5 -> task_6 em sequência

    end = DummyOperator(task_id='end')

    # Definindo a sequência de execução
    start >> tg1 >> tg2 >> end
