from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.utils.task_group import TaskGroup

# Define os argumentos padrão
default_args = {
    'start_date': datetime(2024, 9, 21),
    'catchup': False,
}

def dummy_task(task_name):
    """
    Função dummy que simula uma tarefa.
    """
    print(f'Executando {task_name}')

with DAG(
    'dag_with_pools_and_priority',
    default_args=default_args,
    description='Exemplo de DAG com Pools e Task Priority',
    schedule_interval='@daily',
) as dag:
    
    start = DummyOperator(task_id='start')

    # Task que pertence ao pool 'small_pool'
    task_a = PythonOperator(
        task_id='task_a',
        python_callable=dummy_task,
        op_args=['Task A'],
        pool='small_pool',  # Recurso limitado a 2 tarefas simultâneas
        priority_weight=10,  # Alta prioridade
        dag=dag
    )

    # Task que pertence ao pool 'small_pool'
    task_b = PythonOperator(
        task_id='task_b',
        python_callable=dummy_task,
        op_args=['Task B'],
        pool='small_pool',  # Recurso limitado a 2 tarefas simultâneas
        priority_weight=5,  # Prioridade menor
        dag=dag
    )

    # Task que pertence ao pool 'large_pool'
    task_c = PythonOperator(
        task_id='task_c',
        python_callable=dummy_task,
        op_args=['Task C'],
        pool='large_pool',  # Recurso limitado a 5 tarefas simultâneas
        priority_weight=1,  # Prioridade baixa
        dag=dag
    )

    end = DummyOperator(task_id='end')

    # Definindo a sequência de execução
    start >> [task_a, task_b, task_c] >> end
