from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime
from subdags.example_subdag_task import subdag_factory

# Define o DAG principal
default_args = {
    'start_date': datetime(2024, 9, 21),
    'catchup': False
}

with DAG(
    'main_dag_with_subdags',
    default_args=default_args,
    description='Exemplo de DAG com SubDAGs',
    schedule_interval='@daily',
) as dag:
    
    start = DummyOperator(task_id='start')

    # SubDAG 1
    subdag_1 = SubDagOperator(
        task_id='subdag_task_1',
        subdag=subdag_factory('main_dag_with_subdags', 'subdag_task_1', default_args),
        dag=dag,
    )

    # SubDAG 2
    subdag_2 = SubDagOperator(
        task_id='subdag_task_2',
        subdag=subdag_factory('main_dag_with_subdags', 'subdag_task_2', default_args),
        dag=dag,
    )

    end = DummyOperator(task_id='end')

    # Definindo a sequência de execução
    start >> subdag_1 >> subdag_2 >> end
