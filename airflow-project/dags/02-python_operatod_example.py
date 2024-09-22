from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def funcao_exemplo(posicao:str):
    print(f'Testando {posicao} função python')

with DAG(dag_id='02-python_operator_example', start_date=datetime(2024, 9, 18), schedule_interval=None) as dag:

    task1 = PythonOperator(
        task_id='primeira_funcao',
        python_callable=funcao_exemplo,
        op_args=['primeira']
    )

    task2 = PythonOperator(
        task_id='segunda_funcao',
        python_callable=funcao_exemplo,
        op_kwargs={
            'posicao': 'segunda'
        }
    )

    [task1, task2]
