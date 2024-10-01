from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

DUMMY_VARIABLE = Variable.get(key='dummy_variable')
JSON_TESTE = Variable.get(key='json_test', deserialize_json=True)

def dummy():
    print(f'{DUMMY_VARIABLE}')

def json_var_test():
    json_deserializer = JSON_TESTE
    print(json_deserializer)
    print('#'*40)
    print(f"chave1: {json_deserializer['chave']} \nchave2: {json_deserializer['chave2']} \nchave3: {json_deserializer['chave3']}")


# S3_PATH = Variable.get(key='key')

with DAG(
    dag_id='05-variable',
    start_date=datetime(2023, 9, 21),  # Data no passado
    schedule_interval='@once',  # Define para rodar assim que for detectado
    catchup=False  # Evita que rode execuções pendentes
) as dag:
    task1 = PythonOperator(python_callable=dummy, task_id='task1')
    task2 = PythonOperator(python_callable=json_var_test, task_id='task2')
    task3 = PythonOperator(python_callable=dummy, task_id='task3')
