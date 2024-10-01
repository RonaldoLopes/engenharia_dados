from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime

# Função Python que será chamada nas tarefas
def get_env():
    env_var_test_ronaldo = Variable.get('RONALDO_VAR1')
    env_var_test_ronaldo_json = Variable.get('RONALDO_JSON_VAR1')
    print(env_var_test_ronaldo)
    print(env_var_test_ronaldo_json)

def get_conn():
    airflow_conn = BaseHook.get_connection('MY_PROD_DATABAS')
    airflow_conn_pass = airflow_conn.password
    print(airflow_conn)
    print(airflow_conn_pass)

# Definindo a DAG
with DAG(
    dag_id='06-env-variables',
    start_date=datetime(2024, 9, 21),  # Data no passado
    schedule_interval='@once',  # Define para rodar assim que for detectado
    catchup=False  # Evita que rode execuções pendentes
) as dag:

    # Definindo as tasks com IDs únicos
    test_task1 = PythonOperator(
        task_id='test_task1',  # Task ID único
        python_callable=get_env
    )

    test_task2 = PythonOperator(
        task_id='test_task2',  # Outro Task ID único
        python_callable=get_conn
    )

    # Definindo a sequência de execução das tasks
    test_task1 >> test_task2
