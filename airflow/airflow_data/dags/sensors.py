from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
import requests

def query_api():
    response = requests.get('https://api.funtranslations.com/translate')
    print(response.text)

with DAG('HttpSensor', description='HttpSensor',
         schedule_interval=None,start_date=datetime(2024, 9, 30),
         catchup=False) as dag:

    check_api = HttpSensor(task_id='check_api', http_conn_id='connection',
                           endpoint='translate',
                           poke_interval=5,
                           timeout=20)
    process_data = PythonOperator(task_id='process_data', python_callable=query_api)

check_api >> process_data