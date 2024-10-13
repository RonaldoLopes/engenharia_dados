from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operator.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.tas_group import TaskGroup
from datetime import datetime, timedelta
import json
import os

def process_file(**kwargs):
    with open(Variable.get('path_file')) as f:
        data = json.load(f)
        kwargs

default_args = {
    'depends_on_past': False,
    'email' : ['ronaldorclamaster@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retray_delay' : timedelta(seconds=10)
}

with DAG('windturbine', description='Dados da Turbina',
         schedule_interval='*/3 * * * *',
         start_date=datetime(2024, 10, 11),
         catchup=False, default_args=default_args,
         default_view='graph',
         doc_md='## Dag para registrar dados de turbina') as dag:
    
    group_check_temp = TaskGroup('group_check_temp')
    group_database = TaskGroup('group_database')

    file_sensor_task = FileSensor(
        task_id = 'file_sensor_task',
        filepath = Variable.get('path_file'),
        fs_conn_id = 'fs_default',
        poke_interval = 10
    )

    get_data = PythonOperator(
        task_id = 'get_data',
        python_callable=process_file,
        provide_context = True
    )
    03:40