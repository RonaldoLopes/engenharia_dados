from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

mydataset = Dataset('/opt/airflow/data/Churn_new.csv')

def my_file():
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    dataset.to_csv('/opt/airflow/data/Churn_new.csv', sep=';')

with DAG('producer', description='producer',
         schedule_interval=None,start_date=datetime(2024, 9, 30),
         catchup=False) as dag:

    t1 = PythonOperator(task_id='t1', python_callable=my_file, outlets=[mydataset])
    t1
