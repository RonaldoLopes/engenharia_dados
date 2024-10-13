from airflow import DAG
from datetime import datetime
from big_data_operator import BigDataOperator

with DAG('bigdata', description='bigdata',
         schedule_interval=None, start_date=datetime(2024, 9, 30),
         catchup=False) as dag:
    big_data = BigDataOperator(
        task_id='bigdata', 
        path_to_file='/opt/airflow/data/Churn.csv', 
        path_to_save_file='/opt/airflow/data/Churn.parquet',
        file_type='parquet',
    )