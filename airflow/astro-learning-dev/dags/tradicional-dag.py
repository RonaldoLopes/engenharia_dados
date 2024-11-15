from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Definição da DAG
with DAG(
    dag_id="tradicional_dag",
    start_date=datetime(2024, 10, 29),
    catchup=False,
    schedule_interval=None,
) as dag:

    def extract():
        return {"data": "extract"}

    def transform(ti):
        data = ti.xcom_pull(task_ids="extract_task")
        return {"data": "transform"}
    
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform
    )

    extract_data >> transform_data

