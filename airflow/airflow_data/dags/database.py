from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

def print_result(ti):  # O Airflow injeta `ti` automaticamente
    task_instance = ti.xcom_pull(task_ids='query_data')
    print('==============================')
    for row in task_instance:
        print(row)

with DAG('database', description='database',
         schedule_interval=None, start_date=datetime(2024, 9, 30),
         catchup=False) as dag:

    create_table = PostgresOperator(task_id='create_table',
                                    postgres_conn_id='postgres',
                                    sql='create table if not exists test(id int);')

    insert_data = PostgresOperator(task_id='insert_data',
                                   postgres_conn_id='postgres',
                                   sql='insert into test values(1);')

    query_data = PostgresOperator(task_id='query_data',
                                  postgres_conn_id='postgres',
                                  sql='select * from test;')

    print_result_task = PythonOperator(task_id='print_result_task', 
                                       python_callable=print_result)  # Sem op_kwargs

create_table >> insert_data >> query_data >> print_result_task
