from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def create_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run('create table if not exists teste2(id int);', autocommit=True)

def insert_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run('insert into teste2 values(1);', autocommit=True)

def select_data(ti):
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    records = pg_hook.get_records('select * from teste2;')
    ti.xcom_push(key='query_result', value=records)

def print_data(ti):
    query_result = ti.xcom_pull(key='query_result', task_ids='select_data_task')  # Puxar do XCom corretamente
    print('==============================')
    if query_result:  # Certificar que há resultados
        for row in query_result:
            print(row)

with DAG('hooks', description='hooks',
         schedule_interval=None, start_date=datetime(2024, 9, 30),
         catchup=False) as dag:

    create_table_task = PythonOperator(task_id='create_table_task', python_callable=create_table)
    insert_data_task = PythonOperator(task_id='insert_data_task', python_callable=insert_data)
    select_data_task = PythonOperator(task_id='select_data_task', 
                                      python_callable=select_data,
                                      provide_context=True)
    print_data_task = PythonOperator(task_id='print_data_task', python_callable=print_data,
                                     provide_context=True)

create_table_task >> insert_data_task >> select_data_task >> print_data_task