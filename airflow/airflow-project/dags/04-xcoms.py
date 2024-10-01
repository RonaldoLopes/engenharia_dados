from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def ingest():
    import pandas as pd
    df = pd.read_csv('br_inep_ideb_brasil.csv', delimiter=',', encoding='utf-8')

    df_medio = df[df['ensino'].str.contains('medio', case=False)]
    df_fundamental = df[df['ensino'].str.contains('fundamental', case=False)]

    total_nota_saeb_matematica_medio = df_medio['nota_saeb_matematica'].sum()
    total_nota_saeb_matematica_fundamental = df_fundamental['nota_saeb_matematica'].sum()

    return {
        'total_nota_saeb_matematica_medio': total_nota_saeb_matematica_medio,
        'total_nota_saeb_matematica_fundamental': total_nota_saeb_matematica_fundamental
    }

def consume(xcom):
    import json
    return_value = json.loads(xcom.replace("'", '"'))
    total_nota_saeb_matematica_medio = return_value['total_nota_saeb_matematica_medio']
    total_nota_saeb_matematica_fundamental = return_value['total_nota_saeb_matematica_fundamental']

    print(f'matematica medio: {total_nota_saeb_matematica_medio}')
    print(f'matematica fundamental: {total_nota_saeb_matematica_fundamental}')
    print(f'tipo de dado: {type(return_value)}')

with DAG(
    dag_id='04-xcom',
    start_date=datetime(2024, 9, 19),
    schedule_interval=None
) as dag:

    task1 = PythonOperator(
        task_id='ingest',
        python_callable=ingest
    )

    task2 = PythonOperator(
        task_id='consume',
        python_callable=consume,
        op_kwargs={
            'xcom': '{{ ti.xcom_pull(task_ids="ingest") }}'  # Mantendo Jinja template
        }
    )

    task1 >> task2
