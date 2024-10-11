from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import statistics as sts

def data_cleaner():
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    dataset.columns = ['Id', 'Score', 'Estado', 'Genero', 'Idade',
                       'Patrimonio', 'Saldo', 'Produtos', 'TemCartCredito',
                       'Ativo', 'Salario', 'Saiu']
    mediana = sts.median(dataset['Salario'])
    dataset['Salario'].fillna(mediana, inplace=True)
    dataset['Genero'].fillna('Masculino', inplace=True)

    mediana = sts.median(dataset['Idade'])
    dataset.loc[(dataset['Idade'] < 0) | (dataset['Idade'] > 120), 'Idade'] = mediana

    dataset.drop_duplicates(subset='Id', keep='first', inplace=True)

    dataset.to_csv('/opt/airflow/data/Churn.csv_Clear.csv', sep=';', index=False)

with DAG('pythonoperator', description='pythonoperator',
         schedule_interval=None,
         start_date=datetime(2024, 9, 30),
         catchup=False) as dag:
    t1 = PythonOperator(task_id='t1', python_callable=data_cleaner)
