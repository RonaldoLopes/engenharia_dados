from airflow.decorators import dag, task



# with DAG(
#     dag_id='06-env-variables',
#     start_date=datetime(2024, 9, 21),  # Data no passado
#     schedule_interval='@once',  # Define para rodar assim que for detectado
#     catchup=False  # Evita que rode execuções pendentes
# ) as dag: