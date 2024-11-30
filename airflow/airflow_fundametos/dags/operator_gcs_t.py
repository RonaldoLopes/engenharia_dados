from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

GCS_CONN_ID = "google_cloud_default"
SOURCE_BUCKET = "rclaengdta/data1"
SOURCE_OBJECT = "Fundamentos-Engenharia-de-Dados.pdf"
DESTINATION_BUCKET = "rclaengdta/data2"
DESTINATION_OBJECT = "Fundamentos-Engenharia-de-Dados.pdf"

default_args = {
    "owner": "Ronaldo Lopes",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

@dag(
    dag_id="operator_gcs_t",
    start_date=datetime(2024, 10, 10),
    max_active_runs=1,
    schedule_interval=timedelta(minutes=5),
    default_args=default_args,
    catchup=False,
    owner_links={"ronaldolopes": "https://www.linkedin.com/in/ronaldorcla"},
    tags=["development", "elt", "gcs", "files"]
)
def init():
    print("DAG 'operator_gcs_t' foi carregado com sucesso!")

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    copy_data_gcs_to_gcs = GCSToGCSOperator(
        task_id="copy_data_gcs_to_gcs",
        source_bucket=SOURCE_BUCKET,
        source_object=SOURCE_OBJECT,
        destination_bucket=DESTINATION_BUCKET,
        destination_object=DESTINATION_OBJECT,
        move_object=True,
        gcp_conn_id=GCS_CONN_ID
    )

    start >> copy_data_gcs_to_gcs >> end

dag = init()
