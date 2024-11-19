from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# TODO Connections & Variables
GCS_CONN_ID = "google_cloud_default"
SOURCE_BUCKET = "owshq-airbyte-ingestion"
DESTINATION_BUCKET = "owshq-landing-zone"
USERS_SOURCE_OBJECT = "mongodb-atlas/users/*.parquet"
USERS_DESTINATION_OBJECT = "mongodb-atlas/users/"

# TODO Default Arguments
default_args = {
    "owner": "Ronaldo",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# TODO DAG Definition
@dag(
    dag_id="data-movement-gcs-to-gcs-operator",
    start_date=datetime(2024, 9, 24),
    max_active_runs=1,
    schedule_interval="0 */5 * * *",  # Alterado para string cron
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'gcs', 'files']
)
def init():

    # TODO Tasks Declaration
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    copy_users_gcs_to_gcs = GCSToGCSOperator(
        task_id="copy_users_gcs_to_gcs",
        source_bucket=SOURCE_BUCKET,
        source_object=USERS_SOURCE_OBJECT,
        destination_bucket=DESTINATION_BUCKET,
        destination_object=USERS_DESTINATION_OBJECT,
        move_object=False,
        gcp_conn_id=GCS_CONN_ID
    )

    # TODO Task Dependencies
    start >> copy_users_gcs_to_gcs >> end


# TODO DAG Instantiation
dag = init()
