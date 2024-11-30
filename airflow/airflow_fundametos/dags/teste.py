from datetime import datetime, timedelta
import json
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from bson.json_util import dumps

mongodb_atlas_conn_id = "mongodb_default"

default_args = {
    "owner": "Ronaldo Lopes",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="teste",
    start_date=datetime(2024, 11, 1),
    max_active_runs=1,
    schedule_interval=timedelta(minutes=5),
    default_args=default_args,
    catchup=False,
    owner_links={"ronaldolopes": "https://www.linkedin.com/in/ronaldorcla"},
    tags=["development", "elt", "gcs", "mongodb"]
)
def init():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def get_users():
        hook = MongoHook(conn_id=mongodb_atlas_conn_id)

        database = "teste"
        collection = "teste"

        query = {"nome": "Notebook"}

        try:
            conn = hook.get_conn()
            db = conn[database]
            coll = db[collection]

            data = hook.find(
                mongo_db=database,
                mongo_collection=collection,
                query=query
            )

            documents = list(data)
            doc_count = len(documents)

            if doc_count == 0:
                print("No documents found")
                return None
            elif doc_count > 1:
                print(f"Warning: Found {doc_count} documents. Returning the first one.")
            
            document = json.loads(dumps(documents[0]))
            print(f"Retrieved document: {document}")
        
            return document
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise

    users_collection = get_users()

    start >> users_collection >> end

dag = init()
