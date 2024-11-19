from airflow.decorators import task
from airflow import Dataset

@task(outlets=[Dataset("crypto/aggregate_data")])
def aggregate_metrics(cleaned_data):
    aggregate_data = f"Aggredated metrics from {cleaned_data}"
    return aggregate_data