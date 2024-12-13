import requests

AIRFLOW_API_URL = "http://localhost:8080/api/v1/health"
try:
    response = requests.get(AIRFLOW_API_URL)
    print(response.json())
except Exception as e:
    print(f"Error: {e}")