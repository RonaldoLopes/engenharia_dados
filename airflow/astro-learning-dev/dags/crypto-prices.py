import requests
from airflow.decorators import dag, task
from datetime import datetime
from typing import Dict

@dag(
    dag_id="crypto-prices",
    schedule_interval="@daily",
    start_date=datetime(2024, 10, 29),
    catchup=False,
    tags=["crypto", "api"]
)
def crypto_pipeline():

    @task(retries=2)
    def get_crypto_price() -> Dict:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "bitcoin,ethereum",
            "vs_currencies": "usd"
        }
        response = requests.get(url, params=params)
        return response.json()
    
    @task()
    def process_price(data: Dict) -> Dict:
        return{
            'timestamp': datetime.now().isoformat(),
            'prices': {
                'BTC': data['bitcoin']['usd'],
                'ETH': data['ethereum']['usd']
            }
        }

    @task()
    def load_price(data: Dict):
        print(f"Timestamp: {data['timestamp']}")
        print(f"Crypto Prices: {data['prices']}")

    load_price(process_price(get_crypto_price()))

dag = crypto_pipeline()