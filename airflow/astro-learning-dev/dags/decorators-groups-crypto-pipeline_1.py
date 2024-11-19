from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from typing import Dict, List
import requests

@dag(
    dag_id='decorators-groups-crypto-pipeline_1',
    schedule_interval='@hourly',
    start_date=datetime(2024, 10, 29),
    catchup=False,
    tags=['crypto', 'pipeline'],
    default_args={
        'owner': 'Ronaldo Lopes',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def crypto_pipeline():
    @task(
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
    )
    def get_cripto_price() -> Dict:
        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {
                "ids": "bitcoin,ethereum,cardano",
                "vs_currencies": "usd",
            }
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(e)

    @task(multiple_outputs=True)
    def process_price(data: Dict) -> Dict:
        return {
            'timestamp': datetime.now().isoformat(),
            'btc_price': data['bitcoin']['usd'],
            'eth_price': data['ethereum']['usd'],
            'ada_price': data['cardano']['usd'],
        }

    @task_group(group_id='analytics')
    def analyze_prices(btc_price: float, eth_price: float, ada_price: float) -> Dict:
        @task(task_id='calculate_metrics')
        def calculate_metrics(prices: List[float]) -> Dict:
            return {
                'total': sum(prices),
                'average': sum(prices) / len(prices),
                'max_price': max(prices),
                'min_price': min(prices),
            }

        @task(task_id='calculate_ratios')
        def calculate_ratios(btc: float, eth: float) -> Dict:
            return {
                'eth_btc_ratio': eth / btc,
            }

        prices_list = [btc_price, eth_price, ada_price]
        metrics = calculate_metrics(prices_list)
        ratios = calculate_ratios(btc_price, eth_price)
        return {'metrics': metrics, 'ratios': ratios}

    @task(
        sla=timedelta(seconds=30),
        pool='crypto_pool'
    )
    def load_price(
        timestamp: str,
        btc_price: float,
        eth_price: float,
        ada_price: float,
        analytics: Dict
    ) -> None:
        print(f"""
        Timestamp: {timestamp}
        Prices:
        ------
        BTC: ${btc_price:,.2f}
        ETH: ${eth_price:,.2f}
        ADA: ${ada_price:,.2f}
        Metrics:
        {analytics['metrics']}
        Ratios:
        {analytics['ratios']}
        """)

    # Pipeline execution
    crypto_data = get_cripto_price()
    prices = process_price(crypto_data)
    analytics = analyze_prices(
        btc_price=prices['btc_price'],
        eth_price=prices['eth_price'],
        ada_price=prices['ada_price'],
    )
    load_price(
        timestamp=prices['timestamp'],
        btc_price=prices['btc_price'],
        eth_price=prices['eth_price'],
        ada_price=prices['ada_price'],
        analytics=analytics,
    )


crypto_pipeline()