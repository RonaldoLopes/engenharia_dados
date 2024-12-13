import requests
import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# List of cryptocurrencies to fetch (input for dynamic task mapping)
cryptocurrencies = ["ethereum", "dogecoin", "bitcoin", "tether", "tron"]
api_call_template = "https://api.coingecko.com/api/v3/simple/price?ids={crypto}&vs_currencies=usd"

@dag(
    dag_id="dtm-cryptocurrency-prices",
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 12),
    catchup=False
)
def crypto_prices():

    @task
    def fetch_price(crypto: str):
        """Fetch the price of a given cryptocurrency from the API."""
        
        # API call to fetch the price of the cryptocurrency
        api_url = api_call_template.format(crypto=crypto)
        response = requests.get(api_url).json()
        price = response.get(crypto, {}).get('usd', None)

        if price is not None:
            logging.info(f"The price of {crypto} is ${price}")
        else:
            logging.error(f"Failed to fetch the price for {crypto}")

        return price

    # Dynamically map the fetch_price task over the list of cryptocurrencies
    fetch_price.expand(crypto=cryptocurrencies)

# Instantiate the DAG
crypto_prices()
