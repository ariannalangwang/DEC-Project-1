import requests
from etl_project.connectors.base_api_client import BaseApiClient


class FixerApiClient(BaseApiClient):
    def __init__(self, fixer_access_key: str):
        super().__init__(fixer_access_key)

    def get_exchange_rates(self) -> dict:
        base_url = "http://data.fixer.io/api/latest"
        params = {
            "access_key": self.access_key,
            "symbols": "USD,CNY,INR,AUD"
        }
        return self.get_data(base_url, params)
    
    def get_historical_rates(self, date: str) -> dict:
        base_url = f"http://data.fixer.io/api/{date}"
        params = {
            "access_key": self.access_key,
            "symbols": "USD,CNY,INR,AUD"
        }
        return self.get_data(base_url, params)

