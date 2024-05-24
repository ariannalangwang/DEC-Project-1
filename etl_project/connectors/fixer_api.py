import requests
from etl_project.connectors.base_api_client import BaseApiClient


class FixerApiClient(BaseApiClient):
    def __init__(self, fixer_access_key: str):
        super().__init__("http://data.fixer.io/api/latest", fixer_access_key)

    def get_exchange_rates(self) -> dict:
        params = {
            "access_key": self.access_key,
            "symbols": "USD,CNY,INR,AUD"
        }
        return self.get_data(params)

