import requests
from etl_project.connectors.base_api_client import BaseApiClient


class MarketStackApiClient(BaseApiClient):
    def __init__(self, market_stack_access_key: str):
        super().__init__("http://api.marketstack.com/v1/eod", market_stack_access_key)

    def get_stocks_info(self) -> dict:
        params = {
            "access_key": self.access_key,
            "symbols": "AAPL,MSFT,AMZN,GOOGL,FB"
        }
        return self.get_data(params)
