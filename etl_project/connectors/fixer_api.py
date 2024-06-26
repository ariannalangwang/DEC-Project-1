import requests
import pandas as pd
from etl_project.connectors.base_api_client import BaseApiClient


class FixerApiClient(BaseApiClient):
    def __init__(self, fixer_access_key: str):
        super().__init__(fixer_access_key)
        self.access_key = fixer_access_key

    def _get_latest_date(self) -> str:
        """
        Retrieves the latest date from the Fixer API.

        Returns:
            str: The latest date in the format "YYYY-MM-DD".
        """
        base_url = "http://data.fixer.io/api/latest"
        params = {
            "access_key": self.access_key
        }
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("date")
    
    def _get_date_range(self) -> list[str]:
        """
        Returns a list of date strings representing a date range.

        Returns:
            list[str]: A list of date strings in the format "YYYY-MM-DD".
        """
        latest_date = self._get_latest_date()
        date_range = pd.date_range(end=latest_date, periods=6).strftime("%Y-%m-%d").tolist()
        return date_range[::-1]

    def get_exchange_rates(self) -> list[dict]:
        """
        Retrieves exchange rates for a given date range.

        Returns:
            A list of dictionaries containing exchange rate data for each date in the range.
        """
        date_range = self._get_date_range()
        exchange_rates = []

        for date in date_range:
            base_url = f"http://data.fixer.io/api/{date}"
            params = {
                "access_key": self.access_key,
                "symbols": "USD,CNY,INR,AUD"
            }
            response = self.get_data(base_url, params)
            exchange_rates.append(response)

        return exchange_rates
