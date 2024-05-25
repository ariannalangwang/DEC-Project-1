import requests

class BaseApiClient:
    def __init__(self, access_key: str):
        if access_key is None:
            raise Exception("API key cannot be set to None.")
        self.access_key = access_key

    def get_data(self, base_url: str, params: dict) -> dict:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Failed to extract data from API. Status Code: {response.status_code}. Response: {response.text}"
            )
