import requests

class BaseApiClient:
    def __init__(self, access_key: str):
        if access_key is None:
            raise Exception("API key cannot be set to None.")
        self.access_key = access_key

    def get_data(self, base_url: str, params: dict) -> dict:
        """
        Sends a GET request to the specified base URL with the given parameters and returns the response as a JSON object.

        Args:
            base_url (str): The base URL to send the GET request to.
            params (dict): The parameters to include in the GET request.

        Returns:
            dict: The response from the API as a JSON object.

        Raises:
            Exception: If the GET request fails or returns a non-200 status code.
        """
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Failed to extract data from API. Status Code: {response.status_code}. Response: {response.text}"
            )
