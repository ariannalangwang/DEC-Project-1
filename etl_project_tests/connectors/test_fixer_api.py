import pytest
from dotenv import load_dotenv
import os
from etl_project.connectors.fixer_api import FixerApiClient


@pytest.fixture
def setup():
    load_dotenv()

def test_fixer_client_get_exchange_rates(setup):
    FIXER_ACCESS_KEY = os.environ.get("FIXER_ACCESS_KEY")
    fixer_api_client = FixerApiClient(fixer_access_key=FIXER_ACCESS_KEY)
    data = fixer_api_client.get_exchange_rates()

    assert type(data) == list
    assert len(data) > 0
