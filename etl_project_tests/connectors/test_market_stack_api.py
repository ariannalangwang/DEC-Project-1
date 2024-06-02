import pytest
from dotenv import load_dotenv
import os
from etl_project.connectors.market_stack_api import MarketStackApiClient


@pytest.fixture
def setup():
    load_dotenv()

def test_market_stack_get_stocks_info(setup):
    MARKET_STACK_ACCESS_KEY = os.environ.get("MARKET_STACK_ACCESS_KEY")
    market_stack_api_client = MarketStackApiClient(market_stack_access_key=MARKET_STACK_ACCESS_KEY)
    data = market_stack_api_client.get_stocks_info()

    assert type(data) == dict
    assert len(data) > 0
