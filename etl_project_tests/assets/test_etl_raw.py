import pytest
import pandas as pd
from etl_project.assets.etl_raw import transform_fixer_table, transform_market_stack_table

@pytest.fixture
def setup_input_fixer_df():
    return pd.DataFrame({
        'success': [True, True],
        'timestamp': [1629878400, 1629964800],
        'historical': [False, True],
        'base': ['EUR', 'EUR'],
        'date': ['2024-05-25', '2024-05-24'],
        'rates.USD': [1.08, 1.07],
        'rates.CNY': [7.86, 7.84],
        'rates.INR': [90.16, 90.15],
        'rates.AUD': [1.64, 1.65]
    })

def test_transform_fixer_table(setup_input_fixer_df):
    # Assemble
    df_currency = setup_input_fixer_df
    expected_df = pd.DataFrame({
        'date': ['2024-05-25', '2024-05-24'],
        'base': ['EUR', 'EUR'],
        'rate_usd': [1.08, 1.07],
        'rate_cny': [7.86, 7.84],
        'rate_inr': [90.16, 90.15],
        'rate_aud': [1.64, 1.65]
    })

    # Act
    actual_df = transform_fixer_table(df_currency)

    # Assert
    pd.testing.assert_frame_equal(left=actual_df, right=expected_df, check_exact=True)
 

@pytest.fixture
def setup_input_market_stack_df():
    return pd.DataFrame({
        'data': [[
            {'date': '2024-05-24T00:00:00+0000', 'symbol': 'AAPL', 'open': 145.3, 'high': 147.2, 'low': 144.1, 'close': 146.8, 'volume': 100000,
             'adj_high': 147.2, 'adj_low': 144.1, 'adj_close': 146.8, 'adj_open': 145.3, 'adj_volume': 100000, 'split_factor': 1.0, 'dividend': 0.0},
            {'date': '2024-05-24T00:00:00+0000', 'symbol': 'MSFT', 'open': 250.0, 'high': 255.0, 'low': 248.0, 'close': 252.5, 'volume': 150000,
             'adj_high': 255.0, 'adj_low': 248.0, 'adj_close': 252.5, 'adj_open': 250.0, 'adj_volume': 150000, 'split_factor': 1.0, 'dividend': 0.0}
        ]]
    })

def test_transform_market_stack_table(setup_input_market_stack_df):
    # Assemble
    df_stocks = setup_input_market_stack_df
    expected_df = pd.DataFrame({
        'date': ['2024-05-24', '2024-05-24'],
        'symbol': ['AAPL', 'MSFT'],
        'open': [145.3, 250.0],
        'high': [147.2, 255.0],
        'low': [144.1, 248.0],
        'close': [146.8, 252.5],
        'volume': [100000, 150000]
    })

    # Act
    actual_df = transform_market_stack_table(df_stocks)

    # Assert
    pd.testing.assert_frame_equal(left=actual_df, right=expected_df, check_exact=True)