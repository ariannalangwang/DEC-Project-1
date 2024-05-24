import pytest
from dotenv import load_dotenv
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
from sqlalchemy import Table, MetaData, Column, String, Integer

from etl_project.assets.weather import extract_weather, extract_population, transform, load
from etl_project.connectors.weather_api import WeatherApiClient
from etl_project.connectors.postgresql import PostgreSqlClient


@pytest.fixture
def setup_extract():
    load_dotenv()
    return os.environ.get("API_KEY")


def test_extract_weather(setup_extract):
    API_KEY = setup_extract
    weather_api_client = WeatherApiClient(api_key=API_KEY)
    df = extract_weather(
        weather_api_client=weather_api_client,
        city_reference_path=Path("./etl_project_tests/data/weather/australian_capital_cities.csv"),
    )
    assert len(df) == 8


@pytest.fixture
def setup_input_df_weather():
    return pd.DataFrame(
        [
            {
                "name": "perth",
                "dt": datetime(2020, 1, 1, 0, 0, 0),
                "id": 1,
                "main.temp": 20,
            },
            {
                "name": "sydney",
                "dt": datetime(2020, 1, 1, 0, 0, 0),
                "id": 2,
                "main.temp": 22,
            },
        ]
    )


@pytest.fixture
def setup_input_df_population():
    return extract_population(
        "etl_project_tests/data/weather/australian_city_population.csv"
    )


@pytest.fixture
def setup_transformed_df():
    return pd.DataFrame(
        [
            {
                "datetime": datetime(2020, 1, 1, 0, 0, 0),
                "id": 1,
                "name": "perth",
                "temperature": 20,
                "population": 2141834,
                "unique_id": "2020-01-011",
            },
            {
                "datetime": datetime(2020, 1, 1, 0, 0, 0),
                "id": 2,
                "name": "sydney",
                "temperature": 22,
                "population": 5361466,
                "unique_id": "2020-01-012",
            },
        ]
    ).set_index(["unique_id"])


def test_transform(
    setup_input_df_weather, setup_input_df_population, setup_transformed_df
):
    # Assemble
    df_weather = setup_input_df_weather
    df_population = setup_input_df_population
    expected_df = setup_transformed_df
    # Act
    df = transform(df_weather=df_weather, df_population=df_population)
    # Assert
    pd.testing.assert_frame_equal(left=df, right=expected_df, check_exact=True)
