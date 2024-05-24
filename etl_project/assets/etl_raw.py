import pandas as pd
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.fixer_api import FixerApiClient
from etl_project.connectors.market_stack_api import MarketStackApiClient


def extract_fixer_table(fixer_api_client: FixerApiClient) -> pd.DataFrame:
    """
    Extracts exchange rates data from the Fixer API using the provided FixerApiClient.

    Parameters:
        fixer_api_client (FixerApiClient): An instance of the FixerApiClient class used to interact with the Fixer API.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the extracted exchange rates data.
    """
    data = fixer_api_client.get_exchange_rates()
    df_currency = pd.json_normalize(data)
    return df_currency


def extract_market_stack_table(market_stack_api_client: MarketStackApiClient) -> pd.DataFrame:
    """
    Extracts stock information from the MarketStack API using the provided MarketStackApiClient.

    Parameters:
        market_stack_api_client (MarketStackApiClient): An instance of the MarketStackApiClient class.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted stock information.
    """
    data = market_stack_api_client.get_stocks_info()
    df_stocks = pd.json_normalize(data)
    return df_stocks


def transform_fixer_table(df_currency: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the fixer table.

    Args:
        df_currency (pd.DataFrame): The input DataFrame representing the fixer table.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    df_currency.drop(columns=['success', 'timestamp'], inplace=True)
    df_currency.columns = [col.replace('rates.', 'rate_') for col in df_currency.columns]
    cols = df_currency.columns.tolist()
    cols[0], cols[1] = cols[1], cols[0]
    df_currency = df_currency[cols]
    return df_currency


def transform_market_stack_table(df_stocks: pd.DataFrame) -> pd.DataFrame:
    data_column = df_stocks['data']
    normalized_data = pd.json_normalize(data_column[0])
    normalized_data['symbol'] = [entry['symbol'] for entry in data_column[0]]
    normalized_data = normalized_data[['date', 'symbol'] + [col for col in normalized_data.columns if col not in ['date', 'symbol']]]
    columns_to_delete = ['adj_high', 'adj_low', 'adj_close', 'adj_open', 'adj_volume', 'split_factor', 'dividend']
    normalized_data.drop(columns=columns_to_delete, inplace=True)
    normalized_data['date'] = pd.to_datetime(normalized_data['date'])
    normalized_data['date'] = normalized_data['date'].dt.strftime('%Y-%m-%d')
    return normalized_data


def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "overwrite",
) -> None:
    """
    Load dataframe to a database.

    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )
