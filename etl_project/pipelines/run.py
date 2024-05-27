from dotenv import load_dotenv
import os
import yaml
from pathlib import Path
import time
from sqlalchemy import Table, MetaData, Column, String, Float
from jinja2 import Environment, FileSystemLoader
from graphlib import TopologicalSorter

from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.fixer_api import FixerApiClient
from etl_project.connectors.market_stack_api import MarketStackApiClient

from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from etl_project.assets.pipeline_logging import PipelineLogging

from etl_project.assets.etl_raw import (
    extract_fixer_table,
    extract_market_stack_table,
    transform_fixer_table,
    transform_market_stack_table,
    load,
)
from etl_project.assets.etl_serving import (
    extract_load,
    transform,
    SqlTransform,
)


def load_environment_variables() -> dict:
    """
    Loads the required environment variables for the ETL pipeline.
    """
    load_dotenv()
    required_env_vars = [
        'FIXER_ACCESS_KEY', 'MARKET_STACK_ACCESS_KEY', 
        'SERVER_NAME', 'DATABASE_NAME', 'DB_USERNAME', 'DB_PASSWORD', 'PORT', 
        'LOGGING_SERVER_NAME', 'LOGGING_DATABASE_NAME', 'LOGGING_USERNAME', 'LOGGING_PASSWORD', 'LOGGING_PORT',
        'TARGET_SERVER_NAME', 'TARGET_DATABASE_NAME', 'TARGET_DB_USERNAME', 'TARGET_DB_PASSWORD', 'TARGET_PORT'
    ]
    env_vars = {var: os.getenv(var) for var in required_env_vars}
    missing_vars = [var for var, value in env_vars.items() if value is None]
    
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return env_vars


def setup_clients(env_vars) -> tuple:
    """
    Set up and initialize the required clients for the ETL pipeline.
    """
    fixer_api_client = FixerApiClient(fixer_access_key=env_vars['FIXER_ACCESS_KEY'])
    market_stack_api_client = MarketStackApiClient(market_stack_access_key=env_vars['MARKET_STACK_ACCESS_KEY'])

    postgresql_client = PostgreSqlClient(
        server_name=env_vars['SERVER_NAME'],
        database_name=env_vars['DATABASE_NAME'],
        username=env_vars['DB_USERNAME'],
        password=env_vars['DB_PASSWORD'],
        port=env_vars['PORT'],
    )

    postgresql_logging_client = PostgreSqlClient(
        server_name=env_vars['LOGGING_SERVER_NAME'],
        database_name=env_vars['LOGGING_DATABASE_NAME'],
        username=env_vars['LOGGING_USERNAME'],
        password=env_vars['LOGGING_PASSWORD'],
        port=env_vars['LOGGING_PORT'],
    )

    postgresql_target_client = PostgreSqlClient(
        server_name=env_vars['TARGET_SERVER_NAME'],
        database_name=env_vars['TARGET_DATABASE_NAME'],
        username=env_vars['TARGET_DB_USERNAME'],
        password=env_vars['TARGET_DB_PASSWORD'],
        port=env_vars['TARGET_PORT'],
    )

    return fixer_api_client, market_stack_api_client, postgresql_client, postgresql_logging_client, postgresql_target_client


def raw_pipeline(
        fixer_api_client: FixerApiClient, 
        market_stack_api_client: MarketStackApiClient, 
        postgresql_client: PostgreSqlClient, 
        pipeline_logging: PipelineLogging
    ) -> None:
    """
    Executes the raw data pipeline.
    """
    pipeline_logging.logger.info("Starting raw_pipeline")

    # extract
    pipeline_logging.logger.info("Extracting data from Fixer API")
    df_currency = extract_fixer_table(fixer_api_client=fixer_api_client)
    pipeline_logging.logger.info("Extracting data from MarketStack API")
    df_stocks = extract_market_stack_table(market_stack_api_client=market_stack_api_client)
     
    # transform
    pipeline_logging.logger.info("Transforming Fixer dataframe")
    df_currency_transformed = transform_fixer_table(df_currency=df_currency)
    pipeline_logging.logger.info("Transforming MarketStack dataframe")
    df_stocks_transformed = transform_market_stack_table(df_stocks=df_stocks)

    # load
    metadata = MetaData()
    pipeline_logging.logger.info("Loading Fixer data to Postgres")
    currency_table = Table(
        "currency_exchange_rate",
        metadata,
        Column("date", String, primary_key=True),
        Column("base", String),
        Column("rate_usd", Float),
        Column("rate_cny", Float),
        Column("rate_inr", Float),
        Column("rate_aud", Float),
    )
    load(
        df=df_currency_transformed,
        postgresql_client=postgresql_client,
        table=currency_table,
        metadata=metadata,
        load_method="upsert",
    )

    pipeline_logging.logger.info("Loading MarketStack data to Postgres")
    stock_table = Table(
        "stock_price",
        metadata,
        Column("date", String, primary_key=True),
        Column("symbol", String, primary_key=True),
        Column("open", Float),
        Column("high", Float),
        Column("low", Float),
        Column("close", Float),
        Column("volume", Float),
        Column("exchange", String),
    )
    load(
        df=df_stocks_transformed,
        postgresql_client=postgresql_client,
        table=stock_table,
        metadata=metadata,
        load_method="upsert",
    )
    pipeline_logging.logger.info("Raw pipeline run successful")


def serving_pipeline(
        postgresql_client: PostgreSqlClient, 
        postgresql_target_client: PostgreSqlClient,
        pipeline_logging: PipelineLogging
    ) -> None:
    """
    Executes the serving pipeline.
    """
    pipeline_logging.logger.info("Starting serving_pipeline")

    # extract and load
    extract_template_environment = Environment(
        loader=FileSystemLoader("etl_project/assets/sql/extract")
    )
    pipeline_logging.logger.info("Perform extract and load")
    extract_load(
        template_environment=extract_template_environment,
        source_postgresql_client=postgresql_client,
        target_postgresql_client=postgresql_target_client,
    )

    # transform
    transform_template_environment = Environment(
        loader=FileSystemLoader("etl_project/assets/sql/transform")
    )

    # create nodes
    stock_prices_in_currencies = SqlTransform(
        table_name="stock_prices_in_currencies",
        postgresql_client=postgresql_target_client,
        environment=transform_template_environment,
    )
    aggregated_stock_profiles = SqlTransform(
        table_name="aggregated_stock_profiles",
        postgresql_client=postgresql_target_client,
        environment=transform_template_environment,
    )

    # create DAG
    dag = TopologicalSorter()
    dag.add(stock_prices_in_currencies)
    dag.add(aggregated_stock_profiles, stock_prices_in_currencies)
    # run transform
    pipeline_logging.logger.info("Perform transform")
    transform(dag=dag)
    pipeline_logging.logger.info("Serving pipeline run successful")


def run_pipeline(
        pipeline_name: str, 
        pipeline_config: dict, 
        fixer_api_client: FixerApiClient, 
        market_stack_api_client: MarketStackApiClient, 
        postgresql_client: PostgreSqlClient, 
        postgresql_target_client: PostgreSqlClient,
        postgresql_logging_client: PostgreSqlClient
    ) -> None:
    """
    Runs the ETL pipeline.
    """
    pipeline_logging = PipelineLogging(
        pipeline_name=pipeline_name,
        log_folder_path=pipeline_config.get("config").get("log_folder_path"),
    )
    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_name,
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get("config"),
    )
    try:
        metadata_logger.log()  # log start
        raw_pipeline(
            fixer_api_client=fixer_api_client, 
            market_stack_api_client=market_stack_api_client, 
            postgresql_client=postgresql_client, 
            pipeline_logging=pipeline_logging
        )
        serving_pipeline(
            postgresql_client=postgresql_client, 
            postgresql_target_client=postgresql_target_client,
            pipeline_logging=pipeline_logging
        )
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )  # log end
        pipeline_logging.logger.handlers.clear()
    except Exception as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        pipeline_logging.logger.exception(e)
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )  # log error
        pipeline_logging.logger.handlers.clear()


if __name__ == "__main__":
    env_vars = load_environment_variables()
    fixer_api_client, market_stack_api_client, postgresql_client, postgresql_logging_client, postgresql_target_client = setup_clients(env_vars)

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            PIPELINE_NAME = pipeline_config.get("pipeline_name")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    # run pipelines
    run_pipeline(
        pipeline_name=PIPELINE_NAME,
        pipeline_config=pipeline_config,
        fixer_api_client=fixer_api_client,
        market_stack_api_client=market_stack_api_client,
        postgresql_client=postgresql_client,
        postgresql_target_client=postgresql_target_client,
        postgresql_logging_client=postgresql_logging_client,
    )

     


