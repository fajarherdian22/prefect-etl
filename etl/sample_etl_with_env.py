from prefect import flow, task, get_run_logger
import mysql.connector
import pandas as pd
from datetime import datetime
import yaml
import os


def load_config(env: str = "staging"):
    config_path = f"config/{env}.yaml"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path) as f:
        return yaml.safe_load(f)


@task
def extract_sales_data(env: str):
    logger = get_run_logger()
    try:
        logger.info(f"Loading config for env: {env}")
        config = load_config(env)
        db_config = config["db"]
        logger.info("Connecting to MySQL database...")
        connection = mysql.connector.connect(
            host=db_config["host"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["name"],
            port=db_config["port"],
        )

        query = "SELECT * FROM sales_outlet_daily"
        df = pd.read_sql(query, con=connection)
        connection.close()
        logger.info("Data extraction completed.")
        return df

    except Exception as e:
        logger.error(f"Failed to extract sales data: {e}")
        raise


@task
def transform_data(df: pd.DataFrame):
    logger = get_run_logger()
    try:
        logger.info("Transforming data...")
        df.columns = [col.lower() for col in df.columns]
        logger.info("Data transformation completed.")
        return df
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise


@task
def load_to_csv(df: pd.DataFrame):
    logger = get_run_logger()
    try:
        os.makedirs("export", exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"export/sales_{timestamp}.csv"
        df.to_csv(file_name, index=False)
        logger.info(f"Data saved to {file_name}")
    except Exception as e:
        logger.error(f"Failed to save CSV file: {e}")
        raise


@flow(name="sales-etl-flow")
def sales_etl_flow(env: str = "staging"):
    logger = get_run_logger()
    try:
        logger.info(f"Starting the ETL flow for environment: {env}")
        df = extract_sales_data(env)
        df_clean = transform_data(df)
        load_to_csv(df_clean)
        logger.info("ETL flow completed.")
    except Exception as e:
        logger.error(f"ETL flow failed: {e}")
        raise
