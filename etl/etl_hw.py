from prefect import flow, task, get_run_logger
from prefect.blocks.system import String, JSON
from prefect_sqlalchemy import SqlAlchemyConnector
import pandas as pd
import os
from datetime import datetime

string_block = String.load("block-name")
db_block = SqlAlchemyConnector.load("hw-db")
json_block = JSON.load("block-json")


@task
def extract_sales_data() -> pd.DataFrame:
    logger = get_run_logger()
    try:
        logger.info(f"String block value: {string_block.value}")
        logger.info(f"JSON block value: {json_block.value}")
        with db_block.get_connection(begin=False) as conn:
            query = "select * from pos_ordered_item_feb_superhouse"
            df = pd.read_sql(query, con=conn)
            logger.info("Data extraction completed.")
            return df

    except Exception as e:
        logger.error(f"Failed to extract sales data: {e}")
        raise


@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
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
def load_to_csv(df: pd.DataFrame) -> None:
    logger = get_run_logger()
    try:
        os.makedirs("export", exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H-%M")
        file_name = f"export/sales_superhouse_{timestamp}.csv"
        df.to_csv(file_name, index=False)
        logger.info(f"Data saved to {file_name}")
    except Exception as e:
        logger.error(f"Failed to save CSV file: {e}")
        raise


@flow(name="etl-hw-flow")
def etl_hw_db() -> None:
    logger = get_run_logger()
    try:
        logger.info("Starting the ETL flow")
        df = extract_sales_data()
        df_clean = transform_data(df)
        load_to_csv(df_clean)
        logger.info("ETL flow completed.")
    except Exception as e:
        logger.error(f"ETL flow failed: {e}")
        raise


if __name__ == "__main__":
    etl_hw_db()
