from prefect import flow, task, get_run_logger
import mysql.connector
import pandas as pd
from datetime import datetime


@task
def extract_sales_data():
    logger = get_run_logger()
    logger.info("Starting data extraction from MySQL.")
    connection = mysql.connector.connect(
        host="localhost", user="admin", password="admin1234", database="hw_db"
    )
    query = "SELECT * FROM sales_outlet_daily"
    df = pd.read_sql(query, con=connection)
    connection.close()
    logger.info("Data extraction completed.")
    return df


@task
def transform_data(df: pd.DataFrame):
    logger = get_run_logger()
    logger.info("Starting data transformation.")
    df.columns = [col.lower() for col in df.columns]
    logger.info("Data transformation completed.")
    return df


@task
def load_to_csv(df: pd.DataFrame):
    logger = get_run_logger()
    logger.info("Starting data load to CSV.")
    timestamp = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
    file_name = f"export/test_{timestamp}.csv"
    print(f"Saving data to {file_name}")
    df.to_csv(file_name, index=False)
    logger.info(f"Data saved to {file_name}")


@flow(name="test-flow")
def test_flow():
    logger = get_run_logger()
    logger.info("Starting the ETL flow.")
    df = extract_sales_data()
    df_clean = transform_data(df)
    load_to_csv(df_clean)
    logger.info("ETL flow completed.")
