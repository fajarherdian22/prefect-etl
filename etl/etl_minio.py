from prefect import flow, task, get_run_logger
from prefect.blocks.system import String, JSON
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_aws.s3 import S3Bucket
from datetime import datetime
import pandas as pd
import io

# Constants
TABLE_NAME = "sales_outlet_daily"
FILENAME_TEMPLATE = "sales_{timestamp}.csv"
STRING_BLOCK_NAME = "block-name"
JSON_BLOCK_NAME = "block-json"
DB_BLOCK_NAME = "local-db-sqlalchemy"
S3_BLOCK_NAME = "block-s3"


@task
def load_blocks():
    logger = get_run_logger()
    logger.info("Loading Prefect blocks...")
    string_block = String.load(STRING_BLOCK_NAME)
    json_block = JSON.load(JSON_BLOCK_NAME)
    db_block = SqlAlchemyConnector.load(DB_BLOCK_NAME)
    s3_block = S3Bucket.load(S3_BLOCK_NAME)
    logger.info("Blocks loaded successfully.")
    return string_block, json_block, db_block, s3_block


@task
def extract_data(db_block: SqlAlchemyConnector) -> pd.DataFrame:
    logger = get_run_logger()
    query = f"SELECT * FROM {TABLE_NAME}"
    try:
        with db_block.get_connection(begin=False) as conn:
            df = pd.read_sql(query, con=conn)
        logger.info(f"Extracted {len(df)} rows from table `{TABLE_NAME}`.")
        return df
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise


@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        df.columns = [col.lower() for col in df.columns]
        logger.info("Data transformed successfully.")
        return df
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise


@task
def df_to_csv_buffer(df: pd.DataFrame) -> tuple[io.BytesIO, str]:
    logger = get_run_logger()
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = FILENAME_TEMPLATE.format(timestamp=timestamp)
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        logger.info("DataFrame converted to in-memory CSV buffer.")
        return buffer, filename
    except Exception as e:
        logger.error(f"Buffer conversion failed: {e}")
        raise


@task
def upload_to_minio(s3_block: S3Bucket, buffer: io.BytesIO, key: str):
    logger = get_run_logger()
    try:
        s3_block.upload_from_file_object(buffer, to_path=key)
        logger.info(f"File uploaded to MinIO as: {key}")
    except Exception as e:
        logger.error(f"Upload to MinIO failed: {e}")
        raise


@flow(name="flow-etl-minio")
def flow_etl_minio():
    logger = get_run_logger()
    try:
        logger.info("ETL flow started.")
        string_block, json_block, db_block, s3_block = load_blocks()
        logger.info(f"String block: {string_block.value}")
        logger.info(f"JSON block: {json_block.value}")

        df = extract_data(db_block)
        transformed_df = transform_data(df)
        buffer, filename = df_to_csv_buffer(transformed_df)
        upload_to_minio(s3_block, buffer, filename)

        logger.info("ETL flow completed successfully.")
    except Exception as e:
        logger.error(f"ETL flow failed: {e}")
        raise


if __name__ == "__main__":
    flow_etl_minio()
