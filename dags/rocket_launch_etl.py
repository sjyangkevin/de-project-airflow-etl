from pathlib import Path
import requests
import json
import logging
import pendulum

import pandas as pd

import airflow
import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Define the log message format
    datefmt="%Y-%m-%d %H:%M:%S",  # Define the date and time format
)
logger = logging.getLogger()

def _download_launch_event_data(data_interval_start: pendulum.DateTime, data_interval_end: pendulum.DateTime, **context):
    """
    This function sends an API call to download rocket launch event data, and then store
    the raw content under the S3 bucket.
    """
    # process data interval timestamp
    data_interval_start = data_interval_start.date()
    data_interval_end   = data_interval_end.date()

    headers = {"accept": "application/json"}
    # To process the data incrementally, we need to configure the data interval using the 
    # `net__gte` and `net__lt` parameter provided by the API.
    url = (
        f"https://lldev.thespacedevs.com/2.3.0/launches/?"
        f"mode=list&"
        f"net__gte={data_interval_start}&"
        f"net__lt={data_interval_end}"
    )
    logger.info(f"Request URL: {url}")
    # We get the response, and convert the response to JSON string
    response = requests.get(url, headers=headers)
    # if there is an exception in fetching the data, raise this exception
    response.raise_for_status()
    # covnert the fetched data to JSON string
    data = response.json()
    # connect to S3 bucket using the defined S3 connection
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_key  = f"raw/launch/{data_interval_start}.json"
    # store the raw data under the S3 prefix: raw/launch/
    try:
        # this hook is not idempotent, use this try ... except block to prevent
        # upload data of the same key.
        s3_hook.load_string(
            string_data=json.dumps(data),
            key=s3_key,
            bucket_name="datalake"
        )
        logger.info(f"Uploaded data to {s3_key}")
    except ValueError:
        logger.info(f"File {s3_key} already exists.")

def _generate_rocket_launch_event_partition(data_interval_start, **context):
    # fetch S3 credentials
    connection   = BaseHook.get_connection("aws_default")
    endpoint_url      = json.loads(connection._extra).get('endpoint_url')
    access_key_id     = connection.login
    access_secret_key = connection._password

    storage_options = {
        "key": access_key_id,
        "secret": access_secret_key,
        "client_kwargs": {"endpoint_url": endpoint_url}
    }

    df = pd.read_json(
        f"s3://datalake/raw/launch/{data_interval_start.date()}.json",
        storage_options=storage_options,
    )

    # Normalize the 'results' field into a flat DataFrame
    df_results = pd.json_normalize(df["results"])

    # Extract and rename the desired columns
    df_transformed = df_results[[
        "id",
        "url",
        "name",
        "status.name",
        "image.image_url",
        "image.license.name",
        "net",
    ]].rename(columns={
        "status.name": "status",
        "image.image_url": "image_url",
        "image.license.name": "license",
    })

    # Convert 'net' to date-only format
    df_transformed["net"] = pd.to_datetime(df_transformed["net"]).dt.date

    # Write the DataFrame to a Parquet file, partitioned by 'net'
    df_transformed.to_parquet(
        f"s3://datalake/processed/launch",
        partition_cols=["net"],
        engine="pyarrow",
        index=False,
        storage_options=storage_options,
    )

def _sign_off(data_interval_start, **context):
    # fetch S3 credentials
    connection   = BaseHook.get_connection("aws_default")
    endpoint_url      = json.loads(connection._extra).get('endpoint_url')
    access_key_id     = connection.login
    access_secret_key = connection._password

    storage_options = {
        "key": access_key_id,
        "secret": access_secret_key,
        "client_kwargs": {"endpoint_url": endpoint_url}
    }

    # Read only the corresponding partition from the processed data
    df = pd.read_parquet(
        f"s3://datalake/processed/launch",
        filters=[('net', '=', f"{data_interval_start.date()}")],  # Specify the partition filter
        storage_options=storage_options,
    )

    # Write the data back to the reports location, preserving the partition structure
    df.to_parquet(
        f"s3://datalake/reports/launch",
        partition_cols=["net"],  # Ensure partitioning by 'net'
        engine="pyarrow",
        index=False,
        storage_options=storage_options,
    )

with DAG(
    dag_id=f"{Path(__file__).stem}",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    schedule="@daily",
) as dag:
    # define the Python operator to execute the download launch event task.
    download_launch_event_data = PythonOperator(
        task_id="download_launch_event_data",
        python_callable=_download_launch_event_data,
    )

    generate_rocket_launch_event_partition = PythonOperator(
        task_id="generate_rocket_launch_event_partition",
        python_callable=_generate_rocket_launch_event_partition,
    )
    
    sign_off = PythonOperator(
        task_id="sign_off",
        python_callable=_sign_off,
    )

    download_launch_event_data >> generate_rocket_launch_event_partition >> sign_off