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

# TODO: 
# Update the function's signature, use the proper Airflow templates reference variable to set the start date and 
# the end date to fetch rocket launch data. You can use the following guide to find out the variables:
# https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
# A function signature consists of function name and the list of parameters passed into the function, in this case, the function 
# signature is _download_launch_event_data(**context)
def _download_launch_event_data(**context):
    """
    This function sends an API call to download rocket launch event data, and then store
    the raw content under the S3 bucket.
    """
    # TODO: 
    # You may need to process the parameter. The API accept these date in format YYYY-MM-DD. If a timestamp is passed into
    # the function, you need to extract only the date part from it.
    start_date = None
    end_date   = None

    headers = {"accept": "application/json"}
    # To process the data incrementally, we need to configure the data interval using the 
    # `net__gte` and `net__lt` parameter provided by the API. This API call will fetch the data
    # that has `net` (i.e., event date) greater or equal to the start date, and less than the 
    # end date. For example, if we passed in 2024-12-01 and 2024-12-02. It will fetch all the events
    # between 2024-12-01 00:00:00 and 2024-12-01 23:59:59. This allow us to process data incrementally 
    # and re-run task without affecting other data.
    url = (
        f"https://lldev.thespacedevs.com/2.3.0/launches/?"
        f"mode=list&"
        f"net__gte={start_date}&"
        f"net__lt={end_date}"
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

    # this hook is not idempotent, use this try ... except block to prevent
    # upload data of the same key.
    try:
        # TODO: 
        # Set the value for the following two variables to store the data. The data needs to be stored 
        # under `raw/launch` folder (or prefix, in the context of S3), under the bucket named `datalake`
        s3_prefix   = None
        s3_key      = f"{s3_prefix}/{start_date}.json"
        bucket_name = None

        s3_hook.load_string(
            string_data=json.dumps(data),
            key=s3_key,
            bucket_name=bucket_name
        )
        logger.info(f"Uploaded data to {s3_key}")
    except ValueError:
        logger.info(f"File {s3_key} already exists.")

# TODO: 
# Update the function's signature, use the proper Airflow templates reference variable to set the start date 
# to load rocket launch data from `/raw/launch`. You can use the following guide to find out the variables:
# https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
def _generate_rocket_launch_event_partition(**context):
    """
    This function fetch the credentials to the S3 bucket from Airflow connection. It then uses 
    Pandas to process raw JSON data, and convert it to a structural format, and store it back to
    the datalake.
    """
    connection        = BaseHook.get_connection("aws_default")
    endpoint_url      = json.loads(connection._extra).get('endpoint_url')
    access_key_id     = connection.login
    access_secret_key = connection._password

    storage_options = {
        "key": access_key_id,
        "secret": access_secret_key,
        "client_kwargs": {"endpoint_url": endpoint_url}
    }

    # TODO:
    # Modify the value for the three variables below to read the raw JSON data from datalake
    bucket_name = None
    s3_prefix   = None
    start_date  = None
    df = pd.read_json(
        f"s3://{bucket_name}/{s3_prefix}/{start_date}.json",
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

    # TODO: 
    # Write the DataFrame to a Parquet file
    # 1. store the processed data in to '/processed/launch'. Be careful about the `/` in path.
    # 2. add the partition column name to `partition_cols`, we want to partition by `net` (i.e., date) column
    output_directory = None
    df_transformed.to_parquet(
        f"s3://datalake{output_directory}",
        partition_cols=[],
        engine="pyarrow",
        index=False,
        storage_options=storage_options,
    )

def _sign_off(data_interval_start, **context):
    """
    After we process the data, we will have a sign-off process to publish the data to a folder consumed by downstream clients, 
    applications, views, or dashboards. This process sometimes can be manual, and require testing and validation. The idea is 
    simple here. It copies the data from /processed/launch folder and move it under /reports/launch folder.
    """
    # fetch S3 credentials
    connection        = BaseHook.get_connection("aws_default")
    endpoint_url      = json.loads(connection._extra).get('endpoint_url')
    access_key_id     = connection.login
    access_secret_key = connection._password

    storage_options = {
        "key": access_key_id,
        "secret": access_secret_key,
        "client_kwargs": {"endpoint_url": endpoint_url}
    }

    # TODO:
    # Set `input_directory` to read from the `/processed/launch` folder
    input_directory = None
    df = pd.read_parquet(
        f"s3://datalake{input_directory}",
        filters=[('net', '=', f"{data_interval_start.date()}")],  # Specify the partition filter
        storage_options=storage_options,
    )

    # TODO:
    # Set `output_directory` to write to the `/reports/launch` folder
    output_directory = None
    df.to_parquet(
        f"s3://datalake{output_directory}",
        partition_cols=["net"],
        engine="pyarrow",
        index=False,
        storage_options=storage_options,
    )

# TODO: 
# 1. Configure the DAG to run at 00:00:00 everyday. You can check https://www.astronomer.io/docs/learn/scheduling-in-airflow/
#    to learn how to schedule Airflow DAG.
# 2. Pass the above Python function to the corresponding operator.
# 3. Configure task dependencies at the end. You can check https://www.astronomer.io/docs/learn/managing-dependencies/ 
#    to learn how to setup task dependencies. The task dependencies is as follow
# 
#    download_launch_event_data -> generate_rocket_launch_event_partition -> sign_off
with DAG(
    dag_id=f"{Path(__file__).stem}",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    schedule=None,
) as dag:
    # define the Python operator to execute the download launch event task.
    download_launch_event_data = PythonOperator(
        task_id="download_launch_event_data",
        python_callable=None,
    )

    generate_rocket_launch_event_partition = PythonOperator(
        task_id="generate_rocket_launch_event_partition",
        python_callable=None,
    )
    
    sign_off = PythonOperator(
        task_id="sign_off",
        python_callable=None,
    )