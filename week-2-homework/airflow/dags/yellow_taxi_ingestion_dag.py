from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import os
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

import yellow_taxi_ingestion

DOWNLOAD_BASE_URL = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
CSV_OUTPUT_FILE_TEMPLATE = "yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv"
TABLE_NAME_TEMPLATE = "yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}"
PARQUET_OUTPUT_FILE_TEMPLATE = "yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
GCP_GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_airflow_test")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "dtc-de")
BIGQUERY_DATASET_NAME = os.environ.get("BIGQUERY_DATASET_NAME", 'trips_data_all')

with DAG(
    dag_id='yellow_taxi_ingestion_dax',
    start_date= datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 3),
    concurrency=3, 
    max_active_runs=2,
    schedule_interval='0 4 2 * *',
    default_args= {'retries':1}
    ) as airflow_dag:

    download_data_with_url = BashOperator (
        task_id = 'download_csv_data',
        bash_command= f'curl -sS {DOWNLOAD_BASE_URL} > {AIRFLOW_HOME}/{CSV_OUTPUT_FILE_TEMPLATE}'
    )

    convert_csv_file_to_parquet = PythonOperator (
        task_id = 'convert_csv_file_to_parquet',
        python_callable = yellow_taxi_ingestion.convert_csv_file_to_parquet,
        op_kwargs= {
            'csv_file_path' : f'{AIRFLOW_HOME}/{CSV_OUTPUT_FILE_TEMPLATE}'
        }
    )

    upload_data_to_gcs = LocalFilesystemToGCSOperator (
        task_id="upload_data_to_gcs",
        src= PARQUET_OUTPUT_FILE_TEMPLATE,
        dst = f'yellow_taxi/{PARQUET_OUTPUT_FILE_TEMPLATE}',
        bucket=GCP_GCS_BUCKET
    )

    upload_data_to_big_query = BigQueryCreateExternalTableOperator(
        task_id="create_table_in_big_query",
    table_resource={
        "tableReference": {
            "projectId": GCP_PROJECT_ID,
            "datasetId": BIGQUERY_DATASET_NAME,
            "tableId": TABLE_NAME_TEMPLATE,
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": [f"gs://{GCP_GCS_BUCKET}/yellow_taxi/{PARQUET_OUTPUT_FILE_TEMPLATE}"],
        },
    },

    )

    download_data_with_url >> convert_csv_file_to_parquet >> upload_data_to_gcs >> upload_data_to_big_query
