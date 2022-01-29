import os
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import yellow_taxi_ingestion
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


URL = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
CSV_FILE_NAME = 'taxi_zone_lookup.csv'
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PARQUET_FILE_NAME = f'{AIRFLOW_HOME}/taxi_zone_lookup.parquet'
GCP_GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_airflow_test")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "dtc-de")
BIGQUERY_DATASET_NAME = os.environ.get("BIGQUERY_DATASET_NAME", 'trips_data_all')
TABLE_NAME_TEMPLATE = 'taxi_zone_lookup'

with DAG(
    dag_id='taxi_zone_dag',
    schedule_interval="@daily",
    start_date = days_ago(1),
) as airflow_dag_zone:

    download_zone_csv_file = BashOperator(
        task_id = 'download_zone_csv_file',
        bash_command=f'curl -sS {URL} > {AIRFLOW_HOME}/{CSV_FILE_NAME}'
    )

    convert_zone_csv_file_to_parquet = PythonOperator (
    task_id = 'convert_zone_csv_file_to_parquet',
    python_callable = yellow_taxi_ingestion.convert_csv_file_to_parquet,
    op_kwargs= {
        'csv_file_path' : f'{AIRFLOW_HOME}/{CSV_FILE_NAME}'
        }
    )

    upload_zone_data_to_gcs = LocalFilesystemToGCSOperator (
        task_id="upload_zone_data_to_gcs",
        src= PARQUET_FILE_NAME,
        dst = f'zone/{TABLE_NAME_TEMPLATE}.parquet',
        bucket=GCP_GCS_BUCKET
    )

    uploadz_one__data_to_big_query = BigQueryCreateExternalTableOperator(
        task_id="uploadz_one__data_to_big_query",
    table_resource={
        "tableReference": {
            "projectId": GCP_PROJECT_ID,
            "datasetId": BIGQUERY_DATASET_NAME,
            "tableId": TABLE_NAME_TEMPLATE,
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": [f"gs://{GCP_GCS_BUCKET}/zone/{TABLE_NAME_TEMPLATE}.parquet"],
        },
    },
    )

    download_zone_csv_file >> convert_zone_csv_file_to_parquet >> upload_zone_data_to_gcs >> uploadz_one__data_to_big_query