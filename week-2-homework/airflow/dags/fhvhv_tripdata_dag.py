from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import os
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

import yellow_taxi_ingestion

DOWNLOAD_BASE_URL = 'https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
CSV_OUTPUT_FILE_TEMPLATE = "fhvhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv"
TABLE_NAME_TEMPLATE = "fhvhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}"
PARQUET_OUTPUT_FILE_TEMPLATE = "fhvhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
GCP_GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_airflow_test")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "dtc-de")
BIGQUERY_DATASET_NAME = os.environ.get("BIGQUERY_DATASET_NAME", 'trips_data_all')

with DAG(
    dag_id='fhv_ingestion_dax',
    start_date= datetime(2021, 1, 1),
    end_date=datetime(2021, 1, 3),
    schedule_interval='0 4 2 * *',
    default_args= {'retries':1}
    ) as airflow_dag:

    download_fhv_data_with_url = BashOperator (
        task_id = 'download_fhv_csv_data',
        bash_command= f'curl -sS {DOWNLOAD_BASE_URL} > {AIRFLOW_HOME}/{CSV_OUTPUT_FILE_TEMPLATE}'
    )

    convert_fhv_csv_file_to_parquet = PythonOperator (
        task_id = 'convert_fhv_csv_file_to_parquet',
        python_callable = yellow_taxi_ingestion.convert_csv_file_to_parquet,
        op_kwargs= {
            'csv_file_path' : f'{AIRFLOW_HOME}/{CSV_OUTPUT_FILE_TEMPLATE}'
        }
    )

    # upload_fhv_data_to_gcs = LocalFilesystemToGCSOperator (
    #     task_id="upload_fhv_data_to_gcs",
    #     src= PARQUET_OUTPUT_FILE_TEMPLATE,
    #     dst = f'fhvhv_tripdata/{PARQUET_OUTPUT_FILE_TEMPLATE}',
    #     bucket=GCP_GCS_BUCKET
    # )


    upload_fhv_data_to_gcs = PythonOperator(
        task_id="upload_fhv_data_to_gcs",
        python_callable=yellow_taxi_ingestion.upload_to_gcs,
        op_kwargs={
            "bucket": GCP_GCS_BUCKET,
            "object_name": f'fhvhv_tripdata/{PARQUET_OUTPUT_FILE_TEMPLATE}',
            "local_file": f'{PARQUET_OUTPUT_FILE_TEMPLATE}',
        },
    )

    upload_fhv_data_to_big_query = BigQueryCreateExternalTableOperator(
        task_id="create_fhv_table_in_big_query",
    table_resource={
        "tableReference": {
            "projectId": GCP_PROJECT_ID,
            "datasetId": BIGQUERY_DATASET_NAME,
            "tableId": TABLE_NAME_TEMPLATE,
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": [f"gs://{GCP_GCS_BUCKET}/fhvhv_tripdata/{PARQUET_OUTPUT_FILE_TEMPLATE}"],
        },
    },

    )

    download_fhv_data_with_url >> convert_fhv_csv_file_to_parquet >> upload_fhv_data_to_gcs >> upload_fhv_data_to_big_query
