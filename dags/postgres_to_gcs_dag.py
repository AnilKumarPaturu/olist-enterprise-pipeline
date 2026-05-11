from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import datetime, timedelta

TABLES_TO_EXTRACT = ["customers",'orders','order_items','products']
GCS_BUCKET_NAME = 'olist-bronze-prod-lake'

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}


with DAG(
    'onprem_to_gcs_bronze',
    default_args=default_args,
    description = 'Extracts Olist data from local Postgres and pushes to GCS Bronze Layer',
    schedule_interval = '@daily',
    start_date = datetime(2023,1,1),
    catchup = False,
    tags = ['ingestion','bronze']
) as dag:
    #Dynamically generate the task for each table
    for table in TABLES_TO_EXTRACT:
        extract_task = PostgresToGCSOperator(
            task_id = f'extract_{table}_to_gcs',
            postgres_conn_id = 'postgres_localhost',
            gcp_conn_id = 'google_cloud_conn',
            sql = f'select * from {table}',
            bucket = GCS_BUCKET_NAME,
            filename = f'{table}/{table}_raw_data.csv',
            export_format = 'csv',
            gzip = False,
            use_server_side_cursor=True
        )