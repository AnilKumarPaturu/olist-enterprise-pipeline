from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator,DataprocSubmitPySparkJobOperator,DataprocDeleteClusterOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = "ecommerce-de-project-495511"
REGION = 'asia-south1'
CLUSTER_NAME = 'olist-ephemeral-cluster'
GOLD_BUCKET = "olist-gold-prod-lake"
DATASET = "olist_warehouse_prod"
ARTIFACTS_BUCKET = "olist-artifacts-prod-lake"
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_size_gb": 30}
    },
    "worker_config": {
        "num_instances": 0
    },
    "gce_cluster_config": {
        "service_account": "dataproc-airflow-sa-prod@ecommerce-de-project-495511.iam.gserviceaccount.com"
    }
}

# PYSPARK_URI = f"gs://imp-files/transform_job_to_silver.py"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past' : False,
    'email_on_failure' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

with DAG(
    'enterprise_medallion_pipeline',
    default_args = default_args,
    description='Spins up Dataproc, End-to-End Medallion Architecture (Bronze->Silver->Gold->BQ), tears down cluster',
    schedule_interval = None,
    start_date = days_ago(1),
    tags = ['transformation', 'pyspark'],
) as dag:
    # Task 1: Create the Cluster
    create_cluster  = DataprocCreateClusterOperator(
        task_id = 'create_cluster',
        project_id = PROJECT_ID,
        cluster_name = CLUSTER_NAME,
        region = REGION,
        cluster_config = CLUSTER_CONFIG,
        gcp_conn_id = 'google_cloud_conn'
    )
    # Task 2: Run Bronze to Silver (DQ Checks & Cleaning)
    run_bronze_to_silver = DataprocSubmitPySparkJobOperator(
        task_id="run_bronze_to_silver",
        main=f"gs://{ARTIFACTS_BUCKET}/scripts/transform_job_to_silver.py",
        cluster_name=CLUSTER_NAME,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id='google_cloud_conn'
    )
    # Task 3. Run Silver to Gold (Business Aggregations & Joins)
    run_silver_to_gold = DataprocSubmitPySparkJobOperator(
        task_id="run_silver_to_gold",
        main=f"gs://{ARTIFACTS_BUCKET}//scripts/transform_job_to_gold.py",
        cluster_name=CLUSTER_NAME,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id='google_cloud_conn'
    )

    # Task 4: Delete the Cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        gcp_conn_id='google_cloud_conn',
        trigger_rule="all_done" 
    )

    # 5. Load BigQuery - Sales
    load_sales_bq = GCSToBigQueryOperator(
        task_id = 'load_sales_bq',
        bucket = GOLD_BUCKET,
        source_objects = ["sales_daily_revenue/*.parquet"],
        destination_project_dataset_table = f"{DATASET}.sales_daily_revenue",
        source_format = 'PARQUET', 
        write_disposition = 'WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id = 'google_cloud_conn'
    )
    # 6. Load BigQuery - Logistics
    load_logistics_bq = GCSToBigQueryOperator(
        task_id='load_logistics_bq',
        bucket = GOLD_BUCKET,
        source_objects=['logistics_sla/*.parquet'],
        destination_project_dataset_table=f'{DATASET}.logistics_sla',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_conn'
    )

    # 7. Load BigQuery - Marketing
    load_rfm_bq = GCSToBigQueryOperator(
        task_id='load_rfm_bq',
        bucket = GOLD_BUCKET,
        source_objects=['marketing_rfm/*.parquet'],
        destination_project_dataset_table=f'{DATASET}.marketing_rfm',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_conn'
    )

    create_cluster >> run_bronze_to_silver >> run_silver_to_gold 

    # As soon as Gold is done, fan-out and load all BQ tables in parallel
    run_silver_to_gold >> [load_sales_bq , load_logistics_bq, load_rfm_bq]

    [load_sales_bq,load_logistics_bq,load_rfm_bq] >> delete_cluster