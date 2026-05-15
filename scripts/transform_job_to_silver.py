from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count,when,row_number, expr
from pyspark.sql.window import Window
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.appName("DQ-Pipeline") \
.getOrCreate()

DQ_CONTRACT = {
    "orders" : {
        "valid_rule" :"order_id IS NOT NULL AND order_status IN ('delivered', 'shipped', 'canceled', 'invoiced', 'processing', 'unavailable')"
    },
    "order_items" : {
    "valid_rule" : "order_id IS NOT NULL AND product_id IS NOT NULL and price >= 0 AND freight_value >= 0"
    },
    "products": {
        "valid_rule": "product_id IS NOT NULL AND product_weight_g >= 0"
    },
    "customers": {
        "valid_rule": "customer_id IS NOT NULL AND customer_unique_id IS NOT NULL"
    }

}

def apply_dq_rules(df, table_name, threshold_pct=0.05):
    # Applies metadata-driven rules to split data into Clean and Quarantine.
    total_raw_rows = df.count()
    print(f"{table_name} starting DQ Check. Total input rows: {total_raw_rows}")
    if total_raw_rows == 0:
        print(f"[{table_name}] Skipping: Zero rows ingested.")
        return df, df.limit(0)
    validity_expression = DQ_CONTRACT.get(table_name, {}).get("valid_rule")
    if not validity_expression:
        raise ValueError(f"No Data Contract defined for table: {table_name}")
    clean_df = df.filter(expr(validity_expression))
    clean_df = clean_df.withColumn("ingestion_timestamp", current_timestamp())
    bad_df = df.filter(~expr(validity_expression))
    bad_df = bad_df.withColumn("ingestion_timestamp", current_timestamp())
    

    # 3. Calculate Audit Metrics
    bad_count = bad_df.count()
    clean_count = clean_df.count()
    error_rate = bad_count / clean_count
    print(f"{table_name} DQ Results: Clean={clean_count}, Quarantined = {bad_count}, Error Rate={error_rate:.2%}")

    # 4. The Circuit Breaker
    if error_rate > threshold_pct:
        raise Exception(f"{table_name} CIRCUIT BREAKER TRIPPED! Error rate {error_rate:.2%} exceeds threshold of {threshold_pct:.2%}.")
    
    return clean_df, bad_df

def process_bronze_to_silver(spark):
    tables_to_process = ["orders", "order_items", "products", "customers"]
    for table in tables_to_process:
        print(f"--- Processing {table} ---")
        # Read Bronze
        raw_df = spark.read.format("parquet").load(f"gs://olist-bronze-prod-lake/{table}/")
        # Run DQ Check
        clean_df, bad_df = apply_dq_rules(raw_df, table)

        # Route the Data
        # 1. Write clean data to Silver for further processing/joining
        clean_df.write.mode("overwrite").parquet(f"gs://olist-silver-prod-lake/{table}/")

        # 2. Write bad data to a Dead Letter Queue (Quarantine) bucket for debugging
        if bad_df.count() > 0:
            bad_df.write.mode("append").parquet(f"gs://olist-quarantine-prod-lake/{table}")

process_bronze_to_silver(spark)