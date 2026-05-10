from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, count, broadcast, max as _max, datediff, when,lit,countDistinct,round as _round


def main():
    spark = SparkSession.builder.appName("OlistGoldLayer").getOrCreate()
    silver_path = f"gs://olist-silver-prod-lake/"
    gold_path = f"gs://olist-gold-prod-lake/"

    print("--- 1. Reading Silver Data ---")
    orders = spark.read.parquet(silver_path + "orders/")
    items = spark.read.parquet(silver_path + "order_items/")
    products = spark.read.parquet(silver_path + "products/")
    customers = spark.read.parquet(silver_path + "customers/")


    ''' Standardize dates for the whole script.It’s much easier to calculate "Daily Sales" when you aren't dealing with 
    unique seconds.Joining datasets on a Date is often faster than joining on a precise Timestamp
    '''
    orders = orders.withColumn("purchase_date", to_date(col("order_purchase_timestamp")))

    print("--- 2. Building Data Product 1: Sales Dashboard ---")
    # 1. Perform the joins
    sales_base = orders.join(items, "order_id", "inner") \
                       .join(broadcast(products), "product_id", "left")
    
    # 2. THE NULL FIX: Handle the Null categories so BI dashboards don't break
    sales_clean = sales_base.fillna({"product_category_name": "Unknown Category"})
    
    # 3. THE PRECISION FIX: Aggregate and round the currency to 2 decimal places
    sales_df = sales_clean.groupBy("purchase_date", "product_category_name") \
                          .agg(
                              _round(_sum("price"), 2).alias("total_revenue"), 
                              count("order_id").alias("total_items_sold")
                          )
    
    print("--- 3. Building Data Product 2: Logistics SLA ---")
    # Did the carrier deliver it later than the estimated date?
    logistics_df = orders.join(customers, "customer_id", "inner") \
                         .withColumn("delivery_delay_days", datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date"))) \
                         .withColumn("is_late", when(col("delivery_delay_days") > 0 , 1).otherwise(0)) \
                         .groupBy(to_date(col("order_purchase_timestamp")).alias("purchase_date"), "customer_state") \
                         .agg(
                              count("order_id").alias("total_orders"),
                              _sum("is_late").alias("late_deliveries"),
                              _max("delivery_delay_days").alias("max_delay_days")   
                         )
    print("--- 4. Building Data Product 3: Marketing RFM (Recency, Frequency, Monetary) ---")
    # RFM(RECENT, FREQUNCY, MONETARY) needs a "current date" reference. Since Olist data is old, we use the max date in the dataset.
    max_date_row = orders.select(_max("purchase_date")).collect()[0][0]
    rfm_base = orders.join(items, "order_id", "inner").join(customers, "customer_id","inner")

    rfm_df = rfm_base.groupBy("customer_unique_id").agg(
            datediff(lit(max_date_row),_max("purchase_date")).alias("receny_days"),
            countDistinct("order_id").alias("frequency_orders"),
            _sum("price").alias("monetary_spend")
    )

    print("--- 5. Gold Layer Data Integrity Checks ---")
    # Check 1: Cartesian Explosion Check
    # Total distinct orders in Sales table shouldn't somehow exceed total orders in Silver layer
    silver_order_count = orders.count()
    sales_item_count = sales_df.agg(_sum("total_items_sold")).collect()[0][0]
    print(f"Audit: {silver_order_count} Silver Orders resulted in {sales_item_count} Items Sold.")
    if sales_item_count == 0 or sales_item_count is None:
         raise Exception("GOLD INTEGRITY FAILED: Join resulted in 0 items sold!")
    
    # Check 2: Revenue Sanity Check
    negative_revenue = sales_df.filter(col("total_revenue") < 0).count()
    if negative_revenue > 0:
        raise Exception("GOLD INTEGRITY FAILED: Negative revenue detected after aggregation!")
    
    print("--- 6. Writing to Gold Layer ---")
    sales_df.write.mode("overwrite").partitionBy("purchase_date").parquet(gold_path +"sales_daily_revenue/")
    logistics_df.write.mode("overwrite").partitionBy("purchase_date").parquet(gold_path + "logistics_sla/")
    rfm_df.write.mode("overwrite").parquet(gold_path + "marketing_rfm/")

    print("--- Gold Layer Generation Complete ---")

if __name__ == "__main__":
    main()
