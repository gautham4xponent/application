from pyspark.sql.functions import sum,count,avg,current_timestamp

from common.helpers import load_config

try:

    print("Starting Gold Batch Pipeline...")

    # Load configuration
    config = load_config(
        "src/config/config.yml"
    )

    mongo_collection = config["mongodb"]["collection"]

    silver_schema = config["schemas"]["silver"]

    silver_table = (
        f"{silver_schema}.{mongo_collection}"
    )


    gold_config = config["gold"]

    product_sales_table = gold_config["product_sales_summary"]
    customer_sales_table = gold_config["customer_sales_summary"]
    daily_sales_table = gold_config["daily_sales_summary"]
    status_summary_table = gold_config["order_status_summary"]
    sales_summary_table = gold_config["sales_summary"]   

    # Read Silver Table
    silver_df = spark.table(
        silver_table
    )

    # Validate input data
    record_count = silver_df.count()

    print(f"Records read from Silver: {record_count}")

    if record_count == 0:
        raise Exception(
            "No records found in Silver table."
        )

   
    # 1. Product Sales Summary

    product_sales_df = (
        silver_df
        .groupBy("product")
        .agg(
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_quantity"),
            sum("amount").alias("total_sales"),
            avg("amount").alias("average_sales")
        )
        .withColumn(
            "gold_processed_time",
            current_timestamp()
        )
    )

    # Write Product Product Sales sales to Gold Delta Table

    product_sales_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
            product_sales_table
        )
    print(
        f"Product Sales Records: {product_sales_df.count()}"
    )

 
    # 2. Customer Sales Summary
    customer_sales_df = (
        silver_df
        .groupBy(
            "customer_id",
            "customer_name"
        )
        .agg(
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_quantity"),
            sum("amount").alias("total_sales")
        )
        .withColumn(
            "gold_processed_time",
            current_timestamp()
        )
    )

    # Write Product customer sales to Gold Delta Table
    customer_sales_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
            customer_sales_table
        )
    print(
        f"Customer Sales Records: {customer_sales_df.count()}"
    )

  
    # 3. Daily Sales Summary

    daily_sales_df = (
        silver_df
        .groupBy("order_date")
        .agg(
            count("order_id").alias("total_orders"),
            sum("amount").alias("daily_sales")
        )
        .withColumn(
            "gold_processed_time",
            current_timestamp()
        )
    )

    # Write Product daily sales to Gold Delta Table
    daily_sales_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
            daily_sales_table
        )
    print(
        f"Daily Sales Records: {daily_sales_df.count()}"
    )

  
    # 4. Order Status Summary

    status_summary_df = (
        silver_df
        .groupBy("status")
        .agg(
            count("order_id").alias("total_orders")
        )
        .withColumn(
            "gold_processed_time",
            current_timestamp()
        )
    )

    # Write Product Sales Summary to Gold Delta Table

    status_summary_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
            status_summary_table
        )
    print(
        f"Status Summary Records: {status_summary_df.count()}"
    )

    
    # 5. Overall Sales Summary

    sales_summary_df = (
        silver_df
        .agg(
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_quantity"),
            sum("amount").alias("total_sales"),
            avg("amount").alias("average_order_value")
        )
        .withColumn(
            "gold_processed_time",
            current_timestamp()
        )
    )

# Write Product Overall Sales Summary to Gold Delta Table
    sales_summary_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
            sales_summary_table
        )

    print(
        f"Overall Summary Records: {sales_summary_df.count()}"
    )

    print("Gold Batch Pipeline completed successfully.")

except Exception as e:

    print(f"Gold Batch Pipeline failed: {e}")

    raise