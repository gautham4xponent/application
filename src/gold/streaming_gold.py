from pyspark.sql.functions import (
    sum,
    count,
    avg,
    current_timestamp
)

from common.helpers import load_config


try:

    # Load Configuration

    config = load_config(
        "src/config/config.yml"
    )

    mongo_collection = config["mongodb"]["collection"]

    silver_schema = config["schemas"]["silver"]

    silver_table = (
        f"{silver_schema}.{mongo_collection}"
    )

    checkpoint_location = config["streaming"]["gold_checkpoint_location"]

    trigger_interval = config["streaming"]["trigger_interval"]

    gold_config = config["gold"]

    product_sales_table = gold_config["product_sales_summary"]

    customer_sales_table = gold_config["customer_sales_summary"]

    daily_sales_table = gold_config["daily_sales_summary"]

    status_summary_table = gold_config["order_status_summary"]

    sales_summary_table = gold_config["sales_summary"]


    # Read Silver Stream

    silver_stream = (
        spark.readStream
        .table(silver_table)
    )


    def process_batch(batch_df, batch_id):

        print(f"Processing Batch : {batch_id}")

        record_count = batch_df.count()

        print(f"Input Records : {record_count}")

        if record_count == 0:
            print("No records received.")
            return

        # Product Sales Summary

        product_sales_df = (
            batch_df
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

        product_sales_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(
                product_sales_table
            )


        # Customer Sales Summary

        customer_sales_df = (
            batch_df
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

        customer_sales_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(
                customer_sales_table
            )
    
        # Daily Sales Summary

        daily_sales_df = (
            batch_df
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

        daily_sales_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(
                daily_sales_table
            )


   
        # Order Status Summary

        status_summary_df = (
            batch_df
            .groupBy("status")
            .agg(
                count("order_id").alias("total_orders")
            )
            .withColumn(
                "gold_processed_time",
                current_timestamp()
            )
        )

        status_summary_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(
                status_summary_table
            )


       
        # Overall Sales Summary

        sales_summary_df = (
            batch_df
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

        sales_summary_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(
                sales_summary_table
            )

        print("Gold Batch Processed Successfully.")


    # Start Streaming Query

    query = (
        silver_stream.writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            checkpoint_location
        )
        .trigger(
            processingTime=trigger_interval
        )
        .start()
    )


    # Keep Streaming Running

    query.awaitTermination()


except Exception as e:

    print(f"Streaming Gold Pipeline failed: {e}")

    raise