from pyspark.sql.functions import (col,trim,upper,current_timestamp,to_date)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

from delta.tables import DeltaTable
from common.helpers import load_config


try:

    # Load Pipeline Configuration

    config = load_config(
        "src/config/config.yml"
    )

    mongo_collection = config["mongodb"]["collection"]

    bronze_schema = config["schemas"]["bronze"]
    silver_schema = config["schemas"]["silver"]

    checkpoint_location = config["streaming"]["silver_checkpoint_location"]
    trigger_interval = config["streaming"]["trigger_interval"]

    bronze_table = (
        f"{bronze_schema}.{mongo_collection}"
    )

    silver_table = (
        f"{silver_schema}.{mongo_collection}"
    )

   
    # Read Streaming Data from Bronze Delta Table

    bronze_stream = (
        spark.readStream
        .table(bronze_table)
    )


    # Process Each Streaming Micro-Batch

    def process_batch(batch_df, batch_id):

        print(f"Processing Batch : {batch_id}")

        # Validate Input Records
        record_count = batch_df.count()

        print(f"Input Records : {record_count}")

        if record_count == 0:
            print("No records received.")
            return

        
        # Remove Duplicate Records

        window_spec = Window.partitionBy(
            "order_id"
        ).orderBy(
            col("updated_at").desc()
        )

        silver_df = (
            batch_df
            .withColumn(
                "row_num",
                row_number().over(window_spec)
            )
            .filter(
                col("row_num") == 1
            )
            .drop("row_num")
        )

        
        # Apply Data Quality Validations

        silver_df = silver_df.filter(
            col("order_id").isNotNull()
        )

        silver_df = silver_df.filter(
            col("customer_name").isNotNull()
        )

        silver_df = silver_df.filter(
            col("product").isNotNull()
        )

        silver_df = silver_df.filter(
            col("amount") > 0
        )

        silver_df = silver_df.filter(
            col("quantity") > 0
        )

      
        # Standardize Data

        silver_df = silver_df.withColumn(
            "customer_name",
            trim(col("customer_name"))
        )

        silver_df = silver_df.withColumn(
            "product",
            trim(col("product"))
        )

        silver_df = silver_df.withColumn(
            "status",
            upper(trim(col("status")))
        )

        silver_df = silver_df.withColumn(
            "order_date",
            to_date("order_date")
        )

        valid_status = [
            "PLACED",
            "SHIPPED",
            "DELIVERED",
            "CANCELLED"
        ]

        silver_df = silver_df.filter(
            col("status").isin(valid_status)
        )

        # Add Processing Metadata

        silver_df = silver_df.withColumn(
            "silver_processed_time",
            current_timestamp()
        )

       
        # Calculate Pipeline Metrics

        output_count = silver_df.count()

        print(f"Output Records   : {output_count}")
        print(f"Rejected Records : {record_count - output_count}")

       
        # Merge Data into Silver Delta Table


        if not spark.catalog.tableExists(silver_table):

            silver_df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(silver_table)

        else:

            silver_delta = DeltaTable.forName(
                spark,
                silver_table
            )

            (
                silver_delta.alias("target")
                .merge(
                    silver_df.alias("source"),
                    "target.order_id = source.order_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        print(f"Batch {batch_id} processed successfully.")

    # Start Streaming Query
    

    query = (
        bronze_stream.writeStream
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
 
    # Keep Streaming Query Running

    query.awaitTermination()

except Exception as e:

    print(f"Streaming Silver Pipeline failed: {e}")

    raise