from pyspark.sql.functions import col,trim,upper,current_timestamp
from pyspark.sql.functions import to_date
from common.helpers import load_config
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# Load pipeline configuration and table names

config = load_config(
    "src/config/config.yml"
)

mongo_collection = config["mongodb"]["collection"]
bronze_table = config["tables"]["bronze"]
silver_table = config["tables"]["silver"]

bronze_table = (
    f"{bronze_schema}.{mongo_collection}"
)

silver_table = (
    f"{silver_schema}.{mongo_collection}"
)

try:
    print("Starting Silver Batch Pipeline...")

    # Read data from Bronze Delta Table
    bronze_df = spark.table(bronze_table)

    # Validate data
    record_count = bronze_df.count()
    print(f"Records read from Bronze: {record_count}")

    if record_count == 0:
        raise Exception("No records found in Bronze table.")

    # Remove duplicate records
    window_spec = Window.partitionBy(
        "order_id"
    ).orderBy(
        col("updated_at").desc()
    )

    silver_df = (
        bronze_df
        .withColumn(
            "row_num",
            row_number().over(window_spec)
        )
        .filter(
            col("row_num") == 1
        )
        .drop("row_num")
    )

    # Remove records with null order_id
    silver_df = silver_df.filter(
        col("order_id").isNotNull()
    )

    # Remove records with null customer_name
    silver_df = silver_df.filter(
        col("customer_name").isNotNull()
    )

    # Remove records with null product
    silver_df = silver_df.filter(
        col("product").isNotNull()
    )

    # Remove records with invalid amount
    silver_df = silver_df.filter(
        col("amount") > 0)

    # Remove records with invalid quantity
    silver_df = silver_df.filter(
        col("quantity") > 0
)

    # Standardize customer_name
    silver_df = silver_df.withColumn(
        "customer_name",
        trim(col("customer_name"))
    )

    # Standardize product
    silver_df = silver_df.withColumn(
        "product",
        trim(col("product"))
    )

    silver_df = silver_df.withColumn(
        "order_date",
        to_date("order_date")
    )

    # Standardize status
    silver_df = silver_df.withColumn(
        "status",
        upper(trim(col("status")))
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

    # Add Silver processing timestamp
    silver_df = silver_df.withColumn(
        "silver_processed_time",
        current_timestamp()
    )

    # Calculate pipeline metrics to monitor data quality

    output_count = silver_df.count()

    print(f"Input Records    : {record_count}")
    print(f"Output Records   : {output_count}")
    print(f"Rejected Records : {record_count - output_count}")

    # Print schema
    silver_df.printSchema()

    # Display data
    display(silver_df)

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

    print("Silver Batch Pipeline completed successfully.")

except Exception as e:
    print(f"Silver Batch Pipeline failed: {e}")
    raise