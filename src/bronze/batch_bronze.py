from pyspark.sql.functions import current_timestamp, lit, col, max, to_timestamp
from common.helpers import load_config


config = load_config(
    "src/config/config.yml"
)

mongo_database = config["mongodb"]["database"]
mongo_collection = config["mongodb"]["collection"]
watermark_column = config["mongodb"]["watermark_column"]

mongo_uri = dbutils.secrets.get(
    "mongodb-scope",
    "mongo-uri"
)
bronze_schema = config["schemas"]["bronze"]

watermark_table = (
    f"{bronze_schema}.watermark"
)

bronze_table = (
    f"{bronze_schema}.{mongo_collection}"
)


# Read last processed timestamp
watermark_df = spark.table(
    watermark_table
)

last_timestamp = (
    watermark_df
    .filter(col("table_name") == mongo_collection)
    .select("last_updated_timestamp")
    .collect()[0][0]
)

print(f"Last processed timestamp: {last_timestamp}")

# Read data from MongoDB
mongo_df = (
    spark.read
    .format("mongodb")
    .option(
        "spark.mongodb.read.connection.uri",
        mongo_uri
    )
    .option(
        "spark.mongodb.read.database",
        mongo_database
    )
    .option(
        "spark.mongodb.read.collection",
        mongo_collection
    )
    .load()
)
# Convert watermark column to timestamp

mongo_df = mongo_df.withColumn(
    watermark_column,
    to_timestamp(col(watermark_column))
)

# Incremental filter
incremental_df = mongo_df.filter(
    col(watermark_column) > lit(last_timestamp)
)


# Add Bronze metadata columns
bronze_df = (
    incremental_df
    .withColumn(
        "ingestion_time",
        current_timestamp()
    )
    .withColumn(
        "source_system",
        lit("mongodb")
    )
)

# Validate the data
record_count = bronze_df.count()
print(f"Incremental records: {record_count}")

if record_count == 0:
    print("No new records found. Pipeline completed.")

else:

    # Validate data
    bronze_df.printSchema()

    # Display Bronze data
    display(bronze_df)

    # Write to Bronze Delta Table
    bronze_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(
            bronze_table
        )

    # Update watermark
    new_timestamp = (
        incremental_df
        .select(max(watermark_column))
        .collect()[0][0]
    )

    spark.sql(f"""
        UPDATE {watermark_table}
        SET last_updated_timestamp = '{new_timestamp}'
        WHERE table_name = '{mongo_collection}'
    """)

    print("Data successfully written to Bronze Delta table.")