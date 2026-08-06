from pyspark.sql.functions import current_timestamp, lit
from common.helpers import load_config


try:

    # Load Configuration
    config = load_config(
        "src/config/config.yml"
    )

    mongo_database = config["mongodb"]["database"]
    mongo_collection = config["mongodb"]["collection"]

    bronze_schema = config["schemas"]["bronze"]

    checkpoint_location = config["streaming"]["checkpoint_location"]
    trigger_interval = config["streaming"]["trigger_interval"]

    mongo_uri = dbutils.secrets.get(
        "mongodb-scope",
        "mongo-uri"
    )

    bronze_table = (
        f"{bronze_schema}.{mongo_collection}"
    )

    # Read MongoDB Change Stream
    stream_df = (
        spark.readStream
            .format("mongodb")
            .option(
                "spark.mongodb.read.connection.uri",
                mongo_uri
            )
            .option(
                "change.stream.startup.mode",
                "latest"
            )
            .option(
                "spark.mongodb.read.database",
                mongo_database
            )
            .option(
                "spark.mongodb.read.collection",
                mongo_collection
            )
            .option(
                "change.stream.publish.full.document.only",
                "true"
            )
            .load()
    )

    # Add Bronze Metadata
    bronze_df = (
        stream_df
        .withColumn(
            "ingestion_time",
            current_timestamp()
        )
        .withColumn(
            "source_system",
            lit("mongodb")
        )
    )

    # Write Streaming Data to Bronze Delta
    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            checkpoint_location
        )
        .trigger(
            processingTime=trigger_interval
        )
        .toTable(
            bronze_table
        )
    )

    # Keep Streaming Running
    query.awaitTermination()

except Exception as e:

    print(f"Streaming Bronze Pipeline failed: {e}")

    raise