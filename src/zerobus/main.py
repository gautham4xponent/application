import time
import traceback
from datetime import datetime, timezone
import argparse

from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import (
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
)
from device_events import generate_events

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("table", "")
dbutils.widgets.text("server_endpoint", "")
dbutils.widgets.text("total_records", "")
dbutils.widgets.text("batch_size")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
server_endpoint = dbutils.widgets.get("server_endpoint")

total_records = int(dbutils.widgets.get("total_records"))
batch_size = int(dbutils.widgets.get("batch_size"))

SERVER_ENDPOINT = args.server_endpoint
DATABRICKS_WORKSPACE_URL = args.workspace_url

TABLE_NAME = f"{args.catalog}.{args.schema}.{args.table}"

TOTAL_RECORDS = args.total_records
BATCH_SIZE = args.batch_size
TOTAL_BATCHES = TOTAL_RECORDS // BATCH_SIZE

CLIENT_ID = dbutils.secrets.get(
    scope="zerobus-scope",
    key="client-id"
)

CLIENT_SECRET = dbutils.secrets.get(
    scope="zerobus-scope",
    key="client-secret"
)

sdk = ZerobusSdk(
    SERVER_ENDPOINT,
    DATABRICKS_WORKSPACE_URL
)

table_properties = TableProperties(TABLE_NAME)

options = StreamConfigurationOptions(
    record_type=RecordType.JSON
)

stream_start_time = datetime.now(timezone.utc)

stream_start_time_str = stream_start_time.isoformat(
    timespec="milliseconds"
)

stream = sdk.create_stream(
    CLIENT_ID,
    CLIENT_SECRET,
    table_properties,
    options
)

batch_times = []
record_id = 1

try:

    overall_start = time.perf_counter()

    for batch in range(1, TOTAL_BATCHES + 1):

        print(f"\nSubmitting Batch {batch}")

        batch_start = time.perf_counter()

        last_offset = None

        for _ in range(BATCH_SIZE):
            payload = generate_events(record_id, stream_start_time_str)
            last_offset = stream.ingest_record_offset(payload)
            record_id += 1

        print(f"Waiting for Batch {batch} acknowledgement...")
        stream.wait_for_offset(last_offset)

        batch_time = time.perf_counter() - batch_start
        batch_times.append(batch_time)

        print(f"Batch {batch} completed in {batch_time:.2f} sec")

    overall_end = time.perf_counter()

    total_time = overall_end - overall_start
    throughput = TOTAL_RECORDS / total_time

    print("\n========== PERFORMANCE RESULTS ==========")
    print(f"Total Records        : {TOTAL_RECORDS:,}")
    print(f"Batch Size           : {BATCH_SIZE:,}")
    print(f"Total Batches        : {TOTAL_BATCHES}")

    print("\nBatch Submission Times")

    for i, t in enumerate(batch_times, start=1):
        print(f"Batch {i}: {t:.2f} sec")

    print(f"\nTotal Execution Time : {total_time:.2f} sec")
    print(f"Throughput           : {throughput:.2f} records/sec")
    print("=========================================")

except Exception:
    traceback.print_exc()

finally:
    stream.close()
