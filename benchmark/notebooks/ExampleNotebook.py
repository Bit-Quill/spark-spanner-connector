import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, coalesce, current_timestamp, col
from pyspark.sql.types import StringType

num_records = 10000
write_table = "writeTable"
database_id = "test-gsql"
instance_id = "slord-spark-dev"
project_id = "improvingvancouver"
mutations_per_transaction = 5000

spark = SparkSession.builder.appName("DatabricksSpannerTests").getOrCreate()
payload_sample = """
{
  "data": [
    {"id": 1, "value": "a_long_string_to_make_the_file_bigger_01"},
    {"id": 2, "value": "a_long_string_to_make_the_file_bigger_02"},
    {"id": 3, "value": "a_long_string_to_make_the_file_bigger_03"},
    {"id": 4, "value": "a_long_string_to_make_the_file_bigger_04"},
    {"id": 5, "value": "a_long_string_to_make_the_file_bigger_05"},
    {"id": 6, "value": "a_long_string_to_make_the_file_bigger_06"},
    {"id": 7, "value": "a_long_string_to_make_the_file_bigger_07"},
    {"id": 8, "value": "a_long_string_to_make_the_file_bigger_08"},
    {"id": 9, "value": "a_long_string_to_make_the_file_bigger_09"},
    {"id": 10, "value": "a_long_string_to_make_the_file_bigger_10"},
    {"id": 11, "value": "a_long_string_to_make_the_file_bigger_11"},
    {"id": 12, "value": "a_long_string_to_make_the_file_bigger_12"},
    {"id": 13, "value": "a_long_string_to_make_the_file_bigger_13"},
    {"id": 14, "value": "a_long_string_to_make_the_file_bigger_14"},
    {"id": 15, "value": "a_long_string_to_make_the_file_bigger_15"},
    {"id": 16, "value": "a_long_string_to_make_the_file_bigger_16"}
  ]
}
"""

# UDF to generate random UUIDs for the primary key
generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

print("Test: Writing data with new schema...")
df_write = spark \
    .range(num_records) \
    .select(
    coalesce(generate_uuid(), lit("0")).alias("id"),
    lit(payload_sample).alias("json_payload"),
    lit("short_label_constant").alias("short_label"),
    lit(str(uuid.uuid5(uuid.NAMESPACE_DNS, "related_guid_constant"))).alias("related_guid"),
    lit("A").alias("status_flag"),
    current_timestamp().alias("created_at"),
    current_timestamp().alias("updated_at")
)

print("Test: Setting parameters")

target_size_mb = 200
# size per record is arbitrary guess
average_row_size_bytes = 1085
size_in_bytes = average_row_size_bytes * num_records
size_mb = size_in_bytes / (1024 * 1024)
worker_count = 5
core_count = 4
num_partitions = worker_count * core_count * 2

df_partitioned = df_write.repartitionByRange(num_partitions, col("id")).sortWithinPartitions(col("id"))

bytes_per_transaction = 3 * 1024 * 1024
num_write_threads = 4
max_pending_transactions = 5
assume_idempotent_rows = True

print(f"Test: About to df_partitioned.write projectId: {project_id}")

df_partitioned.write \
    .format("com.google.cloud.spark.spanner.Spark35SpannerTableProvider") \
    .option("mutationsPerTransaction", mutations_per_transaction) \
    .option("bytesPerTransaction", str(bytes_per_transaction)) \
    .option("projectId", project_id) \
    .option("instanceId", instance_id) \
    .option("databaseId", database_id) \
    .option("numWriteThreads", num_write_threads) \
    .option("assumeIdempotentRows", str(assume_idempotent_rows)) \
    .option("maxPendingTransactions", str(max_pending_transactions)) \
    .option("table", write_table) \
    .mode("append") \
    .save()

print("Test completed")