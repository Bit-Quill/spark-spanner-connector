%scala
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, lit, udf}
import org.apache.spark.sql.SaveMode
import java.io.OutputStreamWriter
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID

// Declare widgets to define parameters for the notebook
dbutils.widgets.text("projectId", "", "GCP Project ID")
dbutils.widgets.text("instanceId", "", "Spanner Instance ID")
dbutils.widgets.text("databaseId", "", "Spanner Database ID")
dbutils.widgets.text("writeTable", "", "Spanner Table Name")
dbutils.widgets.text("numRecords", "100000", "Number of records to write")
dbutils.widgets.text("mutationsPerTransaction", "5000", "Mutations per transaction")
dbutils.widgets.text("bytesPerTransaction", (3 * 1024 * 1024L).toString, "Bytes per transaction")
dbutils.widgets.text("numWriteThreads", "4", "Number of write threads")
dbutils.widgets.text("maxPendingTransactions", "5", "Maximum pending transactions")
dbutils.widgets.text("assumeIdempotentRows", "true", "Assume idempotent rows")
dbutils.widgets.text("resultsBucket", "", "GCS Bucket for results")
dbutils.widgets.text("buildSparkVersion", "3.3", "Spark version used for the connector")
dbutils.widgets.text("numPartitions", (5 * 4 * 2).toString, "Number of partitions for the DataFrame")

println("Running Spark Spanner Connector Benchmark...")

private val payloadSample =
  """
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
""".stripMargin

val projectId = dbutils.widgets.get("projectId")
val instanceId = dbutils.widgets.get("instanceId")
val databaseId = dbutils.widgets.get("databaseId")
val writeTable = dbutils.widgets.get("writeTable")
val numRecords = dbutils.widgets.get("numRecords").toLong

// Use get for widgets, then fallback to default if empty
def getOrDefault(name: String, default: String): String = {
  val v = dbutils.widgets.get(name)
  if (v == null || v.isEmpty) default else v
}

val mutationsPerTransaction = getOrDefault("mutationsPerTransaction", "5000").toInt
val bytesPerTransaction = getOrDefault("bytesPerTransaction", (3 * 1024 * 1024L).toString).toLong
val numWriteThreads = getOrDefault("numWriteThreads", "4").toInt
val maxPendingTransactions = getOrDefault("maxPendingTransactions", "5").toInt
val assumeIdempotentRows = getOrDefault("assumeIdempotentRows", "true").toBoolean
val resultsBucket = dbutils.widgets.get("resultsBucket")
val buildSparkVersion = dbutils.widgets.get("buildSparkVersion")

val defaultPartitions = 5 * 4 * 2
val numPartitions = getOrDefault("numPartitions", defaultPartitions.toString).toInt

println(s"projectId: $projectId")
println(s"instanceId: $instanceId")
println(s"databaseId: $databaseId")
println(s"writeTable: $writeTable")
println(s"numRecords: $numRecords")
println(s"numPartitions: $numPartitions")

import spark.implicits._

val generateUUID = udf(() => UUID.randomUUID().toString)

println("Test: Writing data with new schema...")

val dfWrite = spark
  .range(numRecords)
  .select(
    coalesce(generateUUID(), lit("0")).as("id"),
    lit(payloadSample).as("json_payload"),
    lit("short_label_constant").as("short_label"),
    lit(UUID.nameUUIDFromBytes("related_guid_constant".getBytes).toString).as("related_guid"),
    lit("A").as("status_flag"),
    current_timestamp().as("created_at"),
    current_timestamp().as("updated_at")
  )

val averageRowSizeBytes = 1085L
val sizeInBytes = averageRowSizeBytes * numRecords
val sizeMb = sizeInBytes / (1024 * 1024)
println(s"Estimated job size: $sizeMb MB")
println(f"Average row size: $averageRowSizeBytes bytes")
println(s"Number of partitions: $numPartitions")

val dfPartitioned = dfWrite.repartitionByRange(numPartitions, col("id")).sortWithinPartitions(col("id"))

println(s"Beginning write to table '$writeTable' with mutationsPerTransaction: $mutationsPerTransaction")
val startTime = System.nanoTime()
val provider = s"com.google.cloud.spark.spanner.Spark${buildSparkVersion.replace(".", "")}SpannerTableProvider"
dfPartitioned
  .write
  .format(provider)
  .option("mutationsPerTransaction", mutationsPerTransaction)
  .option("bytesPerTransaction", bytesPerTransaction.toString)
  .option("projectId", projectId)
  .option("instanceId", instanceId)
  .option("databaseId", databaseId)
  .option("numWriteThreads", numWriteThreads.toString)
  .option("assumeIdempotentRows", assumeIdempotentRows.toString)
  .option("maxPendingTransactions", maxPendingTransactions.toString)
  .option("table", writeTable)
  .mode(SaveMode.Append)
  .save()
val endTime = System.nanoTime()
val durationSeconds = (endTime - startTime) / 1e9
val throughput = sizeMb / durationSeconds

println("Ending write")
println(s"Write operation took: $durationSeconds seconds")
println(f"Throughput: $throughput%.2f MB/s")

val runId = UUID.randomUUID().toString.take(8)
val runTimestamp = java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now())
val sparkVersion = spark.version
val connectorVersion = "0.0.1-SNAPSHOT"

val resultJson =
  s"""{
    "runId": "$runId",
    "runTimestamp": "$runTimestamp",
    "benchmarkName": "SparkSpannerWriteBenchmark",
    "clusterSparkVersion": "$sparkVersion",
    "connectorVersion": "$connectorVersion",
    "spannerConfig": {
      "projectId": "$projectId",
      "instanceId": "$instanceId",
      "databaseId": "$databaseId",
      "table": "$writeTable"
    },
    "benchmarkParameters": {
      "numRecords": $numRecords,
      "mutationsPerTransaction": $mutationsPerTransaction,
      "bytesPerTransaction": $bytesPerTransaction,
      "numWriteThreads": $numWriteThreads,
      "maxPendingTransactions": $maxPendingTransactions,
      "assumeIdempotentRows": $assumeIdempotentRows
    },
    "performanceMetrics": {
      "durationSeconds": $durationSeconds,
      "throughputMbPerSec": $throughput,
      "totalSizeMb": $sizeMb,
      "recordCount": $numRecords
    },
    "sparkConfig": {
      "numPartitions": $numPartitions
    }
  }"""

val resultsPath = s"gs://$resultsBucket/SparkSpannerWriteBenchmark/${runTimestamp}_$runId.json"
println(s"Writing results to $resultsPath")

val resultsURI = new URI(resultsPath)
val fs = FileSystem.get(resultsURI, spark.sparkContext.hadoopConfiguration)
val outputPath = new Path(resultsPath)
val os = fs.create(outputPath, true)
val writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)
try {
  writer.write(resultJson)
} finally {
  writer.close()
  os.close()
}

println("Finished writing results.")

dbutils.notebook.exit("SUCCESS")