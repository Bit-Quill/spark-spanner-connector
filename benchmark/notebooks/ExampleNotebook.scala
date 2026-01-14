import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util.UUID

val numRecords = 1
val writeTable = "writeTable"
val databaseId = "test-gsql"
val instanceId = "slord-spark-dev"
val projectId = "improvingvancouver"
val mutationsPerTransaction = 5000

val spark = SparkSession.builder().appName("DatabricksSpannerTests").getOrCreate()
val payload_sample =
  """
    |{
    |  "data": [
    |    {"id": 1, "value": "a_long_string_to_make_the_file_bigger_01"},
    |    {"id": 2, "value": "a_long_string_to_make_the_file_bigger_02"},
    |    {"id": 3, "value": "a_long_string_to_make_the_file_bigger_03"},
    |    {"id": 4, "value": "a_long_string_to_make_the_file_bigger_04"},
    |    {"id": 5, "value": "a_long_string_to_make_the_file_bigger_05"},
    |    {"id": 6, "value": "a_long_string_to_make_the_file_bigger_06"},
    |    {"id": 7, "value": "a_long_string_to_make_the_file_bigger_07"},
    |    {"id": 8, "value": "a_long_string_to_make_the_file_bigger_08"},
    |    {"id": 9, "value": "a_long_string_to_make_the_file_bigger_09"},
    |    {"id": 10, "value": "a_long_string_to_make_the_file_bigger_10"},
    |    {"id": 11, "value": "a_long_string_to_make_the_file_bigger_11"},
    |    {"id": 12, "value": "a_long_string_to_make_the_file_bigger_12"},
    |    {"id": 13, "value": "a_long_string_to_make_the_file_bigger_13"},
    |    {"id": 14, "value": "a_long_string_to_make_the_file_bigger_14"},
    |    {"id": 15, "value": "a_long_string_to_make_the_file_bigger_15"},
    |    {"id": 16, "value": "a_long_string_to_make_the_file_bigger_16"}
    |  ]
    |}
    |""".stripMargin

// UDF to generate random UUIDs for the primary key
val generateUUID = udf(() => UUID.randomUUID().toString)
println("Test: Writing data with new schema...")
val dfWrite = spark
  .range(numRecords)
  .select(
    coalesce(generateUUID(), lit("0")).as("id"), // Only generate UUID for ID
    lit(payload_sample).as("json_payload"), // Use JSON from file
    lit("short_label_constant").as("short_label"),                     // Constant string
    lit(UUID.nameUUIDFromBytes("related_guid_constant".getBytes).toString).as("related_guid"), // Constant GUID
    lit("A").as("status_flag"),                                       // Constant single char
    current_timestamp().as("created_at"),                             // Current timestamp
    current_timestamp().as("updated_at")                              // Current timestamp
  )

println("Test: Setting parameters")

val targetSizeMb = 200
// size per record is arbitrary guess
val averageRowSizeBytes = 1085L
val sizeInBytes = averageRowSizeBytes * numRecords
val sizeMb = sizeInBytes / (1024 * 1024)
val workerCount = 5;
val coreCount = 4;
val numPartitions = workerCount * coreCount * 2;

val dfPartitioned = dfWrite.repartitionByRange(numPartitions.toInt, col("id")).sortWithinPartitions(col("id"))

val bytesPerTransaction = 3*1024*1024
val numWriteThreads = 4
val maxPendingTransactions = 5
val assumeIdempotentRows = true

println(s"Test: About to dfPartitioned.write projectId: $projectId")

dfPartitioned
  .write
  .format("com.google.cloud.spark.spanner.Spark35SpannerTableProvider")
  .option("mutationsPerTransaction", mutationsPerTransaction)
  .option("bytesPerTransaction", bytesPerTransaction.toString)
  .option("projectId", projectId)
  .option("instanceId", instanceId)
  .option("databaseId", databaseId)
  .option("numWriteThreads", numWriteThreads)
  .option("assumeIdempotentRows", assumeIdempotentRows.toString)
  .option("maxPendingTransactions", maxPendingTransactions.toString)
  .option("table", writeTable)
  .mode(SaveMode.Append)
  .save()

println("Test completed")