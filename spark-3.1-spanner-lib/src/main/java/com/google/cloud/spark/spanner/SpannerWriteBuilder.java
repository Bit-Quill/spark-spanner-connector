// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spark.spanner;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.Connection;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerWriteBuilder implements WriteBuilder, SupportsTruncate {

  private static final Logger log = LoggerFactory.getLogger(SpannerWriteBuilder.class);
  private final LogicalWriteInfo info;
  private final StructType schema;

  public SpannerWriteBuilder(LogicalWriteInfo info) {
    this.info = info;
    this.schema = info.schema();
  }

  @Override
  public BatchWrite buildForBatch() {
    return new SpannerBatchWrite(info);
  }

  @Override
  public WriteBuilder truncate() {
    CaseInsensitiveStringMap opts = new CaseInsensitiveStringMap(this.info.options());
    String overwriteMode = opts.getOrDefault("overwriteMode", "truncate");

    if (overwriteMode.equalsIgnoreCase("recreate")) {
      recreateTable(opts);
    } else {
      truncateTable(opts);
    }

    return this;
  }

  private void recreateTable(CaseInsensitiveStringMap opts) {
    String instanceId = SpannerUtils.getRequiredOption(opts, "instanceId");
    String databaseId = SpannerUtils.getRequiredOption(opts, "databaseId");
    String tableName = SpannerUtils.getRequiredOption(opts, "table");

    try (Spanner spanner = SpannerUtils.buildSpannerOptions(opts).getService()) {
      DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
      Dialect dialect;
      try (Connection conn = SpannerUtils.connectionFromProperties(opts.asCaseSensitiveMap())) {
        dialect = conn.getDialect();
      }
      // TODO Re-use drop table and create table code from SpannerCatalog
      // Drop the table.
      // Drop the table.
      String quote = dialect == Dialect.POSTGRESQL ? "\"" : "`";
      dbAdminClient
          .updateDatabaseDdl(
              instanceId, databaseId, Arrays.asList("DROP TABLE " + quote + tableName + quote), null)
          .get();

      // Create the table.
      SpannerSchemaConverter converter = new SpannerSchemaConverter(dialect);
      String createTableDdl = converter.sparkSchemaToSpannerDDL(this.schema, tableName);
      dbAdminClient
          .updateDatabaseDdl(instanceId, databaseId, Arrays.asList(createTableDdl), null)
          .get();

    } catch (InterruptedException | ExecutionException e) {
      throw new SpannerConnectorException(
          SpannerErrorCode.DDL_EXCEPTION, "Error recreating table " + tableName, e);
    }
  }

  private void truncateTable(CaseInsensitiveStringMap opts) {
    String projectId = SpannerUtils.getRequiredOption(opts, "projectId");
    String instanceId = SpannerUtils.getRequiredOption(opts, "instanceId");
    String databaseId = SpannerUtils.getRequiredOption(opts, "databaseId");
    String tableName = SpannerUtils.getRequiredOption(opts, "table");

    try (Spanner spanner = SpannerUtils.buildSpannerOptions(opts).getService()) {
      DatabaseClient dbClient =
          spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
      truncateTable(dbClient, tableName);
    } catch (Exception e) {
      throw new SpannerConnectorException(
          SpannerErrorCode.DDL_EXCEPTION, "Error truncating table " + tableName, e);
    }
  }

  private long truncateTable(DatabaseClient dbClient, String tableName) {

    // 1. Construct the DML Statement
    // Spanner requires a WHERE clause for PDML, even if you are deleting everything.
    String sql = "DELETE FROM `" + tableName.replace("`", "``") + "` WHERE true";
    Statement statement = Statement.of(sql);

    try {
      // 2. Execute the Partitioned Update
      // This is a blocking call. The Spanner client will divide the table into
      // partitions and run concurrent background transactions to delete the data.
      long deletedRowCount = dbClient.executePartitionedUpdate(statement);
      log.info("Successfully deleted " + deletedRowCount + " rows.");
      return deletedRowCount;

    } catch (SpannerException e) {
      // SpannerExceptions wrap underlying gRPC errors (e.g., DEADLINE_EXCEEDED, PERMISSION_DENIED)
      log.error("Failed to execute Partitioned DML on table: " + tableName, e);
      throw e;
    }
  }
}
