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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SpannerWriteBuilder implements WriteBuilder, SupportsTruncate {
  private final LogicalWriteInfo info;
  private final CaseInsensitiveStringMap properties;

  public SpannerWriteBuilder(LogicalWriteInfo info, CaseInsensitiveStringMap properties) {
    this.info = info;
    this.properties = properties;
  }

  @Override
  public BatchWrite buildForBatch() {
    return new SpannerBatchWrite(info, properties);
  }

  @Override
  public WriteBuilder truncate() {
    CaseInsensitiveStringMap opts = new CaseInsensitiveStringMap(this.info.options());
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
    return this;
  }

  /**
   *
   * Deletes all rows from a specified Spanner table using Partitioned DML.
   * * @param dbClient The initialized Cloud Spanner DatabaseClient.
   *
   * @param tableName The name of the table to truncate.
   * @return The total number of rows deleted.
   */
  public static long truncateTable(DatabaseClient dbClient, String tableName) {

    // 1. Construct the DML Statement
    // Spanner requires a WHERE clause for PDML, even if you are deleting everything.
    String sql = "DELETE FROM " + tableName + " WHERE true";
    Statement statement = Statement.of(sql);

    System.out.println("Starting Partitioned DML execution: " + sql);

    try {
      // 2. Execute the Partitioned Update
      // This is a blocking call. The Spanner client will divide the table into
      // partitions and run concurrent background transactions to delete the data.
      long deletedRowCount = dbClient.executePartitionedUpdate(statement);

      System.out.println("Successfully deleted " + deletedRowCount + " rows.");
      return deletedRowCount;

    } catch (SpannerException e) {
      // SpannerExceptions wrap underlying gRPC errors (e.g., DEADLINE_EXCEEDED, PERMISSION_DENIED)
      System.err.println("Failed to execute Partitioned DML on table: " + tableName);
      throw e;
    }
  }
}
