// Copyright 2023 Google LLC
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

import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spark.spanner.graph.SpannerGraphBuilder;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public abstract class SparkSpannerTableProviderBase
    implements DataSourceRegister, TableProvider, CreatableRelationProvider {

  /*
   * Infers the schema of the table identified by the given options.
   */
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return getTable(options).schema();
  }

  /*
   * Returns a Table instance with the specified table schema,
   * partitioning and properties to perform a read or write.
   */
  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    final CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(properties);
    boolean enablePartialRowUpdates =
        Boolean.parseBoolean(options.getOrDefault("enablePartialRowUpdates", "false"));

    boolean hasTable = options.containsKey("table");
    boolean hasGraph = options.containsKey("graph");
    if (hasTable && !hasGraph) {
      if (enablePartialRowUpdates) {
        return new SpannerTable(options, schema);
      } else {
        return new SpannerTable(options);
      }
    } else if (!hasTable && hasGraph) {
      return SpannerGraphBuilder.build(options);
    } else {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "properties must contain one of \"table\" or \"graph\"");
    }
  }

  /*
   * Returns true if the source has the ability of
   * accepting external table metadata when getting tables.
   */
  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  /*
   * Implements DataSourceRegister.shortName(). This method allows Spark to match
   * the DataSource when spark.read(...).format("spanner") is invoked.
   */
  @Override
  public String shortName() {
    return "cloud-spanner";
  }

  private Table getTable(Map<String, String> properties) {
    boolean hasTable = properties.containsKey("table");
    boolean hasGraph = properties.containsKey("graph");
    if (hasTable && !hasGraph) {
      return new SpannerTable(properties);
    } else if (!hasTable && hasGraph) {
      return SpannerGraphBuilder.build(properties);
    } else {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "properties must contain one of \"table\" or \"graph\"");
    }
  }

  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext,
      SaveMode mode,
      scala.collection.immutable.Map<String, String> parameters,
      Dataset<Row> data) {
    Map<String, String> properties = scala.collection.JavaConverters.mapAsJavaMap(parameters);
    String tableName = SpannerUtils.getRequiredOption(properties, "table");

    try (Connection connection = SpannerUtils.connectionFromProperties(properties)) {
      boolean tableExists = SpannerUtils.tableExists(connection, tableName);

      if (tableExists) {
        switch (mode) {
          case Ignore:
            // Table exists, do nothing.
            break;
          case ErrorIfExists:
            // Table exists, throw exception.
            throw new SpannerConnectorException(
                SpannerErrorCode.TABLE_ALREADY_EXISTS, "Table '" + tableName + "' already exists.");
          case Overwrite:
            // Table exists, truncate it.
            SpannerUtils.truncateTable(connection, tableName);
            SpannerUtils.writeData(properties, data);
            break;
          case Append:
            // Table exists, append data.
            SpannerUtils.writeData(properties, data);
            break;
        }
      } else {
        // Table does not exist, create it and write data for all modes except Ignore (when table
        // exists).
        String primaryKey = SpannerUtils.getRequiredOption(properties, "primaryKey");

        String projectId = SpannerUtils.getRequiredOption(properties, "projectId");
        String instanceId = SpannerUtils.getRequiredOption(properties, "instanceId");
        String databaseId = SpannerUtils.getRequiredOption(properties, "databaseId");
        SpannerUtils.createTable(
            connection, projectId, instanceId, databaseId, tableName, data.schema(), primaryKey);
        SpannerUtils.writeData(properties, data);
      }
    }

    return new SpannerRelation(sqlContext, properties, data.schema());
  }
}
