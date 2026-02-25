// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spark.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spark.spanner.graph.SpannerGraphBuilder;
import com.google.common.base.Verify;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerCatalog implements TableCatalog {
  public static Metadata PRIMARY_KEY_METADATA =
      new MetadataBuilder().putBoolean(SpannerUtils.PRIMARY_KEY_TAG, true).build();
  private static final Logger log = LoggerFactory.getLogger(SpannerCatalog.class);
  private String catalogName;
  private CaseInsensitiveStringMap options;
  private Spanner spanner;
  private String projectId;
  private String instanceId;
  private String databaseId;

  // For testing purposes.
  protected Spanner createSpanner(CaseInsensitiveStringMap options) {
    return SpannerUtils.buildSpannerOptions(options).getService();
  }

  // For testing purposes.
  protected SpannerInformationSchema createSchemaInfo(Dialect dialect) {
    return SpannerInformationSchema.create(dialect);
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.options = options;
    this.projectId = SpannerUtils.getRequiredOption(options, "projectId");
    this.instanceId = SpannerUtils.getRequiredOption(options, "instanceId");
    this.databaseId = SpannerUtils.getRequiredOption(options, "databaseId");
    this.spanner = createSpanner(options);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    if (namespace.length > 0) {
      log.warn("Invalid namespace for listing tables: {}", String.join(".", namespace));
      return new Identifier[0];
    }

    DatabaseClient dbClient = getDatabaseClient();

    try (ReadContext readContext = dbClient.readOnlyTransaction()) {
      Dialect dialect = dbClient.getDialect();
      SpannerInformationSchema schemaInfo = createSchemaInfo(dialect);
      Identifier[] physicalTables = schemaInfo.listTables(readContext, new String[] {});

      List<Identifier> allTables = new ArrayList<>(Arrays.asList(physicalTables));
      for (String graphName : schemaInfo.listGraphs(readContext)) {
        String[] graphNamespace = asGraphNamespace(graphName);
        allTables.add(Identifier.of(graphNamespace, "node"));
        allTables.add(Identifier.of(graphNamespace, "edge"));
      }
      return allTables.toArray(new Identifier[0]);
    } catch (Exception e) {
      log.error(
          "Error listing tables in namespace {}: {}", String.join(".", namespace), e.getMessage());
      return new Identifier[0];
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    DatabaseClient dbClient = getDatabaseClient();

    try (ReadContext readContext = dbClient.singleUse()) {
      Dialect dialect = dbClient.getDialect();
      String schemaName = SpannerInformationSchema.defaultSchema(dialect);
      SpannerInformationSchema schemaInfo = createSchemaInfo(dialect);
      IdentifierResolution resolution = schemaInfo.resolveIdentifier(ident);

      boolean exists =
          resolution.graph
              ? schemaInfo.graphExists(readContext, resolution.objectName)
              : schemaInfo.tableExists(readContext, resolution.objectName);
      if (!exists) {
        throw new NoSuchTableException(ident);
      }

      if (resolution.graph) {
        return factorySpannerGraph(schemaName, resolution.objectName, resolution.graphType);
      }
      return factorySpannerTable(resolution.objectName);
    }
  }

  protected Table factorySpannerTable(String tableName) {
    Verify.verifyNotNull(projectId, "projectId");
    Verify.verifyNotNull(instanceId, "instanceId");
    Verify.verifyNotNull(databaseId, "databaseId");
    Verify.verifyNotNull(tableName, "table");

    Map<String, String> tableOptions = new HashMap<>();
    tableOptions.put("table", tableName);

    return new SpannerTable(
        projectId,
        instanceId,
        databaseId,
        tableName,
        withCatalogConnectionOptions(tableOptions),
        null);
  }

  protected Table factorySpannerGraph(String schemaName, String graphName, String graphType) {
    Verify.verifyNotNull(projectId, "projectId");
    Verify.verifyNotNull(instanceId, "instanceId");
    Verify.verifyNotNull(databaseId, "databaseId");
    Verify.verifyNotNull(graphName, "graphName");
    Verify.verifyNotNull(graphType, "graphType");

    Map<String, String> graphOptions = new HashMap<>();
    graphOptions.put("graph", graphName);
    graphOptions.put("type", graphType);
    return SpannerGraphBuilder.build(withCatalogConnectionOptions(graphOptions));
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException {
    DatabaseClient dbClient = getDatabaseClient();
    Dialect dialect = dbClient.getDialect();
    SpannerInformationSchema schemaInfo = createSchemaInfo(dialect);
    IdentifierResolution resolution = schemaInfo.resolveIdentifier(ident);

    if (resolution.graph) {
      throw new SpannerConnectorException(
          SpannerErrorCode.UNSUPPORTED,
          "CREATE TABLE does not support graph identifiers: " + ident);
    }

    if (tableExists(ident)) {
      throw new TableAlreadyExistsException(ident);
    }

    String ddl = toDdl(ident, schema, dialect);
    DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        dbAdminClient.updateDatabaseDdl(
            instanceId, databaseId, Collections.singletonList(ddl), null);

    try {
      op.get();
    } catch (ExecutionException | InterruptedException e) {
      throw new SpannerConnectorException(
          SpannerErrorCode.DDL_EXCEPTION,
          "Exception while creating table " + ident.name() + ": " + e.getMessage(),
          e);
    }

    return factorySpannerTable(resolution.objectName);
  }

  public static String toDdl(Identifier ident, StructType schema, Dialect dialect) {
    StringBuilder ddl = new StringBuilder();
    String tableName = ident.name();
    if (dialect == Dialect.POSTGRESQL) {
      tableName = tableName.toLowerCase();
    }
    ddl.append("CREATE TABLE ").append(tableName).append(" (");
    for (StructField field : schema.fields()) {
      ddl.append(field.name()).append(" ").append(sparkTypeToSpannerType(field, dialect));
      if (!field.nullable()) {
        ddl.append(" NOT NULL");
      }
      ddl.append(", ");
    }

    List<String> primaryKeys =
        Arrays.stream(schema.fields())
            .filter(
                f ->
                    f.metadata().contains(SpannerUtils.PRIMARY_KEY_TAG)
                        && f.metadata().getBoolean(SpannerUtils.PRIMARY_KEY_TAG))
            .map(StructField::name)
            .collect(Collectors.toList());

    if (primaryKeys.isEmpty()) {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "No primary key found for table "
              + ident.name()
              + ". Please specify at least one primary key column.");
    }

    ddl.append("PRIMARY KEY (").append(String.join(", ", primaryKeys)).append(")");
    ddl.append(")");
    return ddl.toString();
  }

  private static String sparkTypeToSpannerType(StructField field, Dialect dialect) {
    if (dialect == Dialect.POSTGRESQL) {
      if (field.dataType().equals(DataTypes.LongType)) {
        return "bigint";
      }
      if (field.dataType().equals(DataTypes.StringType)) {
        return "varchar";
      }
      if (field.dataType().equals(DataTypes.BooleanType)) {
        return "boolean";
      }
      if (field.dataType().equals(DataTypes.DoubleType)) {
        return "float8";
      }
      if (field.dataType().equals(DataTypes.BinaryType)) {
        return "bytea";
      }
      if (field.dataType().equals(DataTypes.TimestampType)) {
        return "timestamptz";
      }
      if (field.dataType().equals(DataTypes.DateType)) {
        return "date";
      }
      if (field.dataType() instanceof org.apache.spark.sql.types.DecimalType) {
        return "numeric";
      }
    }

    // GoogleSQL types
    if (field.dataType().equals(DataTypes.LongType)) {
      return "INT64";
    }
    if (field.dataType().equals(DataTypes.StringType)) {
      return "STRING(MAX)";
    }
    if (field.dataType().equals(DataTypes.BooleanType)) {
      return "BOOL";
    }
    if (field.dataType().equals(DataTypes.DoubleType)) {
      return "FLOAT64";
    }
    if (field.dataType().equals(DataTypes.BinaryType)) {
      return "BYTES(MAX)";
    }
    if (field.dataType().equals(DataTypes.TimestampType)) {
      return "TIMESTAMP";
    }
    if (field.dataType().equals(DataTypes.DateType)) {
      return "DATE";
    }
    if (field.dataType() instanceof org.apache.spark.sql.types.DecimalType) {
      return "NUMERIC";
    }

    throw new SpannerConnectorException(
        SpannerErrorCode.UNSUPPORTED_DATATYPE,
        "Unsupported data type in CREATE TABLE: " + field.dataType());
  }

  @Override
  public boolean tableExists(Identifier ident) {
    DatabaseClient dbClient = getDatabaseClient();
    try (ReadContext readContext = dbClient.singleUse()) {
      Dialect dialect = dbClient.getDialect();
      SpannerInformationSchema schemaInfo = createSchemaInfo(dialect);
      IdentifierResolution resolution = schemaInfo.resolveIdentifier(ident);

      if (resolution.graph) {
        return schemaInfo.graphExists(readContext, resolution.objectName);
      }
      return schemaInfo.tableExists(readContext, resolution.objectName);
    } catch (Exception e) {
      log.error(
          "Error checking object existence {}.{}: {}",
          String.join(".", ident.namespace()),
          ident.name(),
          e.getMessage());
      return false;
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) {
    throw new UnsupportedOperationException("ALTER TABLE is not supported for SpannerCatalog");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    DatabaseClient dbClient = getDatabaseClient();
    Dialect dialect = dbClient.getDialect();
    SpannerInformationSchema schemaInfo = createSchemaInfo(dialect);
    IdentifierResolution resolution = schemaInfo.resolveIdentifier(ident);

    if (resolution.graph) {
      throw new SpannerConnectorException(
          SpannerErrorCode.UNSUPPORTED, "DROP TABLE does not support graph identifiers: " + ident);
    }

    if (!tableExists(ident)) {
      return false;
    }

    String ddl = "DROP TABLE " + resolution.objectName;

    DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        dbAdminClient.updateDatabaseDdl(
            instanceId, databaseId, Collections.singletonList(ddl), null);

    try {
      op.get();
      return true;
    } catch (ExecutionException | InterruptedException e) {
      throw new SpannerConnectorException(
          SpannerErrorCode.DDL_EXCEPTION,
          "Exception while dropping table " + ident.name() + ": " + e.getMessage(),
          e);
    }
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw new UnsupportedOperationException("RENAME TABLE is not supported for SpannerCatalog");
  }

  private DatabaseClient getDatabaseClient() {
    return spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
  }

  private static String[] asGraphNamespace(String graphName) {
    return new String[] {"graph", graphName};
  }

  private static String tableRef(Identifier ident, Dialect dialect) {
    if (ident.namespace().length == 0) {
      return ident.name();
    }
    throw new SpannerConnectorException(
        SpannerErrorCode.INVALID_ARGUMENT,
        "Invalid table identifier namespace: " + String.join(".", ident.namespace()));
  }

  private CaseInsensitiveStringMap withCatalogConnectionOptions(
      Map<String, String> additionalOptions) {
    Map<String, String> combined = new HashMap<>(options.asCaseSensitiveMap());
    combined.putAll(options);
    combined.put("projectId", projectId);
    combined.put("instanceId", instanceId);
    combined.put("databaseId", databaseId);
    combined.putAll(additionalOptions);
    return new CaseInsensitiveStringMap(combined);
  }
}
