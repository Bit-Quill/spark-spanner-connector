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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.Spanner;
import java.util.Collections;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerCatalog implements TableCatalog, SupportsNamespaces {

  private static final Logger log = LoggerFactory.getLogger(SpannerCatalog.class);
  private String catalogName;
  private CaseInsensitiveStringMap options;
  private Spanner spanner;

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
    this.spanner = createSpanner(options);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    if (namespace.length != 3) {
      log.warn("Invalid namespace for listing tables: {}", String.join(".", namespace));
      return new Identifier[0];
    }
    String projectId = namespace[0];
    String instanceId = namespace[1];
    String databaseId = namespace[2];

    DatabaseClient dbClient =
        spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

    try (ReadContext readContext = dbClient.singleUse()) {
      Dialect dialect = dbClient.getDialect();
      return createSchemaInfo(dialect).listTables(readContext, namespace);
    } catch (Exception e) {
      log.error(
          "Error listing tables in namespace {}: {}", String.join(".", namespace), e.getMessage());
      return new Identifier[0];
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    if (ident.namespace().length != 3) {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "Invalid identifier namespace: " + String.join(".", ident.namespace()));
    }
    if (!tableExists(ident)) {
      throw new NoSuchTableException(ident);
    }
    return factorySpannerTable(ident);
  }

  protected Table factorySpannerTable(Identifier ident) {
    return new SpannerTable(
        ident.namespace()[0],
        ident.namespace()[1],
        ident.namespace()[2],
        ident.name(),
        this.options);
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException {
    // Check if table exists
    if (tableExists(ident)) {
      throw new TableAlreadyExistsException(ident);
    }
    // TODO: Implement actual table creation logic using Spanner DDL
    // For now, just return a SpannerTable
    return factorySpannerTable(ident);
  }

  @Override
  public boolean tableExists(Identifier ident) {
    if (ident.namespace().length != 3) {
      return false; // Invalid namespace, so table cannot exist here
    }
    String projectId = ident.namespace()[0];
    String instanceId = ident.namespace()[1];
    String databaseId = ident.namespace()[2];
    String tableName = ident.name();

    DatabaseClient dbClient =
        spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

    try (ReadContext readContext = dbClient.singleUse()) {
      return createSchemaInfo(dbClient.getDialect()).tableExists(readContext, tableName);
    } catch (Exception e) {
      log.error(
          "Error checking table existence {}.{}: {}",
          String.join(".", ident.namespace()),
          tableName,
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
    throw new UnsupportedOperationException("DROP TABLE is not supported for SpannerCatalog");
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw new UnsupportedOperationException("RENAME TABLE is not supported for SpannerCatalog");
  }

  // SupportsNamespaces methods
  @Override
  public String[][] listNamespaces() {
    // List projects (top-level)
    // This would require Google Cloud ResourceManager API or similar.
    // For now, return an empty array or a dummy project if needed.
    return new String[0][];
  }

  @Override
  public String[][] listNamespaces(String[] namespace) {
    // Depending on the length of namespace, list instances or databases
    // For simplicity, we'll assume a max depth of [projectId, instanceId, databaseId]
    if (namespace.length == 0) {
      // List projectIds available from options
      if (options.containsKey("projectId")) {
        return new String[][] {{options.get("projectId")}};
      }
      return new String[0][];
    } else if (namespace.length == 1) { // Listing instances in a project
      // For now, return instances within the specified projectId from options
      if (options.containsKey("projectId") && options.get("projectId").equals(namespace[0])) {
        if (options.containsKey("instanceId")) {
          return new String[][] {{options.get("projectId"), options.get("instanceId")}};
        }
      }
      return new String[0][];
    } else if (namespace.length == 2) { // Listing databases in an instance
      // For now, return databases within the specified instanceId from options
      if (options.containsKey("projectId")
          && options.get("projectId").equals(namespace[0])
          && options.containsKey("instanceId")
          && options.get("instanceId").equals(namespace[1])) {
        if (options.containsKey("databaseId")) {
          return new String[][] {
            {options.get("projectId"), options.get("instanceId"), options.get("databaseId")}
          };
        }
      }
      return new String[0][];
    }
    return new String[0][];
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    if (namespace.length == 0) {
      return true; // Root namespace always exists
    } else if (namespace.length == 1) { // projectId
      return options.containsKey("projectId") && options.get("projectId").equals(namespace[0]);
    } else if (namespace.length == 2) { // projectId, instanceId
      return options.containsKey("projectId")
          && options.get("projectId").equals(namespace[0])
          && options.containsKey("instanceId")
          && options.get("instanceId").equals(namespace[1]);
    } else if (namespace.length == 3) { // projectId, instanceId, databaseId
      return options.containsKey("projectId")
          && options.get("projectId").equals(namespace[0])
          && options.containsKey("instanceId")
          && options.get("instanceId").equals(namespace[1])
          && options.containsKey("databaseId")
          && options.get("databaseId").equals(namespace[2]);
    }
    return false;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    if (namespaceExists(namespace)) {
      return Collections.emptyMap(); // For simplicity, return empty metadata
    }
    throw new NoSuchNamespaceException(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) {
    throw new UnsupportedOperationException("CREATE NAMESPACE is not supported for SpannerCatalog");
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) {
    throw new UnsupportedOperationException("ALTER NAMESPACE is not supported for SpannerCatalog");
  }

  @Override
  public boolean dropNamespace(String[] namespace) {
    throw new UnsupportedOperationException("DROP NAMESPACE is not supported for SpannerCatalog");
  }
}
