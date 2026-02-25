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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.Spanner;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Parameterized.class)
public class SpannerCatalogTest {

  @Parameters
  public static Collection<Dialect> dialects() {
    return Arrays.asList(Dialect.GOOGLE_STANDARD_SQL, Dialect.POSTGRESQL);
  }

  private SpannerCatalog catalog;
  private final Dialect dialect;

  @Mock private Spanner spanner;
  @Mock private DatabaseClient dbClient;
  @Mock private SpannerInformationSchema spannerInfoSchema;
  @Mock private DatabaseAdminClient dbAdminClient;
  @Mock private OperationFuture<Void, UpdateDatabaseDdlMetadata> ddlFuture;

  public SpannerCatalogTest(Dialect dialect) {
    this.dialect = dialect;
  }

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);

    catalog =
        new SpannerCatalog() {
          @Override
          protected Spanner createSpanner(CaseInsensitiveStringMap options) {
            return spanner;
          }

          @Override
          protected SpannerInformationSchema createSchemaInfo(Dialect dialect) {
            return spannerInfoSchema;
          }

          @Override
          protected Table factorySpannerTable(String tableName) {
            SpannerTable mockSpannerTable = mock(SpannerTable.class);
            when(mockSpannerTable.name()).thenReturn(tableName);
            return mockSpannerTable;
          }

          @Override
          protected Table factorySpannerGraph(
              String schemaName, String graphName, String graphType) {
            Table mockGraph = mock(Table.class);
            when(mockGraph.name()).thenReturn(graphName + ":" + graphType);
            return mockGraph;
          }
        };

    Map<String, String> opts = new HashMap<>();
    opts.put("projectId", "p");
    opts.put("instanceId", "i");
    opts.put("databaseId", "d");
    opts.put("emulatorHost", "localhost:9010");
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(opts);
    catalog.initialize("test-catalog", options);

    when(spannerInfoSchema.resolveIdentifier(any()))
        .thenAnswer(
            (invocation) -> {
              Identifier ident = invocation.getArgument(0);
              SpannerInformationSchema realImpl = SpannerInformationSchema.create(dialect);
              return realImpl.resolveIdentifier(ident);
            });
    when(dbClient.getDialect()).thenReturn(dialect);
    ReadOnlyTransaction mockReadContext = mock(ReadOnlyTransaction.class);
    when(dbClient.readOnlyTransaction()).thenReturn(mockReadContext);
    when(dbClient.singleUse()).thenReturn(mockReadContext);
    when(spanner.getDatabaseAdminClient()).thenReturn(dbAdminClient);
    when(spanner.getDatabaseClient(any(DatabaseId.class))).thenReturn(dbClient);
    when(dbAdminClient.updateDatabaseDdl(anyString(), anyString(), anyList(), isNull()))
        .thenReturn(ddlFuture);
    when(ddlFuture.get()).thenReturn(null);
  }

  @Test
  public void testName() {
    assertEquals("test-catalog", catalog.name());
  }

  @Test
  public void listTablesShouldReturnTables() {
    String[] namespace = new String[] {};
    Identifier[] expectedTables = {Identifier.of(namespace, "t1"), Identifier.of(namespace, "t2")};
    when(spannerInfoSchema.listTables(any(ReadContext.class), any(String[].class)))
        .thenReturn(expectedTables);
    when(spannerInfoSchema.listGraphs(any(ReadContext.class))).thenReturn(Collections.emptyList());

    Identifier[] tables = catalog.listTables(namespace);
    verify(spannerInfoSchema).listTables(any(ReadContext.class), eq(namespace));
    assertArrayEquals(expectedTables, tables);
  }

  @Test
  public void listTablesShouldReturnEmptyForInvalidNamespace() {
    String[] namespace = new String[] {"s1", "s2"};
    Identifier[] tables = catalog.listTables(namespace);
    assertEquals(0, tables.length);
  }

  @Test
  public void loadTableShouldThrowNoSuchTableException() {
    Identifier ident = Identifier.of(new String[] {}, "non_existent");
    when(spannerInfoSchema.tableExists(any(ReadContext.class), eq("non_existent")))
        .thenReturn(false);
    assertThrows(NoSuchTableException.class, () -> catalog.loadTable(ident));
  }

  @Test
  public void loadTableShouldReturnSpannerTable() throws NoSuchTableException {
    Identifier ident = Identifier.of(new String[] {}, "t1");
    when(spannerInfoSchema.tableExists(any(ReadContext.class), eq("t1"))).thenReturn(true);
    Table table = catalog.loadTable(ident);
    assertNotNull(table);
    assertTrue(table instanceof SpannerTable);
    assertEquals("t1", table.name());
  }

  @Test
  public void loadGraphShouldReturnGraphTable() throws NoSuchTableException {
    if (dialect == Dialect.POSTGRESQL) {
      return;
    }
    Identifier ident = Identifier.of(new String[] {"graph", "MusicGraph"}, "node");
    when(spannerInfoSchema.graphExists(any(ReadContext.class), eq("MusicGraph"))).thenReturn(true);

    Table table = catalog.loadTable(ident);
    assertNotNull(table);
    assertEquals("MusicGraph:node", table.name());
  }

  @Test
  public void loadGraphShouldThrowExceptionForSchemaQualifiedGraphIdentifier() {
    Identifier ident = Identifier.of(new String[] {"custom_schema", "graph", "MusicGraph"}, "edge");
    assertThrows(SpannerConnectorException.class, () -> catalog.loadTable(ident));
  }

  @Test
  public void loadTableShouldThrowExceptionForInvalidNamespace() {
    Identifier ident = Identifier.of(new String[] {"a", "b"}, "t1");
    assertThrows(SpannerConnectorException.class, () -> catalog.loadTable(ident));
  }

  @Test
  public void loadTableShouldThrowExceptionForInvalidGraphType() {
    Identifier ident = Identifier.of(new String[] {"graph", "MusicGraph"}, "vertex");
    assertThrows(SpannerConnectorException.class, () -> catalog.loadTable(ident));
  }

  @Test
  public void tableExistsShouldReturnTrue() {
    Identifier ident = Identifier.of(new String[] {}, "t1");
    when(spannerInfoSchema.tableExists(any(ReadContext.class), eq("t1"))).thenReturn(true);
    assertTrue(catalog.tableExists(ident));
  }

  @Test
  public void tableExistsShouldReturnFalse() {
    Identifier ident = Identifier.of(new String[] {}, "non_existent");
    when(spannerInfoSchema.tableExists(any(ReadContext.class), eq("non_existent")))
        .thenReturn(false);
    assertFalse(catalog.tableExists(ident));
  }

  @Test
  public void graphExistsShouldReturnTrue() {
    if (dialect == Dialect.POSTGRESQL) {
      return;
    }
    Identifier ident = Identifier.of(new String[] {"graph", "MusicGraph"}, "edge");
    when(spannerInfoSchema.graphExists(any(ReadContext.class), eq("MusicGraph"))).thenReturn(true);
    assertTrue(catalog.tableExists(ident));
  }

  @Test
  public void tableExistsShouldReturnFalseForInvalidNamespace() {
    Identifier ident = Identifier.of(new String[] {"a", "b"}, "t1");
    assertFalse(catalog.tableExists(ident));
  }

  @Test
  public void tableExistsShouldReturnFalseForInvalidGraphIdentifierType() {
    Identifier ident = Identifier.of(new String[] {"graph", "MusicGraph"}, "vertex");
    assertFalse(catalog.tableExists(ident));
  }

  @Test
  public void createTableShouldThrowTableAlreadyExistsException() {
    Identifier ident = Identifier.of(new String[] {}, "existing_table");
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, SpannerCatalog.PRIMARY_KEY_METADATA),
              new StructField("name", DataTypes.StringType, true, Metadata.empty())
            });
    when(spannerInfoSchema.tableExists(any(ReadContext.class), eq("existing_table")))
        .thenReturn(true);
    assertThrows(
        TableAlreadyExistsException.class,
        () -> catalog.createTable(ident, schema, null, Collections.emptyMap()));
  }

  @Test
  public void createTableShouldThrowExceptionOnNoPrimaryKey() {
    Identifier ident = Identifier.of(new String[] {}, "no_pk_table");
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, Metadata.empty()),
              new StructField("name", DataTypes.StringType, true, Metadata.empty())
            });
    when(spannerInfoSchema.tableExists(any(ReadContext.class), eq("no_pk_table")))
        .thenReturn(false);
    assertThrows(
        SpannerConnectorException.class,
        () -> catalog.createTable(ident, schema, null, Collections.emptyMap()));
  }

  @Test
  public void createTableShouldRejectGraphIdentifier() {
    Identifier graphIdent = Identifier.of(new String[] {"graph", "MusicGraph"}, "node");
    assertThrows(
        SpannerConnectorException.class,
        () -> catalog.createTable(graphIdent, new StructType(), null, Collections.emptyMap()));
  }

  @Test
  public void createTableShouldRejectSchemaQualifiedIdentifier() {
    Identifier ident = Identifier.of(new String[] {"custom_schema"}, "new_table");
    assertThrows(
        SpannerConnectorException.class,
        () -> catalog.createTable(ident, new StructType(), null, Collections.emptyMap()));
  }

  @Test
  public void dropTableShouldRejectGraphIdentifier() {
    Identifier graphIdent = Identifier.of(new String[] {"graph", "MusicGraph"}, "node");
    assertThrows(SpannerConnectorException.class, () -> catalog.dropTable(graphIdent));
  }

  @Test
  public void dropTableShouldRejectSchemaQualifiedIdentifier() {
    Identifier ident = Identifier.of(new String[] {"custom_schema"}, "existing_table");
    assertThrows(SpannerConnectorException.class, () -> catalog.dropTable(ident));
  }

  @Test
  public void alterTableShouldThrowException() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            catalog.alterTable(null, (org.apache.spark.sql.connector.catalog.TableChange[]) null));
  }

  @Test
  public void renameTableShouldThrowException() {
    assertThrows(UnsupportedOperationException.class, () -> catalog.renameTable(null, null));
  }

  @Test
  public void testToDdl() {
    Identifier ident = Identifier.of(new String[] {}, "my_table");
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, SpannerCatalog.PRIMARY_KEY_METADATA),
              new StructField(
                  "id2", DataTypes.StringType, false, SpannerCatalog.PRIMARY_KEY_METADATA),
              new StructField("name", DataTypes.StringType, true, Metadata.empty()),
              new StructField("active", DataTypes.BooleanType, false, Metadata.empty()),
              new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
              new StructField("data", DataTypes.BinaryType, true, Metadata.empty()),
              new StructField("created_at", DataTypes.TimestampType, true, Metadata.empty()),
              new StructField("created_on", DataTypes.DateType, true, Metadata.empty()),
              new StructField("price", DataTypes.createDecimalType(10, 2), true, Metadata.empty()),
            });

    String ddl = SpannerCatalog.toDdl(ident, schema, dialect);

    if (dialect == Dialect.POSTGRESQL) {
      assertEquals(
          "CREATE TABLE my_table (id bigint NOT NULL, id2 varchar NOT NULL, name varchar, "
              + "active boolean NOT NULL, amount float8, data bytea, "
              + "created_at timestamptz, created_on date, price numeric, "
              + "PRIMARY KEY (id, id2))",
          ddl);
    } else {
      assertEquals(
          "CREATE TABLE my_table (id INT64 NOT NULL, id2 STRING(MAX) NOT NULL, name STRING(MAX), "
              + "active BOOL NOT NULL, amount FLOAT64, data BYTES(MAX), created_at TIMESTAMP, "
              + "created_on DATE, price NUMERIC, PRIMARY KEY (id, id2))",
          ddl);
    }
  }
}
