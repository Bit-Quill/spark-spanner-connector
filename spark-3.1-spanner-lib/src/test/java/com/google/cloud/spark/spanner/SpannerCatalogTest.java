// Copyright 2026 Google LLC
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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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

  @Rule public ExpectedException thrown = ExpectedException.none();

  private SpannerCatalog catalog;
  private final Dialect dialect;

  @Mock private Spanner spanner;
  @Mock private DatabaseClient dbClient;
  @Mock private SpannerInformationSchema spannerInfoSchema;

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
          protected SpannerTable factorySpannerTable(Identifier ident) {
            SpannerTable mockSpannerTable = mock(SpannerTable.class);
            when(mockSpannerTable.name()).thenReturn(ident.name());
            return mockSpannerTable;
          }
        };

    Map<String, String> opts = new HashMap<>();
    opts.put("projectId", "p");
    opts.put("instanceId", "i");
    opts.put("databaseId", "d");
    opts.put("emulatorHost", "localhost:9010");
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(opts);
    catalog.initialize("test-catalog", options);

    when(spanner.getDatabaseClient(any(DatabaseId.class))).thenReturn(dbClient);
    when(dbClient.getDialect()).thenReturn(dialect);
    ReadContext mockReadContext = mock(ReadContext.class);
    when(dbClient.singleUse()).thenReturn(mockReadContext);
  }

  @Test
  public void testName() {
    assertEquals("test-catalog", catalog.name());
  }

  @Test
  public void listTablesShouldReturnTables() {
    String[] namespace = new String[] {"p", "i", "d"};
    Identifier[] expectedTables = {Identifier.of(namespace, "t1"), Identifier.of(namespace, "t2")};
    when(spannerInfoSchema.listTables(any(ReadContext.class), any(String[].class)))
        .thenReturn(expectedTables);

    Identifier[] tables = catalog.listTables(namespace);
    assertArrayEquals(expectedTables, tables);
  }

  @Test
  public void listTablesShouldReturnEmptyForInvalidNamespace() {
    String[] namespace = new String[] {"p", "i"};
    Identifier[] tables = catalog.listTables(namespace);
    assertEquals(0, tables.length);
  }

  @Test
  public void loadTableShouldThrowNoSuchTableException() throws NoSuchTableException {
    Identifier ident = Identifier.of(new String[] {"p", "i", "d"}, "non_existent");
    when(spannerInfoSchema.tableExists(any(ReadContext.class), any(String.class)))
        .thenReturn(false);
    thrown.expect(NoSuchTableException.class);
    catalog.loadTable(ident);
  }

  @Test
  public void loadTableShouldReturnSpannerTable() throws NoSuchTableException {
    Identifier ident = Identifier.of(new String[] {"p", "i", "d"}, "t1");
    when(spannerInfoSchema.tableExists(any(ReadContext.class), any(String.class))).thenReturn(true);
    Table table = catalog.loadTable(ident);
    assertNotNull(table);
    assertTrue(table instanceof SpannerTable);
    assertEquals("t1", table.name());
  }

  @Test
  public void loadTableShouldThrowExceptionForInvalidNamespace() throws NoSuchTableException {
    Identifier ident = Identifier.of(new String[] {"p", "i"}, "t1");
    thrown.expect(SpannerConnectorException.class);
    catalog.loadTable(ident);
  }

  @Test
  public void tableExistsShouldReturnTrue() {
    Identifier ident = Identifier.of(new String[] {"p", "i", "d"}, "t1");
    when(spannerInfoSchema.tableExists(any(ReadContext.class), any(String.class))).thenReturn(true);
    assertTrue(catalog.tableExists(ident));
  }

  @Test
  public void tableExistsShouldReturnFalse() {
    Identifier ident = Identifier.of(new String[] {"p", "i", "d"}, "non_existent");
    when(spannerInfoSchema.tableExists(any(ReadContext.class), any(String.class)))
        .thenReturn(false);
    assertFalse(catalog.tableExists(ident));
  }

  @Test
  public void tableExistsShouldReturnFalseForInvalidNamespace() {
    Identifier ident = Identifier.of(new String[] {"p", "i"}, "t1");
    assertFalse(catalog.tableExists(ident));
  }

  @Test
  public void createTableShouldReturnSpannerTable() throws TableAlreadyExistsException {
    Identifier ident = Identifier.of(new String[] {"p", "i", "d"}, "new_table");
    StructType schema =
        new StructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.LongType, false)});
    when(spannerInfoSchema.tableExists(any(ReadContext.class), any(String.class)))
        .thenReturn(false);
    Table table = catalog.createTable(ident, schema, null, Collections.emptyMap());
    assertNotNull(table);
    assertTrue(table instanceof SpannerTable);
    assertEquals("new_table", table.name());
  }

  @Test
  public void createTableShouldThrowTableAlreadyExistsException()
      throws TableAlreadyExistsException {
    Identifier ident = Identifier.of(new String[] {"p", "i", "d"}, "existing_table");
    StructType schema = new StructType();
    when(spannerInfoSchema.tableExists(any(ReadContext.class), any(String.class))).thenReturn(true);
    thrown.expect(TableAlreadyExistsException.class);
    catalog.createTable(ident, schema, null, Collections.emptyMap());
  }

  @Test
  public void alterTableShouldThrowException() {
    thrown.expect(UnsupportedOperationException.class);
    catalog.alterTable(null, null);
  }

  @Test
  public void dropTableShouldThrowException() {
    thrown.expect(UnsupportedOperationException.class);
    catalog.dropTable(null);
  }

  @Test
  public void renameTableShouldThrowException() {
    thrown.expect(UnsupportedOperationException.class);
    catalog.renameTable(null, null);
  }

  @Test
  public void listNamespacesShouldReturnEmpty() {
    assertEquals(0, catalog.listNamespaces().length);
  }

  @Test
  public void listNamespacesWithNoNamespaceShouldReturnProject() {
    String[][] expected = new String[][] {{"p"}};
    assertArrayEquals(expected, catalog.listNamespaces(new String[0]));
  }

  @Test
  public void listNamespacesWithProjectShouldReturnInstance() {
    String[][] expected = new String[][] {{"p", "i"}};
    assertArrayEquals(expected, catalog.listNamespaces(new String[] {"p"}));
  }

  @Test
  public void listNamespacesWithProjectAndInstanceShouldReturnDatabase() {
    String[][] expected = new String[][] {{"p", "i", "d"}};
    assertArrayEquals(expected, catalog.listNamespaces(new String[] {"p", "i"}));
  }

  @Test
  public void namespaceExistsShouldWork() {
    assertTrue(catalog.namespaceExists(new String[0]));
    assertTrue(catalog.namespaceExists(new String[] {"p"}));
    assertTrue(catalog.namespaceExists(new String[] {"p", "i"}));
    assertTrue(catalog.namespaceExists(new String[] {"p", "i", "d"}));
    assertFalse(catalog.namespaceExists(new String[] {"p", "i", "d", "t"}));
    assertFalse(catalog.namespaceExists(new String[] {"z"}));
  }

  @Test
  public void loadNamespaceMetadataShouldWork() throws NoSuchNamespaceException {
    assertTrue(catalog.loadNamespaceMetadata(new String[] {"p", "i", "d"}).isEmpty());
  }

  @Test
  public void loadNamespaceMetadataShouldThrowException() throws NoSuchNamespaceException {
    thrown.expect(NoSuchNamespaceException.class);
    catalog.loadNamespaceMetadata(new String[] {"z"});
  }

  @Test
  public void createNamespaceShouldThrowException() {
    thrown.expect(UnsupportedOperationException.class);
    catalog.createNamespace(null, null);
  }

  @Test
  public void alterNamespaceShouldThrowException() {
    thrown.expect(UnsupportedOperationException.class);
    catalog.alterNamespace(null, null);
  }

  @Test
  public void dropNamespaceShouldThrowException() {
    thrown.expect(UnsupportedOperationException.class);
    catalog.dropNamespace(null);
  }
}
