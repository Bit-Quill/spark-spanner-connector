// Copyright 2026 Google LLC
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

package com.google.cloud.spark.spanner.integration;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spark.spanner.SpannerCatalog;
import com.google.cloud.spark.spanner.SpannerTable;
import com.google.cloud.spark.spanner.graph.SpannerGraph;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

@RunWith(Parameterized.class)
public class SpannerCatalogIntegrationTest extends SparkSpannerIntegrationTestBase {

  private SpannerCatalog catalog;
  private final boolean usePostgresSql;

  @Parameters
  public static Collection<Object[]> usePostgresSqlValues() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  public SpannerCatalogIntegrationTest(boolean usePostgresSql) {
    super();
    this.usePostgresSql = usePostgresSql;
  }

  @Before
  public void setupCatalog() {
    catalog = new SpannerCatalog();
    Map<String, String> options = new HashMap<>(connectionProperties(usePostgresSql));
    options.remove("table");
    catalog.initialize("spanner", new CaseInsensitiveStringMap(options));
  }

  @Test
  public void testListTables() {
    String[] namespace = new String[] {};
    Identifier[] tables = catalog.listTables(namespace);
    List<String> tableNames =
        Arrays.stream(tables)
            .map(Identifier::name)
            .map(String::toLowerCase)
            .collect(Collectors.toList());

    assertThat(tableNames)
        .containsAtLeast("schema_test_table", "write_array_test_table", "write_test_table");

    Identifier musicGraphNode = Identifier.of(new String[] {"graph", "MusicGraph"}, "node");
    Identifier musicGraphEdge = Identifier.of(new String[] {"graph", "MusicGraph"}, "edge");
    if (usePostgresSql) {
      assertThat(Arrays.asList(tables)).doesNotContain(musicGraphNode);
      assertThat(Arrays.asList(tables)).doesNotContain(musicGraphEdge);
    } else {
      assertThat(Arrays.asList(tables)).contains(musicGraphNode);
      assertThat(Arrays.asList(tables)).contains(musicGraphEdge);
    }
  }

  @Test
  public void testLoadTable() throws NoSuchTableException {
    Identifier ident = Identifier.of(new String[] {}, "schema_test_table");
    Table table = catalog.loadTable(ident);
    assertTrue(table instanceof SpannerTable);
    assertThat(table.name()).isEqualTo("schema_test_table");
    assertThat(table.schema().fields())
        .asList()
        .containsExactly(
            new StructField("id", DataTypes.LongType, false, SpannerCatalog.PRIMARY_KEY_METADATA),
            new StructField("name", DataTypes.StringType, true, Metadata.empty()),
            new StructField("value", DataTypes.DoubleType, true, Metadata.empty()));
  }

  @Test
  public void testLoadTableNotExists() {
    Identifier ident = Identifier.of(new String[] {}, "NonExistentTable");
    assertThrows(NoSuchTableException.class, () -> catalog.loadTable(ident));
  }

  @Test
  public void testLoadGraph() throws NoSuchTableException {
    if (usePostgresSql) {
      // Graphs are not supported bby PostgresSql dialect.
      return;
    }

    Identifier nodeIdent = Identifier.of(new String[] {"graph", "MusicGraph"}, "node");
    Table nodeTable = catalog.loadTable(nodeIdent);
    assertTrue(nodeTable instanceof SpannerGraph);
    assertThat(nodeTable.schema().fieldNames()).asList().containsExactly("id");

    Identifier edgeIdent = Identifier.of(new String[] {"graph", "MusicGraph"}, "edge");
    Table edgeTable = catalog.loadTable(edgeIdent);
    assertTrue(edgeTable instanceof SpannerGraph);
    assertThat(edgeTable.schema().fieldNames()).asList().containsExactly("src", "dst");
  }

  @Test
  public void testCreateTableAlreadyExists() {
    Identifier ident = Identifier.of(new String[] {}, "write_test_table");
    assertThrows(
        TableAlreadyExistsException.class,
        () -> catalog.createTable(ident, new StructType(), null, new HashMap<>()));
  }

  @Test
  public void testTableExists() {
    Identifier ident = Identifier.of(new String[] {}, "write_test_table");
    assertTrue(catalog.tableExists(ident));
  }

  @Test
  public void testGraphExists() {
    Identifier ident = Identifier.of(new String[] {"graph", "MusicGraph"}, "node");
    assertEquals(!usePostgresSql, catalog.tableExists(ident));
  }

  @Test
  public void testCreateTable() throws NoSuchTableException, TableAlreadyExistsException {
    String tableName = "new_test_table";
    Identifier ident = Identifier.of(new String[] {}, tableName);
    StructType createSchema =
        new StructType()
            .add("id", DataTypes.LongType, false, SpannerCatalog.PRIMARY_KEY_METADATA)
            .add("name", DataTypes.StringType, true);
    Map<String, String> properties = new HashMap<>();

    try {
      catalog.createTable(ident, createSchema, null, properties);
      assertTrue(catalog.tableExists(ident));
      Table loadedTable = catalog.loadTable(ident);
      // Connector currently does not retrieve primary key metadata.
      StructType expectedSchema =
          new StructType()
              .add("id", DataTypes.LongType, false, SpannerCatalog.PRIMARY_KEY_METADATA)
              .add("name", DataTypes.StringType, true);
      assertThat(loadedTable.schema()).isEqualTo(expectedSchema);
    } finally {
      catalog.dropTable(ident);
      assertFalse(catalog.tableExists(ident));
    }
  }
}
