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

package com.google.cloud.spark.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SparkSpannerTableProviderBaseTest {

  // A test implementation of the abstract SparkSpannerTableProviderBase
  private static class TestSparkSpannerTableProvider extends SparkSpannerTableProviderBase {}

  @Test
  public void testExtractIdentifier() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("projectId", "p");
                put("instanceId", "i");
                put("databaseId", "d");
                put("table", "t");
              }
            });
    Identifier identifier = provider.extractIdentifier(options);
    assertEquals(Identifier.of(new String[] {"p", "i", "d"}, "t"), identifier);
  }

  @Test
  public void testExtractIdentifierWithGraph() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("projectId", "p");
                put("instanceId", "i");
                put("databaseId", "d");
                put("graph", "g");
              }
            });
    Identifier identifier = provider.extractIdentifier(options);
    assertEquals(Identifier.of(new String[] {"p", "i", "d"}, "g"), identifier);
  }

  @Test
  public void testExtractIdentifierWithTableAndGraph() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("projectId", "p");
                put("instanceId", "i");
                put("databaseId", "d");
                put("table", "t");
                put("graph", "g");
              }
            });
    Identifier identifier = provider.extractIdentifier(options);
    assertNull(identifier);
  }

  @Test
  public void testExtractIdentifierWithNoTableOrGraph() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("projectId", "p");
                put("instanceId", "i");
                put("databaseId", "d");
              }
            });
    Identifier identifier = provider.extractIdentifier(options);
    assertNull(identifier);
  }

  @Test
  public void testExtractCatalog() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("catalog", "cat");
              }
            });
    assertEquals("cat", provider.extractCatalog(options));
    CaseInsensitiveStringMap options2 = new CaseInsensitiveStringMap(new HashMap<>());
    assertEquals("spanner", provider.extractCatalog(options2));
  }

  @Test
  public void testExtractIdentifierWithPartialNamespace() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("instanceId", "i");
                put("databaseId", "d");
                put("table", "t");
              }
            });
    Identifier identifier = provider.extractIdentifier(options);
    assertNull(identifier);

    options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("projectId", "p");
                put("table", "t");
              }
            });
    identifier = provider.extractIdentifier(options);
    assertNull(identifier);

    options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("projectId", "p");
                put("graph", "g");
              }
            });
    identifier = provider.extractIdentifier(options);
    assertNull(identifier);
  }

  @Test
  public void testExtractIdentifierCaseInsensitiveOptions() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();

    // Test with all lowercase
    CaseInsensitiveStringMap options1 =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("projectid", "p");
                put("instanceid", "i");
                put("databaseid", "d");
                put("table", "t");
              }
            });
    Identifier identifier1 = provider.extractIdentifier(options1);
    assertEquals(Identifier.of(new String[] {"p", "i", "d"}, "t"), identifier1);

    // Test with all uppercase
    CaseInsensitiveStringMap options2 =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("PROJECTID", "p");
                put("INSTANCEID", "i");
                put("DATABASEID", "d");
                put("TABLE", "t");
              }
            });
    Identifier identifier2 = provider.extractIdentifier(options2);
    assertEquals(Identifier.of(new String[] {"p", "i", "d"}, "t"), identifier2);

    // Test with mixed case
    CaseInsensitiveStringMap options3 =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("prOjectId", "p");
                put("inStanceId", "i");
                put("datAbaseId", "d");
                put("tAble", "t");
              }
            });
    Identifier identifier3 = provider.extractIdentifier(options3);
    assertEquals(Identifier.of(new String[] {"p", "i", "d"}, "t"), identifier3);
  }
}
