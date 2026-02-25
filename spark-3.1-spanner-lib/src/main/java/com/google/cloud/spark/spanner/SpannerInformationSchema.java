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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Verify;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.spark.sql.connector.catalog.Identifier;

public interface SpannerInformationSchema {
  Identifier[] listTables(ReadContext readContext, String[] namespace);

  boolean tableExists(ReadContext readContext, String tableName);

  List<String> listGraphs(ReadContext readContext);

  boolean graphExists(ReadContext readContext, String graphName);

  IdentifierResolution resolveIdentifier(Identifier ident);

  static String defaultSchema(Dialect dialect) {
    switch (dialect) {
      case POSTGRESQL:
        return "public";
      case GOOGLE_STANDARD_SQL:
        return "";
    }
    throw new IllegalArgumentException("Unsupported dialect: " + dialect);
  }

  static SpannerInformationSchema create(Dialect dialect) {
    switch (dialect) {
      case POSTGRESQL:
        return new PostgresSpannerInformationSchema();
      case GOOGLE_STANDARD_SQL:
        return new GoogleSqlSpannerInformationSchema();
    }
    throw new IllegalArgumentException("Unsupported dialect: " + dialect);
  }
}

class GoogleSqlSpannerInformationSchema implements SpannerInformationSchema {
  @Override
  public Identifier[] listTables(ReadContext readContext, String[] namespace) {
    Statement statement =
        Statement.newBuilder(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''")
            .build();
    try (ResultSet resultSet = readContext.executeQuery(statement)) {
      List<Identifier> tables = new ArrayList<>();
      while (resultSet.next()) {
        tables.add(Identifier.of(namespace, resultSet.getString("TABLE_NAME")));
      }
      return tables.toArray(new Identifier[0]);
    }
  }

  @Override
  public boolean tableExists(ReadContext readContext, String tableName) {
    Statement statement =
        Statement.newBuilder(
                "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @tableName")
            .bind("tableName")
            .to(tableName)
            .build();
    try (ResultSet resultSet = readContext.executeQuery(statement)) {
      return resultSet.next() && resultSet.getLong(0) > 0;
    }
  }

  @Override
  public List<String> listGraphs(ReadContext readContext) {
    Statement statement =
        Statement.newBuilder("SELECT PROPERTY_GRAPH_NAME FROM INFORMATION_SCHEMA.PROPERTY_GRAPHS ")
            .build();
    try (ResultSet resultSet = readContext.executeQuery(statement)) {
      List<String> graphs = new ArrayList<>();
      while (resultSet.next()) {
        graphs.add(resultSet.getString("PROPERTY_GRAPH_NAME"));
      }
      return graphs;
    }
  }

  @Override
  public boolean graphExists(ReadContext readContext, String graphName) {
    Statement statement =
        Statement.newBuilder(
                "SELECT COUNT(1) FROM INFORMATION_SCHEMA.PROPERTY_GRAPHS "
                    + "WHERE PROPERTY_GRAPH_NAME = @graphName")
            .bind("graphName")
            .to(graphName)
            .build();
    try (ResultSet resultSet = readContext.executeQuery(statement)) {
      return resultSet.next() && resultSet.getLong(0) > 0;
    }
  }

  @Override
  public IdentifierResolution resolveIdentifier(Identifier ident) {
    Verify.verifyNotNull(ident, "ident");
    String[] namespace = ident.namespace();
    String name = ident.name();
    Verify.verifyNotNull(name, "identifier name");

    switch (namespace.length) {
      case 0:
        return IdentifierResolution.table(name);
      case 2:
        if ("graph".equalsIgnoreCase(namespace[0])) {
          return IdentifierResolution.graph(namespace[1], name);
        }
        break;
      default:
        break;
    }

    throw new SpannerConnectorException(
        SpannerErrorCode.INVALID_ARGUMENT,
        "Invalid identifier namespace: " + String.join(".", namespace));
  }
}

class PostgresSpannerInformationSchema implements SpannerInformationSchema {
  @Override
  public Identifier[] listTables(ReadContext readContext, String[] namespace) {
    Statement statement =
        Statement.newBuilder(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            .build();
    try (ResultSet resultSet = readContext.executeQuery(statement)) {
      List<Identifier> tables = new ArrayList<>();
      while (resultSet.next()) {
        tables.add(Identifier.of(namespace, resultSet.getString("table_name")));
      }
      return tables.toArray(new Identifier[0]);
    }
  }

  @Override
  public boolean tableExists(ReadContext readContext, String tableName) {
    Statement statement =
        Statement.newBuilder(
                "SELECT COUNT(1) FROM information_schema.tables "
                    + "WHERE table_schema = 'public' AND table_name = $1")
            .bind("p1")
            .to(tableName.toLowerCase(Locale.ROOT))
            .build();
    try (ResultSet resultSet = readContext.executeQuery(statement)) {
      return resultSet.next() && resultSet.getLong(0) > 0;
    }
  }

  @Override
  public List<String> listGraphs(ReadContext readContext) {
    return new ArrayList<>();
  }

  @Override
  public boolean graphExists(ReadContext readContext, String graphName) {
    return false;
  }

  @Override
  public IdentifierResolution resolveIdentifier(Identifier ident) {
    Verify.verifyNotNull(ident, "ident");
    String[] namespace = ident.namespace();
    String name = ident.name();
    Verify.verifyNotNull(name, "identifier name");

    if (namespace.length == 0) {
      return IdentifierResolution.table(name.toLowerCase(Locale.ROOT));
    }

    // No graphs in postgres.
    throw new SpannerConnectorException(
        SpannerErrorCode.INVALID_ARGUMENT,
        "Invalid identifier namespace for PostgreSQL: " + String.join(".", namespace));
  }
}
