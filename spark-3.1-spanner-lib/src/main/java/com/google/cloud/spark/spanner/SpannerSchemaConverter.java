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

import com.google.cloud.spanner.Dialect;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SpannerSchemaConverter {

  private final Dialect dialect;

  public SpannerSchemaConverter(Dialect dialect) {
    this.dialect = dialect;
  }

  public String sparkSchemaToSpannerDDL(StructType schema, String tableName) {
    List<String> colDefs = new ArrayList<>();
    List<String> pkCols = new ArrayList<>();
    String quote = getQuote();

    for (StructField field : schema.fields()) {
      String colName = field.name();
      String spannerType = sparkTypeToSpannerType(field.dataType());
      String suffix = "";
      if (field.metadata().contains("pk") && field.metadata().getBoolean("pk")) {
        pkCols.add(colName);
      }
      if (!field.nullable()) {
        suffix = " NOT NULL";
      }
      colDefs.add(quote + colName + quote + " " + spannerType + suffix);
    }

    String pkDef =
        "PRIMARY KEY ("
            + pkCols.stream().map(c -> quote + c + quote).collect(Collectors.joining(", "))
            + ")";

    return "CREATE TABLE "
        + quote
        + tableName
        + quote
        + " ("
        + String.join(", ", colDefs)
        + ") "
        + pkDef;
  }

  private String getQuote() {
    if (this.dialect == Dialect.POSTGRESQL) {
      return "\"";
    }
    return "`";
  }

  public String sparkTypeToSpannerType(DataType sparkType) {
    if (sparkType instanceof DecimalType) {
      return "NUMERIC";
    }
    if (dialect == Dialect.POSTGRESQL) {
      if (sparkType.equals(DataTypes.LongType)) {
        return "int8";
      }
      if (sparkType.equals(DataTypes.StringType)) {
        return "varchar";
      }
      if (sparkType.equals(DataTypes.BooleanType)) {
        return "bool";
      }
      if (sparkType.equals(DataTypes.DoubleType)) {
        return "float8";
      }
      if (sparkType.equals(DataTypes.BinaryType)) {
        return "bytea";
      }
      if (sparkType.equals(DataTypes.TimestampType)) {
        return "timestamptz";
      }
      if (sparkType.equals(DataTypes.DateType)) {
        return "date";
      }
    }
    // Default to Google Standard SQL
    if (sparkType.equals(DataTypes.LongType)) {
      return "INT64";
    }
    if (sparkType.equals(DataTypes.StringType)) {
      return "STRING(MAX)";
    }
    if (sparkType.equals(DataTypes.BooleanType)) {
      return "BOOL";
    }
    if (sparkType.equals(DataTypes.DoubleType)) {
      return "FLOAT64";
    }
    if (sparkType.equals(DataTypes.BinaryType)) {
      return "BYTES(MAX)";
    }
    if (sparkType.equals(DataTypes.TimestampType)) {
      return "TIMESTAMP";
    }
    if (sparkType.equals(DataTypes.DateType)) {
      return "DATE";
    }
    // Fallback for unknown types.
    return "STRING(MAX)";
  }
}
