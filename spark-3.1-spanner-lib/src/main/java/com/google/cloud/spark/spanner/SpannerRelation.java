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

import java.util.Map;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;

public class SpannerRelation extends BaseRelation {
  private final SQLContext sqlContext;
  private final StructType schema;
  private final Map<String, String> properties;

  public SpannerRelation(SQLContext sqlContext, Map<String, String> properties, StructType schema) {
    this.sqlContext = sqlContext;
    this.properties = properties;
    this.schema = schema;
  }

  @Override
  public SQLContext sqlContext() {
    return this.sqlContext;
  }

  @Override
  public StructType schema() {
    return this.schema;
  }

  @Override
  public long sizeInBytes() {
    // Defaulting to the default size of a Spark application.
    return super.sizeInBytes();
  }

  public Map<String, String> getProperties() {
    return this.properties;
  }
}
