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

import java.util.Locale;

public final class IdentifierResolution {
  public final boolean graph;
  public final String objectName;
  public final String graphType;

  public IdentifierResolution(boolean graph, String objectName, String graphType) {
    this.graph = graph;
    this.objectName = objectName;
    this.graphType = graphType;
  }

  static IdentifierResolution table(String tableName) {
    return new IdentifierResolution(false, tableName, null);
  }

  static IdentifierResolution graph(String graphName, String graphType) {
    if (!"node".equalsIgnoreCase(graphType) && !"edge".equalsIgnoreCase(graphType)) {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "Graph identifier name must be one of 'node' or 'edge': " + graphType);
    }
    return new IdentifierResolution(true, graphName, graphType.toLowerCase(Locale.ROOT));
  }
}
