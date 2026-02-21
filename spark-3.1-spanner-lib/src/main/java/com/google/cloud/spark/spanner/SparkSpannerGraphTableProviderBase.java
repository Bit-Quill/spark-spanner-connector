package com.google.cloud.spark.spanner;

import com.google.cloud.spark.spanner.graph.SpannerGraphBuilder;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkSpannerGraphTableProviderBase implements TableProvider, DataSourceRegister {
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return getGraph(options).schema();
  }

  private Table getGraph(CaseInsensitiveStringMap options) {
    boolean hasGraph = options.containsKey("graph");
    if (hasGraph) {
      return SpannerGraphBuilder.build(options);
    } else {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT, "properties must contain \"graph\"");
    }
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return getGraph(new CaseInsensitiveStringMap(properties));
  }

  @Override
  public String shortName() {
    return "cloud-spanner-graph";
  }
}
