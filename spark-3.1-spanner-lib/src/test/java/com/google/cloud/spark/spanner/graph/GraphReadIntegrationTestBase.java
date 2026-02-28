package com.google.cloud.spark.spanner.graph;

import com.google.cloud.spark.spanner.integration.SparkSpannerIntegrationTestBase;
import com.google.gson.Gson;
import java.util.Map;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class GraphReadIntegrationTestBase extends SparkSpannerIntegrationTestBase {

  @Override
  protected String getDataFrameFormat() {
    return "cloud-spanner-graph";
  }

  public DataFrameReader reader() {
    Map<String, String> props = connectionProperties();
    DataFrameReader reader =
        spark
            .read()
            .format(getDataFrameFormat())
            .option("viewsEnabled", true)
            .option("projectId", props.get("projectId"))
            .option("instanceId", props.get("instanceId"))
            .option("databaseId", props.get("databaseId"));
    String emulatorHost = props.get("emulatorHost");
    if (emulatorHost != null) reader = reader.option("emulatorHost", props.get("emulatorHost"));
    return reader;
  }

  public DataFrameReader flexibleGraphReader(SpannerGraphConfigs configs) {
    DataFrameReader reader =
        reader().option("enableDataBoost", "true").option("graph", "FlexibleGraph");
    return configs == null ? reader : reader.option("configs", new Gson().toJson(configs));
  }

  public DataFrameReader musicGraphReader(SpannerGraphConfigs configs) {
    DataFrameReader reader =
        reader().option("enableDataBoost", "true").option("graph", "MusicGraph");
    return configs == null ? reader : reader.option("configs", new Gson().toJson(configs));
  }

  public static Dataset<Row> readNodes(DataFrameReader reader) {
    return reader.option("type", "node").load();
  }

  public static Dataset<Row> readEdges(DataFrameReader reader) {
    return reader.option("type", "edge").load();
  }
}
