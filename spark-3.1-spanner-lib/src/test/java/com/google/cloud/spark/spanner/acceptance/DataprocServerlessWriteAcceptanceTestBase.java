package com.google.cloud.spark.spanner.acceptance;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.longrunning.OperationSnapshot;
import java.util.Arrays;
import org.junit.Test;

public class DataprocServerlessWriteAcceptanceTestBase
    extends DataprocServerlessAcceptanceTestBase {
  public DataprocServerlessWriteAcceptanceTestBase(
      String connectorJarDirectory, String connectorJarPrefix, String s8sImageVersion) {
    super(connectorJarDirectory, connectorJarPrefix, s8sImageVersion);
  }

  @Test
  public void testWrite() throws Exception {
    OperationSnapshot operationSnapshot =
        createAndRunPythonBatch(
            context,
            testName,
            "write_test_table.py",
            null,
            Arrays.asList(
                context.getResultsDirUri(testName), PROJECT_ID, INSTANCE_ID, DATABASE_ID));
    assertThat(operationSnapshot.isDone()).isTrue();
    assertThat(operationSnapshot.getErrorMessage()).isEmpty();
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).startsWith("PASS");
  }
}
