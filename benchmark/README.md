# Spark Spanner Connector Benchmark

This benchmark is designed to test the performance of the Spark Spanner Connector, particularly for write operations. It can be run on Google Cloud Dataproc.

## Getting Started

This guide walks through setting up your Google Cloud environment to run the Spark Spanner Connector benchmarks.

### 1. Configure Your Environment

The benchmark runner and supporting sbt tasks now use a structured configuration:
*   **Benchmark Definitions**: Defined in `benchmark/benchmark_definitions.json`. These describe what to test.
*   **Data Sources**: Defined in `benchmark/data_sources.json`. These map logical data names to DDLs, allowing separation of schema from specific table names.
*   **Environment Configuration**: Defined in `benchmark/environment.json`. This file contains all environment-specific settings (GCP project IDs, Dataproc cluster names, GCS bucket names, Spanner instance IDs, etc.) and mappings from logical data source names to physical table names in your environment.

You need to create and configure your local `environment.json` file:

1.  Copy the template:
    ```bash
    cp benchmark/environment.json.template benchmark/environment.json
    ```
2.  Open `benchmark/environment.json` in your editor and fill in the values for the `dataproc` section with your specific details. You can ignore the `databricks` section if you are only running Dataproc benchmarks.

**Important**: `benchmark/environment.json` is in `.gitignore` and should **not** be committed to the repository.

### 2. Set up Your GCP Project

Make sure you have the Google Cloud SDK (`gcloud`) installed and authenticated.

```bash
# Set your project ID
gcloud config set project your-gcp-project-id

# Set your region (matching spannerRegion)
gcloud config set compute/region your-gcp-region
```

You'll also need to enable the Spanner and Dataproc APIs:
```bash
gcloud services enable spanner.googleapis.com
gcloud services enable dataproc.googleapis.com
```

### 3. Create Spanner Resources

Use the provided sbt tasks to create the Spanner instance, database, and table. These tasks read their configuration from `benchmark.json`.

```bash
# Create a Spanner instance (ensure spannerRegion is set in benchmark.json or use --region argument)
# Example: sbt "createSpannerInstance --region us-central1"
sbt createSpannerInstance

# Create a Spanner database
sbt createSpannerDatabase

# Create the table for the write benchmark
sbt createSpannerTable
```
*Note: The `createSpannerTable` task will read the DDL from `./ddl/create_source_table.sql` and replace "TransferTest" with the `writeTable` value from your `benchmark.json`.*

### 4. Create GCS Buckets

The benchmark requires two GCS buckets:
*   A staging bucket for Dataproc.
*   A bucket to store benchmark results.

You can create the results bucket using the `createResultsBucket` sbt task. You'll need to create the Dataproc staging bucket manually.

```bash
# Create the Dataproc staging bucket
gsutil mb -p your-gcp-project-id -l your-gcp-region gs://your-dataproc-staging-bucket/

# Create the GCS bucket for benchmark results
sbt createResultsBucket
```

### 5. Service Account and Permissions

The benchmarks run on Dataproc and authenticate to Spanner using the VM's service account. This service account needs permissions to access Spanner and GCS.

By default, Dataproc clusters use the project's default Compute Engine service account. For simplicity, you can grant this service account the required roles.

```bash
# Get your project number
PROJECT_NUMBER=$(gcloud projects describe your-gcp-project-id --format="value(projectNumber)")

# Get your default Compute Engine service account email
SERVICE_ACCOUNT_EMAIL="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# Grant roles
# These roles are generally sufficient for the benchmark to run.
gcloud projects add-iam-policy-binding your-gcp-project-id --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" --role="roles/spanner.databaseUser"
gcloud projects add-iam-policy-binding your-gcp-project-id --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" --role="roles/spanner.databaseAdmin"
gcloud projects add-iam-policy-binding your-gcp-project-id --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" --role="roles/storage.objectAdmin"
```

## Prerequisites

Before you begin, make sure you have the following tools installed:
- Java (version 8 or higher)
- Apache Maven
- sbt (Scala Build Tool)
- Google Cloud SDK (`gcloud`)
- `jq` (a lightweight and flexible command-line JSON processor)

## Authentication

The benchmark authenticates to Google Cloud Spanner using the service account of the Dataproc cluster's VM instances.

When the Dataproc cluster is created using the `createDataprocCluster` task, it is configured with the `https://www.googleapis.com/auth/cloud-platform` scope. This scope grants the cluster's service account broad access to Google Cloud APIs, including Spanner.

This means that as long as the service account has the necessary IAM permissions for Spanner (e.g., `roles/spanner.databaseUser`), the benchmark will be able to authenticate and write to the Spanner table.

There is no need to configure any additional authentication credentials (like service account keys) in the benchmark code or options.

## Benchmarking Workflow

This section describes how to run benchmarks using sbt tasks.

### sbt Tasks Overview

*   `sbt "runBenchmark <benchmark_name>"`: Builds the connector, runs the specified benchmark on your Dataproc or Databricks cluster, and outputs the GCS path of the result file.
*   `sbt "setBnechmarkBaseline <benchmark_name> <gcs_path>"`: Copies a specific benchmark run's result (identified by its full GCS path) to establish it as the baseline for future comparisons.
*   `sbt "compareBenchmarkResults <benchmark_name> <gcs_path>"`: Downloads a specific benchmark run's result (identified by its full GCS path) and its corresponding baseline, then outputs a formatted comparison report.

### Workflow

1.  **Build the Connector**: From the root of the repository, build the connector and install it into your local Maven repository. This makes it available to the benchmark project.
    ```bash
    # Use the Spark version that matches your benchmark environment
    mvn clean install -P3.3 -DskipTests
    ```
2.  **Run a Benchmark and Establish Baseline**:
    *   Navigate to the `benchmark` directory.
    *   Run your chosen benchmark (e.g., `dataproc-100k-records`):
        ```bash
        cd benchmark
        sbt "runBenchmark dataproc-100k-records"
        ```
    *   The script will print the GCS path where the result JSON was uploaded (e.g., `gs://<your-results-bucket>/SparkSpannerWriteBenchmark/<timestamp>_<githash>.json`). Note down the full GCS path.
    *   Use this path to set it as the baseline:
        ```bash
        sbt "setBnechmarkBaseline dataproc-100k-records <full_gcs_path_from_above>"
        ```
    This step effectively tags a known-good performance run as your reference point.

3.  **Make Code Changes and Compare**:
    *   Make any desired code changes to the Spark-Spanner connector.
    *   Re-build the connector to ensure your changes are included:
        ```bash
        # From the project root
        mvn clean install -P3.3 -DskipTests
        ```
    *   Run the same benchmark again to generate new results:
        ```bash
        # Still in the benchmark directory
        sbt "runBenchmark dataproc-100k-records"
        ```
    *   Note down the GCS path for the new run from the output.
    *   Compare the new results against your established baseline:
        ```bash
        sbt "compareBenchmarkResults dataproc-100k-records <full_gcs_path_for_new_run>"
        ```
    The task will output a formatted comparison report, showing performance deltas between your baseline and the new run.

## Benchmark Results

After a benchmark run is complete, the results are stored as a JSON file in the results GCS bucket.

### Location

You can find the results in the bucket specified by the `resultsBucket` property in your `environment.json` file.

The directory structure and file naming convention is as follows:
- **Bucket:** `gs://<results_bucket_name>/`
- **Directory:** `/SparkSpannerWriteBenchmark/`
- **File:** `/<run_id>.json`

For example:
`gs://my-spark-spanner-bench-results/SparkSpannerWriteBenchmark/2026-01-07T12-00-00Z_a1b2c3d4.json`

Each JSON file contains detailed information about the run, including performance metrics, configuration parameters, and versions. For the detailed schema, see `RESULTS_SCHEMA.md`.

## Troubleshooting

### "Error: Catalog 'X' is not accessible in current workspace"

This error indicates a mismatch between the Databricks workspace targeted by your CLI configuration and the one specified in `benchmarkDatabricks.json`, or a lack of proper permissions.

**Possible Causes and Solutions:**

1.  **Workspace Host Mismatch**:
    *   **Diagnosis**: Your `databricks auth describe` command might show a different `Host` than the `databricksHost` value in your `benchmarkDatabricks.json`. The `sbt` task uses the host from `benchmarkDatabricks.json`.
    *   **Solution**: Update the `databricksHost` in `benchmarkDatabricks.json` to match the host of the Databricks workspace where your Unity Catalog is correctly configured and accessible. Ensure all other Databricks-specific settings (`clusterId`, `ucVolumePath`) are valid for this workspace.

2.  **Insufficient Permissions**: Even if the catalog is bound to the workspace, the user or service principal associated with your `databricksToken` might lack the necessary permissions.
    *   **Diagnosis**:
        *   Identify your user: `databricks auth describe`
        *   List accessible catalogs: `databricks catalogs list` (check if your catalog is listed)
        *   Check catalog permissions: `databricks grants get catalog your_catalog_name` (look for `USE_CATALOG`)
        *   Check schema permissions: `databricks grants get schema your_catalog_name.your_schema_name` (look for `USE_SCHEMA`)
        *   Check volume permissions: `databricks grants get volume your_catalog_name.your_schema_name.your_volume_name` (look for `WRITE_VOLUME` and `READ_VOLUME`)
    *   **Solution**: A Databricks administrator needs to grant the required privileges to your user or a group you belong to. Specifically, ensure `USE_CATALOG`, `USE_SCHEMA`, and `WRITE_VOLUME`/`READ_VOLUME` (or `ALL_PRIVILEGES`) are granted.