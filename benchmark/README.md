# Spark Spanner Connector Benchmark

This benchmark is designed to test the performance of the Spark Spanner Connector, particularly for write operations. It can be run on Google Cloud Dataproc or Databricks.

## Getting Started

This guide walks through setting up your Google Cloud environment to run the Spark Spanner Connector benchmarks.

### 1. Configure Your Environment

The benchmark runner and supporting sbt tasks read a JSON file to configure your GCP project, Spanner instance, Dataproc cluster, and benchmark parameters. 
Each task accepts an optional parameter specifying the path to the JSON file. If omitted, it defaults to `benchmark.json`.

Create a `benchmark.json` file with the following structure, filling in the values for your environment.

```json
{
  "projectId": "your-gcp-project-id",
  "instanceId": "your-spanner-instance-id",
  "databaseId": "your-spanner-database-id",
  "writeTable": "your-spanner-table-name",
  "spannerRegion": "your-gcp-region",
  "dataprocCluster": "spark-spanner-benchmark-cluster",
  "dataprocRegion": "your-gcp-region",
  "dataprocBucket": "your-dataproc-staging-bucket",
  "resultsBucket": "your-benchmark-results-bucket",
  "numRecords": 100000,
  "numPartitions": 40,
  "mutationsPerTransaction": 5000,
  "bytesPerTransaction": 3145728,
  "numWriteThreads": 4,
  "maxPendingTransactions": 5,
  "assumeIdempotentRows": true
}
```

### 2. Set up Your GCP Project

Make sure you have the Google Cloud SDK (`gcloud`) installed and authenticated.

```bash
# Set your project ID
gcloud config set project your-gcp-project-id

# Set your region (matching spannerRegion and dataprocRegion)
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
*Note: The `createSpannerTable` task will read the DDL from `./ddl/create_test_table.sql` and replace "TransferTest" with the `writeTable` value from your `benchmark.json`.*

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
- Databricks CLI (if using Databricks)

## Authentication

The benchmark authenticates to Google Cloud Spanner using the service account of the Dataproc cluster's VM instances.

When the Dataproc cluster is created using the `createDataprocCluster` task, it is configured with the `https://www.googleapis.com/auth/cloud-platform` scope. This scope grants the cluster's service account broad access to Google Cloud APIs, including Spanner.

This means that as long as the service account has the necessary IAM permissions for Spanner (e.g., `roles/spanner.databaseUser`), the benchmark will be able to authenticate and write to the Spanner table.

There is no need to configure any additional authentication credentials (like service account keys) in the benchmark code or options.

## Workflow

The benchmark is designed to be run against a locally built version of the Spark Spanner Connector. This allows you to test changes you've made to the connector before creating a pull request.

The general workflow is:
1.  Build and install the connector from your feature branch.
2.  Build the benchmark, which packages the locally installed connector.
3.  Run the benchmark on your Spark cluster.

### Step 1: Build and Install the Connector

1.  Check out the branch of the connector that you want to test (e.g., your feature branch with write support).
2.  Build and install the connector to your local Maven repository. This makes it available to the benchmark project.

    ```bash
    # From the root of the spark-spanner-connector repository
    mvn clean install -P3.3
    ```

### Step 2: Build the Benchmark

The benchmark is configured to be packaged as a self-contained "fat JAR" that includes the connector and all its dependencies.

1.  Navigate to the `benchmark` directory.
2.  Build the fat JAR using `sbt-assembly`.

    ```bash
    # From the benchmark directory
    sbt assembly
    ```
    This will create a JAR file in the `target/scala-2.12/` directory, for example: `spanner-spark-benchmark-assembly-0.1.jar`.

### Step 3: Run the Benchmark

You can run the benchmark on Google Cloud Dataproc. The `build.sbt` file provides convenient tasks for this.

Before running, you need to configure your environment in `benchmark.json`. This file contains all the settings for your GCP project, Spanner instance, Dataproc cluster, and benchmark parameters.

#### Creating a Dataproc Cluster

The `createDataprocCluster` task can be used to create a new Dataproc cluster for running the benchmark.

**Configuration:**

This task reads the following properties from `benchmark.json`:
- `dataprocCluster`: The name for the new cluster.
- `dataprocRegion`: The region for the cluster.
- `dataprocBucket`: The GCS bucket to be associated with the cluster.
- `projectId`: Your Google Cloud project ID.

**Command:**

The task accepts the following optional arguments to override the values in `benchmark.json`:
- `--numWorkers`: The number of worker nodes.
- `--masterMachineType`: The machine type for the master node.
- `--workerMachineType`: The machine type for the worker nodes.
- `--imageVersion`: The Dataproc image version.

```bash
# Example from the benchmark directory
sbt "createDataprocCluster --numWorkers 4"
```

#### Creating the Results Bucket

The `createResultsBucket` task creates a GCS bucket to store the JSON results from benchmark runs.

**Configuration:**

This task reads the following properties from `benchmark.json`:
- `resultsBucket`: The name of the GCS bucket to create.
- `projectId`: Your Google Cloud project ID.
- `dataprocRegion`: The location for the bucket (e.g., `us-central1`).

**Command:**

```bash
# From the benchmark directory
sbt createResultsBucket
```
This command will create the bucket if it does not already exist.

#### Running on Google Cloud Dataproc

The `runDataproc` task submits the benchmark job to a Dataproc cluster. All configuration for the benchmark is read from `benchmark.json`.

**Command:**

```bash
# Example from the benchmark directory, using settings from benchmark.json
sbt runDataproc
```

You can also specify a different configuration file as an argument:
```bash
sbt "runDataproc my_benchmark_config.json"
```

### Running on Databricks

To run benchmarks on Databricks, you will use the `runDatabricksNotebook` sbt task. This task uploads the necessary connector JAR to a Unity Catalog Volume, imports a benchmark notebook to your Databricks workspace, and then executes the notebook on a specified cluster.

#### Configuration

The `runDatabricksNotebook` task reads its configuration from `benchmarkDatabricks.json`. This file contains Databricks-specific settings and the benchmark parameters.

Ensure your `benchmarkDatabricks.json` file is configured correctly:

```json
{
  "databricksHost": "https://your-databricks-host",
  "databricksToken": "your-databricks-token",
  "clusterId": "your-cluster-id",
  "notebookPath": "/Users/Shared/SparkSpannerBenchmark",
  "localNotebookPath": "notebooks/SparkSpannerBenchmark.scala",
  "ucVolumePath": "/Volumes/your_catalog/your_schema/your_volume/jars",
  
  "projectId": "your-gcp-project-id",
  "instanceId": "your-spanner-instance-id",
  "databaseId": "your-spanner-database-id",
  "writeTable": "your-spanner-table",

  "numRecords": 100000,
  "numPartitions": 40,
  "mutationsPerTransaction": 5000,
  "bytesPerTransaction": 3145728,
  "numWriteThreads": 4,
  "maxPendingTransactions": 5,
  "assumeIdempotentRows": true
}
```

*   `databricksHost`: The URL of your Databricks workspace.
*   `databricksToken`: A Databricks personal access token.
*   `clusterId`: The ID of the Databricks cluster where the notebook will be executed.
*   `notebookPath`: The absolute path in your Databricks workspace where the notebook will be imported (e.g., `/Users/your.email@example.com/SparkSpannerBenchmark`).
*   `localNotebookPath`: The path to the local notebook file (relative to the `benchmark` directory).
*   `ucVolumePath`: The Unity Catalog Volume path where the connector JAR will be uploaded (e.g., `/Volumes/catalog/schema/volume/jars`).

#### How to get Cluster ID

To obtain the Cluster ID:
1.  Navigate to your Databricks workspace.
2.  In the sidebar, click **Compute**.
3.  Click on the name of the cluster you intend to use for the benchmark.
4.  In the cluster configuration page, the Cluster ID will be displayed in the URL, usually after `/clusters/`. For example, in `https://<databricks-host>/#setting/clusters/<cluster-id>/configuration`, `<cluster-id>` is your Cluster ID.

#### Running the Benchmark

Before running, ensure you have:
1.  Built and installed the Spark Spanner Connector to your local Maven repository using `mvn clean install -P<spark_version>` (e.g., `mvn clean install -P3.3`).
2.  Configured `benchmarkDatabricks.json` with all necessary Databricks and benchmark parameters.
3.  Set up your Databricks CLI with authentication to your workspace.

To execute the benchmark on Databricks:

```bash
# From the benchmark directory
sbt runDatabricksNotebook
```

You can also specify a different configuration file:
```bash
sbt "runDatabricksNotebook my_databricks_config.json"
```

The task will:
1.  Find the locally built Spark Spanner Connector JAR.
2.  Upload the JAR to the specified Unity Catalog Volume.
3.  Install the JAR as a library on the Databricks cluster.
4.  Import the local benchmark notebook to your Databricks workspace.
5.  Run the notebook on the cluster, passing the benchmark parameters.
6.  Uninstall the JAR from the cluster after the notebook run completes.

#### Providing GCP Credentials on Databricks

When running the benchmark notebook on Databricks, the cluster needs to authenticate to Google Cloud to access Spanner. The recommended and most secure method is to use Databricks secrets backed by an init script.

This method avoids exposing credentials in notebooks and uses the cluster's environment variables to securely pass the credentials to the Google Cloud client libraries.

##### Step 1: Store GCP Service Account Key in Databricks Secrets

First, you need to store your GCP service account JSON key file as a secret in your Databricks workspace.

1.  **Create a Secret Scope:** If you don't have one already, create a secret scope.
    ```bash
    databricks secrets create-scope --scope your-secret-scope
    ```

2.  **Add the Secret:** Add the content of your GCP service account JSON key file as a secret within this scope.
    ```bash
    databricks secrets put-secret --scope your-secret-scope --key your-gcp-key-name --binary-file /path/to/your/gcp-credentials.json
    ```

##### Step 2: Create and Upload the Init Script

Next, create an init script that will run on cluster startup. This script reads the secret content from an environment variable and writes it to the location where Google Cloud libraries expect to find Application Default Credentials (ADC).

1.  **Create the script file:** Create a file named `setup_gcp_credentials.sh` with the following content. A copy of this script is also available in the `benchmark` directory.
    ```bash
    #!/bin/bash
    # This script configures Google Application Default Credentials on a Databricks cluster.
    set -e

    # This environment variable must be configured on the cluster to point to the Databricks secret.
    GCP_CREDENTIALS_CONTENT="$GCP_CREDENTIALS"

    if [ -z "$GCP_CREDENTIALS_CONTENT" ]; then
      echo "Error: The GCP_CREDENTIALS environment variable is not set." >&2
      echo "Please configure this in the Spark Cluster Environment Variables:" >&2
      echo "GCP_CREDENTIALS={{secrets/your-secret-scope/your-gcp-key-name}}" >&2
      exit 1
    fi

    ADC_DIR="/root/.config/gcloud"
    ADC_FILE="$ADC_DIR/application_default_credentials.json"

    echo "Creating directory for ADC file: $ADC_DIR"
    mkdir -p "$ADC_DIR"

    echo "Writing credentials to $ADC_FILE"
    cat > "$ADC_FILE" <<EOF
$GCP_CREDENTIALS_CONTENT
EOF

    echo "Successfully configured Google Application Default Credentials."
    ```

2.  **Upload the script:** Upload this script to a location in your Databricks File System (DBFS).
    ```bash
    databricks fs cp setup_gcp_credentials.sh dbfs:/databricks/init_scripts/setup_gcp_credentials.sh
    ```

##### Step 3: Configure the Databricks Cluster

Finally, configure your cluster to use the secret and the init script.

1.  Navigate to your cluster's configuration page.
2.  Under **Advanced Options**, select the **Spark** tab.
3.  In the **Environment Variables** text box, add the following line. This tells Databricks to securely inject your secret's content into the `GCP_CREDENTIALS` environment variable.
    ```
    GCP_CREDENTIALS={{secrets/your-secret-scope/your-gcp-key-name}}
    ```
    Replace `your-secret-scope` and `your-gcp-key-name` with the actual scope and key you used in Step 1.

4.  Select the **Init Scripts** tab.
5.  Add the path to the init script you uploaded to DBFS, for example: `dbfs:/databricks/init_scripts/setup_gcp_credentials.sh`.
6.  Restart your cluster for the changes to take effect. The init script will now run on every startup, ensuring credentials are in place.


## Benchmark Results

After a benchmark run is complete, the results are stored as a JSON file in the results GCS bucket.

### Location

You can find the results in the bucket specified by the `resultsBucket` property in your `benchmark.json` file.

The directory structure and file naming convention is as follows:
- **Bucket:** `gs://<results_bucket_name>/`
- **Directory:** `/<benchmark_name>/`
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