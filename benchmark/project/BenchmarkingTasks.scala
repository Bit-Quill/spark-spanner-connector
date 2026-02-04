import sbt._
import Keys._
import scala.sys.process._
import sbtassembly.AssemblyKeys.assembly
import play.api.libs.json._

object BenchmarkingTasks {

  lazy val createSpannerInstance = inputKey[Unit]("Creates a spanner instance")
  lazy val createSpannerDatabase = inputKey[Unit]("Creates a spanner database")
  lazy val buildBenchmarkJar = taskKey[File]("Builds the spanner test suite JAR.")
  lazy val runDataproc = inputKey[Unit]("Runs the spark job on Google Cloud Dataproc")
  lazy val createDataprocCluster = inputKey[Unit]("Creates a Google Cloud Dataproc cluster.")
  lazy val createResultsBucket = inputKey[Unit]("Creates a GCS bucket for benchmark results.")
  lazy val createSpannerTable = inputKey[Unit]("Creates a Spanner table.")
  lazy val runDatabricksNotebook = inputKey[Unit]("Runs a notebook on Databricks.")
  lazy val addJarToAllowlist = inputKey[Unit]("Adds a JAR path prefix to the Databricks artifact allowlist.")
  lazy val removeJarFromAllowlist = inputKey[Unit]("Removes a JAR path prefix from the Databricks artifact allowlist.")
  lazy val refreshDatabricksToken = inputKey[Unit]("Refreshes the Databricks token.")
  lazy val spannerUp = inputKey[Unit]("Ensures spanner instance, database and table exist for benchmark.")
  lazy val spannerDown = inputKey[Unit]("Removes the spanner instance, and its databases, referenced in the benchmark config.")
  lazy val createBenchmarkSpannerTable = inputKey[Unit]("Creates the Spanner table required for a specific benchmark scenario.")
  lazy val prepareDatabricksSource = inputKey[Unit]("Creates a delta table with source data for the benchmark.")
  lazy val databricksInstallJar = inputKey[Unit]("Uploads and installs the connector JAR on Databricks.")
  lazy val databricksUninstallJar = inputKey[Unit]("Uninstalls the connector JAR from Databricks.")
  lazy val runBenchmark = inputKey[Unit]("Runs a specified benchmark scenario in a given environment.")
  lazy val setBaseline = inputKey[Unit]("Sets a specific benchmark run as the baseline.")
  lazy val compare = inputKey[Unit]("Compares a specific benchmark run against the baseline.")

  private def loadBenchmarkConfig(file: File): JsValue = {
    Json.parse(IO.read(file))
  }

  // Common helper to get benchmark and environment info
  private def getBenchmarkContext(benchmarkName: String, baseDir: File): (JsObject, JsObject, String) = {
    val envFile = baseDir / "environment.json"
    if (!envFile.exists()) sys.error(s"Environment file not found at ${envFile.getAbsolutePath}.")
    val environmentConfig = loadBenchmarkConfig(envFile)

    val defsFile = baseDir / "benchmark_definitions.json"
    if (!defsFile.exists()) sys.error(s"Benchmark definitions file not found at ${defsFile.getAbsolutePath}.")
    val benchmarkDefs = (Json.parse(IO.read(defsFile)) \ "benchmarks").as[JsArray]

    val benchmarkDef = benchmarkDefs.value.find(b => (b \ "name").as[String] == benchmarkName).getOrElse {
      sys.error(s"Benchmark definition for '$benchmarkName' not found in benchmark_definitions.json.")
    }.as[JsObject]

    val environmentType = (benchmarkDef \ "environment").asOpt[String].getOrElse {
      sys.error(s"Benchmark '$benchmarkName' does not have an 'environment' field in its definition.")
    }

    val specificEnvConfig = (environmentConfig \ environmentType).asOpt[JsObject].getOrElse {
      sys.error(s"Configuration for environment '$environmentType' not found in environment.json.")
    }

    (benchmarkDef, specificEnvConfig, environmentType)
  }

  // Helper to derive the write table name
  private def deriveWriteTableName(benchmarkName: String): String = {
    val sanitizedName = benchmarkName.replaceAll("[^a-zA-Z0-9_]", "_")
    s"benchmark_${sanitizedName}_dest"
  }


  private def runDatabricksNotebookHelper(
      configFile: File,
      localNotebookPathKey: String,
      baseDirectory: File
  ): Unit = {
    val config = loadBenchmarkConfig(configFile)

    val databricksHost = (config \ "databricksHost").as[String]
    val databricksToken = (config \ "databricksToken").as[String]
    val clusterId = (config \ "clusterId").as[String]
    val baseDatabricksNotebookPath = (config \ "notebookPath").as[String]
    val localNotebookPath = (config \ localNotebookPathKey).as[String]
    val notebookBasename = new java.io.File(localNotebookPath).getName
    val notebookPath = s"${baseDatabricksNotebookPath.stripSuffix("/")}/$notebookBasename"

    // 1. Import notebook
    val language = localNotebookPath.substring(localNotebookPath.lastIndexOf('.') + 1).toUpperCase match {
      case "PY" => "PYTHON"
      case "SQL" => "SQL"
      case "R" => "R"
      case "SCALA" => "SCALA"
      case other => sys.error(s"Unsupported notebook language extension: .$other")
    }

    println(s"Importing notebook $localNotebookPath to $notebookPath on Databricks...")
    val importCommand = Seq(
      "databricks", "workspace", "import", notebookPath,
      "--file", (baseDirectory / localNotebookPath).toString,
      "--language", language,
      "--format", "SOURCE",
      "--overwrite"
    )
    println(s"Executing command: ${importCommand.mkString(" ")}")
    val importExitCode = Process(importCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!
    if (importExitCode != 0) {
      sys.error(s"Failed to import notebook to Databricks.")
    }
    println("Notebook imported successfully.")

    // 2. Prepare parameters
    val databricksKeys = Set("databricksHost", "databricksToken", "clusterId", "notebookPath", "localNotebookPath", "localPrepareNotebookPath", "ucVolumePath")
    val allParams = config.as[JsObject].value.filterKeys(k => !databricksKeys.contains(k))
    val baseParameters = JsObject(
      allParams.map { case (key, value) =>
        key -> (value match {
          case s: JsString => s
          case other => Json.toJson(other.toString)
        })
      }.toSeq
    )

    // 3. Run notebook
    val jobJson = Json.obj(
      "run_name" -> "Spark Spanner Benchmark",
      "tasks" -> Json.arr(
        Json.obj(
          "task_key" -> "benchmark_task",
          "notebook_task" -> Json.obj(
            "notebook_path" -> notebookPath,
            "source" -> "WORKSPACE",
            "base_parameters" -> baseParameters
          ),
          "existing_cluster_id" -> clusterId
        )
      )
    )
    val jobJsonString = Json.stringify(jobJson)

      println(s"Submitting job for notebook $notebookPath on cluster $clusterId...")
      val runCommand = Seq(
        "databricks", "jobs", "submit", "--json", jobJsonString
      )
      println(s"Executing command: databricks jobs submit --json '...' and capturing output.")
      
      // Capture the output of the command
      val output = Process(runCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!!.trim
      
      // Parse the JSON output
      val resultJson = Json.parse(output)
      val runId = (resultJson \ "run_id").as[Long]
      val runPageUrl = (resultJson \ "run_page_url").as[String]
      
      println(s"Job submitted successfully. Run ID: $runId")
      println(s"Monitor progress at: $runPageUrl")
    }
  
    lazy val customTaskSettings: Seq[Setting[_]] = Seq(
      runBenchmark := {
        import scala.util.Try

        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        if (args.isEmpty) {
          sys.error("Usage: sbt \"runBenchmark <benchmarkName>\"")
        }
        val benchmarkName = args(0)
        val baseDir = baseDirectory.value
        val (benchmarkDef, specificEnvConfig, environmentType) = getBenchmarkContext(benchmarkName, baseDir)

        // Load data sources to resolve source table
        val dataSourcesFile = baseDir / "data_sources.json"
        if (!dataSourcesFile.exists()) {
          sys.error(s"Data sources file not found at ${dataSourcesFile.getAbsolutePath}.")
        }
        val dataSources = (Json.parse(IO.read(dataSourcesFile)) \ "dataSources").as[JsArray]

        val logicalDataSourceName = (benchmarkDef \ "dataSource").asOpt[String]
        var resolvedSourceTable: Option[String] = None

        logicalDataSourceName.foreach {
          dsName =>
          val dataSourceMappings = (specificEnvConfig \ "dataSourceMappings").asOpt[JsObject].getOrElse(Json.obj())
          resolvedSourceTable = (dataSourceMappings \ dsName).asOpt[String]
          if (resolvedSourceTable.isEmpty) {
              sys.error(s"Physical table mapping for logical data source '$dsName' not found in environment.json for environment '$environmentType'.")
          }
        }

        // --- Generate writeTableName ---
        val physicalWriteTableName = deriveWriteTableName(benchmarkName)


        // Merge configurations
        var tempConfig = benchmarkDef.deepMerge(specificEnvConfig)
        tempConfig = tempConfig - "writeTableName" + ("writeTable" -> Json.toJson(physicalWriteTableName))
        resolvedSourceTable.foreach(s => tempConfig = tempConfig + ("sourceTable" -> Json.toJson(s)))
        tempConfig = tempConfig + ("buildSparkVersion" -> Json.toJson(sys.props.get("spark.version").getOrElse("3.3")))
        
        val finalMergedConfig = tempConfig
        val configJsonString = Json.stringify(finalMergedConfig)
        println(s"Running benchmark with merged configuration: ${configJsonString}")

        // Execute the Spark job
        environmentType match {
          case "dataproc" =>
            val appJar = (assembly in ThisProject).value
            val mc = (Compile / mainClass).value.getOrElse(throw new RuntimeException("mainClass not found"))

            val cluster = (finalMergedConfig \ "dataprocCluster").as[String]
            val region = (finalMergedConfig \ "dataprocRegion").as[String]
            val bucketName = (finalMergedConfig \ "dataprocBucket").as[String]
            val projectId = (finalMergedConfig \ "projectId").as[String]

            val runId = java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now()) + "_" + java.util.UUID.randomUUID().toString.take(8)
            val gcsPath = s"gs://$bucketName/connector-test-$runId"
            
            val dest = s"$gcsPath/${appJar.getName}"
            println(s"Uploading ${appJar.getAbsolutePath} to $dest")
            s"gcloud storage cp ${appJar.getAbsolutePath} $dest".!

            val command = Seq(
              "gcloud", "dataproc", "jobs", "submit", "spark",
              s"--cluster=$cluster",
              s"--region=$region",
              s"--project=$projectId",
              s"--class=$mc",
              s"--jars=$dest",
              "--"
            ) ++ Seq(configJsonString)

            println(s"Submitting Dataproc job: ${command.mkString(" ")}")
            
            val jobOutput = new StringBuilder
            val errorOutput = new StringBuilder
            val exitCode = command.!(ProcessLogger(
              line => {
                println(line)
                jobOutput.append(line).append("\n")
              },
              line => {
                System.err.println(line)
                errorOutput.append(line).append("\n")
              }
            ))

            if (exitCode != 0) {
              sys.error(s"Dataproc job submission failed with exit code $exitCode.\nStderr:\n${errorOutput.toString}")
            }

            val resultPathPattern = """Writing results to (gs://[^\s]+)""" .r
            resultPathPattern.findFirstMatchIn(jobOutput.toString).map(_.group(1)) match {
              case Some(path) =>
                println("\n" + ("-" * 50))
                println("Benchmark Run Complete")
                println(s"Result file created at: $path")
                println(("-" * 50) + "\n")
              case None =>
                println("\nWarning: Could not automatically find the results file path in the Dataproc job output.")
            }

          case "databricks" =>
            sys.error("Databricks benchmark execution not yet implemented for the new runner.")

          case _ =>
            sys.error(s"Unsupported environment type: '$environmentType'.")
        }
      },
      
      setBaseline := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        if (args.length < 2) {
          sys.error("Usage: sbt \"setBaseline <benchmarkName> <gcsPath>\"")
        }
        val benchmarkName = args(0)
        val sourceGcsPath = args(1)
        val baseDir = baseDirectory.value
        
        val (_, specificEnvConfig, _) = getBenchmarkContext(benchmarkName, baseDir)
        val resultsBucket = (specificEnvConfig \ "resultsBucket").as[String]
        
        val baselineGcsPath = s"gs://${resultsBucket}/SparkSpannerWriteBenchmark/${benchmarkName}-baseline.json"
        
        println(s"--- Setting Baseline for ${benchmarkName} ---")
        println(s"Copying ${sourceGcsPath} to ${baselineGcsPath}")
        
        val command = Seq("gsutil", "cp", sourceGcsPath, baselineGcsPath)
        if (command.! != 0) {
          sys.error("Failed to set baseline.")
        }
        println("Baseline set successfully.")
      },

      compare := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        if (args.length < 2) {
          sys.error("Usage: sbt \"compare <benchmarkName> <gcsPath>\"")
        }
        val benchmarkName = args(0)
        val currentGcsPath = args(1)
        val baseDir = baseDirectory.value

        val (_, specificEnvConfig, _) = getBenchmarkContext(benchmarkName, baseDir)
        val resultsBucket = (specificEnvConfig \ "resultsBucket").as[String]
        
        val baselineGcsPath = s"gs://${resultsBucket}/SparkSpannerWriteBenchmark/${benchmarkName}-baseline.json"
        
        val tempDir = IO.createTemporaryDirectory
        
        println(s"--- Comparing ${benchmarkName} (run ${currentGcsPath}) against baseline ---")
        println(s"Downloading files to temporary directory: ${tempDir.getAbsolutePath}")
        
        val baselineFile = tempDir / "baseline.json"
        val currentFile = tempDir / "current.json"

        val gsutilCpBaseline = Seq("gsutil", "cp", baselineGcsPath, baselineFile.getAbsolutePath)
        val gsutilCpCurrent = Seq("gsutil", "cp", currentGcsPath, currentFile.getAbsolutePath)
        
        if (gsutilCpBaseline.! != 0) {
          IO.delete(tempDir)
          sys.error(s"Failed to download baseline file from GCS: ${baselineGcsPath}")
        }
        if (gsutilCpCurrent.! != 0) {
          IO.delete(tempDir)
          sys.error(s"Failed to download current result file from GCS: ${currentGcsPath}")
        }
        
        println("--- Generating Comparison Report ---")

        val baselineJson = Json.parse(IO.read(baselineFile))
        val currentJson = Json.parse(IO.read(currentFile))

        val baselineMetrics = (baselineJson \ "performanceMetrics").as[JsObject]
        val currentMetrics = (currentJson \ "performanceMetrics").as[JsObject]

        case class Metric(name: String, key: String)
        val metricsToCompare = Seq(
          Metric("Duration (s)", "durationSeconds"),
          Metric("Throughput (MB/s)", "throughputMbPerSec"),
          Metric("Records Written", "recordCount")
        )

        def formatChange(baseline: Double, current: Double): String = {
          if (baseline == 0) "N/A"
          else {
            val change = ((current - baseline) / baseline) * 100
            val emoji = if (change > 0) "📈" else "📉"
            f"$change%+.2f%% $emoji"
          }
        }

        println("\n## 🚀 Spark Spanner Connector Benchmark Report\n")
        println("| Metric              | Baseline   | Current PR | Change      |")
        println("|---------------------|------------|------------|-------------|")

        metricsToCompare.foreach {
          metric =>
          val baselineValue = (baselineMetrics \ metric.key).asOpt[Double].getOrElse(0.0)
          val currentValue = (currentMetrics \ metric.key).asOpt[Double].getOrElse(0.0)
          val changeStr = formatChange(baselineValue, currentValue)
          
          val formattedBaseline = f"$baselineValue%10.2f"
          val formattedCurrent = f"$currentValue%10.2f"
          
          println(f"| ${metric.name}%-19s | ${formattedBaseline} | ${formattedCurrent} | ${changeStr}%-11s |")
        }
        
        println("")
        
        IO.delete(tempDir)
      },

      createBenchmarkSpannerTable := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        if (args.isEmpty) {
          sys.error("Usage: sbt \"createBenchmarkSpannerTable <benchmarkName>\"")
        }
        val benchmarkName = args(0)
        val baseDir = baseDirectory.value

        val (benchmarkDef, specificEnvConfig, _) = getBenchmarkContext(benchmarkName, baseDir)
        
        val dataSourcesFile = baseDir / "data_sources.json"
        if (!dataSourcesFile.exists()) sys.error(s"Data sources file not found at ${dataSourcesFile.getAbsolutePath}.")
        val dataSources = (Json.parse(IO.read(dataSourcesFile)) \ "dataSources").as[JsArray]

        val projectId = (specificEnvConfig \ "projectId").as[String]
        val instanceId = (specificEnvConfig \ "instanceId").as[String]
        val databaseId = (specificEnvConfig \ "databaseId").as[String]
        val writeTableName = deriveWriteTableName(benchmarkName)

        val logicalDataSourceName = (benchmarkDef \ "dataSource").asOpt[String].getOrElse {
          sys.error(s"Benchmark '$benchmarkName' does not specify a 'dataSource' to infer DDL from.")
        }
        val dataSourceDef = dataSources.value.find(ds => (ds \ "name").as[String] == logicalDataSourceName).getOrElse {
          sys.error(s"Logical data source '$logicalDataSourceName' not found in data_sources.json.")
        }
        val ddlFile = (dataSourceDef \ "ddlFile").as[String]
        val ddlContent = IO.read(baseDir / ddlFile).replace("TransferTest", writeTableName)


        println(s"Checking for Spanner table '$writeTableName' in database '$databaseId'...")
        val checkTableCommand = Seq(
          "gcloud", "spanner", "databases", "ddl", "describe", databaseId,
          s"--instance=$instanceId",
          s"--project=$projectId"
        )
        val ddlOutput = checkTableCommand.!!
        if (ddlOutput.contains(s"CREATE TABLE $writeTableName")) {
          println(s"Table '$writeTableName' already exists in database '$databaseId'.")
        } else {
          println(s"Table '$writeTableName' not found, creating it...")
          val createTableCommand = Seq(
            "gcloud", "spanner", "databases", "ddl", "update", databaseId,
            s"--instance=$instanceId",
            s"--project=$projectId",
            s"--ddl=$ddlContent"
          )

          println(s"Executing DDL to create table '$writeTableName' in database '$databaseId':")
          println(ddlContent)

          if (createTableCommand.! != 0) {
            sys.error(s"Failed to create table '$writeTableName'.")
          } else {
            println(s"Successfully created table '$writeTableName'.")
          }
        }
      },

      databricksInstallJar := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmarkDatabricks.json")
        val config = loadBenchmarkConfig(configFile)
        val databricksHost = (config \ "databricksHost").as[String]
        val databricksToken = (config \ "databricksToken").as[String]
        val clusterId = (config \ "clusterId").as[String]
        val ucVolumeBasePath = (config \ "ucVolumePath").as[String]
        val ucVolumePath = s"${ucVolumeBasePath.stripSuffix("/")}/$clusterId"
  
        // Find the connector JAR
        val sparkVersion = sys.props.get("spark.version").getOrElse("3.3")
        val connectorVersion = "0.0.1-SNAPSHOT" // from parent pom
        val artifactId = s"spark-$sparkVersion-spanner"
        val connectorJarName = s"$artifactId-$connectorVersion.jar"
        val localJarPath = Path.userHome / ".m2" / "repository" / "com" / "google" / "cloud" / "spark" / "spanner" / artifactId / connectorVersion / connectorJarName
  
        if (!localJarPath.exists()) {
          sys.error(s"Connector JAR not found at $localJarPath. Please build and publish it to your local Maven repository first using 'mvn clean install -P$sparkVersion'.")
        }
  
        println(s"Using connector JAR: $localJarPath")
  
        // 1. Prepare paths
        val remoteJarPath = s"$ucVolumePath/${localJarPath.getName}"
        val remoteJarDir = remoteJarPath.substring(0, remoteJarPath.lastIndexOf('/'))
  
        // 2. Create remote directory
        println(s"Ensuring directory exists: $remoteJarDir")
        val mkdirsCommand = Seq("databricks", "fs", "mkdirs", remoteJarDir)
        println(s"Executing command: ${mkdirsCommand.mkString(" ")}")
        Process(mkdirsCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!
  
        // 3. Upload JAR to UC Volume
        println(s"Uploading $localJarPath to $remoteJarPath on Databricks...")
        val uploadCommand = Seq(
          "databricks", "fs", "cp", "--overwrite", localJarPath.toString, remoteJarPath
        )
        println(s"Executing command: ${uploadCommand.mkString(" ")}")
        val uploadExitCode = Process(uploadCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!
        if (uploadExitCode != 0) {
          sys.error(s"Failed to upload JAR to Databricks UC Volume.")
        }
        println("JAR uploaded successfully to UC Volume.")
  
        // 4. Install JAR on cluster
        println(s"Installing JAR $remoteJarPath on cluster $clusterId")
        val installJson = Json.obj(
          "cluster_id" -> clusterId,
          "libraries" -> Json.arr(Json.obj("jar" -> remoteJarPath))
        )
        val installJsonString = Json.stringify(installJson)
        val installCommand = Seq("databricks", "libraries", "install", "--json", installJsonString)
        println(s"Executing command: ${installCommand.mkString(" ")}")
        val installExitCode = Process(installCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!
        if (installExitCode != 0) {
          sys.error("Failed to install JAR on cluster.")
        }
        println("JAR installed on cluster successfully.")
      },
      databricksUninstallJar := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmarkDatabricks.json")
        val config = loadBenchmarkConfig(configFile)
        val databricksHost = (config \ "databricksHost").as[String]
        val databricksToken = (config \ "databricksToken").as[String]
        val clusterId = (config \ "clusterId").as[String]
        val ucVolumeBasePath = (config \ "ucVolumePath").as[String]
        val ucVolumePath = s"${ucVolumeBasePath.stripSuffix("/")}/$clusterId"
        
        // Construct remote JAR path
        val sparkVersion = sys.props.get("spark.version").getOrElse("3.3")
        val connectorVersion = "0.0.1-SNAPSHOT"
        val artifactId = s"spark-$sparkVersion-spanner"
        val connectorJarName = s"$artifactId-$connectorVersion.jar"
        val remoteJarPath = s"$ucVolumePath/$connectorJarName"
  
        println(s"Uninstalling JAR $remoteJarPath from cluster $clusterId")
        val uninstallJson = Json.obj(
          "cluster_id" -> clusterId,
          "libraries" -> Json.arr(Json.obj("jar" -> remoteJarPath))
        )
        val uninstallJsonString = Json.stringify(uninstallJson)
        val uninstallCommand = Seq("databricks", "libraries", "uninstall", "--json", uninstallJsonString)
        println(s"Executing command: ${uninstallCommand.mkString(" ")}")
        val uninstallExitCode = Process(uninstallCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!
        if (uninstallExitCode != 0) {
          println(s"Warning: Failed to uninstall JAR from cluster. You may need to do it manually.")
        } else {
          println("JAR uninstalled successfully.")
        }
      },
      spannerDown := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
        val config = loadBenchmarkConfig(configFile)
  
        val instanceId = (config \ "instanceId").as[String]
        val projectId = (config \ "projectId").as[String]
  
        println(s"Attempting to tear down Spanner instance '$instanceId' in project '$projectId'...")
  
        // 1. Check if instance exists
        val checkInstanceCommand = Seq("gcloud", "spanner", "instances", "describe", instanceId, s"--project=$projectId")
        if (checkInstanceCommand.! != 0) {
          println(s"Spanner instance '$instanceId' does not exist or you don't have permissions. Nothing to delete.")
        } else {
          // 2. List and delete databases within the instance
          println(s"Listing databases in instance '$instanceId'...")
          val listDatabasesCommand = Seq(
            "gcloud", "spanner", "databases", "list",
            s"--instance=$instanceId",
            s"--project=$projectId",
            "--format=json"
          )
          val databasesJson = Process(listDatabasesCommand).!!.trim
          val databases = Json.parse(databasesJson).as[JsArray]
  
          if (databases.value.isEmpty) {
            println(s"No databases found in instance '$instanceId'.")
          } else {
            databases.value.foreach {
              db =>
              val databaseId = (db \ "name").as[String].split("/").last
              println(s"Deleting database '$databaseId' from instance '$instanceId'...")
              val deleteDbCommand = Seq(
                "gcloud", "spanner", "databases", "delete", databaseId,
                s"--instance=$instanceId",
                s"--project=$projectId",
                "--quiet"
              )
              println(s"Running command: ${deleteDbCommand.mkString(" ")}")
              if (deleteDbCommand.! != 0) {
                println(s"Warning: Failed to delete database '$databaseId'. It might be already gone or permissions issue.")
              } else {
                println(s"Successfully initiated deletion of database '$databaseId'.")
              }
            }
          }
  
          // 3. Delete the Spanner instance
          println(s"Deleting Spanner instance '$instanceId'...")
          val deleteInstanceCommand = Seq(
            "gcloud", "spanner", "instances", "delete", instanceId,
            s"--project=$projectId",
            "--quiet"
          )
          println(s"Running command: ${deleteInstanceCommand.mkString(" ")}")
          if (deleteInstanceCommand.! != 0) {
            sys.error(s"Failed to delete Spanner instance '$instanceId'.")
          } else {
            println(s"Successfully initiated deletion of Spanner instance '$instanceId'.")
          }
        }
      },
      
      spannerUp := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
        val config = loadBenchmarkConfig(configFile)

        val instanceId = (config \ "instanceId").as[String]
        val projectId = (config \ "projectId").as[String]
        val databaseId = (config \ "databaseId").as[String]

        // 1. Ensure Spanner instance exists.
        println(s"Checking for Spanner instance '$instanceId'...")
        val checkInstanceCommand = Seq("gcloud", "spanner", "instances", "describe", instanceId, s"--project=$projectId")
        if (checkInstanceCommand.! != 0) {
          println(s"Spanner instance '$instanceId' not found, creating it...")
          val spannerRegion = (config \ "spannerRegion").asOpt[String].getOrElse("us-central1")
          val createInstanceCommand = Seq(
            "gcloud", "spanner", "instances", "create", instanceId,
            s"--project=$projectId",
            s"--config=regional-$spannerRegion",
            s"--description=$instanceId",
            s"--autoscaling-min-processing-units=2000",
            s"--autoscaling-max-processing-units=20000",
            s"--autoscaling-high-priority-cpu-target=65",
            s"--autoscaling-storage-target=90",
            s"--edition=ENTERPRISE"
          )
          println(s"Running command: ${createInstanceCommand.mkString(" ")}")
          if (createInstanceCommand.! != 0) {
            sys.error(s"Failed to create Spanner instance '$instanceId'.")
          }
          println(s"Successfully initiated creation of Spanner instance '$instanceId'.")
        } else {
          println(s"Spanner instance '$instanceId' already exists.")
        }

        // 2. Ensure Spanner database exists.
        println(s"Checking for Spanner database '$databaseId' in instance '$instanceId'...")
        val checkDbCommand = Seq("gcloud", "spanner", "databases", "describe", databaseId, s"--instance=$instanceId", s"--project=$projectId")
        if (checkDbCommand.! != 0) {
          println(s"Spanner database '$databaseId' not found, creating it...")
          val createDbCommand = Seq(
            "gcloud", "spanner", "databases", "create", databaseId,
            s"--instance=$instanceId",
            s"--project=$projectId"
          )
          println(s"Running command: ${createDbCommand.mkString(" ")}")
          if (createDbCommand.! != 0) {
            sys.error(s"Failed to create Spanner database '$databaseId' in instance '$instanceId'.")
          }
          println(s"Successfully initiated creation of Spanner database '$databaseId'.")
        } else {
          println(s"Spanner database '$databaseId' already exists.")
        }

        // 3. Ensure Spanner table exists.
        val tableName = (config \ "writeTable").as[String]
        println(s"Checking for Spanner table '$tableName' in database '$databaseId'...")
        val checkTableCommand = Seq(
          "gcloud", "spanner", "databases", "ddl", "describe", databaseId,
          s"--instance=$instanceId",
          s"--project=$projectId"
        )
        val ddlOutput = checkTableCommand.!!
        if (ddlOutput.contains(s"CREATE TABLE $tableName")) {
          println(s"Table '$tableName' already exists in database '$databaseId'.")
        } else {
          println(s"Table '$tableName' not found, creating it...")
          val ddlFile = (config \ "ddlFile").asOpt[String]
            .map(f => baseDirectory.value / f)
            .getOrElse(baseDirectory.value / "ddl" / "create_source_table.sql")

          if (!ddlFile.exists()) {
            sys.error(s"DDL file not found at ${ddlFile.getAbsolutePath}")
          }

          val ddlContent = IO.read(ddlFile).replace("TransferTest", tableName)
          val createTableCommand = Seq(
            "gcloud", "spanner", "databases", "ddl", "update", databaseId,
            s"--instance=$instanceId",
            s"--project=$projectId",
            s"--ddl=$ddlContent"
          )

          println(s"Executing DDL to create table '$tableName' in database '$databaseId':")
          println(ddlContent)

          if (createTableCommand.! != 0) {
            sys.error(s"Failed to create table '$tableName'.")
          } else {
            println(s"Successfully created table '$tableName'.")
          }
        }
      },
    runDatabricksNotebook := {
      val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmarkDatabricks.json")
      runDatabricksNotebookHelper(configFile, "localNotebookPath", baseDirectory.value)
    },
    prepareDatabricksSource := {
      val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmarkDatabricks.json")
      runDatabricksNotebookHelper(configFile, "localPrepareNotebookPath", baseDirectory.value)
    },

    refreshDatabricksToken := {
      val baseDir = baseDirectory.value
      val envFile = baseDir / "environment.json"
      if (!envFile.exists()) {
        sys.error(s"Environment file not found at ${envFile.getAbsolutePath}. Please create it from the template.")
      }

      println("Requesting new Databricks token via Databricks CLI...")
      val lifetimeSeconds = 3600 // 1 hour
      val createTokenCommand = Seq(
        "databricks", "tokens", "create",
        "--comment", "Temporary token for Spark Spanner benchmark",
        "--lifetime-seconds", lifetimeSeconds.toString
      )

      val output = try {
        Process(createTokenCommand).!!.trim
      } catch {
        case ex: Exception => sys.error("Failed to create Databricks token. Make sure the Databricks CLI is installed and configured with a valid token that has permission to create new tokens.")
      }

      val tokenJson = Json.parse(output)
      val newToken = (tokenJson \ "token_value").as[String]

      val environmentConfig = loadBenchmarkConfig(envFile).as[JsObject]
      
      val databricksConfig = (environmentConfig \ "databricks").asOpt[JsObject].getOrElse {
        sys.error("'databricks' section not found in environment.json.")
      }
      
      val updatedDatabricksConfig = databricksConfig + ("databricksToken" -> Json.toJson(newToken))
      val updatedEnvironmentConfig = environmentConfig + ("databricks" -> updatedDatabricksConfig)
      
      IO.write(envFile, Json.prettyPrint(updatedEnvironmentConfig))
      println(s"Successfully updated databricksToken in ${envFile.getAbsolutePath}.")
    },

    addJarToAllowlist := {
      val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmarkDatabricks.json")
      val config = loadBenchmarkConfig(configFile)
      val databricksHost = (config \ "databricksHost").as[String]
      val databricksToken = (config \ "databricksToken").as[String]
      val ucVolumeBasePath = (config \ "ucVolumePath").as[String]
      val clusterId = (config \ "clusterId").as[String]
      val pathToAdd = s"${ucVolumeBasePath.stripSuffix("/")}/$clusterId/"

      val getCommand = Seq("databricks", "artifact-allowlists", "get", "LIBRARY_JAR", "-o", "json")
      println(s"Executing command: ${getCommand.mkString(" ")}")
      val currentAllowlistJsonString = Process(getCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!!
      val currentAllowlist = Json.parse(currentAllowlistJsonString)
      val currentMatchers = (currentAllowlist \ "artifact_matchers").as[JsArray]

      val newMatcher = Json.obj("artifact" -> pathToAdd, "match_type" -> "PREFIX_MATCH")
      if (currentMatchers.value.contains(newMatcher)) {
        println(s"Path $pathToAdd is already in the allowlist. Nothing to do.")
      } else {
        val newMatchers = currentMatchers :+ newMatcher
        val newAllowlist = Json.obj("artifact_matchers" -> newMatchers)
        
        println(s"Attempting to update allowlist with:\n${Json.prettyPrint(newAllowlist)}")
        val tempFile = java.io.File.createTempFile("allowlist", ".json")
        IO.write(tempFile, Json.stringify(newAllowlist))

        val updateCommand = Seq("databricks", "artifact-allowlists", "update", "LIBRARY_JAR", "--json", s"@${tempFile.getAbsolutePath}")
        println(s"Executing command: ${updateCommand.mkString(" ")}")
        val updateExitCode = Process(updateCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!
        tempFile.delete()

        if (updateExitCode != 0) {
          sys.error("Failed to update allowlist.")
        }
        println(s"Successfully added $pathToAdd to the allowlist.")
      }
    },

    removeJarFromAllowlist := {
      val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmarkDatabricks.json")
      val config = loadBenchmarkConfig(configFile)
      val databricksHost = (config \ "databricksHost").as[String]
      val databricksToken = (config \ "databricksToken").as[String]
      val ucVolumeBasePath = (config \ "ucVolumePath").as[String]
      val clusterId = (config \ "clusterId").as[String]
      val pathToRemove = s"${ucVolumeBasePath.stripSuffix("/")}/$clusterId/"

      val getCommand = Seq("databricks", "artifact-allowlists", "get", "LIBRARY_JAR", "-o", "json")
      println(s"Executing command: ${getCommand.mkString(" ")}")
      val currentAllowlistJsonString = Process(getCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!!
      val currentAllowlist = Json.parse(currentAllowlistJsonString)
      val currentMatchers = (currentAllowlist \ "artifact_matchers").as[JsArray]

      val matcherToRemove = Json.obj("artifact" -> pathToRemove, "match_type" -> "PREFIX_MATCH")
      if (!currentMatchers.value.contains(matcherToRemove)) {
        println(s"Path $pathToRemove is not in the allowlist. Nothing to do.")
      } else {
        val newMatchers = JsArray(currentMatchers.value.filterNot(_ == matcherToRemove))
        val newAllowlist = Json.obj("artifact_matchers" -> newMatchers)
        
        println(s"Attempting to update allowlist with:\n${Json.prettyPrint(newAllowlist)}")
        val tempFile = java.io.File.createTempFile("allowlist", ".json")
        IO.write(tempFile, Json.stringify(newAllowlist))

        val updateCommand = Seq("databricks", "artifact-allowlists", "update", "LIBRARY_JAR", "--json", s"@${tempFile.getAbsolutePath}")
        println(s"Executing command: ${updateCommand.mkString(" ")}")
        val updateExitCode = Process(updateCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!
        tempFile.delete()

        if (updateExitCode != 0) {
          sys.error("Failed to update allowlist.")
        }
        println(s"Successfully removed $pathToRemove from the allowlist.")
      }
    },

    createSpannerInstance := {
      import scala.util.Try

      val args = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
      val config = loadBenchmarkConfig(configFile)
      
      val instanceName = (config \ "instanceId").as[String]
      val projectId = (config \ "projectId").as[String]
      var spannerRegionFromConfig: Option[String] = (config \ "spannerRegion").asOpt[String]
      var processingUnits: Int = 1000 // Default value

      val argsIterator = args.drop(1).iterator // Drop config file path
      while (argsIterator.hasNext) {
        val arg = argsIterator.next()
        arg match {
          case "--region" if argsIterator.hasNext => spannerRegionFromConfig = Some(argsIterator.next())
          case "--processingUnits" if argsIterator.hasNext =>
            val next = argsIterator.next()
            Try(next.toInt).toOption match {
              case Some(value) => processingUnits = value
              case None => sys.error(s"Invalid value for --processingUnits: '$next'. Must be an integer.")
            }
          case other if other.startsWith("--") => sys.error(s"Unknown option: $other")
          case _ => // Ignore non-option arguments
        }
      }

      spannerRegionFromConfig match {
        case Some(r) =>
          val command = Seq(
            "gcloud", "spanner", "instances", "create", instanceName,
            s"--project=$projectId",
            s"--config=regional-$r",
            s"--description=$instanceName",
            s"--processing-units=$processingUnits"
          )

          println(s"Running command: ${command.mkString(" ")}")
          val exitCode = command.!
          if (exitCode != 0) {
            sys.error(s"Failed to create Spanner instance '$instanceName'.")
          } else {
            println(s"Successfully initiated creation of Spanner instance '$instanceName'.")
          }

        case None =>
          sys.error("Error: --region or 'spannerRegion' in config is required.")
      }
    },

    createSpannerDatabase := {
      val args = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
      val config = loadBenchmarkConfig(configFile)

      val instanceId = (config \ "instanceId").as[String]
      val databaseId = (config \ "databaseId").as[String]
      val projectId = (config \ "projectId").as[String]

      val command = Seq(
        "gcloud", "spanner", "databases", "create", databaseId,
        s"--instance=$instanceId",
        s"--project=$projectId"
      )

      println(s"Running command: ${command.mkString(" ")}")
      val exitCode = command.!
      if (exitCode != 0) {
        sys.error(s"Failed to create Spanner database '$databaseId' in instance '$instanceId'.")
      } else {
        println(s"Successfully initiated creation of Spanner database '$databaseId' in instance '$instanceId'.")
      }
    },

    buildBenchmarkJar := (assembly in ThisProject).value,

    runDataproc := {
      val appJar = (assembly in ThisProject).value
      val mc = (Compile / mainClass).value.getOrElse(throw new RuntimeException("mainClass not found"))
      
      val args = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
      val config = loadBenchmarkConfig(configFile)

      val mainClassArgs = args.drop(1)

      val cluster = (config \ "dataprocCluster").asOpt[String].getOrElse(sys.error("dataprocCluster not found in config"))
      val region = (config \ "dataprocRegion").asOpt[String].getOrElse(sys.error("dataprocRegion not found in config"))
      val bucketName = (config \ "dataprocBucket").as[String]
      val bucketUri = s"gs://$bucketName"

      val runId = java.util.UUID.randomUUID().toString.take(8)
      val gcsPath = s"$bucketUri/connector-test-$runId"
      
      val dest = s"$gcsPath/${appJar.getName}"
      println(s"Uploading ${appJar.getAbsolutePath} to $dest")
      s"gcloud storage cp ${appJar.getAbsolutePath} $dest".!
      
      val sparkVersion = sys.props.get("spark.version").getOrElse("3.3")
      val updatedConfig = config.as[JsObject] + ("buildSparkVersion" -> Json.toJson(sparkVersion))
      val benchmarkArgs = Seq(Json.stringify(updatedConfig))

      val projectId = (config \ "projectId").as[String]
      
      val command = Seq(
        "gcloud", "dataproc", "jobs", "submit", "spark",
        s"--cluster=$cluster",
        s"--region=$region",
        s"--project=$projectId",
        s"--class=$mc",
        s"--jars=$dest",
        "--"
      ) ++ benchmarkArgs

      println(s"Submitting Dataproc job: ${command.mkString(" ")}")
      command.!(ProcessLogger(line => println(line)))
    },

    createDataprocCluster := {
      import scala.util.Try
      
      val args = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
      val config = loadBenchmarkConfig(configFile)
      
      val clusterName = (config \ "dataprocCluster").asOpt[String].getOrElse(sys.error("dataprocCluster not found in config"))
      val region = (config \ "dataprocRegion").asOpt[String].getOrElse("us-central1")
      var numWorkers: Int = 2 // Default value
      var masterMachineType: String = "n2-standard-4" // Default value
      var workerMachineType: String = "n2-standard-4" // Default value
      var imageVersion: String = "2.1-debian11" // Default value
      
      val argsIterator = args.drop(1).iterator // Drop config file path
      while (argsIterator.hasNext) {
        val arg = argsIterator.next()
        arg match {
          case "--numWorkers" if argsIterator.hasNext =>
            val next = argsIterator.next()
            Try(next.toInt).toOption match {
              case Some(value) => numWorkers = value
              case None => sys.error(s"Invalid value for --numWorkers: '$next'. Must be an integer.")
            }
          case "--masterMachineType" if argsIterator.hasNext => masterMachineType = argsIterator.next()
          case "--workerMachineType" if argsIterator.hasNext => workerMachineType = argsIterator.next()
          case "--imageVersion" if argsIterator.hasNext => imageVersion = argsIterator.next()
          case other if other.startsWith("--") => sys.error(s"Unknown option: $other")
          case _ => // Ignore non-option arguments
        }
      }

      val bucketName = (config \ "dataprocBucket").as[String]
      val projectId = (config \ "projectId").as[String]

      println(s"Attempting to create Dataproc cluster '$clusterName' in project '$projectId' region '$region'...")
      val command = Seq(
        "gcloud", "dataproc", "clusters", "create", clusterName,
        s"--project=$projectId",
        s"--region=$region",
        s"--bucket=$bucketName",
        "--no-address",
        s"--num-workers=$numWorkers",
        s"--image-version=$imageVersion",
        "--enable-component-gateway",
        s"--master-machine-type=$masterMachineType",
        "--master-boot-disk-type=hyperdisk-balanced",
        "--master-boot-disk-size=100",
        s"--worker-machine-type=$workerMachineType",
        "--worker-boot-disk-type=hyperdisk-balanced",
        "--worker-boot-disk-size=200",
        "--scopes=https://www.googleapis.com/auth/cloud-platform"
      )

      println(s"Running command: ${command.mkString(" ")}")
      val exitCode = command.!(ProcessLogger(line => println(line)))

      if (exitCode != 0) {
        sys.error(s"Failed to create Dataproc cluster '$clusterName'. It may already exist or you lack permissions.")
      } else {
        println(s"Successfully initiated creation of Dataproc cluster '$clusterName'.")
      }
    },

    createResultsBucket := {
      val args = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
      val config = loadBenchmarkConfig(configFile)

      val resultsBucket = (config \ "resultsBucket").asOpt[String].getOrElse(sys.error("resultsBucket not found in config"))
      val projectId = (config \ "projectId").as[String]
      val location = (config \ "dataprocRegion").asOpt[String].getOrElse("us-central1")

      val command = Seq(
        "gcloud", "storage", "buckets", "create", s"gs://$resultsBucket",
        s"--project=$projectId",
        s"--location=$location",
        "--uniform-bucket-level-access"
      )

      println(s"Running command: ${command.mkString(" ")}")
      // Don't fail if the bucket already exists
      val exitCode = command.!(ProcessLogger(_ => ()))
      if (exitCode != 0) {
        println(s"Could not create bucket '$resultsBucket'. It may already exist.")
      } else {
        println(s"Successfully created GCS bucket '$resultsBucket'.")
      }
    },
    
    createSpannerTable := {
      val args = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
      val config = loadBenchmarkConfig(configFile)

      val tableName = (config \ "writeTable").as[String]
      val ddlFile = baseDirectory.value / "ddl" / "create_source_table.sql"
      val ddlContent = IO.read(ddlFile).replace("TransferTest", tableName)

      val instanceId = (config \ "instanceId").as[String]
      val databaseId = (config \ "databaseId").as[String]
      val projectId = (config \ "projectId").as[String]
      
      val command = Seq(
        "gcloud", "spanner", "databases", "ddl", "update", databaseId,
        s"--instance=$instanceId",
        s"--project=$projectId",
        s"--ddl=$ddlContent"
      )
      
      println(s"Executing DDL to create table '$tableName' in database '$databaseId':")
      println(ddlContent)
      
      val exitCode = command.!
      if (exitCode != 0) {
        sys.error(s"Failed to create table '$tableName'.")
      } else {
        println(s"Successfully created table '$tableName'.")
      }
    }
  )
}