import sbt._
import Keys._
import scala.sys.process._
import complete.DefaultParsers._
import sbtassembly.AssemblyKeys.assembly
import play.api.libs.json._

object CustomTasks {

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

  private def loadBenchmarkConfig(file: File): JsValue = {
    Json.parse(IO.read(file))
  }

  lazy val customTaskSettings: Seq[Setting[_]] = Seq(
    runDatabricksNotebook := {
      val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmarkDatabricks.json")
      val config = loadBenchmarkConfig(configFile)

      val databricksHost = (config \ "databricksHost").as[String]
      val databricksToken = (config \ "databricksToken").as[String]
      val clusterId = (config \ "clusterId").as[String]
      val baseDatabricksNotebookPath = (config \ "notebookPath").as[String]
      val localNotebookPath = (config \ "localNotebookPath").as[String]
      val notebookBasename = new java.io.File(localNotebookPath).getName
      val notebookPath = s"${baseDatabricksNotebookPath.stripSuffix("/")}/$notebookBasename"
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
      
      // 5. Import notebook
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
        "--file", (baseDirectory.value / localNotebookPath).toString(),
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

      // 6. Prepare parameters
      val databricksKeys = Set("databricksHost", "databricksToken", "clusterId", "notebookPath", "localNotebookPath", "ucVolumePath")
      val allParams = config.as[JsObject].value.filterKeys(k => !databricksKeys.contains(k))
      val baseParameters = JsObject(
        allParams.map { case (key, value) =>
          key -> (value match {
            case s: JsString => s
            case other => Json.toJson(other.toString)
          })
        }.toSeq
      )

      // 7. Run notebook
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

      try {
        println(s"Submitting job for notebook $notebookPath on cluster $clusterId...")
        val runCommand = Seq(
          "databricks", "jobs", "submit", "--json", jobJsonString
        )
        println(s"Executing command: databricks jobs submit --json '...'")
        val runExitCode = Process(runCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!
        if (runExitCode != 0) {
          sys.error(s"Failed to submit job on Databricks.")
        }
        println("Job submitted successfully.")
      } finally {
        // 8. Uninstall JAR from cluster
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
      }
    },

    refreshDatabricksToken := {
      val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmarkDatabricks.json")

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

      val config = loadBenchmarkConfig(configFile)
      val updatedConfig = config.as[JsObject] + ("databricksToken" -> Json.toJson(newToken))
      val updatedJsonString = Json.prettyPrint(updatedConfig)

      IO.write(configFile, updatedJsonString)
      println(s"Successfully updated databricksToken in $configFile.")
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
        s"--no-address",
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
      val ddlFile = baseDirectory.value / "ddl" / "create_test_table.sql"
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
