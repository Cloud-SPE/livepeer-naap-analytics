name := "livepeer-analytics-jobs"
version := "0.1"
scalaVersion := "2.12.18"

val flinkVersion = "1.18.0"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" % "flink-connector-kafka" % "3.0.2-1.18",
  "org.apache.flink" % "flink-connector-jdbc" % "3.1.2-1.18",
  "org.apache.flink" % "flink-parquet" % flinkVersion,
  "org.apache.flink" % "flink-connector-files" % flinkVersion,
  "com.clickhouse" % "clickhouse-jdbc" % "0.6.0",  // REMOVED % "provided"
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1",  // REMOVED % "provided"
  "org.apache.parquet" % "parquet-avro" % "1.13.1",
  "org.json4s" %% "json4s-jackson" % "4.0.6"
)

// Assembly settings
assembly / assemblyJarName := "livepeer-analytics-jobs-assembly-0.1.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => xs match {
    case "services" :: _ => MergeStrategy.concat
    case "MANIFEST.MF" :: Nil => MergeStrategy.discard
    case _ => MergeStrategy.discard
  }
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case PathList("mozilla", xs @ _*) => MergeStrategy.last  // For httpcomponents
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.contains("slf4j") => MergeStrategy.first
  case _ => MergeStrategy.first
}

// Exclude Flink classes (they're provided by the cluster)
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp.filter { f =>
    f.data.getName.startsWith("scala-library")
  }
}