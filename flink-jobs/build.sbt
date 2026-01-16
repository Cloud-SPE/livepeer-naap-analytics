name := "livepeer-analytics-jobs"
version := "0.1"
scalaVersion := "2.12.18"

val flinkVersion = "1.20.0"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-clients"         % flinkVersion % "provided",

  // Use these versions which are verified on Maven Central
  "org.apache.flink" % "flink-connector-kafka" % "3.1.0-1.18",
  "org.apache.flink" % "flink-connector-jdbc"  % "3.1.2-1.18",

  "org.apache.flink" % "flink-parquet"         % flinkVersion,
  "org.apache.flink" % "flink-connector-files" % flinkVersion,
  "org.apache.flink" % "flink-avro"            % flinkVersion,

  "com.clickhouse" % "clickhouse-jdbc" % "0.6.0",
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1",
  "org.apache.parquet" % "parquet-avro" % "1.13.1",
  "org.json4s" %% "json4s-jackson" % "4.0.6"
)

// Assembly settings
assembly / assemblyJarName := "livepeer-analytics-jobs-assembly-0.1.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case "services" :: _ => MergeStrategy.concat
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case _ => MergeStrategy.discard
    }
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.contains("slf4j") => MergeStrategy.first
  case _ => MergeStrategy.first
}