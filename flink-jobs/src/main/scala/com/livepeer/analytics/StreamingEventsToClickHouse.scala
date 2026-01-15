package com.livepeer.analytics

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.sql.{PreparedStatement, Timestamp}

object StreamingEventsToClickHouse {

  implicit val formats: DefaultFormats.type = DefaultFormats

  // Case classes for each event type
  case class AiStreamStatus(
    eventTimestamp: Long,
    streamId: String,
    requestId: String,
    gateway: String,
    orchestratorAddress: String,
    orchestratorUrl: String,
    gpuId: Option[String],
    region: Option[String],
    pipeline: String,
    pipelineId: String,
    outputFps: Float,
    inputFps: Float,
    state: String,
    restartCount: Int,
    lastError: Option[String],
    promptText: Option[String],
    promptWidth: Int,
    promptHeight: Int,
    paramsHash: String
  )

  case class StreamIngestMetrics(
    eventTimestamp: Long,
    streamId: String,
    requestId: String,
    gateway: String,
    orchestratorAddress: Option[String],
    pipelineId: String,
    connectionQuality: String,
    videoJitter: Float,
    videoPacketsReceived: Int,
    videoPacketsLost: Int,
    videoPacketLossPct: Float,
    audioJitter: Float,
    audioPacketsReceived: Int,
    audioPacketsLost: Int,
    audioPacketLossPct: Float,
    bytesReceived: Long,
    bytesSent: Long
  )

  case class StreamTraceEvent(
    eventTimestamp: Long,
    streamId: String,
    requestId: String,
    gateway: String,
    orchestratorAddress: String,
    orchestratorUrl: String,
    pipelineId: String,
    traceType: String,
    dataTimestamp: Long
  )

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    env.enableCheckpointing(60000)

    val config = env.getCheckpointConfig
    config.setCheckpointTimeout(120000)
    config.setMinPauseBetweenCheckpoints(30000)

    env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(
      3,
      org.apache.flink.api.common.time.Time.seconds(10)
    ))

    println("STARTING JOB: StreamingEventsToClickHouse - Comprehensive NaaP Metrics")

    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers("kafka:9092")
      .setTopics("streaming-events")
      .setGroupId("flink-naap-comprehensive-group-v1")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setProperty("request.timeout.ms", "60000")
      .setProperty("session.timeout.ms", "45000")
      .setProperty("heartbeat.interval.ms", "10000")
      .setProperty("metadata.max.age.ms", "180000")
      .setProperty("enable.auto.commit", "false")
      .build()

    val watermarkStrategy = WatermarkStrategy.noWatermarks[String]()
    val stream = env.fromSource(kafkaSource, watermarkStrategy, "Kafka Source")

    // Parse and route events by type
    val parsedEvents = stream.flatMap { (rawJson: String) =>
      println(s"KAFKA_RAW_IN: ${rawJson.take(200)}...")

      try {
        val parsed = parse(rawJson)
        val eventType = (parsed \ "type").extractOrElse[String]("")

        eventType match {
          case "ai_stream_status" => Some(("ai_stream_status", rawJson))
          case "stream_ingest_metrics" => Some(("stream_ingest_metrics", rawJson))
          case "stream_trace" => Some(("stream_trace", rawJson))
          case "create_new_payment" => Some(("create_new_payment", rawJson))
          case "ai_stream_events" => Some(("ai_stream_events", rawJson))
          case "network_capabilities" => Some(("network_capabilities", rawJson))
          case _ =>
            println(s"SKIP_EVENT: Type=$eventType")
            None
        }
      } catch {
        case e: Exception =>
          System.err.println(s"PARSE_ERROR: ${e.getMessage}")
          None
      }
    }

    // Split stream by event type - using explicit filter functions
    val aiStreamStatusStream = parsedEvents
      .filter((tuple: (String, String)) => tuple._1 == "ai_stream_status")
      .flatMap((tuple: (String, String)) => parseAiStreamStatus(tuple._2))

    val ingestMetricsStream = parsedEvents
      .filter((tuple: (String, String)) => tuple._1 == "stream_ingest_metrics")
      .flatMap((tuple: (String, String)) => parseStreamIngestMetrics(tuple._2))

    val traceEventsStream = parsedEvents
      .filter((tuple: (String, String)) => tuple._1 == "stream_trace")
      .flatMap((tuple: (String, String)) => parseStreamTraceEvent(tuple._2))

    // Sink ai_stream_status
    aiStreamStatusStream.addSink(createAiStreamStatusSink())
      .name("ClickHouse Sink - AI Stream Status")

    // Sink stream_ingest_metrics
    ingestMetricsStream.addSink(createStreamIngestMetricsSink())
      .name("ClickHouse Sink - Stream Ingest Metrics")

    // Sink stream_trace_events
    traceEventsStream.addSink(createStreamTraceEventsSink())
      .name("ClickHouse Sink - Stream Trace Events")

    env.execute("StreamingEventsToClickHouse - Comprehensive NaaP")
  }

  // Parser functions
  def parseAiStreamStatus(json: String): Option[AiStreamStatus] = {
    try {
      val parsed = parse(json)
      val data = parsed \ "data"

      Some(AiStreamStatus(
        eventTimestamp = (parsed \ "timestamp").extractOrElse[Long](System.currentTimeMillis()),
        streamId = (data \ "stream_id").extractOrElse[String]("unknown"),
        requestId = (data \ "request_id").extractOrElse[String]("unknown"),
        gateway = (parsed \ "gateway").extractOrElse[String](""),
        orchestratorAddress = (data \ "orchestrator_info" \ "address").extractOrElse[String](""),
        orchestratorUrl = (data \ "orchestrator_info" \ "url").extractOrElse[String](""),
        gpuId = (data \ "orchestrator_info" \ "gpu_id").extractOpt[String],
        region = (data \ "orchestrator_info" \ "region").extractOpt[String],
        pipeline = (data \ "pipeline").extractOrElse[String](""),
        pipelineId = (data \ "pipeline_id").extractOrElse[String](""),
        outputFps = (data \ "inference_status" \ "fps").extractOrElse[Double](0.0).toFloat,
        inputFps = (data \ "input_status" \ "fps").extractOrElse[Double](0.0).toFloat,
        state = (data \ "state").extractOrElse[String]("unknown"),
        restartCount = (data \ "inference_status" \ "restart_count").extractOrElse[Int](0),
        lastError = (data \ "inference_status" \ "last_error").extractOpt[String],
        promptText = (data \ "inference_status" \ "last_params" \ "prompt").extractOpt[String],
        promptWidth = (data \ "inference_status" \ "last_params" \ "width").extractOrElse[Int](0),
        promptHeight = (data \ "inference_status" \ "last_params" \ "height").extractOrElse[Int](0),
        paramsHash = (data \ "inference_status" \ "last_params_hash").extractOrElse[String]("")
      ))
    } catch {
      case e: Exception =>
        System.err.println(s"ERROR parsing ai_stream_status: ${e.getMessage}")
        None
    }
  }

  def parseStreamIngestMetrics(json: String): Option[StreamIngestMetrics] = {
    try {
      val parsed = parse(json)
      val data = parsed \ "data"
      val stats = data \ "stats"
      val trackStats = (stats \ "track_stats").extract[List[JObject]]

      val videoTrack = trackStats.find(t => (t \ "type").extractOrElse[String]("") == "video")
      val audioTrack = trackStats.find(t => (t \ "type").extractOrElse[String]("") == "audio")
      val peerConnStats = stats \ "peer_conn_stats"

      Some(StreamIngestMetrics(
        eventTimestamp = (parsed \ "timestamp").extractOrElse[Long](System.currentTimeMillis()),
        streamId = (data \ "stream_id").extractOrElse[String]("unknown"),
        requestId = (data \ "request_id").extractOrElse[String]("unknown"),
        gateway = (parsed \ "gateway").extractOrElse[String](""),
        orchestratorAddress = None,
        pipelineId = (data \ "pipeline_id").extractOrElse[String](""),
        connectionQuality = (stats \ "conn_quality").extractOrElse[String]("unknown"),
        videoJitter = videoTrack.flatMap(t => (t \ "jitter").extractOpt[Double]).getOrElse(0.0).toFloat,
        videoPacketsReceived = videoTrack.flatMap(t => (t \ "packets_received").extractOpt[Int]).getOrElse(0),
        videoPacketsLost = videoTrack.flatMap(t => (t \ "packets_lost").extractOpt[Int]).getOrElse(0),
        videoPacketLossPct = videoTrack.flatMap(t => (t \ "packet_loss_pct").extractOpt[Double]).getOrElse(0.0).toFloat,
        audioJitter = audioTrack.flatMap(t => (t \ "jitter").extractOpt[Double]).getOrElse(0.0).toFloat,
        audioPacketsReceived = audioTrack.flatMap(t => (t \ "packets_received").extractOpt[Int]).getOrElse(0),
        audioPacketsLost = audioTrack.flatMap(t => (t \ "packets_lost").extractOpt[Int]).getOrElse(0),
        audioPacketLossPct = audioTrack.flatMap(t => (t \ "packet_loss_pct").extractOpt[Double]).getOrElse(0.0).toFloat,
        bytesReceived = (peerConnStats \ "BytesReceived").extractOrElse[Long](0),
        bytesSent = (peerConnStats \ "BytesSent").extractOrElse[Long](0)
      ))
    } catch {
      case e: Exception =>
        System.err.println(s"ERROR parsing stream_ingest_metrics: ${e.getMessage}")
        None
    }
  }

  def parseStreamTraceEvent(json: String): Option[StreamTraceEvent] = {
    try {
      val parsed = parse(json)
      val data = parsed \ "data"

      Some(StreamTraceEvent(
        eventTimestamp = (parsed \ "timestamp").extractOrElse[Long](System.currentTimeMillis()),
        streamId = (data \ "stream_id").extractOrElse[String]("unknown"),
        requestId = (data \ "request_id").extractOrElse[String]("unknown"),
        gateway = (parsed \ "gateway").extractOrElse[String](""),
        orchestratorAddress = (data \ "orchestrator_info" \ "address").extractOrElse[String](""),
        orchestratorUrl = (data \ "orchestrator_info" \ "url").extractOrElse[String](""),
        pipelineId = (data \ "pipeline_id").extractOrElse[String](""),
        traceType = (data \ "type").extractOrElse[String]("unknown"),
        dataTimestamp = (data \ "timestamp").extractOrElse[Long](System.currentTimeMillis())
      ))
    } catch {
      case e: Exception =>
        System.err.println(s"ERROR parsing stream_trace: ${e.getMessage}")
        None
    }
  }

  // ClickHouse sink creators
  def createAiStreamStatusSink() = {
    JdbcSink.sink(
      """INSERT INTO ai_stream_status (
        event_timestamp, stream_id, request_id, gateway,
        orchestrator_address, orchestrator_url, gpu_id, region,
        pipeline, pipeline_id, output_fps, input_fps,
        state, restart_count, last_error,
        prompt_text, prompt_width, prompt_height, params_hash
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
      new JdbcStatementBuilder[AiStreamStatus] {
        override def accept(ps: PreparedStatement, s: AiStreamStatus): Unit = {
          ps.setTimestamp(1, new Timestamp(s.eventTimestamp))
          ps.setString(2, s.streamId)
          ps.setString(3, s.requestId)
          ps.setString(4, s.gateway)
          ps.setString(5, s.orchestratorAddress)
          ps.setString(6, s.orchestratorUrl)
          ps.setString(7, s.gpuId.orNull)
          ps.setString(8, s.region.orNull)
          ps.setString(9, s.pipeline)
          ps.setString(10, s.pipelineId)
          ps.setFloat(11, s.outputFps)
          ps.setFloat(12, s.inputFps)
          ps.setString(13, s.state)
          ps.setInt(14, s.restartCount)
          ps.setString(15, s.lastError.orNull)
          ps.setString(16, s.promptText.orNull)
          ps.setInt(17, s.promptWidth)
          ps.setInt(18, s.promptHeight)
          ps.setString(19, s.paramsHash)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(100)
        .withBatchIntervalMs(5000)
        .withMaxRetries(3)
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:clickhouse://clickhouse:8123/livepeer_analytics")
        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
        .withUsername("analytics_user")
        .withPassword("analytics_password")
        .build()
    )
  }

  def createStreamIngestMetricsSink() = {
    JdbcSink.sink(
      """INSERT INTO stream_ingest_metrics (
        event_timestamp, stream_id, request_id, gateway,
        orchestrator_address, pipeline_id, connection_quality,
        video_jitter, video_packets_received, video_packets_lost, video_packet_loss_pct,
        audio_jitter, audio_packets_received, audio_packets_lost, audio_packet_loss_pct,
        bytes_received, bytes_sent
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
      new JdbcStatementBuilder[StreamIngestMetrics] {
        override def accept(ps: PreparedStatement, s: StreamIngestMetrics): Unit = {
          ps.setTimestamp(1, new Timestamp(s.eventTimestamp))
          ps.setString(2, s.streamId)
          ps.setString(3, s.requestId)
          ps.setString(4, s.gateway)
          ps.setString(5, s.orchestratorAddress.orNull)
          ps.setString(6, s.pipelineId)
          ps.setString(7, s.connectionQuality)
          ps.setFloat(8, s.videoJitter)
          ps.setInt(9, s.videoPacketsReceived)
          ps.setInt(10, s.videoPacketsLost)
          ps.setFloat(11, s.videoPacketLossPct)
          ps.setFloat(12, s.audioJitter)
          ps.setInt(13, s.audioPacketsReceived)
          ps.setInt(14, s.audioPacketsLost)
          ps.setFloat(15, s.audioPacketLossPct)
          ps.setLong(16, s.bytesReceived)
          ps.setLong(17, s.bytesSent)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(100)
        .withBatchIntervalMs(5000)
        .withMaxRetries(3)
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:clickhouse://clickhouse:8123/livepeer_analytics")
        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
        .withUsername("analytics_user")
        .withPassword("analytics_password")
        .build()
    )
  }

  def createStreamTraceEventsSink() = {
    JdbcSink.sink(
      """INSERT INTO stream_trace_events (
        event_timestamp, stream_id, request_id, gateway,
        orchestrator_address, orchestrator_url, pipeline_id,
        trace_type, data_timestamp
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
      new JdbcStatementBuilder[StreamTraceEvent] {
        override def accept(ps: PreparedStatement, s: StreamTraceEvent): Unit = {
          ps.setTimestamp(1, new Timestamp(s.eventTimestamp))
          ps.setString(2, s.streamId)
          ps.setString(3, s.requestId)
          ps.setString(4, s.gateway)
          ps.setString(5, s.orchestratorAddress)
          ps.setString(6, s.orchestratorUrl)
          ps.setString(7, s.pipelineId)
          ps.setString(8, s.traceType)
          ps.setTimestamp(9, new Timestamp(s.dataTimestamp))
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(100)
        .withBatchIntervalMs(5000)
        .withMaxRetries(3)
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:clickhouse://clickhouse:8123/livepeer_analytics")
        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
        .withUsername("analytics_user")
        .withPassword("analytics_password")
        .build()
    )
  }
}