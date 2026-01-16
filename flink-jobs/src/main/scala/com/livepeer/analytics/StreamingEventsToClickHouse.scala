package com.livepeer.analytics

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.AvroParquetWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.time.Duration
import java.sql.{PreparedStatement, Timestamp}

object StreamingEventsToClickHouse {

  // ============================================
  // CASE CLASSES
  // ============================================

  case class StreamingEvent(
    eventId: String,
    eventType: String,
    timestamp: Long,
    gateway: String,
    rawJson: String
  )

  case class OrchestratorMetadata(
    orchestratorAddress: String,  // REAL address (from "address" field)
    localAddress: String,
    orchUri: String,              // CRITICAL: Each URI = different GPU!
    gpuId: Option[String],
    gpuName: Option[String],
    modelId: String,
    lastUpdated: Long
  )

  case class AiStreamStatus(
    eventTimestamp: Timestamp,
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
    eventTimestamp: Timestamp,
    streamId: String,
    requestId: String,
    gateway: String,
    orchestratorAddress: Option[String],
    pipelineId: String,
    connectionQuality: String,
    videoJitter: Float,
    videoPacketsReceived: Long,
    videoPacketsLost: Long,
    videoPacketLossPct: Float,
    audioJitter: Float,
    audioPacketsReceived: Long,
    audioPacketsLost: Long,
    audioPacketLossPct: Float,
    bytesReceived: Long,
    bytesSent: Long
  )

  case class StreamTraceEvent(
    eventTimestamp: Timestamp,
    streamId: String,
    requestId: String,
    gateway: String,
    orchestratorAddress: String,
    orchestratorUrl: String,
    pipelineId: String,
    traceType: String,
    dataTimestamp: Timestamp
  )

  case class NetworkCapability(
    eventTimestamp: Timestamp,
    orchestratorAddress: String,
    localAddress: String,
    orchUri: String,
    gpuId: Option[String],
    gpuName: Option[String],
    gpuMemoryTotal: Option[Long],
    gpuMemoryFree: Option[Long],
    gpuMajor: Option[Int],
    pipeline: String,
    modelId: String,
    runnerVersion: Option[String],
    capacity: Option[Int],
    capacityInUse: Option[Int],
    pricePerUnit: Option[Int],
    orchestratorVersion: String,
    rawJson: String
  )

  case class AiStreamEvent(
    eventTimestamp: Timestamp,
    streamId: String,
    requestId: String,
    gateway: String,
    pipeline: String,
    pipelineId: String,
    eventType: String,
    message: String,
    capability: String
  )

  case class DiscoveryResult(
    eventTimestamp: Timestamp,
    orchestratorAddress: String,
    orchestratorUrl: String,
    latencyMs: Int,
    gateway: String
  )

  case class PaymentEvent(
    eventTimestamp: Timestamp,
    requestId: String,
    sessionId: String,
    manifestId: String,
    sender: String,
    recipient: String,
    orchestrator: String,
    faceValue: String,
    price: String,
    numTickets: String,
    winProb: String,
    gateway: String,
    clientIp: String,
    capability: String
  )

  // ============================================
  // MAIN
  // ============================================

  def main(args: Array[String]): Unit = {
    // This will now correctly use the Scala StreamExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000)

    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers("kafka:9092")
      .setTopics("streaming-events")
      .setGroupId("flink-streaming-events-consumer")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val rawStream = env
      .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    // This will now work without type erasure errors
    val parsedStream = rawStream
      .map(s => parseEvent(s))
      .filter(_.isDefined)
      .map(_.get)

    // ============================================
    // STEP 1: Extract & Store Network Capabilities
    // ============================================

    val networkCapabilities = parsedStream
      .filter(_.eventType == "network_capabilities")
      .flatMap(new FlatMapFunction[StreamingEvent, NetworkCapability] {
        override def flatMap(event: StreamingEvent, out: Collector[NetworkCapability]): Unit = {
          toNetworkCapabilities(event).foreach(out.collect)
        }
      })

    networkCapabilities
      .addSink(createNetworkCapabilitiesSink())
      .name("ClickHouse: network_capabilities")

    // ============================================
    // STEP 2: Build Broadcast State
    // ============================================

    val metadataStateDescriptor = new MapStateDescriptor[String, OrchestratorMetadata](
      "orchestrator-metadata",
      classOf[String],
      classOf[OrchestratorMetadata]
    )

    val orchestratorMetadataStream = networkCapabilities
      .map(toOrchestratorMetadata(_))

    val broadcastStream: BroadcastStream[OrchestratorMetadata] =
      orchestratorMetadataStream.broadcast(metadataStateDescriptor)

    // ============================================
    // STEP 3: Enrich AI Stream Status
    // ============================================

    val aiStreamStatusRaw = parsedStream
      .filter(_.eventType == "ai_stream_status")
      .map(toAiStreamStatusRaw(_))
      .filter(_.isDefined)
      .map(_.get)

    val enrichedAiStreamStatus = aiStreamStatusRaw
      .connect(broadcastStream)
      .process(new EnrichAiStreamStatusFunction(metadataStateDescriptor))

    enrichedAiStreamStatus
      .addSink(createAiStreamStatusSink())
      .name("ClickHouse: ai_stream_status")

    // ============================================
    // STEP 4: Other Event Types
    // ============================================

    parsedStream
      .filter(_.eventType == "stream_ingest_metrics")
      .map(toStreamIngestMetrics(_))
      .filter(_.isDefined)
      .map(_.get)
      .addSink(createStreamIngestMetricsSink())
      .name("ClickHouse: stream_ingest_metrics")

    parsedStream
      .filter(_.eventType == "stream_trace")
      .map(toStreamTraceEvent(_))
      .filter(_.isDefined)
      .map(_.get)
      .addSink(createStreamTraceEventsSink())
      .name("ClickHouse: stream_trace_events")

    parsedStream
      .filter(_.eventType == "ai_stream_events")
      .map(toAiStreamEvent(_))
      .filter(_.isDefined)
      .map(_.get)
      .addSink(createAiStreamEventsSink())
      .name("ClickHouse: ai_stream_events")

    parsedStream
      .filter(_.eventType == "discovery_results")
      .flatMap(new FlatMapFunction[StreamingEvent, DiscoveryResult] {
        override def flatMap(event: StreamingEvent, out: Collector[DiscoveryResult]): Unit = {
          toDiscoveryResults(event).foreach(out.collect)
        }
      })
      .addSink(createDiscoveryResultsSink())
      .name("ClickHouse: discovery_results")

    parsedStream
      .filter(_.eventType == "create_new_payment")
      .map(toPaymentEvent(_))
      .filter(_.isDefined)
      .map(_.get)
      .addSink(createPaymentEventsSink())
      .name("ClickHouse: payment_events")

    // ============================================
    // STEP 5: MinIO Parquet Archive
    // ============================================
    val rollingPolicy = org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
          .build[GenericRecord, String]()

        val parquetSink = FileSink
          .forBulkFormat(
            new Path("s3://livepeer-events/raw/"),
            AvroParquetWriters.forGenericRecord(createAvroSchema())
          )
          .withRollingPolicy(rollingPolicy)
          .withBucketAssigner(new DateTimeBucketAssigner[GenericRecord]("yyyy/MM/dd"))
          .build()

    parsedStream
      .map(toAvroRecord(_))
      .sinkTo(parquetSink)
      .name("MinIO: Parquet Archive")

    env.execute("Streaming Events to ClickHouse + MinIO")
  }

  // ============================================
  // BROADCAST ENRICHMENT FUNCTION
  // ============================================

  class EnrichAiStreamStatusFunction(
    metadataDescriptor: MapStateDescriptor[String, OrchestratorMetadata]
  ) extends BroadcastProcessFunction[AiStreamStatus, OrchestratorMetadata, AiStreamStatus] {

    override def processElement(
      status: AiStreamStatus,
      ctx: BroadcastProcessFunction[AiStreamStatus, OrchestratorMetadata, AiStreamStatus]#ReadOnlyContext,
      out: Collector[AiStreamStatus]
    ): Unit = {
      val metadataState = ctx.getBroadcastState(metadataDescriptor)

      // CRITICAL: Match on orchestrator_address + orchestrator_url + pipeline
      // Each orch_uri = different GPU, so we MUST include URL in lookup
      val key = s"${status.orchestratorAddress}:${status.orchestratorUrl}:${status.pipeline}"
      val metadata = Option(metadataState.get(key))

      // Enrich and emit
      out.collect(status.copy(
        gpuId = metadata.flatMap(_.gpuId),
        region = metadata.flatMap(_ => inferRegion(status.orchestratorUrl))
      ))
    }

    override def processBroadcastElement(
      metadata: OrchestratorMetadata,
      ctx: BroadcastProcessFunction[AiStreamStatus, OrchestratorMetadata, AiStreamStatus]#Context,
      out: Collector[AiStreamStatus]
    ): Unit = {
      val metadataState = ctx.getBroadcastState(metadataDescriptor)

      // CRITICAL: Key must include orch_uri since each URI = different GPU
      val key = s"${metadata.orchestratorAddress}:${metadata.orchUri}:${metadata.modelId}"
      metadataState.put(key, metadata)
    }
  }

  def inferRegion(orchUrl: String): Option[String] = {
    val url = orchUrl.toLowerCase
    if (url.contains("us-east") || url.contains("xodeapp")) Some("us-east-1")
    else if (url.contains("eu-west") || url.contains("europe")) Some("eu-west-1")
    else if (url.contains("asia") || url.contains("ap-")) Some("ap-southeast-1")
    else None
  }

  // ============================================
  // PARSING FUNCTIONS
  // ============================================

  def parseEvent(json: String): Option[StreamingEvent] = {
    try {
      implicit val formats = DefaultFormats
      val parsed = parse(json)
      Some(StreamingEvent(
        eventId = (parsed \ "id").extract[String],
        eventType = (parsed \ "type").extract[String],
        timestamp = (parsed \ "timestamp").extract[String].toLong,
        gateway = (parsed \ "gateway").extractOrElse[String](""),
        rawJson = json
      ))
    } catch {
      case e: Exception =>
        System.err.println(s"Failed to parse event: ${e.getMessage}")
        None
    }
  }

  def toAiStreamStatusRaw(event: StreamingEvent): Option[AiStreamStatus] = {
    try {
      implicit val formats = DefaultFormats
      val data = parse(event.rawJson) \ "data"
      val inferenceStatus = data \ "inference_status"
      val inputStatus = data \ "input_status"
      val orchestratorInfo = data \ "orchestrator_info"
      val lastParams = inferenceStatus \ "last_params"

      Some(AiStreamStatus(
        eventTimestamp = new Timestamp(event.timestamp),
        streamId = (data \ "stream_id").extractOrElse[String](""),
        requestId = (data \ "request_id").extractOrElse[String](""),
        gateway = event.gateway,
        orchestratorAddress = (orchestratorInfo \ "address").extractOrElse[String](""),
        orchestratorUrl = (orchestratorInfo \ "url").extractOrElse[String](""),
        gpuId = None,
        region = None,
        pipeline = (data \ "pipeline").extractOrElse[String](""),
        pipelineId = (data \ "pipeline_id").extractOrElse[String](""),
        outputFps = (inferenceStatus \ "fps").extractOrElse[Double](0.0).toFloat,
        inputFps = (inputStatus \ "fps").extractOrElse[Double](0.0).toFloat,
        state = (data \ "state").extractOrElse[String](""),
        restartCount = (inferenceStatus \ "restart_count").extractOrElse[Int](0),
        lastError = (inferenceStatus \ "last_error").extractOpt[String],
        promptText = (lastParams \ "prompt").extractOpt[String],
        promptWidth = (lastParams \ "width").extractOrElse[Int](0),
        promptHeight = (lastParams \ "height").extractOrElse[Int](0),
        paramsHash = (inferenceStatus \ "last_params_hash").extractOrElse[String]("")
      ))
    } catch {
      case e: Exception =>
        System.err.println(s"Failed to parse ai_stream_status: ${e.getMessage}")
        None
    }
  }

  def toNetworkCapabilities(event: StreamingEvent): List[NetworkCapability] = {
    try {
      implicit val formats = DefaultFormats
      val data = (parse(event.rawJson) \ "data").extract[List[JObject]]

      data.flatMap { orch =>
        val hardwareList = (orch \ "hardware").extract[List[JObject]]

        hardwareList.map { hw =>
          val gpuInfo = (hw \ "gpu_info" \ "0").extractOpt[JObject]
          val modelId = (hw \ "model_id").extractOrElse[String]("")
          val constraints = orch \ "capabilities" \ "constraints" \ "PerCapability" \ "35" \ "models"
          val modelInfo = (constraints \ modelId).extractOpt[JObject]
          val pricesArray = (orch \ "capabilities_prices").extract[List[JObject]]
          val priceInfo = pricesArray.find(p => (p \ "constraint").extractOrElse[String]("") == modelId)

          NetworkCapability(
            eventTimestamp = new Timestamp(event.timestamp),
            orchestratorAddress = (orch \ "address").extractOrElse[String](""),
            localAddress = (orch \ "local_address").extractOrElse[String](""),
            orchUri = (orch \ "orch_uri").extractOrElse[String](""),
            gpuId = gpuInfo.flatMap(g => (g \ "id").extractOpt[String]),
            gpuName = gpuInfo.flatMap(g => (g \ "name").extractOpt[String]),
            gpuMemoryTotal = gpuInfo.flatMap(g => (g \ "memory_total").extractOpt[Long]),
            gpuMemoryFree = gpuInfo.flatMap(g => (g \ "memory_free").extractOpt[Long]),
            gpuMajor = gpuInfo.flatMap(g => (g \ "major").extractOpt[Int]),
            pipeline = (hw \ "pipeline").extractOrElse[String](""),
            modelId = modelId,
            runnerVersion = modelInfo.flatMap(m => (m \ "runnerVersion").extractOpt[String]),
            capacity = modelInfo.flatMap(m => (m \ "capacity").extractOpt[Int]),
            capacityInUse = modelInfo.flatMap(m => (m \ "capacityInUse").extractOpt[Int]),
            pricePerUnit = priceInfo.flatMap(p => (p \ "pricePerUnit").extractOpt[Int]),
            orchestratorVersion = (orch \ "capabilities" \ "version").extractOrElse[String](""),
            rawJson = compact(render(orch))
          )
        }
      }
    } catch {
      case e: Exception =>
        System.err.println(s"Failed to parse network_capabilities: ${e.getMessage}")
        List.empty
    }
  }

  def toOrchestratorMetadata(cap: NetworkCapability): OrchestratorMetadata = {
    OrchestratorMetadata(
      orchestratorAddress = cap.orchestratorAddress,
      localAddress = cap.localAddress,
      orchUri = cap.orchUri,
      gpuId = cap.gpuId,
      gpuName = cap.gpuName,
      modelId = cap.modelId,
      lastUpdated = System.currentTimeMillis()
    )
  }

  def toStreamIngestMetrics(event: StreamingEvent): Option[StreamIngestMetrics] = {
    try {
      implicit val formats = DefaultFormats
      val data = parse(event.rawJson) \ "data"
      val stats = data \ "stats"
      val trackStats = (stats \ "track_stats").extract[List[JObject]]
      val videoTrack = trackStats.find(t => (t \ "type").extractOrElse[String]("") == "video")
      val audioTrack = trackStats.find(t => (t \ "type").extractOrElse[String]("") == "audio")
      val peerConnStats = stats \ "peer_conn_stats"

      Some(StreamIngestMetrics(
        eventTimestamp = new Timestamp(event.timestamp),
        streamId = (data \ "stream_id").extractOrElse[String](""),
        requestId = (data \ "request_id").extractOrElse[String](""),
        gateway = event.gateway,
        orchestratorAddress = None,
        pipelineId = (data \ "pipeline_id").extractOrElse[String](""),
        connectionQuality = (stats \ "conn_quality").extractOrElse[String](""),
        videoJitter = videoTrack.map(v => (v \ "jitter").extractOrElse[Double](0.0).toFloat).getOrElse(0f),
        videoPacketsReceived = videoTrack.map(v => (v \ "packets_received").extractOrElse[Long](0L)).getOrElse(0L),
        videoPacketsLost = videoTrack.map(v => (v \ "packets_lost").extractOrElse[Long](0L)).getOrElse(0L),
        videoPacketLossPct = videoTrack.map(v => (v \ "packet_loss_pct").extractOrElse[Double](0.0).toFloat).getOrElse(0f),
        audioJitter = audioTrack.map(v => (v \ "jitter").extractOrElse[Double](0.0).toFloat).getOrElse(0f),
        audioPacketsReceived = audioTrack.map(v => (v \ "packets_received").extractOrElse[Long](0L)).getOrElse(0L),
        audioPacketsLost = audioTrack.map(v => (v \ "packets_lost").extractOrElse[Long](0L)).getOrElse(0L),
        audioPacketLossPct = audioTrack.map(v => (v \ "packet_loss_pct").extractOrElse[Double](0.0).toFloat).getOrElse(0f),
        bytesReceived = (peerConnStats \ "BytesReceived").extractOrElse[Long](0L),
        bytesSent = (peerConnStats \ "BytesSent").extractOrElse[Long](0L)
      ))
    } catch {
      case e: Exception =>
        System.err.println(s"Failed to parse stream_ingest_metrics: ${e.getMessage}")
        None
    }
  }

  def toStreamTraceEvent(event: StreamingEvent): Option[StreamTraceEvent] = {
    try {
      implicit val formats = DefaultFormats
      val data = parse(event.rawJson) \ "data"
      val orchestratorInfo = data \ "orchestrator_info"

      Some(StreamTraceEvent(
        eventTimestamp = new Timestamp(event.timestamp),
        streamId = (data \ "stream_id").extractOrElse[String](""),
        requestId = (data \ "request_id").extractOrElse[String](""),
        gateway = event.gateway,
        orchestratorAddress = (orchestratorInfo \ "address").extractOrElse[String](""),
        orchestratorUrl = (orchestratorInfo \ "url").extractOrElse[String](""),
        pipelineId = (data \ "pipeline_id").extractOrElse[String](""),
        traceType = (data \ "type").extractOrElse[String](""),
        dataTimestamp = new Timestamp((data \ "timestamp").extractOrElse[Long](event.timestamp))
      ))
    } catch {
      case e: Exception =>
        System.err.println(s"Failed to parse stream_trace: ${e.getMessage}")
        None
    }
  }

  def toAiStreamEvent(event: StreamingEvent): Option[AiStreamEvent] = {
    try {
      implicit val formats = DefaultFormats
      val data = parse(event.rawJson) \ "data"

      Some(AiStreamEvent(
        eventTimestamp = new Timestamp(event.timestamp),
        streamId = (data \ "stream_id").extractOrElse[String](""),
        requestId = (data \ "request_id").extractOrElse[String](""),
        gateway = event.gateway,
        pipeline = (data \ "pipeline").extractOrElse[String](""),
        pipelineId = (data \ "pipeline_id").extractOrElse[String](""),
        eventType = (data \ "type").extractOrElse[String](""),
        message = (data \ "message").extractOrElse[String](""),
        capability = (data \ "capability").extractOrElse[String]("")
      ))
    } catch {
      case e: Exception =>
        System.err.println(s"Failed to parse ai_stream_events: ${e.getMessage}")
        None
    }
  }

  def toDiscoveryResults(event: StreamingEvent): List[DiscoveryResult] = {
    try {
      implicit val formats = DefaultFormats
      val data = (parse(event.rawJson) \ "data").extract[List[JObject]]

      data.map { disc =>
        DiscoveryResult(
          eventTimestamp = new Timestamp(event.timestamp),
          orchestratorAddress = (disc \ "address").extractOrElse[String](""),
          orchestratorUrl = (disc \ "url").extractOrElse[String](""),
          latencyMs = (disc \ "latency_ms").extractOrElse[String]("0").toInt,
          gateway = event.gateway
        )
      }
    } catch {
      case e: Exception =>
        System.err.println(s"Failed to parse discovery_results: ${e.getMessage}")
        List.empty
    }
  }

  def toPaymentEvent(event: StreamingEvent): Option[PaymentEvent] = {
    try {
      implicit val formats = DefaultFormats
      val data = parse(event.rawJson) \ "data"

      Some(PaymentEvent(
        eventTimestamp = new Timestamp(event.timestamp),
        requestId = (data \ "requestID").extractOrElse[String](""),
        sessionId = (data \ "sessionID").extractOrElse[String](""),
        manifestId = (data \ "manifestID").extractOrElse[String](""),
        sender = (data \ "sender").extractOrElse[String](""),
        recipient = (data \ "recipient").extractOrElse[String](""),
        orchestrator = (data \ "orchestrator").extractOrElse[String](""),
        faceValue = (data \ "faceValue").extractOrElse[String](""),
        price = (data \ "price").extractOrElse[String](""),
        numTickets = (data \ "numTickets").extractOrElse[String](""),
        winProb = (data \ "winProb").extractOrElse[String](""),
        gateway = event.gateway,
        clientIp = (data \ "clientIP").extractOrElse[String](""),
        capability = (data \ "capability").extractOrElse[String]("")
      ))
    } catch {
      case e: Exception =>
        System.err.println(s"Failed to parse create_new_payment: ${e.getMessage}")
        None
    }
  }

  // ============================================
  // CLICKHOUSE SINKS
  // ============================================

  def createAiStreamStatusSink() = {
    val insertSQL =
      """INSERT INTO ai_stream_status (
        |  event_timestamp, stream_id, request_id, gateway,
        |  orchestrator_address, orchestrator_url, gpu_id, region,
        |  pipeline, pipeline_id, output_fps, input_fps, state, restart_count,
        |  last_error, prompt_text, prompt_width, prompt_height, params_hash
        |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin

    JdbcSink.sink(
      insertSQL,
      new JdbcStatementBuilder[AiStreamStatus] {
        override def accept(ps: PreparedStatement, e: AiStreamStatus): Unit = {
          ps.setTimestamp(1, e.eventTimestamp)
          ps.setString(2, e.streamId)
          ps.setString(3, e.requestId)
          ps.setString(4, e.gateway)
          ps.setString(5, e.orchestratorAddress)
          ps.setString(6, e.orchestratorUrl)
          ps.setString(7, e.gpuId.orNull)
          ps.setString(8, e.region.orNull)
          ps.setString(9, e.pipeline)
          ps.setString(10, e.pipelineId)
          ps.setFloat(11, e.outputFps)
          ps.setFloat(12, e.inputFps)
          ps.setString(13, e.state)
          ps.setInt(14, e.restartCount)
          ps.setString(15, e.lastError.orNull)
          ps.setString(16, e.promptText.orNull)
          ps.setInt(17, e.promptWidth)
          ps.setInt(18, e.promptHeight)
          ps.setString(19, e.paramsHash)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(1000)
        .withBatchIntervalMs(200)
        .withMaxRetries(3)
        .build(),
      getClickHouseConnectionOptions()
    )
  }

  def createStreamIngestMetricsSink() = {
    val insertSQL =
      """INSERT INTO stream_ingest_metrics (
        |  event_timestamp, stream_id, request_id, gateway, orchestrator_address, pipeline_id,
        |  connection_quality,
        |  video_jitter, video_packets_received, video_packets_lost, video_packet_loss_pct,
        |  audio_jitter, audio_packets_received, audio_packets_lost, audio_packet_loss_pct,
        |  bytes_received, bytes_sent
        |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin

    JdbcSink.sink(
      insertSQL,
      new JdbcStatementBuilder[StreamIngestMetrics] {
        override def accept(ps: PreparedStatement, e: StreamIngestMetrics): Unit = {
          ps.setTimestamp(1, e.eventTimestamp)
          ps.setString(2, e.streamId)
          ps.setString(3, e.requestId)
          ps.setString(4, e.gateway)
          ps.setString(5, e.orchestratorAddress.orNull)
          ps.setString(6, e.pipelineId)
          ps.setString(7, e.connectionQuality)
          ps.setFloat(8, e.videoJitter)
          ps.setLong(9, e.videoPacketsReceived)
          ps.setLong(10, e.videoPacketsLost)
          ps.setFloat(11, e.videoPacketLossPct)
          ps.setFloat(12, e.audioJitter)
          ps.setLong(13, e.audioPacketsReceived)
          ps.setLong(14, e.audioPacketsLost)
          ps.setFloat(15, e.audioPacketLossPct)
          ps.setLong(16, e.bytesReceived)
          ps.setLong(17, e.bytesSent)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(1000)
        .withBatchIntervalMs(200)
        .withMaxRetries(3)
        .build(),
      getClickHouseConnectionOptions()
    )
  }

  def createStreamTraceEventsSink() = {
    val insertSQL =
      """INSERT INTO stream_trace_events (
        |  event_timestamp, stream_id, request_id, gateway,
        |  orchestrator_address, orchestrator_url, pipeline_id,
        |  trace_type, data_timestamp
        |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin

    JdbcSink.sink(
      insertSQL,
      new JdbcStatementBuilder[StreamTraceEvent] {
        override def accept(ps: PreparedStatement, e: StreamTraceEvent): Unit = {
          ps.setTimestamp(1, e.eventTimestamp)
          ps.setString(2, e.streamId)
          ps.setString(3, e.requestId)
          ps.setString(4, e.gateway)
          ps.setString(5, e.orchestratorAddress)
          ps.setString(6, e.orchestratorUrl)
          ps.setString(7, e.pipelineId)
          ps.setString(8, e.traceType)
          ps.setTimestamp(9, e.dataTimestamp)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(1000)
        .withBatchIntervalMs(200)
        .withMaxRetries(3)
        .build(),
      getClickHouseConnectionOptions()
    )
  }

  def createNetworkCapabilitiesSink() = {
    val insertSQL =
      """INSERT INTO network_capabilities (
        |  event_timestamp, orchestrator_address, local_address, orch_uri,
        |  gpu_id, gpu_name, gpu_memory_total, gpu_memory_free, gpu_major,
        |  pipeline, model_id, runner_version, capacity, capacity_in_use,
        |  price_per_unit, orchestrator_version, raw_json
        |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin

    JdbcSink.sink(
      insertSQL,
      new JdbcStatementBuilder[NetworkCapability] {
        override def accept(ps: PreparedStatement, e: NetworkCapability): Unit = {
          ps.setTimestamp(1, e.eventTimestamp)
          ps.setString(2, e.orchestratorAddress)
          ps.setString(3, e.localAddress)
          ps.setString(4, e.orchUri)
          ps.setString(5, e.gpuId.orNull)
          ps.setString(6, e.gpuName.orNull)
          ps.setObject(7, e.gpuMemoryTotal.map(Long.box).orNull)
          ps.setObject(8, e.gpuMemoryFree.map(Long.box).orNull)
          ps.setObject(9, e.gpuMajor.map(Int.box).orNull)
          ps.setString(10, e.pipeline)
          ps.setString(11, e.modelId)
          ps.setString(12, e.runnerVersion.orNull)
          ps.setObject(13, e.capacity.map(Int.box).orNull)
          ps.setObject(14, e.capacityInUse.map(Int.box).orNull)
          ps.setObject(15, e.pricePerUnit.map(Int.box).orNull)
          ps.setString(16, e.orchestratorVersion)
          ps.setString(17, e.rawJson)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(100)
        .withBatchIntervalMs(500)
        .withMaxRetries(3)
        .build(),
      getClickHouseConnectionOptions()
    )
  }

  def createAiStreamEventsSink() = {
    val insertSQL =
      """INSERT INTO ai_stream_events (
        |  event_timestamp, stream_id, request_id, gateway,
        |  pipeline, pipeline_id, event_type, message, capability
        |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin

    JdbcSink.sink(
      insertSQL,
      new JdbcStatementBuilder[AiStreamEvent] {
        override def accept(ps: PreparedStatement, e: AiStreamEvent): Unit = {
          ps.setTimestamp(1, e.eventTimestamp)
          ps.setString(2, e.streamId)
          ps.setString(3, e.requestId)
          ps.setString(4, e.gateway)
          ps.setString(5, e.pipeline)
          ps.setString(6, e.pipelineId)
          ps.setString(7, e.eventType)
          ps.setString(8, e.message)
          ps.setString(9, e.capability)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(500)
        .withBatchIntervalMs(200)
        .withMaxRetries(3)
        .build(),
      getClickHouseConnectionOptions()
    )
  }

  def createDiscoveryResultsSink() = {
    val insertSQL =
      """INSERT INTO discovery_results (
        |  event_timestamp, orchestrator_address, orchestrator_url, latency_ms, gateway
        |) VALUES (?, ?, ?, ?, ?)
        |""".stripMargin

    JdbcSink.sink(
      insertSQL,
      new JdbcStatementBuilder[DiscoveryResult] {
        override def accept(ps: PreparedStatement, e: DiscoveryResult): Unit = {
          ps.setTimestamp(1, e.eventTimestamp)
          ps.setString(2, e.orchestratorAddress)
          ps.setString(3, e.orchestratorUrl)
          ps.setInt(4, e.latencyMs)
          ps.setString(5, e.gateway)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(100)
        .withBatchIntervalMs(500)
        .withMaxRetries(3)
        .build(),
      getClickHouseConnectionOptions()
    )
  }

  def createPaymentEventsSink() = {
    val insertSQL =
      """INSERT INTO payment_events (
        |  event_timestamp, request_id, session_id, manifest_id,
        |  sender, recipient, orchestrator, face_value, price, num_tickets, win_prob,
        |  gateway, client_ip, capability
        |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin

    JdbcSink.sink(
      insertSQL,
      new JdbcStatementBuilder[PaymentEvent] {
        override def accept(ps: PreparedStatement, e: PaymentEvent): Unit = {
          ps.setTimestamp(1, e.eventTimestamp)
          ps.setString(2, e.requestId)
          ps.setString(3, e.sessionId)
          ps.setString(4, e.manifestId)
          ps.setString(5, e.sender)
          ps.setString(6, e.recipient)
          ps.setString(7, e.orchestrator)
          ps.setString(8, e.faceValue)
          ps.setString(9, e.price)
          ps.setString(10, e.numTickets)
          ps.setString(11, e.winProb)
          ps.setString(12, e.gateway)
          ps.setString(13, e.clientIp)
          ps.setString(14, e.capability)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(500)
        .withBatchIntervalMs(200)
        .withMaxRetries(3)
        .build(),
      getClickHouseConnectionOptions()
    )
  }

  def getClickHouseConnectionOptions(): JdbcConnectionOptions = {
    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl("jdbc:clickhouse://clickhouse:8123/livepeer_analytics")
      .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
      .withUsername("analytics_user")
      .withPassword("analytics_password")
      .build()
  }

  // ============================================
  // AVRO/PARQUET
  // ============================================

  def createAvroSchema(): Schema = {
    new Schema.Parser().parse(
      """{"type":"record","name":"StreamingEvent","namespace":"com.livepeer.analytics",
        |"fields":[
        |  {"name":"event_id","type":"string"},
        |  {"name":"event_type","type":"string"},
        |  {"name":"timestamp","type":"long"},
        |  {"name":"gateway","type":"string"},
        |  {"name":"raw_json","type":"string"}
        |]}""".stripMargin
    )
  }

  def toAvroRecord(event: StreamingEvent): GenericRecord = {
    val record = new org.apache.avro.generic.GenericData.Record(createAvroSchema())
    record.put("event_id", event.eventId)
    record.put("event_type", event.eventType)
    record.put("timestamp", event.timestamp)
    record.put("gateway", event.gateway)
    record.put("raw_json", event.rawJson)
    record
  }
}