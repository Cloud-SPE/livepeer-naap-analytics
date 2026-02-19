package com.livepeer.analytics.sink;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.RejectedEventEnvelope;

/**
 * Maps parsed event payloads into ClickHouse CSV rows that match the table schemas.
 */
public final class ClickHouseRowMappers {
    private ClickHouseRowMappers() {}

    public static String aiStreamStatusRow(EventPayloads.AiStreamStatus s) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("event_timestamp", s.eventTimestamp)
                .addString("stream_id", s.streamId)
                .addString("request_id", s.requestId)
                .addString("gateway", s.gateway)
                .addString("orchestrator_address", s.orchestratorAddress)
                .addString("orchestrator_url", s.orchestratorUrl)
                .addString("pipeline", s.pipeline)
                .addFloat("output_fps", s.outputFps)
                .addFloat("input_fps", s.inputFps)
                .addString("state", s.state)
                .addInt("restart_count", s.restartCount)
                .addNullableString("last_error", s.lastError)
                .addNullableTimestampMillis("last_error_time", s.lastErrorTime)
                .addNullableString("prompt_text", s.promptText)
                .addInt("prompt_width", s.promptWidth)
                .addInt("prompt_height", s.promptHeight)
                .addString("params_hash", s.paramsHash)
                .addNullableTimestampMillis("start_time", s.startTime)
                .addString("raw_json", s.rawJson)
                .build();
    }

    public static String streamIngestMetricsRow(EventPayloads.StreamIngestMetrics m) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("event_timestamp", m.eventTimestamp)
                .addString("stream_id", m.streamId)
                .addString("request_id", m.requestId)
                .addString("connection_quality", m.connectionQuality)
                .addFloat("video_jitter", m.videoJitter)
                .addInt("video_packets_received", m.videoPacketsReceived)
                .addInt("video_packets_lost", m.videoPacketsLost)
                .addFloat("video_packet_loss_pct", m.videoPacketLossPct)
                .addFloat("video_rtt", m.videoRtt)
                .addFloat("video_last_input_ts", m.videoLastInputTs)
                .addFloat("video_latency", m.videoLatency)
                .addFloat("audio_jitter", m.audioJitter)
                .addInt("audio_packets_received", m.audioPacketsReceived)
                .addInt("audio_packets_lost", m.audioPacketsLost)
                .addFloat("audio_packet_loss_pct", m.audioPacketLossPct)
                .addFloat("audio_rtt", m.audioRtt)
                .addFloat("audio_last_input_ts", m.audioLastInputTs)
                .addFloat("audio_latency", m.audioLatency)
                .addLong("bytes_received", m.bytesReceived)
                .addLong("bytes_sent", m.bytesSent)
                .addString("raw_json", m.rawJson)
                .build();
    }

    public static String streamTraceRow(EventPayloads.StreamTraceEvent t) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("event_timestamp", t.eventTimestamp)
                .addString("stream_id", t.streamId)
                .addString("request_id", t.requestId)
                .addString("orchestrator_address", t.orchestratorAddress)
                .addString("orchestrator_url", t.orchestratorUrl)
                .addString("trace_type", t.traceType)
                .addTimestampMillis("data_timestamp", t.dataTimestamp)
                .addString("raw_json", t.rawJson)
                .build();
    }

    public static String networkCapabilitiesRow(EventPayloads.NetworkCapability n) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("event_timestamp", n.eventTimestamp)
                .addString("source_event_id", n.sourceEventId)
                .addString("orchestrator_address", n.orchestratorAddress)
                .addString("local_address", n.localAddress)
                .addString("orch_uri", n.orchUri)
                .addNullableString("gpu_id", n.gpuId)
                .addNullableString("gpu_name", n.gpuName)
                .addNullableLong("gpu_memory_total", n.gpuMemoryTotal)
                .addNullableLong("gpu_memory_free", n.gpuMemoryFree)
                .addNullableInt("gpu_major", n.gpuMajor)
                .addNullableInt("gpu_minor", n.gpuMinor)
                .addString("pipeline", n.pipeline)
                .addString("model_id", n.modelId)
                .addNullableInt("capability_id", n.capabilityId)
                .addNullableString("capability_name", n.capabilityName)
                .addNullableString("capability_group", n.capabilityGroup)
                .addNullableString("capability_catalog_version", n.capabilityCatalogVersion)
                .addNullableString("runner_version", n.runnerVersion)
                .addNullableInt("capacity", n.capacity)
                .addNullableInt("capacity_in_use", n.capacityInUse)
                .addNullableInt("warm", n.warm)
                .addNullableInt("price_per_unit", n.pricePerUnit)
                .addNullableInt("pixels_per_unit", n.pixelsPerUnit)
                .addString("orchestrator_version", n.orchestratorVersion)
                .addString("raw_json", n.rawJson)
                .build();
    }

    public static String networkCapabilitiesAdvertisedRow(EventPayloads.NetworkCapabilityAdvertised n) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("event_timestamp", n.eventTimestamp)
                .addString("source_event_id", n.sourceEventId)
                .addString("orchestrator_address", n.orchestratorAddress)
                .addString("local_address", n.localAddress)
                .addString("orch_uri", n.orchUri)
                .addInt("capability_id", n.capabilityId == null ? -1 : n.capabilityId)
                .addString("capability_name", n.capabilityName)
                .addString("capability_group", n.capabilityGroup)
                .addString("capability_catalog_version", n.capabilityCatalogVersion)
                .addNullableInt("capacity", n.capacity)
                .addString("raw_json", n.rawJson)
                .build();
    }

    public static String networkCapabilitiesModelConstraintsRow(EventPayloads.NetworkCapabilityModelConstraint n) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("event_timestamp", n.eventTimestamp)
                .addString("source_event_id", n.sourceEventId)
                .addString("orchestrator_address", n.orchestratorAddress)
                .addString("local_address", n.localAddress)
                .addString("orch_uri", n.orchUri)
                .addInt("capability_id", n.capabilityId == null ? -1 : n.capabilityId)
                .addString("capability_name", n.capabilityName)
                .addString("capability_group", n.capabilityGroup)
                .addString("capability_catalog_version", n.capabilityCatalogVersion)
                .addString("model_id", n.modelId)
                .addNullableString("runner_version", n.runnerVersion)
                .addNullableInt("capacity", n.capacity)
                .addNullableInt("capacity_in_use", n.capacityInUse)
                .addNullableInt("warm", n.warm)
                .addString("raw_json", n.rawJson)
                .build();
    }

    public static String networkCapabilitiesPricesRow(EventPayloads.NetworkCapabilityPrice n) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("event_timestamp", n.eventTimestamp)
                .addString("source_event_id", n.sourceEventId)
                .addString("orchestrator_address", n.orchestratorAddress)
                .addString("local_address", n.localAddress)
                .addString("orch_uri", n.orchUri)
                .addInt("capability_id", n.capabilityId == null ? -1 : n.capabilityId)
                .addString("capability_name", n.capabilityName)
                .addString("capability_group", n.capabilityGroup)
                .addString("capability_catalog_version", n.capabilityCatalogVersion)
                .addNullableString("constraint_name", n.constraint)
                .addNullableInt("price_per_unit", n.pricePerUnit)
                .addNullableInt("pixels_per_unit", n.pixelsPerUnit)
                .addString("raw_json", n.rawJson)
                .build();
    }

    public static String aiStreamEventsRow(EventPayloads.AiStreamEvent e) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("event_timestamp", e.eventTimestamp)
                .addString("stream_id", e.streamId)
                .addString("request_id", e.requestId)
                .addString("pipeline", e.pipeline)
                .addString("event_type", e.eventType)
                .addString("message", e.message)
                .addString("capability", e.capability)
                .addString("raw_json", e.rawJson)
                .build();
    }

    public static String discoveryResultsRow(EventPayloads.DiscoveryResult d) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("event_timestamp", d.eventTimestamp)
                .addString("orchestrator_address", d.orchestratorAddress)
                .addString("orchestrator_url", d.orchestratorUrl)
                .addInt("latency_ms", d.latencyMs)
                .addString("raw_json", d.rawJson)
                .build();
    }

    public static String paymentEventsRow(EventPayloads.PaymentEvent p) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("event_timestamp", p.eventTimestamp)
                .addString("request_id", p.requestId)
                .addString("session_id", p.sessionId)
                .addString("manifest_id", p.manifestId)
                .addString("sender", p.sender)
                .addString("recipient", p.recipient)
                .addString("orchestrator", p.orchestrator)
                .addString("face_value", p.faceValue)
                .addString("price", p.price)
                .addString("num_tickets", p.numTickets)
                .addString("win_prob", p.winProb)
                .addString("client_ip", p.clientIp)
                .addString("capability", p.capability)
                .addString("raw_json", p.rawJson)
                .build();
    }

    public static String factWorkflowSessionsRow(EventPayloads.FactWorkflowSession f) {
        return ClickHouseJsonRow.create()
                .addString("workflow_session_id", f.workflowSessionId)
                .addString("workflow_type", f.workflowType)
                .addString("workflow_id", f.workflowId)
                .addString("stream_id", f.streamId)
                .addString("request_id", f.requestId)
                .addString("session_id", f.sessionId)
                .addString("pipeline", f.pipeline)
                .addString("gateway", f.gateway)
                .addString("orchestrator_address", f.orchestratorAddress)
                .addString("orchestrator_url", f.orchestratorUrl)
                .addNullableString("model_id", f.modelId)
                .addNullableString("gpu_id", f.gpuId)
                .addNullableString("region", f.region)
                .addString("gpu_attribution_method", f.attributionMethod)
                .addFloat("gpu_attribution_confidence", f.attributionConfidence)
                .addTimestampMillis("session_start_ts", f.sessionStartTs)
                .addNullableTimestampMillis("session_end_ts", f.sessionEndTs)
                .addInt("known_stream", f.knownStream)
                .addInt("startup_success", f.startupSuccess)
                .addInt("startup_excused", f.startupExcused)
                .addInt("startup_unexcused", f.startupUnexcused)
                .addInt("swap_count", f.swapCount)
                .addLong("error_count", f.errorCount)
                .addLong("excusable_error_count", f.excusableErrorCount)
                .addNullableTimestampMillis("first_stream_request_ts", f.firstStreamRequestTs)
                .addNullableTimestampMillis("first_processed_ts", f.firstProcessedTs)
                .addNullableTimestampMillis("first_playable_ts", f.firstPlayableTs)
                .addLong("event_count", f.eventCount)
                .addLong("version", f.version)
                .addString("source_first_event_uid", f.sourceFirstEventUid)
                .addString("source_last_event_uid", f.sourceLastEventUid)
                .build();
    }

    public static String factWorkflowSessionSegmentsRow(EventPayloads.FactWorkflowSessionSegment f) {
        return ClickHouseJsonRow.create()
                .addString("workflow_session_id", f.workflowSessionId)
                .addInt("segment_index", f.segmentIndex)
                .addTimestampMillis("segment_start_ts", f.segmentStartTs)
                .addNullableTimestampMillis("segment_end_ts", f.segmentEndTs)
                .addString("gateway", f.gateway)
                .addString("orchestrator_address", f.orchestratorAddress)
                .addString("orchestrator_url", f.orchestratorUrl)
                .addNullableString("worker_id", f.workerId)
                .addNullableString("gpu_id", f.gpuId)
                .addNullableString("model_id", f.modelId)
                .addNullableString("region", f.region)
                .addString("gpu_attribution_method", f.attributionMethod)
                .addFloat("gpu_attribution_confidence", f.attributionConfidence)
                .addString("reason", f.reason)
                .addString("source_trace_type", f.sourceTraceType)
                .addString("source_event_uid", f.sourceEventUid)
                .addLong("version", f.version)
                .build();
    }

    public static String factWorkflowParamUpdatesRow(EventPayloads.FactWorkflowParamUpdate f) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("update_ts", f.updateTs)
                .addString("workflow_session_id", f.workflowSessionId)
                .addString("stream_id", f.streamId)
                .addString("request_id", f.requestId)
                .addString("pipeline", f.pipeline)
                .addString("gateway", f.gateway)
                .addString("orchestrator_address", f.orchestratorAddress)
                .addString("orchestrator_url", f.orchestratorUrl)
                .addNullableString("model_id", f.modelId)
                .addNullableString("gpu_id", f.gpuId)
                .addString("gpu_attribution_method", f.attributionMethod)
                .addFloat("gpu_attribution_confidence", f.attributionConfidence)
                .addString("update_type", f.updateType)
                .addString("message", f.message)
                .addString("source_event_uid", f.sourceEventUid)
                .addLong("version", f.version)
                .build();
    }

    public static String factLifecycleEdgeCoverageRow(EventPayloads.FactLifecycleEdgeCoverage f) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("signal_ts", f.signalTs)
                .addString("workflow_session_id", f.workflowSessionId)
                .addString("stream_id", f.streamId)
                .addString("request_id", f.requestId)
                .addString("pipeline", f.pipeline)
                .addString("gateway", f.gateway)
                .addString("orchestrator_address", f.orchestratorAddress)
                .addString("trace_type", f.traceType)
                .addString("source_event_uid", f.sourceEventUid)
                .addInt("known_stream", f.knownStream)
                .addInt("has_first_processed_edge", f.hasFirstProcessedEdge)
                .addInt("has_first_playable_edge", f.hasFirstPlayableEdge)
                .addInt("startup_edge_matched", f.startupEdgeMatched)
                .addInt("playable_edge_matched", f.playableEdgeMatched)
                .addInt("is_terminal_signal", f.isTerminalSignal)
                .addString("unmatched_reason", f.unmatchedReason)
                .addLong("version", f.version)
                .build();
    }

    public static String factWorkflowLatencySamplesRow(EventPayloads.FactWorkflowLatencySample f) {
        return ClickHouseJsonRow.create()
                .addTimestampMillis("sample_ts", f.sampleTs)
                .addString("workflow_session_id", f.workflowSessionId)
                .addString("stream_id", f.streamId)
                .addString("request_id", f.requestId)
                .addString("gateway", f.gateway)
                .addString("orchestrator_address", f.orchestratorAddress)
                .addString("pipeline", f.pipeline)
                .addNullableString("model_id", f.modelId)
                .addNullableString("gpu_id", f.gpuId)
                .addNullableString("region", f.region)
                .addNullableDouble("prompt_to_first_frame_ms", f.promptToFirstFrameMs)
                .addNullableDouble("startup_time_ms", f.startupTimeMs)
                .addNullableDouble("e2e_latency_ms", f.e2eLatencyMs)
                .addInt("has_prompt_to_first_frame", f.hasPromptToFirstFrame)
                .addInt("has_startup_time", f.hasStartupTime)
                .addInt("has_e2e_latency", f.hasE2eLatency)
                .addString("edge_semantics_version", f.edgeSemanticsVersion)
                .addLong("version", f.version)
                .build();
    }

    public static String dlqRow(RejectedEventEnvelope envelope) {
        long sourceRecordTimestamp = envelope.source == null
                ? envelope.ingestionTimestamp
                : envelope.source.recordTimestamp;
        return ClickHouseJsonRow.create()
                .addString("schema_version", envelope.schemaVersion)
                .addString("source_topic", envelope.source == null ? null : envelope.source.topic)
                .addInt("source_partition", envelope.source == null ? 0 : envelope.source.partition)
                .addLong("source_offset", envelope.source == null ? 0L : envelope.source.offset)
                .addTimestampMillis("source_record_timestamp", sourceRecordTimestamp)
                .addString("event_id", envelope.identity == null ? null : envelope.identity.eventId)
                .addString("event_type", envelope.identity == null ? null : envelope.identity.eventType)
                .addNullableString("event_version", envelope.identity == null ? null : envelope.identity.eventVersion)
                .addNullableTimestampMillis("event_timestamp", envelope.eventTimestamp)
                .addNullableString("dedup_key", envelope.dedupKey)
                .addNullableString("dedup_strategy", envelope.dedupStrategy)
                .addBoolean("replay", envelope.replay)
                .addString("failure_stage", envelope.failure == null ? null : envelope.failure.stage)
                .addString("failure_class", envelope.failure == null ? null : envelope.failure.failureClass)
                .addString("failure_reason", envelope.failure == null ? null : envelope.failure.reason)
                .addString("failure_details", envelope.failure == null ? null : envelope.failure.details)
                .addNullableString("orchestrator", envelope.dimensions == null ? null : envelope.dimensions.orchestrator)
                .addNullableString("broadcaster", envelope.dimensions == null ? null : envelope.dimensions.broadcaster)
                .addNullableString("region", envelope.dimensions == null ? null : envelope.dimensions.region)
                .addString("payload_encoding", envelope.payload == null ? null : envelope.payload.encoding)
                .addString("payload_body", envelope.payload == null ? null : envelope.payload.body)
                .addNullableString("payload_canonical_json", envelope.payload == null ? null : envelope.payload.canonicalJson)
                .addTimestampMillis("ingestion_timestamp", envelope.ingestionTimestamp)
                .build();
    }
}
