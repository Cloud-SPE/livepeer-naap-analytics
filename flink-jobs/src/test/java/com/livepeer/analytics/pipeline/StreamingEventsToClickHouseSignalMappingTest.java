package com.livepeer.analytics.pipeline;

import org.junit.jupiter.api.Test;

import com.livepeer.analytics.lifecycle.LifecycleSignal;
import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.model.ParsedEvent;
import com.livepeer.analytics.model.StreamingEvent;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamingEventsToClickHouseSignalMappingTest {

    @Test
    void traceSignalMappingCarriesGatewayFromSourceEvent() throws Exception {
        StreamingEvent source = new StreamingEvent();
        source.eventId = "evt-trace-1";
        source.gateway = "gateway-a";
        source.timestamp = 1_739_000_000_000L;

        EventPayloads.StreamTraceEvent payload = new EventPayloads.StreamTraceEvent();
        payload.eventTimestamp = 1_739_000_000_100L;
        payload.dataTimestamp = 1_739_000_000_200L;
        payload.streamId = "stream-1";
        payload.requestId = "req-1";
        payload.traceType = "gateway_receive_stream_request";

        LifecycleSignal signal = invokeTraceMapper(new ParsedEvent<>(source, payload));

        assertEquals("gateway-a", signal.gateway);
        assertEquals("stream-1|req-1", signal.workflowSessionId);
    }

    @Test
    void aiEventSignalMappingCarriesGatewayFromSourceEvent() throws Exception {
        StreamingEvent source = new StreamingEvent();
        source.eventId = "evt-ai-1";
        source.gateway = "gateway-b";
        source.timestamp = 1_739_000_001_000L;

        EventPayloads.AiStreamEvent payload = new EventPayloads.AiStreamEvent();
        payload.eventTimestamp = 1_739_000_001_100L;
        payload.streamId = "stream-2";
        payload.requestId = "req-2";
        payload.pipeline = "streamdiffusion-sdxl";
        payload.eventType = "error";
        payload.message = "no orchestrators available within 2s timeout";

        LifecycleSignal signal = invokeAiEventMapper(new ParsedEvent<>(source, payload));

        assertEquals("gateway-b", signal.gateway);
        assertEquals("stream-2|req-2", signal.workflowSessionId);
    }

    @SuppressWarnings("unchecked")
    private static LifecycleSignal invokeTraceMapper(ParsedEvent<EventPayloads.StreamTraceEvent> parsed) throws Exception {
        Method method = StreamingEventsToClickHouse.class.getDeclaredMethod("toLifecycleSignalFromTrace", ParsedEvent.class);
        method.setAccessible(true);
        return (LifecycleSignal) method.invoke(null, parsed);
    }

    @SuppressWarnings("unchecked")
    private static LifecycleSignal invokeAiEventMapper(ParsedEvent<EventPayloads.AiStreamEvent> parsed) throws Exception {
        Method method = StreamingEventsToClickHouse.class.getDeclaredMethod("toLifecycleSignalFromAiEvent", ParsedEvent.class);
        method.setAccessible(true);
        return (LifecycleSignal) method.invoke(null, parsed);
    }
}
