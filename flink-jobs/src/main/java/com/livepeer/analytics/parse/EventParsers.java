package com.livepeer.analytics.parse;

import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.quality.ValidatedEvent;

import java.util.List;

/**
 * Facade entry point for event parsing.
 *
 * <p>Keeps public parsing API stable while delegating to focused parser classes.</p>
 */
public final class EventParsers {
    private EventParsers() {}

    public static List<EventPayloads.AiStreamStatus> parseAiStreamStatus(ValidatedEvent event) throws Exception {
        return AiStreamStatusParser.parse(event);
    }

    public static List<EventPayloads.StreamIngestMetrics> parseStreamIngestMetrics(ValidatedEvent event) throws Exception {
        return StreamIngestMetricsParser.parse(event);
    }

    public static List<EventPayloads.StreamTraceEvent> parseStreamTrace(ValidatedEvent event) throws Exception {
        return StreamTraceParser.parse(event);
    }

    public static List<EventPayloads.NetworkCapability> parseNetworkCapabilities(ValidatedEvent event) throws Exception {
        return NetworkCapabilitiesParser.parseSnapshots(event);
    }

    public static List<EventPayloads.NetworkCapabilityAdvertised> parseNetworkCapabilitiesAdvertised(ValidatedEvent event) throws Exception {
        return NetworkCapabilitiesParser.parseAdvertisedCapabilities(event);
    }

    public static List<EventPayloads.NetworkCapabilityModelConstraint> parseNetworkCapabilitiesModelConstraints(ValidatedEvent event) throws Exception {
        return NetworkCapabilitiesParser.parseModelConstraints(event);
    }

    public static List<EventPayloads.NetworkCapabilityPrice> parseNetworkCapabilitiesPrices(ValidatedEvent event) throws Exception {
        return NetworkCapabilitiesParser.parsePrices(event);
    }

    public static List<EventPayloads.AiStreamEvent> parseAiStreamEvent(ValidatedEvent event) throws Exception {
        return AiStreamEventParser.parse(event);
    }

    public static List<EventPayloads.DiscoveryResult> parseDiscoveryResults(ValidatedEvent event) throws Exception {
        return DiscoveryResultsParser.parse(event);
    }

    public static List<EventPayloads.PaymentEvent> parsePaymentEvent(ValidatedEvent event) throws Exception {
        return PaymentEventParser.parse(event);
    }

}
