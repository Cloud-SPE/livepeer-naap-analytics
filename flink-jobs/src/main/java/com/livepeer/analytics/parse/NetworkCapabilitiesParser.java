package com.livepeer.analytics.parse;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.capability.CapabilityCatalog;
import com.livepeer.analytics.capability.CapabilityCatalogLoader;
import com.livepeer.analytics.capability.CapabilityDescriptor;
import com.livepeer.analytics.model.EventPayloads;
import com.livepeer.analytics.quality.ValidatedEvent;
import com.livepeer.analytics.util.AddressNormalizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Parser suite for `network_capabilities` events.
 */
final class NetworkCapabilitiesParser {
    private static final CapabilityCatalog CAPABILITY_CATALOG = CapabilityCatalogLoader.loadDefault();

    private NetworkCapabilitiesParser() {}

    static List<EventPayloads.NetworkCapability> parseSnapshots(ValidatedEvent event) throws Exception {
        List<EventPayloads.NetworkCapability> results = new ArrayList<>();
        for (JsonNode orch : orchestrators(event.data())) {
            String orchestratorAddress = AddressNormalizer.normalizeOrEmpty(orch.path("address").asText(""));
            String localAddress = AddressNormalizer.normalizeOrEmpty(orch.path("local_address").asText(""));
            String orchUri = orch.path("orch_uri").asText("");
            String orchestratorVersion = orch.path("capabilities").path("version").asText("");

            JsonNode constraintsByCap = orch.path("capabilities").path("constraints").path("PerCapability");
            JsonNode priceArray = orch.path("capabilities_prices");
            JsonNode hardwareArray = orch.path("hardware");
            if (!hardwareArray.isArray() || hardwareArray.size() == 0) {
                continue;
            }

            for (JsonNode hw : hardwareArray) {
                String pipeline = hw.path("pipeline").asText("");
                String modelId = hw.path("model_id").asText("");

                ModelSelection modelSelection = selectModelConfig(constraintsByCap, modelId);
                JsonNode modelCfg = modelSelection.modelCfg;
                Integer capabilityId = modelSelection.capabilityId;
                JsonNode priceInfo = selectPrice(priceArray, capabilityId, modelId);
                if (capabilityId == null && priceInfo != null && !priceInfo.isMissingNode()) {
                    capabilityId = JsonNodeUtils.asNullableInt(priceInfo.path("capability"));
                }

                CapabilityDescriptor capability = CAPABILITY_CATALOG.resolve(capabilityId);
                Integer warm = parseWarm(modelCfg);

                JsonNode gpuInfo = hw.path("gpu_info");
                if (gpuInfo.isObject() && gpuInfo.size() > 0) {
                    Iterator<String> gpuKeys = gpuInfo.fieldNames();
                    while (gpuKeys.hasNext()) {
                        JsonNode gpu = gpuInfo.path(gpuKeys.next());
                        EventPayloads.NetworkCapability row = buildBaseSnapshot(event, orchestratorAddress, localAddress, orchUri,
                                pipeline, modelId, orchestratorVersion, capability, modelCfg, warm, priceInfo);
                        row.gpuId = JsonNodeUtils.asNullableText(gpu.path("id"));
                        row.gpuName = JsonNodeUtils.asNullableText(gpu.path("name"));
                        row.gpuMemoryTotal = JsonNodeUtils.asNullableLong(gpu.path("memory_total"));
                        row.gpuMemoryFree = JsonNodeUtils.asNullableLong(gpu.path("memory_free"));
                        row.gpuMajor = JsonNodeUtils.asNullableInt(gpu.path("major"));
                        row.gpuMinor = JsonNodeUtils.asNullableInt(gpu.path("minor"));
                        row.rawJson = orch.toString();
                        results.add(row);
                    }
                } else {
                    EventPayloads.NetworkCapability row = buildBaseSnapshot(event, orchestratorAddress, localAddress, orchUri,
                            pipeline, modelId, orchestratorVersion, capability, modelCfg, warm, priceInfo);
                    row.rawJson = orch.toString();
                    results.add(row);
                }
            }
        }

        return results;
    }

    static List<EventPayloads.NetworkCapabilityAdvertised> parseAdvertisedCapabilities(ValidatedEvent event) throws Exception {
        List<EventPayloads.NetworkCapabilityAdvertised> results = new ArrayList<>();
        for (JsonNode orch : orchestrators(event.data())) {
            String orchestratorAddress = AddressNormalizer.normalizeOrEmpty(orch.path("address").asText(""));
            String localAddress = AddressNormalizer.normalizeOrEmpty(orch.path("local_address").asText(""));
            String orchUri = orch.path("orch_uri").asText("");

            JsonNode capacities = orch.path("capabilities").path("capacities");
            if (capacities == null || !capacities.isObject()) {
                continue;
            }
            Iterator<String> capabilityIds = capacities.fieldNames();
            while (capabilityIds.hasNext()) {
                String capabilityIdRaw = capabilityIds.next();
                Integer capabilityId = parseCapId(capabilityIdRaw);
                if (capabilityId == null) {
                    continue;
                }
                CapabilityDescriptor capability = CAPABILITY_CATALOG.resolve(capabilityId);

                EventPayloads.NetworkCapabilityAdvertised row = new EventPayloads.NetworkCapabilityAdvertised();
                row.eventTimestamp = event.event.timestamp;
                row.sourceEventId = event.event.eventId;
                row.orchestratorAddress = orchestratorAddress;
                row.localAddress = localAddress;
                row.orchUri = orchUri;
                row.capabilityId = capability.id();
                row.capabilityName = capability.name();
                row.capabilityGroup = capability.group();
                row.capabilityCatalogVersion = CAPABILITY_CATALOG.version();
                row.capacity = JsonNodeUtils.asNullableInt(capacities.path(capabilityIdRaw));
                row.rawJson = orch.toString();
                results.add(row);
            }
        }

        return results;
    }

    static List<EventPayloads.NetworkCapabilityModelConstraint> parseModelConstraints(ValidatedEvent event) throws Exception {
        List<EventPayloads.NetworkCapabilityModelConstraint> results = new ArrayList<>();
        for (JsonNode orch : orchestrators(event.data())) {
            String orchestratorAddress = AddressNormalizer.normalizeOrEmpty(orch.path("address").asText(""));
            String localAddress = AddressNormalizer.normalizeOrEmpty(orch.path("local_address").asText(""));
            String orchUri = orch.path("orch_uri").asText("");

            JsonNode perCapability = orch.path("capabilities").path("constraints").path("PerCapability");
            if (perCapability == null || !perCapability.isObject()) {
                continue;
            }
            Iterator<String> capabilityIds = perCapability.fieldNames();
            while (capabilityIds.hasNext()) {
                String capabilityIdRaw = capabilityIds.next();
                Integer capabilityId = parseCapId(capabilityIdRaw);
                if (capabilityId == null) {
                    continue;
                }
                CapabilityDescriptor capability = CAPABILITY_CATALOG.resolve(capabilityId);
                JsonNode models = perCapability.path(capabilityIdRaw).path("models");
                if (!models.isObject()) {
                    continue;
                }
                Iterator<String> modelIds = models.fieldNames();
                while (modelIds.hasNext()) {
                    String modelId = modelIds.next();
                    JsonNode modelCfg = models.path(modelId);

                    EventPayloads.NetworkCapabilityModelConstraint row = new EventPayloads.NetworkCapabilityModelConstraint();
                    row.eventTimestamp = event.event.timestamp;
                    row.sourceEventId = event.event.eventId;
                    row.orchestratorAddress = orchestratorAddress;
                    row.localAddress = localAddress;
                    row.orchUri = orchUri;
                    row.capabilityId = capability.id();
                    row.capabilityName = capability.name();
                    row.capabilityGroup = capability.group();
                    row.capabilityCatalogVersion = CAPABILITY_CATALOG.version();
                    row.modelId = modelId;
                    row.runnerVersion = JsonNodeUtils.asNullableText(modelCfg.path("runnerVersion"));
                    row.capacity = JsonNodeUtils.asNullableInt(modelCfg.path("capacity"));
                    row.capacityInUse = JsonNodeUtils.asNullableInt(modelCfg.path("capacityInUse"));
                    row.warm = parseWarm(modelCfg);
                    row.rawJson = orch.toString();
                    results.add(row);
                }
            }
        }

        return results;
    }

    static List<EventPayloads.NetworkCapabilityPrice> parsePrices(ValidatedEvent event) throws Exception {
        List<EventPayloads.NetworkCapabilityPrice> results = new ArrayList<>();
        for (JsonNode orch : orchestrators(event.data())) {
            String orchestratorAddress = AddressNormalizer.normalizeOrEmpty(orch.path("address").asText(""));
            String localAddress = AddressNormalizer.normalizeOrEmpty(orch.path("local_address").asText(""));
            String orchUri = orch.path("orch_uri").asText("");

            JsonNode prices = orch.path("capabilities_prices");
            if (!prices.isArray()) {
                continue;
            }
            for (JsonNode price : prices) {
                Integer capabilityId = JsonNodeUtils.asNullableInt(price.path("capability"));
                CapabilityDescriptor capability = CAPABILITY_CATALOG.resolve(capabilityId);

                EventPayloads.NetworkCapabilityPrice row = new EventPayloads.NetworkCapabilityPrice();
                row.eventTimestamp = event.event.timestamp;
                row.sourceEventId = event.event.eventId;
                row.orchestratorAddress = orchestratorAddress;
                row.localAddress = localAddress;
                row.orchUri = orchUri;
                row.capabilityId = capability.id();
                row.capabilityName = capability.name();
                row.capabilityGroup = capability.group();
                row.capabilityCatalogVersion = CAPABILITY_CATALOG.version();
                row.constraint = JsonNodeUtils.asNullableText(price.path("constraint"));
                row.pricePerUnit = JsonNodeUtils.asNullableInt(price.path("pricePerUnit"));
                row.pixelsPerUnit = JsonNodeUtils.asNullableInt(price.path("pixelsPerUnit"));
                row.rawJson = orch.toString();
                results.add(row);
            }
        }

        return results;
    }

    private static List<JsonNode> orchestrators(JsonNode dataNode) {
        if (dataNode == null || dataNode.isMissingNode() || dataNode.isNull()) {
            return Collections.emptyList();
        }
        // `network_capabilities` data can be one orchestrator object or an array of objects.
        if (dataNode.isObject()) {
            return dataNode.size() == 0 ? Collections.emptyList() : Collections.singletonList(dataNode);
        }
        if (!dataNode.isArray() || dataNode.size() == 0) {
            return Collections.emptyList();
        }

        List<JsonNode> orchestrators = new ArrayList<>();
        for (JsonNode item : dataNode) {
            if (item != null && item.isObject() && item.size() > 0) {
                orchestrators.add(item);
            }
        }
        return orchestrators;
    }

    private static EventPayloads.NetworkCapability buildBaseSnapshot(
            ValidatedEvent event,
            String orchestratorAddress,
            String localAddress,
            String orchUri,
            String pipeline,
            String modelId,
            String orchestratorVersion,
            CapabilityDescriptor capability,
            JsonNode modelCfg,
            Integer warm,
            JsonNode priceInfo) {
        EventPayloads.NetworkCapability row = new EventPayloads.NetworkCapability();
        row.eventTimestamp = event.event.timestamp;
        row.sourceEventId = event.event.eventId;
        row.orchestratorAddress = orchestratorAddress;
        row.localAddress = localAddress;
        row.orchUri = orchUri;
        row.pipeline = pipeline;
        row.modelId = modelId;
        row.capabilityId = capability.id();
        row.capabilityName = capability.name();
        row.capabilityGroup = capability.group();
        row.capabilityCatalogVersion = CAPABILITY_CATALOG.version();
        row.runnerVersion = modelCfg == null ? null : JsonNodeUtils.asNullableText(modelCfg.path("runnerVersion"));
        row.capacity = modelCfg == null ? null : JsonNodeUtils.asNullableInt(modelCfg.path("capacity"));
        row.capacityInUse = modelCfg == null ? null : JsonNodeUtils.asNullableInt(modelCfg.path("capacityInUse"));
        row.warm = warm;
        row.pricePerUnit = priceInfo == null ? null : JsonNodeUtils.asNullableInt(priceInfo.path("pricePerUnit"));
        row.pixelsPerUnit = priceInfo == null ? null : JsonNodeUtils.asNullableInt(priceInfo.path("pixelsPerUnit"));
        row.orchestratorVersion = orchestratorVersion;
        return row;
    }

    private static Integer parseWarm(JsonNode modelCfg) {
        if (modelCfg == null || modelCfg.isMissingNode() || modelCfg.isNull()) {
            return null;
        }
        JsonNode warmNode = modelCfg.path("warm");
        if (warmNode.isBoolean()) {
            return warmNode.asBoolean(false) ? 1 : 0;
        }
        return JsonNodeUtils.asNullableInt(warmNode);
    }

    private static ModelSelection selectModelConfig(JsonNode constraintsByCap, String modelId) {
        if (constraintsByCap != null && constraintsByCap.isObject()) {
            Iterator<String> capIds = constraintsByCap.fieldNames();
            while (capIds.hasNext()) {
                String capId = capIds.next();
                JsonNode models = constraintsByCap.path(capId).path("models");
                if (models.isObject() && modelId != null && !modelId.isEmpty() && models.has(modelId)) {
                    return new ModelSelection(models.path(modelId), parseCapId(capId));
                }
            }

            capIds = constraintsByCap.fieldNames();
            if (capIds.hasNext()) {
                String capId = capIds.next();
                JsonNode models = constraintsByCap.path(capId).path("models");
                if (models.isObject()) {
                    Iterator<String> modelNames = models.fieldNames();
                    if (modelNames.hasNext()) {
                        String modelName = modelNames.next();
                        return new ModelSelection(models.path(modelName), parseCapId(capId));
                    }
                }
            }
        }
        return new ModelSelection(null, null);
    }

    private static JsonNode selectPrice(JsonNode priceArray, Integer capabilityId, String modelId) {
        if (priceArray == null || !priceArray.isArray() || priceArray.size() == 0) {
            return null;
        }
        JsonNode fallback = priceArray.get(0);
        for (JsonNode price : priceArray) {
            Integer cap = JsonNodeUtils.asNullableInt(price.path("capability"));
            String constraint = price.path("constraint").asText(null);
            boolean capMatches = capabilityId == null || capabilityId.equals(cap);
            boolean modelMatches = modelId == null || modelId.isEmpty() || modelId.equals(constraint);
            if (capMatches && modelMatches) {
                return price;
            }
        }
        return fallback;
    }

    private static Integer parseCapId(String capId) {
        if (capId == null) {
            return null;
        }
        try {
            return Integer.parseInt(capId);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static final class ModelSelection {
        private final JsonNode modelCfg;
        private final Integer capabilityId;

        private ModelSelection(JsonNode modelCfg, Integer capabilityId) {
            this.modelCfg = modelCfg;
            this.capabilityId = capabilityId;
        }
    }
}
