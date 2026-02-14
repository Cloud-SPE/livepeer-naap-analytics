package com.livepeer.analytics.capability;

import com.fasterxml.jackson.databind.JsonNode;

import com.livepeer.analytics.util.JsonSupport;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads capability catalog metadata from classpath or filesystem.
 *
 * Lookup order:
 * 1) JVM property `livepeer.capability.catalog.path`
 * 2) classpath resource `/reference/capability_catalog.v1.json`
 * 3) repository fallback `configs/reference/capability_catalog.v1.json`
 */
public final class CapabilityCatalogLoader {
    public static final String CATALOG_PROPERTY = "livepeer.capability.catalog.path";
    public static final String DEFAULT_CLASSPATH_RESOURCE = "reference/capability_catalog.v1.json";
    public static final Path DEFAULT_REPO_PATH = Path.of("configs/reference/capability_catalog.v1.json");

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CapabilityCatalogLoader.class);

    private CapabilityCatalogLoader() {}

    public static CapabilityCatalog loadDefault() {
        String overridePath = System.getProperty(CATALOG_PROPERTY);
        if (overridePath != null && !overridePath.isBlank()) {
            return loadFromFile(Path.of(overridePath));
        }

        CapabilityCatalog fromClasspath = loadFromClasspath(DEFAULT_CLASSPATH_RESOURCE);
        if (fromClasspath != null) {
            return fromClasspath;
        }

        return loadFromFile(DEFAULT_REPO_PATH);
    }

    static CapabilityCatalog loadFromClasspath(String resourcePath) {
        try (InputStream in = CapabilityCatalogLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                return null;
            }
            JsonNode root = JsonSupport.MAPPER.readTree(in);
            return parseCatalog(root);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to load capability catalog from classpath: " + resourcePath, ex);
        }
    }

    static CapabilityCatalog loadFromFile(Path path) {
        try {
            if (!Files.exists(path)) {
                throw new IllegalStateException("Capability catalog file not found: " + path);
            }
            JsonNode root = JsonSupport.MAPPER.readTree(path.toFile());
            CapabilityCatalog catalog = parseCatalog(root);
            LOG.info("Loaded capability catalog version={} entries={} from {}",
                    catalog.version(), catalog.size(), path);
            return catalog;
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to load capability catalog from file: " + path, ex);
        }
    }

    private static CapabilityCatalog parseCatalog(JsonNode root) {
        if (root == null || !root.isObject()) {
            throw new IllegalStateException("Capability catalog is not a JSON object");
        }

        String version = root.path("catalog_version").asText("unknown");
        JsonNode capabilities = root.path("capabilities");
        if (!capabilities.isArray()) {
            throw new IllegalStateException("Capability catalog missing capabilities array");
        }

        Map<Integer, CapabilityDescriptor> byId = new HashMap<>();
        for (JsonNode cap : capabilities) {
            if (!cap.isObject()) {
                continue;
            }
            Integer id = cap.path("id").isNumber() ? cap.path("id").asInt() : null;
            if (id == null) {
                continue;
            }
            String name = cap.path("name").asText("unknown");
            String group = cap.path("group").asText("unknown");
            byId.put(id, new CapabilityDescriptor(id, name, group));
        }

        return new CapabilityCatalog(version, byId);
    }
}
