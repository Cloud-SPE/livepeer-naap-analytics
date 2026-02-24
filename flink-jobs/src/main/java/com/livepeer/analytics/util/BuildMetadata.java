package com.livepeer.analytics.util;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * Central build metadata accessor used by runtime logs and diagnostics.
 */
public final class BuildMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String BUILD_INFO_RESOURCE = "build-info.properties";
    private static final BuildMetadata INSTANCE = load();

    private final String version;
    private final String gitCommit;

    private BuildMetadata(String version, String gitCommit) {
        this.version = normalize(version, "dev");
        this.gitCommit = normalize(gitCommit, "unknown");
    }

    public static BuildMetadata current() {
        return INSTANCE;
    }

    public String version() {
        return version;
    }

    public String gitCommit() {
        return gitCommit;
    }

    public String identity() {
        return version + "+" + gitCommit;
    }

    private static BuildMetadata load() {
        Properties props = new Properties();
        try (InputStream in = BuildMetadata.class.getClassLoader().getResourceAsStream(BUILD_INFO_RESOURCE)) {
            if (in != null) {
                props.load(in);
            }
        } catch (Exception ignored) {
            // Best effort. Runtime should still start if metadata is unavailable.
        }
        return new BuildMetadata(props.getProperty("build.version"), props.getProperty("build.git.commit"));
    }

    private static String normalize(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return fallback;
        }
        if (trimmed.startsWith("${") && trimmed.endsWith("}")) {
            return fallback;
        }
        return trimmed;
    }
}
