package com.livepeer.analytics.capability;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CapabilityCatalogLoaderTest {

    @Test
    void loadDefaultCatalogResolvesKnownAndUnknownCapabilities() {
        CapabilityCatalog catalog = CapabilityCatalogLoader.loadDefault();

        CapabilityDescriptor known = catalog.resolve(35);
        assertEquals(35, known.id());
        assertEquals("Live video to video", known.name());
        assertEquals("live-video", known.group());

        CapabilityDescriptor unknown = catalog.resolve(9999);
        assertEquals(9999, unknown.id());
        assertEquals("unknown", unknown.name());
        assertEquals("unknown", unknown.group());
    }
}
