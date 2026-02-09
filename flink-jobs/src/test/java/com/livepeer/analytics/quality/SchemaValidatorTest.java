package com.livepeer.analytics.quality;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import com.livepeer.analytics.util.JsonSupport;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class SchemaValidatorTest {

    @Test
    void validEventPassesValidation() throws Exception {
        String json = "{\"id\":\"evt-1\",\"type\":\"ai_stream_status\",\"timestamp\":1710000000000,\"data\":{\"stream_id\":\"s1\",\"request_id\":\"r1\",\"pipeline\":\"p1\",\"pipeline_id\":\"pid\"}}";
        JsonNode node = JsonSupport.MAPPER.readTree(json);

        SchemaValidator validator = new SchemaValidator(Collections.singleton("1"));
        SchemaValidator.ValidationResult result = validator.validate(node);

        assertTrue(result.valid);
        assertEquals("1", result.eventVersion);
    }

    @Test
    void missingFieldFailsValidation() throws Exception {
        String json = "{\"id\":\"evt-2\",\"type\":\"stream_ingest_metrics\",\"timestamp\":1710000000000,\"data\":{\"request_id\":\"r1\"}}";
        JsonNode node = JsonSupport.MAPPER.readTree(json);

        SchemaValidator validator = new SchemaValidator(Collections.singleton("1"));
        SchemaValidator.ValidationResult result = validator.validate(node);

        assertFalse(result.valid);
        assertEquals("SCHEMA_INVALID", result.failureClass);
    }

    @Test
    void unsupportedVersionFailsValidation() throws Exception {
        String json = "{\"id\":\"evt-3\",\"type\":\"ai_stream_status\",\"version\":\"2\",\"timestamp\":1710000000000,\"data\":{\"stream_id\":\"s1\",\"request_id\":\"r1\",\"pipeline\":\"p1\",\"pipeline_id\":\"pid\"}}";
        JsonNode node = JsonSupport.MAPPER.readTree(json);

        SchemaValidator validator = new SchemaValidator(Collections.singleton("1"));
        SchemaValidator.ValidationResult result = validator.validate(node);

        assertFalse(result.valid);
        assertEquals("UNSUPPORTED_VERSION", result.failureClass);
    }
}
