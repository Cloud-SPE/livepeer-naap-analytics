package com.livepeer.analytics.quality;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RefactorDriftGuardTest {

    @Test
    void duplicatedLocalHelperSignaturesAreNotReintroduced() throws IOException {
        Path root = Path.of("src/main/java");
        List<String> bannedSignatures = List.of(
                "private static String firstNonEmpty(",
                "private static String normalizeAddress(",
                "private static boolean isEmpty(String value)",
                "private static int rowSizeBytes(",
                "private static RejectedEventEnvelope.SourcePointer buildSourcePointer(KafkaInboundRecord record)",
                "private static RejectedEventEnvelope.EventDimensions extractDimensions(JsonNode root)"
        );

        List<String> hits;
        try (Stream<Path> walk = Files.walk(root)) {
            hits = walk
                    .filter(path -> path.toString().endsWith(".java"))
                    .flatMap(path -> findSignatureHits(path, bannedSignatures).stream())
                    .collect(Collectors.toList());
        }
        assertTrue(hits.isEmpty(), "Found banned helper duplicates:\n" + String.join("\n", hits));
    }

    private static List<String> findSignatureHits(Path path, List<String> signatures) {
        try {
            List<String> lines = Files.readAllLines(path);
            return lines.stream()
                    .map(String::trim)
                    .flatMap(line -> signatures.stream()
                            .filter(line::contains)
                            .map(sig -> path + " => " + sig))
                    .collect(Collectors.toList());
        } catch (IOException ex) {
            throw new IllegalStateException("Failed reading " + path, ex);
        }
    }
}
