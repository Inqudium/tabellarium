package eu.inqudium.tabellarium;

import ch.qos.logback.classic.spi.LoggingEvent;
import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.util.Arrays;

/**
 * Fuzzes the enrichment step around the partitioning key - the one value on
 * the send path that originates in the MDC and is therefore potentially
 * attacker-influenced (see SECURITY.md).
 *
 * Invariants under test: construction rejects exactly the blank identity
 * fields; enrich() never throws; the partitioning key is passed through
 * verbatim when non-blank and at most MAX_PARTITIONING_KEY_LENGTH characters,
 * and treated as ABSENT (not truncated) otherwise; the static header set is
 * complete and carries the identity verbatim as UTF-8.
 *
 * Runs as a regression test (checked-in inputs plus the empty input) in every
 * build; the scheduled Fuzz workflow explores for real (JAZZER_FUZZ=1).
 */
class MessageEnricherFuzzTest {
    @FuzzTest(maxDuration = "10m")
    void enrichmentUpholdsItsContract(FuzzedDataProvider data) {
        String component = data.consumeString(24);
        String cmdbId = data.consumeString(24);
        String environment = data.consumeString(24);
        String key = data.consumeBoolean() ? null : data.consumeRemainingAsString();

        boolean anyBlank = kotlinBlank(component) || kotlinBlank(cmdbId) || kotlinBlank(environment);
        MessageEnricher enricher;
        try {
            enricher = new MessageEnricher(component, cmdbId, environment, event -> key);
        } catch (IllegalArgumentException expected) {
            if (!anyBlank) {
                throw new IllegalStateException(
                        "non-blank identity rejected: '" + component + "'/'" + cmdbId + "'/'" + environment + "'");
            }
            return;
        }
        if (anyBlank) {
            throw new IllegalStateException("blank identity accepted");
        }

        var record = enricher.enrich(new LoggingEvent());

        boolean expectKey =
                key != null && !kotlinBlank(key) && key.length() <= MessageEnricher.MAX_PARTITIONING_KEY_LENGTH;
        String actual = record.getPartitioningKey();
        if (expectKey ? !key.equals(actual) : actual != null) {
            throw new IllegalStateException(
                    "key contract violated: extractor='" + key + "', record='" + actual + "'");
        }

        if (record.getHeaders().size() != 5) {
            throw new IllegalStateException("header set incomplete: " + record.getHeaders());
        }
        for (String identity : new String[] {component, cmdbId, environment}) {
            byte[] utf8 = identity.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            boolean found = record.getHeaders().stream().anyMatch(h -> Arrays.equals(h.value(), utf8));
            if (!found) {
                throw new IllegalStateException("identity value missing from headers: '" + identity + "'");
            }
        }
    }

    /**
     * Kotlin's isBlank(), which the library uses: a char is whitespace per
     * Character.isWhitespace OR isSpaceChar - NBSP counts, unlike in Java's
     * String.isBlank(). The oracle must speak the library's dialect.
     */
    private static boolean kotlinBlank(String s) {
        return s.codePoints().allMatch(c -> Character.isWhitespace(c) || Character.isSpaceChar(c));
    }
}
