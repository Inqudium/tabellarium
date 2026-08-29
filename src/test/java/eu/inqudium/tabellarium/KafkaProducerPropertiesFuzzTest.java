package eu.inqudium.tabellarium;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.util.Map;

/**
 * Fuzzes the parser of the operator-supplied &lt;kafkaProducerProperties&gt;
 * element - free-form .properties text from the Logback XML.
 *
 * Invariants under test: the parser throws only its documented
 * IllegalArgumentException (malformed Unicode escapes); an accepted result
 * never carries null keys or values, and values carry no trailing whitespace
 * (the parser trims it deliberately - XML indentation must not leak into
 * producer configuration).
 *
 * Runs as a regression test (checked-in inputs plus the empty input) in every
 * build; the scheduled Fuzz workflow explores for real (JAZZER_FUZZ=1).
 */
class KafkaProducerPropertiesFuzzTest {
    @FuzzTest(maxDuration = "10m")
    void parserUpholdsItsContract(FuzzedDataProvider data) {
        String text = data.consumeRemainingAsString();
        Map<String, String> parsed;
        try {
            parsed = KafkaProducerPropertiesParserKt.parseKafkaProducerProperties(text);
        } catch (IllegalArgumentException expected) {
            // Documented: malformed Unicode escape in the properties text.
            return;
        }
        for (Map.Entry<String, String> entry : parsed.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                throw new IllegalStateException("null key or value parsed from: " + text);
            }
            if (!entry.getValue().equals(entry.getValue().stripTrailing())) {
                throw new IllegalStateException(
                        "trailing whitespace survived for key '" + entry.getKey() + "': '" + entry.getValue() + "'");
            }
        }
    }
}
