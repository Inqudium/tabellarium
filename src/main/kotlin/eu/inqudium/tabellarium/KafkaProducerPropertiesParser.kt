package eu.inqudium.tabellarium

import java.io.IOException
import java.io.StringReader
import java.util.Properties

/**
 * Parses the multi-line text content of a Logback `<kafkaProducerProperties>`
 * element into a Kafka producer-property map.
 *
 * ## Expected format
 *
 * One property per line, `key=value` separated:
 *
 * ```
 * bootstrap.servers=broker1:9092,broker2:9092
 * security.protocol=SSL
 * ssl.keystore.location=/cert/identity.pkcs12
 * ```
 *
 * The element text is parsed as standard `.properties` content,
 * including:
 *
 * - **Leading whitespace** (XML indentation) - handled by the underlying
 *   [Properties.load] implementation.
 * - **Blank lines** for visual grouping - skipped by [Properties.load].
 * - **Helm/template values** like `bootstrap.servers={{ .Values.logback_kafka }}` -
 *   already substituted by the renderer before the parser sees them.
 *
 * ## Parsing rules
 *
 * Delegated to [Properties.load], which implements the full Java
 * `.properties` specification:
 *
 * - Whitespace around keys and values is trimmed.
 * - Blank lines are skipped.
 * - Lines starting with `#` or `!` are treated as comments and skipped.
 * - The first `=` (or `:`) on a line is treated as the key/value
 *   separator; subsequent occurrences are part of the value. This is
 *   essential for SASL JAAS configurations such as
 *   `sasl.jaas.config=...required username="..." password="...";`.
 * - **Multi-line continuations** with a trailing backslash are supported.
 *   This is the typical layout for long JAAS configurations:
 *
 *   ```
 *   sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule \
 *       required \
 *       username="serviceuser" \
 *       password="${KAFKA_PASSWORD}";
 *   ```
 *
 * - Unicode escapes (`\uXXXX`) and standard escape sequences (`\n`,
 *   `\t`, `\\`) in keys and values are decoded by [Properties.load].
 *   This is generally what operators expect from a `.properties`-style
 *   input.
 * - Empty values are accepted (`client.id=` is valid; `client.id`
 *   becomes the empty string).
 *
 * ## Order of the returned map
 *
 * The returned map's iteration order is **unspecified**: [Properties]
 * is backed by [java.util.Hashtable], which does not preserve insertion
 * order. Callers must not depend on the order. The producer-properties
 * map is consumed by Kafka's `ProducerConfig` constructor, which is
 * order-insensitive.
 *
 * ## Error reporting
 *
 * [Properties.load] is permissive: it only throws on encoding errors
 * (invalid Unicode escapes), never on missing `=` or other structural
 * issues - those are silently treated as keys with empty values.
 * Malformed inputs surface later as Kafka configuration-validation
 * errors during producer construction.
 *
 * @throws IllegalArgumentException on malformed Unicode escapes.
 */
fun parseKafkaProducerProperties(text: String): Map<String, String> {
    val props = Properties()
    try {
        props.load(StringReader(text))
    } catch (e: IllegalArgumentException) {
        // Properties.load throws IllegalArgumentException for malformed
        // Unicode escapes (e.g. "\u12X" with non-hex character or
        // truncated). Rewrap with a more context-rich message identifying
        // the source as the <kafkaProducerProperties> element.
        throw IllegalArgumentException(
            "Invalid kafkaProducerProperties content (malformed Unicode escape): ${e.message}",
            e,
        )
    } catch (e: IOException) {
        // StringReader does not throw IOException in practice, but the
        // Properties.load signature declares it. Defensive catch in case
        // a future refactor changes the reader.
        throw IllegalArgumentException(
            "Invalid kafkaProducerProperties content: ${e.message}",
            e,
        )
    }
    // Java's Properties.load() strips leading whitespace from values
    // but preserves trailing whitespace, treating it as part of the
    // value per the .properties spec. For Kafka producer properties,
    // trailing whitespace is virtually always an accident (XML
    // indentation or operator typo), so we trim it here. Keys cannot
    // contain whitespace at all per the .properties spec, so no
    // trimming is needed for them.
    return props.entries.associate { (k, v) -> k.toString() to v.toString().trimEnd() }
}
