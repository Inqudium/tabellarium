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
 * ## Parsing rules
 *
 * The text is standard Java `.properties` content, parsed by
 * [Properties.load] with exactly its specification (XML indentation,
 * blank lines, `#`/`!` comments, first `=`/`:` as the separator with
 * later ones part of the value, backslash continuations, escape
 * decoding, empty values); Helm-style template values are substituted
 * by the renderer before the parser sees them. Two behaviors are this
 * function's own, not the specification's: trailing whitespace of a
 * value is trimmed (see the body), and a malformed Unicode escape is
 * rewrapped as an [IllegalArgumentException] naming the element. The
 * case that matters most to operators is a SASL JAAS configuration
 * split over continuation lines:
 *
 * ```
 * sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule \
 *     required \
 *     username="serviceuser" \
 *     password="${KAFKA_PASSWORD}";
 * ```
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
internal fun parseKafkaProducerProperties(text: String): Map<String, String> {
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
