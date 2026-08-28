package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class KafkaProducerPropertiesParserTest {
    @Nested
    inner class `Basic parsing` {
        @Test
        fun `should parse a single property`() {
            // Given / When
            val result = parseKafkaProducerProperties("bootstrap.servers=broker:9092")

            // Then
            assertThat(result).containsExactlyInAnyOrderEntriesOf(
                mapOf("bootstrap.servers" to "broker:9092"),
            )
        }

        @Test
        fun `should parse multiple properties on consecutive lines`() {
            // Given
            val text =
                """
                bootstrap.servers=broker:9092
                acks=all
                retries=10
                """.trimIndent()

            // When
            val result = parseKafkaProducerProperties(text)

            // Then
            assertThat(result).containsExactlyInAnyOrderEntriesOf(
                mapOf(
                    "bootstrap.servers" to "broker:9092",
                    "acks" to "all",
                    "retries" to "10",
                ),
            )
        }

        @Test
        fun `should parse the real-world example from production XML`() {
            // What is to be tested? Whether the parser handles the exact format
            //   that appears in production logback configurations: XML
            //   indentation, blank lines for visual grouping, and SSL property
            //   names with multiple dots.
            // How will the test case be deemed successful and why? Successful
            //   if every property from the <kafkaProducerProperties>
            //   element is extracted with the expected key and (trimmed) value.
            //   This pins down compatibility with the existing config.
            // Why is it important to test this test case? Any regression that
            //   broke parsing of the real-world layout would render the
            //   appender unable to start in any of the existing deployments -
            //   the most visible failure mode possible.

            // Given: a production-style kafkaProducerProperties block
            val text =
                """
                bootstrap.servers=broker.example.com:9092
                security.protocol=SSL

                ssl.keystore.location=/cert/identity.pkcs12
                ssl.keystore.password=keystorePassword123
                ssl.keystore.type=PKCS12

                ssl.truststore.type=JKS
                ssl.truststore.location=/configs/http-trust.jks
                ssl.truststore.password=publicpass
                """.trimIndent()

            // When
            val result = parseKafkaProducerProperties(text)

            // Then
            assertThat(result).containsExactlyInAnyOrderEntriesOf(
                mapOf(
                    "bootstrap.servers" to "broker.example.com:9092",
                    "security.protocol" to "SSL",
                    "ssl.keystore.location" to "/cert/identity.pkcs12",
                    "ssl.keystore.password" to "keystorePassword123",
                    "ssl.keystore.type" to "PKCS12",
                    "ssl.truststore.type" to "JKS",
                    "ssl.truststore.location" to "/configs/http-trust.jks",
                    "ssl.truststore.password" to "publicpass",
                ),
            )
        }
    }

    @Nested
    inner class `Whitespace and formatting` {
        @Test
        fun `should trim whitespace around keys and values`() {
            // Given / When
            val result = parseKafkaProducerProperties("  acks  =  all  ")

            // Then
            assertThat(result).containsExactlyInAnyOrderEntriesOf(
                mapOf("acks" to "all"),
            )
        }

        @Test
        fun `should ignore blank lines`() {
            // Given
            val text = "\n\nacks=all\n\nretries=10\n\n"

            // When
            val result = parseKafkaProducerProperties(text)

            // Then
            assertThat(result).hasSize(2)
        }

        @Test
        fun `should ignore lines starting with hash as comments`() {
            // Given
            val text =
                """
                # this is a comment
                acks=all
                # another comment
                retries=10
                """.trimIndent()

            // When
            val result = parseKafkaProducerProperties(text)

            // Then
            assertThat(result).containsExactlyInAnyOrderEntriesOf(
                mapOf("acks" to "all", "retries" to "10"),
            )
        }
    }

    @Nested
    inner class `Edge cases` {
        @Test
        fun `should preserve equals signs inside values`() {
            // What is to be tested? Whether the parser splits only on the
            //   first '=', leaving any subsequent '=' as part of the value.
            // How will the test case be deemed successful and why? Successful
            //   if a SASL JAAS-config line (which contains multiple '=')
            //   round-trips exactly as written. This pins down support for
            //   real-world SSL/SASL configurations.
            // Why is it important to test this test case? SASL configurations
            //   are critical for production Kafka security; a parser that
            //   munged them at the second '=' would silently corrupt auth
            //   credentials with no clear error.

            // Given: a real SASL JAAS configuration line
            val jaasConfig =
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"secret\";"

            // When
            val result = parseKafkaProducerProperties("sasl.jaas.config=$jaasConfig")

            // Then: the value contains the literal '=' characters of the JAAS config
            assertThat(result).containsEntry("sasl.jaas.config", jaasConfig)
        }

        @Test
        fun `should accept an empty value`() {
            // Given / When
            val result = parseKafkaProducerProperties("client.id=")

            // Then
            assertThat(result).containsExactlyInAnyOrderEntriesOf(
                mapOf("client.id" to ""),
            )
        }

        @Test
        fun `should return an empty map for whitespace-only input`() {
            // Given / When
            val result = parseKafkaProducerProperties("   \n\n  ")

            // Then
            assertThat(result).isEmpty()
        }
    }

    @Nested
    inner class `Multi-line and special separators` {
        @Test
        fun `should join lines with a trailing backslash into a single property value`() {
            // What is to be tested? Whether the parser supports the standard
            //   Java .properties multi-line continuation, which is the typical
            //   layout for long SASL JAAS configurations.
            // How will the test case be deemed successful and why? Successful
            //   if a line ending in '\' is joined with the next, with the
            //   intermediate whitespace collapsed. This pins down JAAS-config
            //   compatibility - the entire reason for switching to
            //   Properties.load.
            // Why is it important to test this test case? Real-world banking
            //   Kafka configs span 5-10 lines for a single JAAS entry. A
            //   parser that broke at the first '\' would prevent production
            //   deployment in SASL-secured environments.

            // Given: a JAAS-style multi-line property
            val text =
                """
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule \
                    required \
                    username="serviceuser" \
                    password="topsecret";
                """.trimIndent()

            // When
            val result = parseKafkaProducerProperties(text)

            // Then: one property; the value contains all four parts of the line
            assertThat(result).hasSize(1)
            assertThat(result["sasl.jaas.config"])
                .contains("PlainLoginModule")
                .contains("required")
                .contains("username=\"serviceuser\"")
                .contains("password=\"topsecret\";")
        }

        @Test
        fun `should accept colon as a key-value separator`() {
            // Given
            val result = parseKafkaProducerProperties("acks:all")

            // Then: Properties.load treats ':' as a separator in addition to '='
            assertThat(result).containsEntry("acks", "all")
        }

        @Test
        fun `should accept space as a key-value separator`() {
            // What is to be tested? Whether whitespace works as a separator
            //   per the Java .properties spec.
            // How will the test case be deemed successful and why? Successful
            //   if "acks all" is parsed as key=acks, value=all. This is a
            //   behavior change from the previous custom parser, which
            //   rejected this form; the change is intentional, as it matches
            //   the established .properties convention.
            // Why is it important to test this test case? An operator copying
            //   a properties snippet from documentation that uses whitespace
            //   separators should not be confused by spurious errors.

            // Given / When
            val result = parseKafkaProducerProperties("acks all")

            // Then
            assertThat(result).containsEntry("acks", "all")
        }

        @Test
        fun `should ignore lines starting with exclamation mark as comments`() {
            // Given: '!' is the second comment character per the Java spec
            val text =
                """
                ! this is also a comment
                acks=all
                """.trimIndent()

            // When
            val result = parseKafkaProducerProperties(text)

            // Then
            assertThat(result).containsExactlyInAnyOrderEntriesOf(mapOf("acks" to "all"))
        }
    }

    @Nested
    inner class `Error cases` {
        @Test
        fun `should reject a line with a malformed Unicode escape`() {
            // What is to be tested? Whether the parser surfaces malformed
            //   Unicode escapes as IllegalArgumentException rather than
            //   silently producing garbage.
            // How will the test case be deemed successful and why? Successful
            //   if a value with an incomplete \uXXXX escape (only two hex
            //   digits) causes a clear IllegalArgumentException. This pins
            //   down the only failure mode of Properties.load.
            // Why is it important to test this test case? Without a test, a
            //   later refactor that swallowed the IOException would lose the
            //   only diagnostic signal the operator gets for this error class.

            // Given: a value with a truncated Unicode escape
            // When / Then
            assertThatThrownBy {
                parseKafkaProducerProperties("client.id=\\u12")
            }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("Unicode")
        }
    }
}
