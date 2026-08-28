package eu.inqudium.tabellarium

import org.apache.kafka.clients.producer.ProducerConfig
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class ProducerPropertiesBuilderTest {
    @Nested
    inner class `Base property handling` {
        @Test
        fun `should produce only default and mandatory overrides when the base is empty`() {
            // Given
            val builder = ProducerPropertiesBuilder(emptyMap())

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then
            assertThat(result.properties)
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                .containsEntry(ProducerConfig.LINGER_MS_CONFIG, "50")
                .containsEntry(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
            assertThat(result.mandatoryOverrideViolations).isEmpty()
        }

        @Test
        fun `should preserve a base property that has no matching override`() {
            // Given: a property that neither default nor mandatory touch
            val base = mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "kafka-broker:9092")
            val builder = ProducerPropertiesBuilder(base)

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then
            assertThat(result.properties)
                .containsEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker:9092")
        }

        @Test
        fun `should not modify the base properties map across multiple buildFor calls`() {
            // What is to be tested? Whether the builder leaves its input map untouched,
            //   even when called multiple times with different topic classes.
            // How will the test case be deemed successful and why? Successful if the
            //   original input map still equals its initial contents after the builds.
            //   This confirms that the builder is a pure function with no input mutation.
            // Why is it important to test this test case? If the builder mutated its
            //   input, calling buildFor multiple times (which the appender will do at
            //   startup, once per active topic class) would produce different results
            //   for the same logical input - a particularly insidious bug class.

            // Given
            val originalBase =
                mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "kafka-broker:9092",
                    ProducerConfig.LINGER_MS_CONFIG to "999",
                )
            val builder = ProducerPropertiesBuilder(originalBase)

            // When
            builder.buildFor(TopicClass.AUDIT)
            builder.buildFor(TopicClass.FUNCTIONAL)
            builder.buildFor(TopicClass.TECHNICAL)
            builder.buildFor(TopicClass.PERFORMANCE)

            // Then
            assertThat(originalBase).containsExactlyInAnyOrderEntriesOf(
                mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "kafka-broker:9092",
                    ProducerConfig.LINGER_MS_CONFIG to "999",
                ),
            )
        }
    }

    @Nested
    inner class `Default overrides` {
        @Test
        fun `should apply a default override when the property is not set in the base`() {
            // Given
            val builder = ProducerPropertiesBuilder(emptyMap())

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then: linger.ms is a default override (not mandatory) for AUDIT
            assertThat(result.properties).containsEntry(ProducerConfig.LINGER_MS_CONFIG, "50")
        }

        @Test
        fun `should preserve a user-set property when a default override exists for the same key`() {
            // Given: user sets linger.ms to 999
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.LINGER_MS_CONFIG to "999"),
                )

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then: user value wins over the default
            assertThat(result.properties).containsEntry(ProducerConfig.LINGER_MS_CONFIG, "999")
            assertThat(result.mandatoryOverrideViolations).isEmpty()
        }
    }

    @Nested
    inner class `Mandatory overrides` {
        @Test
        fun `should apply a mandatory override regardless of any base value`() {
            // Given: user explicitly sets acks=1
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.ACKS_CONFIG to "1"),
                )

            // When: building for AUDIT (which mandates acks=all)
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then: enforced value wins
            assertThat(result.properties).containsEntry(ProducerConfig.ACKS_CONFIG, "all")
        }

        @Test
        fun `should record no violation when the user did not set a mandatory-override property`() {
            // Given
            val builder = ProducerPropertiesBuilder(emptyMap())

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then: no conflict, because nothing to conflict with
            assertThat(result.mandatoryOverrideViolations).isEmpty()
        }

        @Test
        fun `should record no violation when the user value already matches the enforced value`() {
            // Given: user sets acks=all, which matches the mandatory value
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.ACKS_CONFIG to "all"),
                )

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then: no conflict, since the values agree
            assertThat(result.mandatoryOverrideViolations).isEmpty()
        }

        @Test
        fun `should record a violation when the user value conflicts with the enforced value`() {
            // What is to be tested? Whether a user-supplied value that disagrees
            //   with a mandatory override is recorded as a violation, with the
            //   correct details about which class, key, user value, and enforced
            //   value were involved.
            // How will the test case be deemed successful and why? Successful if
            //   exactly one violation is recorded with the precise expected
            //   contents. This pins down the violation reporting contract that
            //   the appender will rely on when forwarding to the status manager.
            // Why is it important to test this test case? The violation list is
            //   the only mechanism by which operators can learn that their
            //   configuration intent was overruled. A regression in the
            //   reporting would silently hide compliance enforcement.

            // Given: user sets acks=1, conflicting with AUDIT's mandatory acks=all
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.ACKS_CONFIG to "1"),
                )

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then
            assertThat(result.mandatoryOverrideViolations).containsExactly(
                MandatoryOverrideViolation(
                    topicClass = TopicClass.AUDIT,
                    propertyKey = ProducerConfig.ACKS_CONFIG,
                    userValue = "1",
                    enforcedValue = "all",
                ),
            )
        }

        @Test
        fun `should record multiple violations when multiple user values conflict with mandatory overrides`() {
            // Given: user sets both acks and enable.idempotence in conflicting ways
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(
                        ProducerConfig.ACKS_CONFIG to "0",
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "false",
                    ),
                )

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then: both violations recorded; enforced values applied
            assertThat(result.mandatoryOverrideViolations).hasSize(2)
            assertThat(result.properties)
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        }
    }

    @Nested
    inner class `Per topic class enforcement` {
        @Test
        fun `should enforce acks all and idempotence for AUDIT topics`() {
            // Given: a user attempting to weaken both audit guarantees
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(
                        ProducerConfig.ACKS_CONFIG to "1",
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "false",
                    ),
                )

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then: both mandates win, two violations recorded
            assertThat(result.properties)
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
            assertThat(result.mandatoryOverrideViolations).hasSize(2)
        }

        @Test
        fun `should enforce acks all for FUNCTIONAL topics`() {
            // Given
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.ACKS_CONFIG to "0"),
                )

            // When
            val result = builder.buildFor(TopicClass.FUNCTIONAL)

            // Then
            assertThat(result.properties).containsEntry(ProducerConfig.ACKS_CONFIG, "all")
            assertThat(result.mandatoryOverrideViolations).hasSize(1)
        }

        @Test
        fun `should not enforce any mandatory overrides for TECHNICAL topics`() {
            // Given: user sets a weak acks value that would be a mandate conflict elsewhere
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.ACKS_CONFIG to "0"),
                )

            // When
            val result = builder.buildFor(TopicClass.TECHNICAL)

            // Then: user value preserved, no violations
            assertThat(result.properties).containsEntry(ProducerConfig.ACKS_CONFIG, "0")
            assertThat(result.mandatoryOverrideViolations).isEmpty()
        }

        @Test
        fun `should not enforce any mandatory overrides for PERFORMANCE topics`() {
            // Given
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.ACKS_CONFIG to "0"),
                )

            // When
            val result = builder.buildFor(TopicClass.PERFORMANCE)

            // Then
            assertThat(result.properties).containsEntry(ProducerConfig.ACKS_CONFIG, "0")
            assertThat(result.mandatoryOverrideViolations).isEmpty()
        }
    }

    @Nested
    inner class `Purity guarantees` {
        @Test
        fun `should return the same result for the same input across multiple calls`() {
            // What is to be tested? Whether the builder is deterministic - the
            //   same (baseProperties, topicClass) pair must yield equal results
            //   on every call.
            // How will the test case be deemed successful and why? Successful if
            //   two independent buildFor calls with the same arguments produce
            //   results that compare equal under data-class equality. This is
            //   the operational definition of a pure function.
            // Why is it important to test this test case? In Logback's startup
            //   sequence the builder may be queried multiple times (once per
            //   active topic class, plus possibly diagnostic calls). Non-determinism
            //   here would manifest as flaky tests and surprising production
            //   behavior.

            // Given
            val base = mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "broker:9092")
            val builder = ProducerPropertiesBuilder(base)

            // When
            val firstResult = builder.buildFor(TopicClass.AUDIT)
            val secondResult = builder.buildFor(TopicClass.AUDIT)

            // Then
            assertThat(firstResult).isEqualTo(secondResult)
        }

        @Test
        fun `should return an immutable properties map`() {
            // Given
            val builder = ProducerPropertiesBuilder(emptyMap())

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then: attempting to mutate the returned map throws
            @Suppress("UNCHECKED_CAST")
            assertThatThrownBy {
                (result.properties as MutableMap<String, String>)["intruder"] = "value"
            }.isInstanceOf(UnsupportedOperationException::class.java)
        }
    }

    @Nested
    inner class `Idempotence compatibility validation` {
        @Test
        fun `should reject retries zero when the class mandates idempotence`() {
            // What is to be tested? Whether a configuration the Kafka
            //   producer constructor would refuse anyway (idempotence
            //   requires retries > 0) is rejected here with a clear,
            //   named message instead of surfacing later as a generic
            //   "Failed to build pipeline".
            // How will the test case be deemed successful and why? Successful
            //   if AUDIT (mandated idempotence) with operator retries=0
            //   throws an IllegalArgumentException naming both properties.
            // Why is it important to test this test case? Operators
            //   debugging a refused startup need the conflicting property
            //   named; the generic constructor failure hides it.

            // Given
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.RETRIES_CONFIG to "0"),
                )

            // When / Then
            assertThatThrownBy { builder.buildFor(TopicClass.AUDIT) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("retries=0")
                .hasMessageContaining("enable.idempotence")
        }

        @Test
        fun `should reject more than five in-flight requests when the class mandates idempotence`() {
            // Given
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to "6"),
                )

            // When / Then
            assertThatThrownBy { builder.buildFor(TopicClass.AUDIT) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("max.in.flight.requests.per.connection=6")
                .hasMessageContaining("at most 5")
        }

        @Test
        fun `should not apply the idempotence checks to classes without the mandate`() {
            // Given: the same tuning that AUDIT rejects
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(
                        ProducerConfig.RETRIES_CONFIG to "0",
                        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to "6",
                    ),
                )

            // When: TECHNICAL has no idempotence mandate
            val result = builder.buildFor(TopicClass.TECHNICAL)

            // Then: accepted verbatim
            assertThat(result.properties)
                .containsEntry(ProducerConfig.RETRIES_CONFIG, "0")
                .containsEntry(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6")
        }
    }

    @Nested
    inner class `Client id default` {
        @Test
        fun `should derive a per-class client id from the prefix`() {
            // What is to be tested? Whether a configured defaultClientIdPrefix
            //   yields a distinct client.id per topic class.
            // How will the test case be deemed successful and why? Successful if
            //   two classes built from the same builder carry
            //   <prefix>-<lowercase class name> as their client.id. This pins
            //   down the id scheme operators will see in broker logs, quotas,
            //   and kafka.producer metrics.
            // Why is it important to test this test case? If two classes shared
            //   one client.id, their producers would collide on JMX MBean
            //   registration in the same JVM and their per-client broker
            //   metrics would be indistinguishable.

            // Given
            val builder = ProducerPropertiesBuilder(emptyMap(), defaultClientIdPrefix = "tabellarium-checkout")

            // When
            val audit = builder.buildFor(TopicClass.AUDIT)
            val technical = builder.buildFor(TopicClass.TECHNICAL)

            // Then
            assertThat(audit.properties)
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "tabellarium-checkout-audit")
            assertThat(technical.properties)
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "tabellarium-checkout-technical")
        }

        @Test
        fun `should let an operator-supplied client id win over the default`() {
            // What is to be tested? Whether an explicit client.id in the base
            //   properties survives the per-class default.
            // How will the test case be deemed successful and why? Successful if
            //   the built properties carry the operator's value verbatim. This
            //   pins down the putIfAbsent semantics of the default layer.
            // Why is it important to test this test case? Operators may rely on
            //   a fixed client.id for broker-side quotas or ACLs; silently
            //   replacing it would change broker behavior on upgrade.

            // Given
            val base = mapOf(ProducerConfig.CLIENT_ID_CONFIG to "my-fixed-id")
            val builder = ProducerPropertiesBuilder(base, defaultClientIdPrefix = "tabellarium-checkout")

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then
            assertThat(result.properties)
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "my-fixed-id")
            assertThat(result.mandatoryOverrideViolations).isEmpty()
        }

        @Test
        fun `should set no client id when no prefix is configured`() {
            // Given
            val builder = ProducerPropertiesBuilder(emptyMap())

            // When
            val result = builder.buildFor(TopicClass.TECHNICAL)

            // Then: Kafka's own auto-generated producer-N id applies
            assertThat(result.properties).doesNotContainKey(ProducerConfig.CLIENT_ID_CONFIG)
        }
    }
}
