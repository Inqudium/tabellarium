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
            // What is to be tested? Whether an empty base still yields a complete AUDIT
            //   configuration - both mandates plus the class defaults - without a violation.
            // How will the test case be deemed successful and why? Successful if the result
            //   carries acks=all, enable.idempotence=true, linger.ms=50 and compression lz4
            //   and the violation list is empty: nothing user-set means nothing to overrule.
            // Why is it important to test this test case? An operator who configures nothing
            //   class-specific must still get the compliance mandates; the class itself, not
            //   the operator, is the source of acks=all for AUDIT.

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
            // What is to be tested? Whether a base property that neither the default nor the
            //   mandatory layer mentions passes through the merge unchanged.
            // How will the test case be deemed successful and why? Successful if
            //   bootstrap.servers survives an AUDIT build with its original value. This pins
            //   down that the override layers add to the base rather than replace it.
            // Why is it important to test this test case? bootstrap.servers, SSL and SASL
            //   settings all belong to this untouched category; dropping them would leave
            //   the producer unable to reach or authenticate against the broker.

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
            // What is to be tested? Whether a class default fills in a property the operator
            //   left unset.
            // How will the test case be deemed successful and why? Successful if an AUDIT
            //   build from an empty base carries linger.ms=50 - a default override, not a
            //   mandate, so its presence proves the default layer ran.
            // Why is it important to test this test case? The defaults are the appender's
            //   tuned batching/compression baseline; if the layer silently stopped applying,
            //   every deployment would fall back to Kafka's raw defaults with no warning.

            // Given
            val builder = ProducerPropertiesBuilder(emptyMap())

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then: linger.ms is a default override (not mandatory) for AUDIT
            assertThat(result.properties).containsEntry(ProducerConfig.LINGER_MS_CONFIG, "50")
        }

        @Test
        fun `should preserve a user-set property when a default override exists for the same key`() {
            // What is to be tested? Whether the default layer is putIfAbsent: an operator's
            //   value for a key that also has a class default must win.
            // How will the test case be deemed successful and why? Successful if linger.ms
            //   stays at the operator's 999 instead of AUDIT's default 50, and no violation
            //   is recorded - defaults are suggestions, never conflicts.
            // Why is it important to test this test case? Defaults exist to be tunable; if
            //   they overruled the operator, latency or throughput tuning would be silently
            //   discarded, and a spurious violation would misreport it as a mandate clash.

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
            // What is to be tested? Whether the mandatory layer overrules an explicit,
            //   conflicting operator value.
            // How will the test case be deemed successful and why? Successful if AUDIT
            //   yields acks=all although the base set acks=1: the enforced value, not the
            //   operator's, ends up in the produced properties.
            // Why is it important to test this test case? acks=all for AUDIT is the
            //   compliance guarantee in regulated environments; an operator tuning acks for
            //   throughput must not be able to weaken it, intentionally or by accident.

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
            // What is to be tested? Whether applying a mandate to a key the operator never
            //   set is treated as a plain default, not as a conflict.
            // How will the test case be deemed successful and why? Successful if an AUDIT
            //   build from an empty base records no violation although acks and
            //   enable.idempotence were both injected.
            // Why is it important to test this test case? Violations become startup
            //   warnings; reporting one for every mandate on every start would train
            //   operators to ignore the warning that matters - a real overruled intent.

            // Given
            val builder = ProducerPropertiesBuilder(emptyMap())

            // When
            val result = builder.buildFor(TopicClass.AUDIT)

            // Then: no conflict, because nothing to conflict with
            assertThat(result.mandatoryOverrideViolations).isEmpty()
        }

        @Test
        fun `should record no violation when the user value already matches the enforced value`() {
            // What is to be tested? Whether an operator value that already equals the
            //   mandated value passes without being reported as a conflict.
            // How will the test case be deemed successful and why? Successful if a base
            //   with acks=all yields no violation for AUDIT: a violation requires a
            //   difference in value, not merely the presence of the key.
            // Why is it important to test this test case? Operators who deliberately spell
            //   out acks=all for documentation should not be warned that their intent was
            //   overruled - it was not.

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
            // What is to be tested? Whether every conflicting key yields its own violation
            //   and every mandate is still applied when several conflicts occur at once.
            // How will the test case be deemed successful and why? Successful if acks=0 and
            //   enable.idempotence=false against AUDIT produce exactly two violations while
            //   the properties carry acks=all and enable.idempotence=true.
            // Why is it important to test this test case? A short-circuit after the first
            //   conflict would leave one weakened setting unreported - the operator would
            //   fix the warned key and never learn about the other.

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
            // What is to be tested? The complete AUDIT mandate set: acks=all and
            //   enable.idempotence=true, applied against an operator weakening both.
            // How will the test case be deemed successful and why? Successful if both
            //   enforced values appear in the properties and two violations are recorded,
            //   one per overruled key.
            // Why is it important to test this test case? AUDIT is the class for
            //   compliance-relevant streams (BaFin/MaRisk); this test pins its exact
            //   mandate set so a change to TopicClass cannot quietly drop one guarantee.

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
            // What is to be tested? The FUNCTIONAL mandate set: acks=all, and nothing more.
            // How will the test case be deemed successful and why? Successful if acks=0 is
            //   replaced by acks=all with exactly one violation - idempotence is not
            //   mandated here, so no second violation may appear.
            // Why is it important to test this test case? FUNCTIONAL sits between AUDIT and
            //   TECHNICAL: durability is enforced, idempotence stays tunable. Pinning the
            //   count guards both the guarantee and the deliberate absence of the second.

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
            // What is to be tested? Whether TECHNICAL has no mandates at all: an operator's
            //   weak acks value must be kept verbatim.
            // How will the test case be deemed successful and why? Successful if acks=0
            //   survives the build and no violation is recorded - the same input AUDIT
            //   would overrule and report.
            // Why is it important to test this test case? TECHNICAL is the high-volume
            //   debug class where throughput tuning (acks=0/1) is legitimate; a mandate
            //   creeping in would cut every deployment's log throughput for no compliance
            //   gain.

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
            // What is to be tested? Whether PERFORMANCE, like TECHNICAL, carries no
            //   mandates and keeps an operator's acks=0.
            // How will the test case be deemed successful and why? Successful if acks=0 is
            //   returned unchanged and the violation list is empty.
            // Why is it important to test this test case? PERFORMANCE is the very-high-
            //   volume metrics class whose default acks=1 is documented as a balance, not a
            //   requirement; the operator must retain full control over the trade-off.

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
            // What is to be tested? Whether the properties map handed out in the result is
            //   read-only rather than the builder's internal working map.
            // How will the test case be deemed successful and why? Successful if a cast to
            //   MutableMap followed by a put throws UnsupportedOperationException - the
            //   observable signature of java.util.Map.copyOf.
            // Why is it important to test this test case? The same result is shared with
            //   the producer factory and the registry's effectiveProperties diagnostics; a
            //   mutable map would let one consumer alter what another one reports or uses.

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
            // What is to be tested? Whether the second idempotence precondition - at most
            //   five in-flight requests per connection - is validated with a named message.
            // How will the test case be deemed successful and why? Successful if AUDIT with
            //   max.in.flight.requests.per.connection=6 throws an IllegalArgumentException
            //   quoting the offending value and the "at most 5" limit.
            // Why is it important to test this test case? Raising in-flight requests is a
            //   common throughput tweak; without this check the Kafka constructor rejects it
            //   with a ConfigException the appender withholds unless <debug> is on.

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
        fun `should reject acks below all when idempotence is enabled`() {
            // What is to be tested? Whether the named validation covers
            //   the third idempotence precondition, acks=all - the one
            //   an operator hits most easily, because acks=1 is a common
            //   throughput tuning.
            // How will the test case be deemed successful and why? Successful
            //   if an explicit enable.idempotence=true together with
            //   acks=1 on a class without the mandate throws an
            //   IllegalArgumentException naming acks and idempotence.
            // Why is it important to test this test case? Without the
            //   check the Kafka client rejects the same combination with
            //   a ConfigException whose text the appender withholds
            //   unless <debug> is on - the operator then sees only the
            //   exception type.

            // Given
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "true",
                        ProducerConfig.ACKS_CONFIG to "1",
                    ),
                )

            // When / Then
            assertThatThrownBy { builder.buildFor(TopicClass.TECHNICAL) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("acks=1")
                .hasMessageContaining("enable.idempotence=true")
        }

        @Test
        fun `should not let the class acks default contradict an explicit idempotence request`() {
            // What is to be tested? Whether the TECHNICAL/PERFORMANCE
            //   acks=1 default steps aside when the operator explicitly
            //   requests enable.idempotence=true, so the appender's own
            //   default never manufactures the acks/idempotence conflict.
            // How will the test case be deemed successful and why? Successful
            //   if building TECHNICAL from a base that sets only
            //   enable.idempotence=true yields no acks entry at all (the
            //   Kafka default acks=all then applies) and no exception.
            // Why is it important to test this test case? Before the fix,
            //   exactly this minimal, reasonable configuration refused to
            //   start with a withheld ConfigException - caused by a value
            //   the operator never wrote.

            // Given
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "true"),
                )

            // When
            val result = builder.buildFor(TopicClass.TECHNICAL)

            // Then: no acks default injected, idempotence kept
            assertThat(result.properties)
                .doesNotContainKey(ProducerConfig.ACKS_CONFIG)
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
            assertThat(result.mandatoryOverrideViolations).isEmpty()
        }

        @Test
        fun `should not apply the idempotence checks to classes without the mandate`() {
            // What is to be tested? Whether the idempotence validation is conditional on
            //   idempotence actually being in effect, not applied to every class.
            // How will the test case be deemed successful and why? Successful if TECHNICAL
            //   accepts retries=0 and six in-flight requests verbatim - the very tuning
            //   AUDIT refuses - without throwing.
            // Why is it important to test this test case? Without idempotence those values
            //   are valid Kafka configuration; refusing them for TECHNICAL would block
            //   legitimate fire-and-forget throughput tuning on the high-volume class.

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
    inner class `Max block cap` {
        @Test
        fun `should keep an operator value at or below the class cap`() {
            // What is to be tested? Whether max.block.ms behaves as a CAP,
            //   not a fixed mandate: an operator tightening the bound must
            //   win.
            // How will the test case be deemed successful and why? Successful
            //   if a value below the 500 ms TECHNICAL ceiling survives
            //   unchanged and produces no violation.
            // Why is it important to test this test case? Latency-sensitive
            //   deployments legitimately configure a lower block budget; a
            //   mandate-style enforcement would overrule the safer choice.

            // Given
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.MAX_BLOCK_MS_CONFIG to "100"),
                )

            // When
            val result = builder.buildFor(TopicClass.TECHNICAL)

            // Then
            assertThat(result.properties).containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100")
            assertThat(result.mandatoryOverrideViolations).isEmpty()
        }

        @Test
        fun `should clamp an operator value above the class cap and record a violation`() {
            // What is to be tested? Whether a max.block.ms above the class
            //   ceiling is clamped and surfaced. producer.send blocks the
            //   logging caller's thread for up to max.block.ms when
            //   metadata is missing or the buffer is full; the appender's
            //   documented worst-case caller latency only holds if this
            //   bound cannot be raised through configuration.
            // How will the test case be deemed successful and why? Successful
            //   if the built properties carry the 500 ms ceiling instead of
            //   the operator's 60000 and the overruled intent is recorded
            //   as a violation for the startup warning.
            // Why is it important to test this test case? Before the cap,
            //   an operator could - with Kafka's own 60 s default in mind -
            //   configure a value that lets every request thread hang for
            //   a minute per log event during a broker outage.

            // Given: Kafka's own default of 60 seconds
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.MAX_BLOCK_MS_CONFIG to "60000"),
                )

            // When
            val result = builder.buildFor(TopicClass.TECHNICAL)

            // Then: clamped to the ceiling, conflict recorded
            assertThat(result.properties).containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, "500")
            assertThat(result.mandatoryOverrideViolations).anySatisfy { violation ->
                assertThat(violation.propertyKey).isEqualTo(ProducerConfig.MAX_BLOCK_MS_CONFIG)
                assertThat(violation.userValue).isEqualTo("60000")
                assertThat(violation.enforcedValue).isEqualTo("500")
            }
        }

        @Test
        fun `should apply the tighter PERFORMANCE cap`() {
            // What is to be tested? Whether the max.block.ms cap is read per class: 400 ms
            //   is fine for TECHNICAL (500) but must be clamped for PERFORMANCE (200).
            // How will the test case be deemed successful and why? Successful if the
            //   PERFORMANCE build carries max.block.ms=200 and records a violation with that
            //   enforced value.
            // Why is it important to test this test case? A cap hard-wired to 500 would
            //   pass this input untouched; the per-class value is what keeps the dispatcher
            //   stall on the highest-volume class shortest during a broker outage.

            // Given: below the TECHNICAL cap but above PERFORMANCE's 200 ms
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.MAX_BLOCK_MS_CONFIG to "400"),
                )

            // When
            val result = builder.buildFor(TopicClass.PERFORMANCE)

            // Then
            assertThat(result.properties).containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, "200")
            assertThat(result.mandatoryOverrideViolations)
                .anySatisfy { violation ->
                    assertThat(violation.propertyKey).isEqualTo(ProducerConfig.MAX_BLOCK_MS_CONFIG)
                    assertThat(violation.enforcedValue).isEqualTo("200")
                }
        }

        @Test
        fun `should clamp an unparseable value and record a violation`() {
            // What is to be tested? Whether garbage in max.block.ms falls
            //   back to the safe ceiling instead of reaching the Kafka
            //   client (which would refuse producer construction and take
            //   the whole appender down with it).
            // How will the test case be deemed successful and why? Successful
            //   if the ceiling is enforced and the discarded operator value
            //   appears in a violation, so the typo is visible at startup.
            // Why is it important to test this test case? The property
            //   arrives as free text from XML; a typo must degrade to a
            //   safe default with a warning, not to a dead logging pipeline.

            // Given
            val builder =
                ProducerPropertiesBuilder(
                    mapOf(ProducerConfig.MAX_BLOCK_MS_CONFIG to "half a second"),
                )

            // When
            val result = builder.buildFor(TopicClass.TECHNICAL)

            // Then
            assertThat(result.properties).containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, "500")
            assertThat(result.mandatoryOverrideViolations)
                .anySatisfy { violation ->
                    assertThat(violation.propertyKey).isEqualTo(ProducerConfig.MAX_BLOCK_MS_CONFIG)
                    assertThat(violation.userValue).isEqualTo("half a second")
                }
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
            // What is to be tested? Whether the client.id default is opt-in: without a
            //   defaultClientIdPrefix the builder must not invent one.
            // How will the test case be deemed successful and why? Successful if the built
            //   properties contain no client.id key at all, so Kafka's own producer-N
            //   auto-generation applies.
            // Why is it important to test this test case? Deployments that never set the
            //   prefix must keep the client ids they had before the feature existed; an
            //   unconditional default would change broker-side metrics and quota keys on
            //   upgrade.

            // Given
            val builder = ProducerPropertiesBuilder(emptyMap())

            // When
            val result = builder.buildFor(TopicClass.TECHNICAL)

            // Then: Kafka's own auto-generated producer-N id applies
            assertThat(result.properties).doesNotContainKey(ProducerConfig.CLIENT_ID_CONFIG)
        }
    }
}
