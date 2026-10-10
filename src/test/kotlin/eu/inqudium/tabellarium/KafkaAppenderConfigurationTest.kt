package eu.inqudium.tabellarium

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

/**
 * Configuration-time behaviour of [KafkaAppender]: the start-time
 * validation of the mandatory properties and the topic mapping, the
 * construction of the producer pipeline (one producer per active topic
 * class, client-id derivation), the warnings for operator values that
 * collide with a topic class's mandatory overrides, and the transport
 * security signalling for compliance-graded classes.
 */
class KafkaAppenderConfigurationTest : KafkaAppenderTestBase() {
    @Nested
    inner class `Configuration validation` {
        @Test
        fun `should refuse to start when no encoder is configured`() {
            // What is to be tested? Whether start() rejects a configuration
            //   without an <encoder> - the validateConfiguration gate that runs
            //   before any producer is built.
            // How will the test case be deemed successful and why? Successful
            //   if the appender stays unstarted and the status manager carries
            //   the "No <encoder> configured" error. The hot path relies on
            //   checkNotNull(encoder); refusing at start is what keeps that
            //   check from ever firing.
            // Why is it important to test this test case? Without the gate a
            //   missing encoder would surface as the first (and then suppressed)
            //   hot-path error on the first log event instead of as a clear
            //   startup error next to the offending configuration.

            // Given
            val appender = newAppender(encoder = null)

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("No <encoder> configured") }
        }

        @Test
        fun `should refuse to start when component is blank`() {
            // What is to be tested? Whether a blank <component> is rejected by
            //   validateConfiguration before the pipeline is built.
            // How will the test case be deemed successful and why? Successful
            //   if the appender stays unstarted and a status error names
            //   <component> as blank. The component feeds the derived client.id
            //   prefix and the meta.component record header, so it cannot be
            //   defaulted away.
            // Why is it important to test this test case? A blank component
            //   would yield a "tabellarium--technical" client.id and records
            //   nobody can attribute to a service; MessageEnricher would reject
            //   it anyway, but as an opaque pipeline failure rather than a
            //   startup error that points at the missing element.

            // Given
            val appender = newAppender(component = "  ")

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("<component>") && it.contains("blank") }
        }

        @Test
        fun `should refuse to start when cmdbId is blank`() {
            // What is to be tested? Whether a blank <cmdbId> is rejected by
            //   validateConfiguration before the pipeline is built.
            // How will the test case be deemed successful and why? Successful
            //   if the appender stays unstarted and a status error names
            //   <cmdbId> as blank. The value becomes the meta.cmdbId header on
            //   every record, so an empty one would be stamped onto all events.
            // Why is it important to test this test case? The CMDB id is what
            //   ties audit records to the deploying instance; shipping events
            //   without it would break attribution silently. The startup error
            //   is the only signal that points at the missing element.

            // Given
            val appender = newAppender(cmdbId = "")

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("<cmdbId>") && it.contains("blank") }
        }

        @Test
        fun `should refuse to start when environment is blank`() {
            // What is to be tested? Whether a blank <environment> is rejected
            //   by validateConfiguration before the pipeline is built.
            // How will the test case be deemed successful and why? Successful
            //   if the appender stays unstarted and a status error names
            //   <environment> as blank. The value becomes the meta.environment
            //   header on every record and cannot be defaulted.
            // Why is it important to test this test case? Consumers separate
            //   prod from staging traffic by this header; an empty value would
            //   let events from one environment blend into another without any
            //   startup signal that the configuration was incomplete.

            // Given
            val appender = newAppender(environment = "  ")

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("<environment>") && it.contains("blank") }
        }

        @Test
        fun `should refuse to start when default topic is blank and name it with debug off`() {
            // What is to be tested? Whether a blank <defaultTopic> in
            //   <topicMapping> is caught during pipeline construction (via
            //   TopicRouter validation) AND reported with its message on the
            //   documented production default, debug=false - the "named
            //   error" README and guide promise.
            // How will the test case be deemed successful and why? Successful
            //   if the appender stays unstarted and one status error carries
            //   the TopicRouter text "Default topic must not be blank". The
            //   assertion is conjunctive on purpose: the former disjunction
            //   ("Failed to build" OR "Default topic") passed on the generic
            //   line alone and hid that the message was withheld
            //   (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   findings 2 and 3).
            // Why is it important to test this test case? Without the text the
            //   operator sees only the exception type, must enable <debug>,
            //   restart, read, fix and restart again - with every event lost
            //   in between, because a refused appender has no active fallback.

            // Given: the fixture default, debug = false
            val appender = newAppender(defaultTopic = "")

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Failed to build KafkaAppender pipeline") && it.contains("Default topic must not be blank") }
        }

        @Test
        fun `should name an unknown topic class with debug off`() {
            // What is to be tested? Whether a <mapping> with a <topicClass>
            //   that is not a TopicClass constant refuses start() with the
            //   TopicMappingConfig message visible on the default debug=false
            //   path.
            // How will the test case be deemed successful and why? Successful
            //   if a status error names the offending value and the marker;
            //   the plan-stage exception is the library's own validation and
            //   must not be routed under the credential-withholding path.
            // Why is it important to test this test case? A typo in a class
            //   name is the most likely mapping error; "IllegalArgumentException,
            //   enable debug" tells the operator nothing about which of a
            //   dozen mappings is wrong.

            // Given
            val appender = newAppender()
            appender.topicMapping.addMapping(
                TopicMappingEntry().apply {
                    marker = "SECURITY"
                    topic = "audit.security"
                    topicClass = "AUDTI"
                },
            )

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Unknown <topicClass> 'AUDTI'") && it.contains("SECURITY") }
        }

        @Test
        fun `should name a marker mapped twice with debug off`() {
            // What is to be tested? Whether two <mapping> entries for the same
            //   marker refuse start() with the TopicMappingConfig message
            //   naming the marker, on the default debug=false path.
            // How will the test case be deemed successful and why? Successful
            //   if a status error contains "mapped more than once" and the
            //   marker name; the appender must stay unstarted.
            // Why is it important to test this test case? Duplicate markers
            //   are silent in XML (Joran adds both entries); the start-up
            //   message is the only place the operator learns which one.

            // Given
            val appender = newAppender()
            repeat(2) {
                appender.topicMapping.addMapping(
                    TopicMappingEntry().apply {
                        marker = "PERF"
                        topic = "perf.topic-$it"
                        topicClass = "PERFORMANCE"
                    },
                )
            }

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("mapped more than once") && it.contains("'PERF'") }
        }

        @Test
        fun `should name idempotence-incompatible tuning with debug off`() {
            // What is to be tested? Whether the ProducerPropertiesBuilder's
            //   idempotence-compatibility refusal - raised inside
            //   ProducerRegistry.create, on the transport stage - reaches the
            //   operator with its message on debug=false, because it is the
            //   library's own validation and not a producer-construction
            //   failure.
            // How will the test case be deemed successful and why? Successful
            //   if the appender stays unstarted, no producer was created (the
            //   check runs before the factory) and a status error names the
            //   incompatible in-flight value. The builder's KDoc says the
            //   named message exists precisely so the operator does not get
            //   the withheld generic failure - this is the appender-level pin
            //   of that promise.
            // Why is it important to test this test case? The withheld form
            //   "(java.lang.IllegalArgumentException)" reads like a Kafka
            //   construction failure and sends the operator to the broker
            //   configuration instead of to their own acks/in-flight tuning.

            // Given: idempotence requested with too many in-flight requests
            val factory = RecordingProducerFactory()
            val appender =
                newAppender(
                    producerFactory = factory,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG}=true
                        ${ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}=6
                        """.trimIndent(),
                )

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(factory.createdProducers).isEmpty()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("max.in.flight.requests.per.connection=6 is incompatible with enable.idempotence=true") }
        }
    }

    @Nested
    inner class `Pipeline construction` {
        @Test
        fun `should mark the appender as started after a successful start`() {
            // What is to be tested? Whether a valid minimal configuration
            //   completes start() end to end - validation, encoder start,
            //   pipeline build - and finally flips Logback's isStarted flag.
            // How will the test case be deemed successful and why? Successful
            //   if isStarted is true afterwards. super.start() is the last step
            //   of start(); every early return before it leaves the flag false,
            //   so the flag proves that none of the guards fired on a healthy
            //   configuration.
            // Why is it important to test this test case? isStarted is the
            //   ingress gate doAppend checks; a regression that made start()
            //   bail out on valid input would silently drop every event while
            //   the configuration looks correct to the operator.

            // Given
            val appender = newAppender()

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isTrue()
        }

        @Test
        fun `should start the encoder when starting the appender`() {
            // What is to be tested? Whether start() starts the configured
            //   encoder itself instead of relying on Logback's outer
            //   initialization order to have done so.
            // How will the test case be deemed successful and why? Successful
            //   if the encoder reports isStarted after appender.start(). The
            //   encoder is started before the pipeline is built, so a failing
            //   encoder aborts startup while nothing needs rolling back.
            // Why is it important to test this test case? An unstarted encoder
            //   (e.g. LogstashEncoder without its initialized formatter) throws
            //   on the first encode - a hot-path error on every event rather than
            //   a startup failure, in deployments where Logback's own start order
            //   did not reach the nested encoder.

            // Given
            val encoder = RecordingEncoder()
            val appender = newAppender(encoder = encoder)

            // When
            appender.start()

            // Then
            assertThat(encoder.isStarted).isTrue()
        }

        @Test
        fun `should instantiate one producer for the active topic class`() {
            // What is to be tested? Whether the minimal configuration
            //   (only <defaultTopic>) results in a single producer instantiation
            //   for the fallback class.
            // How will the test case be deemed successful and why? Successful
            //   if exactly one MockProducer was created via the factory.
            //   This pins down the single-producer guarantee: existing
            //   deployments do not silently spin up four Kafka producers
            //   when the configuration only mentions one default topic.
            // Why is it important to test this test case? A regression where
            //   the appender always instantiated all four producer classes
            //   would quadruple network threads and buffer memory in every
            //   deployment that uses the minimal configuration - silent
            //   resource bloat that operators would not notice until it
            //   showed up in capacity planning.

            // Given
            val factory = RecordingProducerFactory()
            val appender = newAppender(producerFactory = factory)

            // When
            appender.start()

            // Then
            assertThat(factory.createdProducers).hasSize(1)
        }

        @Test
        fun `should give the producer a client id derived from the component`() {
            // What is to be tested? Whether the appender wires a per-class
            //   client.id default of tabellarium-<component>-<topicclass>
            //   into the producer configuration.
            // How will the test case be deemed successful and why? Successful
            //   if the created producer's properties carry that client.id.
            //   This pins down broker-side attributability: connections,
            //   quotas, and kafka.producer metrics name the service instead
            //   of Kafka's generic producer-N.
            // Why is it important to test this test case? Without the
            //   default, every producer in the JVM shows up as producer-N on
            //   the broker, and operators cannot tell which service (or
            //   which topic class) a connection belongs to.

            // Given
            val factory = RecordingProducerFactory()
            val appender = newAppender(producerFactory = factory, component = "checkout-service")

            // When
            appender.start()

            // Then
            assertThat(factory.createdWithProperties.single())
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "tabellarium-checkout-service-technical")
        }

        @Test
        fun `should sanitize the component for the client id`() {
            // What is to be tested? Whether characters that are unsafe in JMX
            //   object names and metric tags are replaced before the component
            //   becomes part of the client.id.
            // How will the test case be deemed successful and why? Successful
            //   if a component with spaces and special characters yields a
            //   client.id containing only [a-zA-Z0-9._-]. This pins down the
            //   sanitization contract of the derived id.
            // Why is it important to test this test case? An unsanitized
            //   client.id breaks JMX MBean registration in the Kafka client,
            //   which surfaces as confusing warnings at startup in every
            //   deployment whose component name contains a space.

            // Given
            val factory = RecordingProducerFactory()
            val appender = newAppender(producerFactory = factory, component = "My Service (prod)")

            // When
            appender.start()

            // Then
            assertThat(factory.createdWithProperties.single())
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "tabellarium-My-Service--prod--technical")
        }

        @Test
        fun `should let an operator-supplied client id win`() {
            // What is to be tested? Whether an explicit client.id in
            //   <kafkaProducerProperties> is respected - the derived
            //   tabellarium-<component>-<class> id is only a putIfAbsent default.
            // How will the test case be deemed successful and why? Successful
            //   if the created producer carries the operator's "pinned-id"
            //   instead of the derived value. This pins the precedence rule of
            //   ProducerPropertiesBuilder for the client.id default.
            // Why is it important to test this test case? Operators pin the
            //   client.id for broker quotas, ACLs and existing dashboards; an
            //   appender that overruled it would break those silently, with
            //   the mismatch only visible on the broker side.

            // Given: the operator pins client.id in kafkaProducerProperties
            val factory = RecordingProducerFactory()
            val appender =
                newAppender(
                    producerFactory = factory,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${ProducerConfig.CLIENT_ID_CONFIG}=pinned-id
                        """.trimIndent(),
                )

            // When
            appender.start()

            // Then
            assertThat(factory.createdWithProperties.single())
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "pinned-id")
        }
    }

    @Nested
    inner class `Mandatory override warnings` {
        @Test
        fun `should emit a status warning when a user value conflicts with a mandatory override`() {
            // What is to be tested? Whether mandatory-override violations
            //   from ProducerPropertiesBuilder are surfaced to operators
            //   via Logback's status manager - the addWarn wiring in
            //   start(), not just the builder-level violation records.
            //   Activated through the real configuration surface: a
            //   <mapping> classifying a topic as AUDIT.
            // How will the test case be deemed successful and why? Successful
            //   if a warning naming the property key, the user value, and
            //   the enforced value reaches the status manager. The warning
            //   is the only mechanism by which operators learn that their
            //   configuration intent was overruled for compliance reasons.
            // Why is it important to test this test case? Silent enforcement
            //   would let an operator believe their acks=1 had taken effect
            //   for audit topics, when in fact acks=all was forced. Auditors
            //   would later find a discrepancy between the documented
            //   configuration and the actual broker behavior.

            // Given: an AUDIT mapping and a conflicting operator acks=1
            val factory = RecordingProducerFactory()
            val appender =
                newAppender(
                    producerFactory = factory,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${ProducerConfig.ACKS_CONFIG}=1
                        """.trimIndent(),
                )
            appender.topicMapping.addMapping(
                TopicMappingEntry().apply {
                    marker = "SECURITY"
                    topic = "audit.security"
                    topicClass = "AUDIT"
                },
            )

            // When
            appender.start()

            // Then: the appender started and the violation was surfaced
            assertThat(appender.isStarted).isTrue()
            assertThat(appender.statusMessages())
                .anyMatch {
                    it.contains("Mandatory override applied") &&
                        it.contains(ProducerConfig.ACKS_CONFIG) &&
                        it.contains("'1'") &&
                        it.contains("'all'")
                }
        }

        @Test
        fun `should apply the mandatory overrides of a defaultTopicClass to the single producer`() {
            // What is to be tested? Whether <defaultTopicClass>AUDIT
            //   upgrades the default stream itself: one producer, AUDIT
            //   client id, mandatory overrides enforced, violation warned.
            // How will the test case be deemed successful and why? Successful
            //   if exactly one producer exists, its client.id carries the
            //   -audit suffix, acks was forced to all despite the operator's
            //   acks=1, and the override warning reached the status manager.
            // Why is it important to test this test case? This is the direct
            //   path for compliance-grading the default stream - previously
            //   only reachable via a synthetic marker mapping that left a
            //   dormant TECHNICAL producer running.

            // Given
            val factory = RecordingProducerFactory()
            val appender =
                newAppender(
                    producerFactory = factory,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${ProducerConfig.ACKS_CONFIG}=1
                        """.trimIndent(),
                )
            appender.topicMapping.defaultTopicClass = "AUDIT"

            // When
            appender.start()

            // Then: single AUDIT producer with enforced overrides
            assertThat(appender.isStarted).isTrue()
            assertThat(factory.createdProducers).hasSize(1)
            assertThat(factory.createdWithProperties.single())
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "tabellarium-test-service-audit")
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Mandatory override applied") && it.contains("'all'") }
        }

        @Test
        fun `should instantiate one producer per class activated by mappings`() {
            // What is to be tested? Whether a <mapping> with a non-default
            //   topic class activates a second producer next to the
            //   TECHNICAL fallback producer - the multi-class model
            //   reached through the real configuration surface.
            // How will the test case be deemed successful and why? Successful
            //   if exactly two producers exist (AUDIT + TECHNICAL) and the
            //   AUDIT one carries the per-class client.id suffix. This
            //   pins the activation path end-to-end at the appender level.
            // Why is it important to test this test case? The per-class
            //   producer model was previously verifiable only by driving
            //   internals directly; with the surface shipped, the
            //   activation itself is part of the operator contract.

            // Given
            val factory = RecordingProducerFactory()
            val appender = newAppender(producerFactory = factory)
            appender.topicMapping.addMapping(
                TopicMappingEntry().apply {
                    marker = "SECURITY"
                    topic = "audit.security"
                    topicClass = "AUDIT"
                },
            )

            // When
            appender.start()

            // Then: AUDIT + TECHNICAL fallback producers
            assertThat(factory.createdProducers).hasSize(2)
            assertThat(factory.createdWithProperties)
                .anyMatch { it[ProducerConfig.CLIENT_ID_CONFIG] == "tabellarium-test-service-audit" }
            assertThat(factory.createdWithProperties)
                .anyMatch { it[ProducerConfig.CLIENT_ID_CONFIG] == "tabellarium-test-service-technical" }
        }

        @Test
        fun `should emit no override warning with the minimal configuration`() {
            // What is to be tested? Whether the mandatory-override warning is
            //   emitted only on actual conflicts: with the default-topic-only
            //   configuration every topic is TECHNICAL, a class without mandates.
            // How will the test case be deemed successful and why? Successful
            //   if no "Mandatory override applied" status message exists after
            //   start(). Together with the previous test this pins the warning
            //   to the presence of a real violation record.
            // Why is it important to test this test case? A warning that fires
            //   on every plain deployment would train operators to ignore it -
            //   and with it the one signal that tells them their acks or
            //   idempotence setting was overruled on an audit topic.

            // Given: the minimal configuration (everything TECHNICAL, no
            //   mandates) - the baseline the previous test's setup departs from
            val appender = newAppender()

            // When
            appender.start()

            // Then
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("Mandatory override applied") }
        }
    }

    @Nested
    inner class `Transport security signalling` {
        @Test
        fun `should warn when a compliance-graded class ships over cleartext`() {
            // What is to be tested? Whether an active AUDIT class combined
            //   with an unset (i.e. PLAINTEXT) security.protocol produces a
            //   startup warning.
            // How will the test case be deemed successful and why? Successful
            //   if a status warning names the graded class and the
            //   security.protocol setting. The appender enforces durability
            //   for graded classes and warns when overruling the operator;
            //   staying silent about cleartext transport would be the one
            //   compliance dimension without a signal.
            // Why is it important to test this test case? Audit records that
            //   travel unencrypted are readable and tamperable by anyone on
            //   the network path - the operator has to learn that from the
            //   startup log, since the appender deliberately does not (and
            //   cannot) enforce TLS itself.

            // Given: an AUDIT mapping, no security.protocol configured
            val appender = newAppender()
            appender.topicMapping.addMapping(
                TopicMappingEntry().apply {
                    marker = "SECURITY"
                    topic = "audit.security"
                    topicClass = "AUDIT"
                },
            )

            // When
            appender.start()

            // Then
            assertThat(appender.statusMessages())
                .anyMatch {
                    it.contains("cleartext transport") &&
                        it.contains("AUDIT") &&
                        it.contains(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
                }
        }

        @Test
        fun `should warn when another running appender derives the same client id`() {
            // What is to be tested? Whether a second running appender with
            //   the same <component> - and therefore the same derived
            //   client.id per class - is warned about at start, and only it.
            // How will the test case be deemed successful and why? Successful
            //   if the second appender's status names the shared client.id,
            //   the first appender (started alone) has no such warning, an
            //   appender with a distinct component gets none either, and a
            //   restart of the same component after the first stopped is
            //   quiet again. The guide used to promise that derived ids
            //   never collide
            //   (docs/assessment/DEFECT_ANALYSIS-2026-10-10T15-19-31.md,
            //   finding 7).
            // Why is it important to test this test case? Kafka keys its JMX
            //   MBeans by client.id: the two producers' JMX metrics collide
            //   and the first stop removes them for both, with nothing in the
            //   appender's own output pointing at the cause.

            // Given: two appenders with one component, one with another
            val first = newAppender(component = "shared-service")
            val second = newAppender(component = "shared-service")
            val distinct = newAppender(component = "other-service")
            val sharedWarning: (String) -> Boolean = {
                it.contains("tabellarium-shared-service-technical") && it.contains("also used by another running KafkaAppender")
            }

            // When
            first.start()
            second.start()
            distinct.start()

            // Then: only the second is warned
            assertThat(first.statusMessages()).noneMatch(sharedWarning)
            assertThat(second.statusMessages()).anyMatch(sharedWarning)
            assertThat(distinct.statusMessages()).noneMatch { it.contains("also used by another running KafkaAppender") }

            // And: once both are stopped, a new one with the component is quiet
            first.stop()
            second.stop()
            val successor = newAppender(component = "shared-service")
            successor.start()
            assertThat(successor.statusMessages()).noneMatch(sharedWarning)
        }

        @Test
        fun `should not warn when the graded class is configured for SSL`() {
            // What is to be tested? Whether the cleartext warning is keyed on
            //   the effective security.protocol: the same AUDIT mapping as the
            //   previous test, but with SSL configured, must stay quiet.
            // How will the test case be deemed successful and why? Successful
            //   if no "cleartext transport" status message exists after start().
            //   This pins the negative half of the check - PLAINTEXT (explicit
            //   or absent) warns, an encrypted protocol does not.
            // Why is it important to test this test case? A warning that also
            //   fires for correctly secured deployments would be noise, and
            //   operators who learn to ignore it would miss the case where the
            //   graded class really does travel in the clear.

            // Given: the same AUDIT mapping, but SSL configured
            val appender =
                newAppender(
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${CommonClientConfigs.SECURITY_PROTOCOL_CONFIG}=SSL
                        """.trimIndent(),
                )
            appender.topicMapping.addMapping(
                TopicMappingEntry().apply {
                    marker = "SECURITY"
                    topic = "audit.security"
                    topicClass = "AUDIT"
                },
            )

            // When
            appender.start()

            // Then
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("cleartext transport") }
        }

        @Test
        fun `should not warn for the minimal configuration without graded classes`() {
            // What is to be tested? Whether the warning is scoped to classes
            //   that actually carry compliance mandates. TECHNICAL has none,
            //   so a plain default-topic deployment must stay quiet.
            // How will the test case be deemed successful and why? Successful
            //   if no cleartext warning appears for the minimal (TECHNICAL-
            //   only) configuration, which is the common case.
            // Why is it important to test this test case? A warning that
            //   fires for every deployment would be trained away within a
            //   week, and would be gone when it finally matters.

            // Given: minimal configuration, no security.protocol
            val appender = newAppender()

            // When
            appender.start()

            // Then
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("cleartext transport") }
        }
    }
}
