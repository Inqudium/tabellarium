package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.encoder.EncoderBase
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.ResourceLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.ContextRefreshedEvent

/**
 * Mutates the GLOBAL SLF4J LoggerContext (the binding discovers
 * appenders through LoggerFactory.getILoggerFactory, so a private
 * context cannot be used). The [ResourceLock] serializes this class
 * against every other test that touches the global context, keeping
 * the suite safe if JUnit parallel execution is ever enabled.
 */
@ResourceLock("logback.global-logger-context")
class KafkaAppenderMetricsBindingTest {
    // -- Test fixtures --------------------------------------------------

    /**
     * Minimal encoder: formatted message → UTF-8 bytes. Enough to
     * exercise the appender hot path without pulling in Logstash.
     */
    private class TestEncoder : EncoderBase<ILoggingEvent>() {
        override fun encode(event: ILoggingEvent): ByteArray = event.formattedMessage.toByteArray(Charsets.UTF_8)

        override fun headerBytes(): ByteArray = ByteArray(0)

        override fun footerBytes(): ByteArray = ByteArray(0)
    }

    /**
     * Producer factory returning auto-completing MockProducers, so
     * `producer.send` callbacks fire synchronously. No real Kafka
     * cluster involved.
     */
    private class MockProducerFactory : ProducerFactory {
        override fun create(properties: Map<String, String>): Producer<ByteArray, ByteArray> = MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
    }

    private lateinit var loggerContext: LoggerContext
    private lateinit var appender: KafkaAppender

    @BeforeEach
    fun setUp() {
        // The binding discovers appenders via LoggerFactory.getILoggerFactory()
        // - i.e. the global SLF4J context, not a freshly constructed one.
        loggerContext = LoggerFactory.getILoggerFactory() as LoggerContext

        appender = newStartedAppender()
        loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).addAppender(appender)
    }

    @AfterEach
    fun tearDown() {
        loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).detachAppender(appender)
        appender.stop()
    }

    private fun newStartedAppender(): KafkaAppender =
        KafkaAppender().apply {
            context = loggerContext
            name = "TEST_KAFKA"
            encoder =
                TestEncoder().also {
                    it.context = loggerContext
                    it.start()
                }
            component = "test-service"
            cmdbId = "CMDB-TEST"
            environment = "test"
            kafkaProducerProperties = "${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092"
            topicMapping = TopicMappingConfig().apply { defaultTopic = "default.topic" }
            // Inject the mock producer factory so start() succeeds without
            // a real Kafka cluster. Same hook KafkaAppenderTest uses.
            producerFactory = MockProducerFactory()
            start()
        }

    private fun loggingEvent(message: String = "test message"): ILoggingEvent = newTestLoggingEvent(message = message)

    // -- Tests ----------------------------------------------------------

    @Nested
    inner class `Binding lifecycle` {
        @Test
        fun `should register the appender's counters in the registry after the context refreshes`() {
            // What is to be tested? Whether the binding's
            //   ContextRefreshedEvent handler discovers the appender on
            //   the root logger and invokes bindMeterRegistry on it
            //   end-to-end, such that the registry contains the
            //   appender's metric counters after the context refreshes.
            // How will the test case be deemed successful and why? Successful
            //   if, after the ApplicationContext refresh, the registry
            //   has a `kafka.appender.events.accepted` counter present.
            //   This confirms the binding wired up successfully - a
            //   stronger guarantee than just "the bean was created",
            //   because it asserts the actual outcome of bindMeterRegistry.
            // Why is it important to test this test case? The whole
            //   point of KafkaAppenderMetricsBinding is to spare the
            //   operator the manual binding call. A regression where
            //   the bean is registered but the listener never fires
            //   would not surface in a bean-presence test; it would
            //   surface here as a missing counter.

            // Given/When
            ApplicationContextRunner()
                .withUserConfiguration(MeterRegistryConfig::class.java, BindingConfig::class.java)
                .run { ctx ->
                    val registry = ctx.getBean(MeterRegistry::class.java)
                    // Then: counters are registered for each active class
                    val accepted = registry.find("kafka.appender.events.accepted").counters()
                    assertThat(accepted).isNotEmpty
                }
        }

        @Test
        fun `should produce counter increments through a real appender hot path`() {
            // What is to be tested? Whether the wiring works end-to-end:
            //   binding registers the appender, hot-path events actually
            //   increment the metric, the registry sees the change.
            // How will the test case be deemed successful and why? Successful
            //   if calling appender.doAppend(...) after the binding has
            //   wired up moves the events.accepted counter from 0 to 1.
            //   Pins the actual data path of the integration.
            // Why is it important to test this test case? A regression
            //   that replaced the metrics field with a stale or null
            //   reference would still register the counters (the
            //   binding does that eagerly) but the hot path would no
            //   longer increment them.

            ApplicationContextRunner()
                .withUserConfiguration(MeterRegistryConfig::class.java, BindingConfig::class.java)
                .run { ctx ->
                    val registry = ctx.getBean(MeterRegistry::class.java)
                    val before =
                        registry
                            .find("kafka.appender.events.accepted")
                            .counters()
                            .sumOf { it.count() }

                    // When
                    appender.doAppend(loggingEvent())

                    // Then
                    val after =
                        registry
                            .find("kafka.appender.events.accepted")
                            .counters()
                            .sumOf { it.count() }
                    assertThat(after - before).isEqualTo(1.0)
                }
        }

        @Test
        fun `should attach the configured common tags to every metric`() {
            // Note: this test uses the before/after delta pattern (same
            //   as the hot-path test) rather than an absolute count.
            //   Spring's own lifecycle may emit log events through the
            //   appender between ContextRefreshedEvent (which performs
            //   the binding) and the test's doAppend(...). Those events
            //   are observed by the binding and increment the counter -
            //   asserting absolute counts would make the test brittle
            //   against Spring's internal logging.

            // Given: a binding with two common tags
            ApplicationContextRunner()
                .withUserConfiguration(
                    MeterRegistryConfig::class.java,
                    BindingWithTagsConfig::class.java,
                ).run { ctx ->
                    val registry = ctx.getBean(MeterRegistry::class.java)

                    fun taggedCounters() =
                        registry
                            .find("kafka.appender.events.accepted")
                            .tag("application", "test-service")
                            .tag("region", "eu-central-1")
                            .counters()

                    // First: counters with the expected tags actually exist -
                    //   binding registered them with the right common tags.
                    assertThat(taggedCounters()).isNotEmpty

                    // When: a single hot-path event
                    val before = taggedCounters().sumOf { it.count() }
                    appender.doAppend(loggingEvent())
                    val after = taggedCounters().sumOf { it.count() }

                    // Then: the tagged counter advanced by exactly one,
                    //   proving the common tags reach the actual hot-
                    //   path metric (not just the initial registration).
                    assertThat(after - before).isEqualTo(1.0)
                }
        }

        @Test
        fun `should bind only once even if the context publishes refresh multiple times`() {
            // What is to be tested? Whether the binding is idempotent
            //   on repeated ContextRefreshedEvent firings, which can
            //   happen in tests using ContextHierarchy or in some
            //   reload-on-property-change setups.
            // How will the test case be deemed successful and why? Successful
            //   if, after a second context-refresh event, a single
            //   hot-path event still increments the counter by exactly
            //   one - not by two as would happen if double-binding had
            //   registered duplicate counter references.
            // Why is it important to test this test case? Double-binding
            //   would cause double-counting in production dashboards -
            //   a silent and nearly undetectable data-corruption bug.

            ApplicationContextRunner()
                .withUserConfiguration(MeterRegistryConfig::class.java, BindingConfig::class.java)
                .run { ctx ->
                    val registry = ctx.getBean(MeterRegistry::class.java)
                    // Publish a second refresh event manually
                    val publisher = ctx.sourceApplicationContext
                    publisher.publishEvent(ContextRefreshedEvent(publisher))

                    val before =
                        registry
                            .find("kafka.appender.events.accepted")
                            .counters()
                            .sumOf { it.count() }
                    appender.doAppend(loggingEvent())
                    val after =
                        registry
                            .find("kafka.appender.events.accepted")
                            .counters()
                            .sumOf { it.count() }
                    assertThat(after - before).isEqualTo(1.0)
                }
        }
    }

    // -- Spring test configurations -------------------------------------

    @Configuration
    class MeterRegistryConfig {
        @Bean
        fun meterRegistry(): MeterRegistry = SimpleMeterRegistry()
    }

    @Configuration
    class BindingConfig {
        @Bean
        fun kafkaAppenderMetricsBinding(registry: MeterRegistry) = KafkaAppenderMetricsBinding(registry)
    }

    @Configuration
    class BindingWithTagsConfig {
        @Bean
        fun kafkaAppenderMetricsBinding(registry: MeterRegistry) =
            KafkaAppenderMetricsBinding(
                registry,
                Tags.of(
                    "application",
                    "test-service",
                    "region",
                    "eu-central-1",
                ),
            )
    }
}
