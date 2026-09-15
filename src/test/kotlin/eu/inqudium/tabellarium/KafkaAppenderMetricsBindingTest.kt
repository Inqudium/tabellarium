package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.encoder.EncoderBase
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Meter
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
                MessageBytesEncoder().also {
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
            producerFactory = RecordingProducerFactory()
            start()
        }

    private fun loggingEvent(message: String = "test message"): ILoggingEvent = newTestLoggingEvent(message = message)

    /** Same configuration as the fixture appender, not started - the deferral case. */
    private fun newUnstartedAppender(appenderName: String): KafkaAppender =
        KafkaAppender().apply {
            context = loggerContext
            name = appenderName
            encoder =
                MessageBytesEncoder().also {
                    it.context = loggerContext
                    it.start()
                }
            component = "test-service"
            cmdbId = "CMDB-TEST"
            environment = "test"
            kafkaProducerProperties = "${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092"
            topicMapping = TopicMappingConfig().apply { defaultTopic = "default.topic" }
            producerFactory = RecordingProducerFactory()
        }

    /**
     * Registry whose FIRST counter registration throws - the transient
     * start-up failure of the retry test. MicrometerKafkaAppenderMetrics
     * registers its counters in the constructor, so the throw escapes
     * bindMeterRegistry before anything is bound.
     */
    private class ThrowOnceMeterRegistry : SimpleMeterRegistry() {
        private var thrown = false

        override fun newCounter(id: Meter.Id): Counter {
            if (!thrown) {
                thrown = true
                throw IllegalStateException("registry not ready")
            }
            return super.newCounter(id)
        }
    }

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
            // What is to be tested? Whether the commonTags passed to
            //   KafkaAppenderMetricsBinding travel through bindMeterRegistry into the
            //   MicrometerKafkaAppenderMetrics instance, so the hot-path counters carry
            //   application and region on the real Spring-wired path.
            // How will the test case be deemed successful and why? Successful if
            //   counters found under both tags exist after the refresh and one doAppend
            //   advances that tagged series by exactly 1.0 - registration with the tags
            //   and increments on the tagged meter are both required.
            // Why is it important to test this test case? Common tags are how a shared
            //   registry tells one service's appender metrics from another's; if the
            //   binding dropped them, every dashboard scoped by application would show
            //   nothing while an untagged series silently absorbed the data.

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
            //   reload-on-property-change setups: an appender that is
            //   already bound must be skipped, not re-bound.
            // How will the test case be deemed successful and why? Successful
            //   if the accepted count reached BEFORE the second refresh
            //   event is still there after it, and one more hot-path
            //   event then advances it by exactly one. A re-bind does
            //   not duplicate meters - MetricsBindings.bind unbinds
            //   first - its symptom is a counter reset to zero (fresh
            //   meters), which only an absolute count taken across the
            //   second refresh can see; a delta measured after it stays
            //   1.0 either way (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 4).
            // Why is it important to test this test case? A lost
            //   idempotency guard would reset every counter on each
            //   refresh event - visible on dashboards as spurious
            //   resets and undercounts, and invisible to a delta
            //   assertion.

            // Given: a bound context with events already counted
            ApplicationContextRunner()
                .withUserConfiguration(MeterRegistryConfig::class.java, BindingConfig::class.java)
                .run { ctx ->
                    val registry = ctx.getBean(MeterRegistry::class.java)

                    fun accepted() =
                        registry
                            .find("kafka.appender.events.accepted")
                            .counters()
                            .sumOf { it.count() }
                    repeat(5) { appender.doAppend(loggingEvent()) }
                    val beforeSecondRefresh = accepted()
                    assertThat(beforeSecondRefresh).isGreaterThanOrEqualTo(5.0)

                    // When: the context publishes a second refresh event
                    val publisher = ctx.sourceApplicationContext
                    publisher.publishEvent(ContextRefreshedEvent(publisher))

                    // Then: the count survived (a re-bind would have
                    //   replaced the meters with fresh ones at zero) ...
                    val afterSecondRefresh = accepted()
                    assertThat(afterSecondRefresh).isGreaterThanOrEqualTo(beforeSecondRefresh)

                    // ... and the hot path still feeds the same meters
                    appender.doAppend(loggingEvent())
                    assertThat(accepted() - afterSecondRefresh).isEqualTo(1.0)
                }
        }
    }

    @Nested
    inner class `Deferral and retry` {
        @Test
        fun `should defer an appender that is not started and bind it on a later call`() {
            // What is to be tested? Whether bindAppenders() skips an
            //   appender that has not started yet (bindMeterRegistry on
            //   it would be a no-op) WITHOUT remembering it as done, so
            //   a later bindAppenders() - after the appender started -
            //   binds it.
            // How will the test case be deemed successful and why? Successful
            //   if the not-yet-started appender is unbound after the
            //   context refresh while the started fixture appender is
            //   bound, and a second bindAppenders() after start() binds
            //   the late one. Pins the "never dark forever" promise of
            //   the class KDoc for the deferral branch.
            // Why is it important to test this test case? A regression
            //   that turned "defer" into "skip permanently" (the shape of
            //   finding R2-4 in the 2026-09-07 follow-up analysis) would
            //   leave every appender that starts after the context
            //   refresh without metrics, with no error anywhere.

            // Given: a second appender on the root logger that is not started
            val late = newUnstartedAppender("LATE_KAFKA")
            loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).addAppender(late)
            try {
                ApplicationContextRunner()
                    .withUserConfiguration(MeterRegistryConfig::class.java, BindingConfig::class.java)
                    .run { ctx ->
                        // Then: the refresh bound the started one only
                        assertThat(appender.isMeterRegistryBound).isTrue()
                        assertThat(late.isMeterRegistryBound).isFalse()

                        // When: the late appender starts and the binding is asked again
                        late.start()
                        assertThat(late.isStarted).isTrue()
                        ctx.getBean(KafkaAppenderMetricsBinding::class.java).bindAppenders()

                        // Then: bound now
                        assertThat(late.isMeterRegistryBound).isTrue()
                    }
            } finally {
                loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).detachAppender(late)
                late.stop()
            }
        }

        @Test
        fun `should retry a bind that failed on the previous call`() {
            // What is to be tested? Whether a bindMeterRegistry that
            //   throws (a registry that is not ready, a meter clash) is
            //   reported and leaves the appender unbound, so the next
            //   bindAppenders() retries instead of treating it as done.
            // How will the test case be deemed successful and why? Successful
            //   if the appender is unbound after the context refresh
            //   against a registry that throws on its first counter
            //   registration, and bound after a second bindAppenders()
            //   against the same, now healthy registry. The decision is
            //   made on the appender's bound state, not on identity - the
            //   contract the KDoc states.
            // Why is it important to test this test case? The retry
            //   branch is what keeps a transient start-up failure from
            //   silencing the metrics for the process lifetime; nothing
            //   else would ever call bindMeterRegistry again.

            // Given: a registry whose first counter registration throws
            ApplicationContextRunner()
                .withUserConfiguration(ThrowOnceRegistryConfig::class.java, BindingConfig::class.java)
                .run { ctx ->
                    // Then: the first bind failed and left the appender unbound
                    assertThat(appender.isMeterRegistryBound).isFalse()

                    // When: the binding is asked again
                    ctx.getBean(KafkaAppenderMetricsBinding::class.java).bindAppenders()

                    // Then: bound, with the counters in the registry
                    assertThat(appender.isMeterRegistryBound).isTrue()
                    val registry = ctx.getBean(MeterRegistry::class.java)
                    assertThat(registry.find("kafka.appender.events.accepted").counters()).isNotEmpty
                }
        }
    }

    // -- Spring test configurations -------------------------------------
    // Explicitly open (class and @Bean methods): Spring subclasses
    // @Configuration classes via CGLIB, and the build deliberately does
    // not use the Kotlin all-open/spring compiler plugin - the only
    // classes Spring ever proxies in this project are these fixtures
    // and the (equally explicit) KafkaAppenderMetricsBinding.

    @Configuration
    open class MeterRegistryConfig {
        @Bean
        open fun meterRegistry(): MeterRegistry = SimpleMeterRegistry()
    }

    @Configuration
    open class ThrowOnceRegistryConfig {
        @Bean
        open fun meterRegistry(): MeterRegistry = ThrowOnceMeterRegistry()
    }

    @Configuration
    open class BindingConfig {
        @Bean
        open fun kafkaAppenderMetricsBinding(registry: MeterRegistry) = KafkaAppenderMetricsBinding(registry)
    }

    @Configuration
    open class BindingWithTagsConfig {
        @Bean
        open fun kafkaAppenderMetricsBinding(registry: MeterRegistry) =
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
