package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.status.Status
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.jupiter.api.AfterEach

/**
 * Shared fixture of the `KafkaAppender*Test` classes, which together
 * prove the composition root: configuration, startup, the hot path and
 * its guards, dispatch, shutdown, metrics lifecycle and fallback wiring
 * each live in their own class (the single file had grown to 4 000
 * lines).
 *
 * Send and fallback dispatch are asynchronous (the production path).
 * Tests therefore either stop() the appender before asserting - the
 * stop sequence drains the send dispatchers through the still-open
 * producers and then the fallback dispatcher, and the thread joins
 * in close() establish the happens-before edge for reading the
 * MockProducer history - or poll with [pollUntil] where the appender
 * must keep running across the assertion.
 */
abstract class KafkaAppenderTestBase {
    /**
     * Every appender a test built; stopped after the test (stop() is
     * idempotent) so no dispatcher worker, fallback worker or
     * MockProducer outlives the test that created it.
     */
    private val createdAppenders = mutableListOf<KafkaAppender>()

    @AfterEach
    fun stopAppenders() {
        createdAppenders.forEach { it.stop() }
        createdAppenders.clear()
    }

    internal fun newAppender(
        encoder: Encoder<ILoggingEvent>? = RecordingEncoder(),
        component: String = "test-service",
        cmdbId: String = "CMDB-TEST",
        environment: String = "test",
        defaultTopic: String = "default.topic",
        debug: Boolean = false,
        producerFactory: ProducerFactory = RecordingProducerFactory(),
        fallback: Appender<ILoggingEvent>? = null,
        kafkaProducerProperties: String = "${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092",
    ): KafkaAppender =
        KafkaAppender().also { createdAppenders += it }.apply {
            this.context = LoggerContext()
            this.encoder = encoder
            this.component = component
            this.cmdbId = cmdbId
            this.environment = environment
            this.kafkaProducerProperties = kafkaProducerProperties
            this.topicMapping =
                TopicMappingConfig().apply {
                    this.defaultTopic = defaultTopic
                }
            this.debug = debug
            this.producerFactory = producerFactory
            // The fallback slot is filled via addAppender (the same path
            // Joran's AppenderRefAction takes for <appender-ref>).
            fallback?.let { addAppender(it) }
        }

    internal fun KafkaAppender.statusMessages(): List<String> = context.statusManager.copyOfStatusList.map { it.message }

    internal fun KafkaAppender.statusEntries(): List<Status> = context.statusManager.copyOfStatusList
}
