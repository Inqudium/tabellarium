package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.encoder.EncoderBase
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.TimeGauge
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.search.Search
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer

/*
 * Shared test support for the pipeline tests. One recorder, one
 * producer factory, three encoders - the fixtures every test class
 * used to declare privately (four recorders, ~17 producer doubles,
 * six encoders across the suite;
 * docs/assessment/ARCHITECTURE_REVIEW-2026-09-07T20-23-00.md, finding 4).
 * Test-specific producer doubles that model one
 * behavior (blocking, self-logging, synchronous callback errors) stay
 * next to the test that needs them; the shared factory takes them as
 * the `wrap` function.
 */

/**
 * A started, thread-safe recording appender: the fallback recorder of
 * the suite. Events are read from the test thread while a dispatcher
 * worker appends - the copy-on-write list of [ThreadSafeListAppender]
 * gives every read a fully published snapshot.
 *
 * @param context Logback context; set so `doAppend` emits no
 *                "No context given" status noise. A fresh one per
 *                recorder is fine for the programmatic tests.
 */
internal class RecordingAppender(
    context: LoggerContext = LoggerContext(),
) : ThreadSafeListAppender() {
    init {
        this.context = context
        start()
    }

    fun eventCount(): Int = events.size
}

/**
 * [ProducerFactory] returning [MockProducer]s and recording what it
 * created and with which properties.
 *
 * @param autoComplete `true` completes every send synchronously with
 *                     success; `false` defers to `completeNext()` /
 *                     `errorNext(...)` on the test's side.
 * @param wrap Optional decorator around each created mock - the seam
 *             for producer doubles that model client behavior
 *             `MockProducer` lacks.
 */
internal class RecordingProducerFactory(
    private val autoComplete: Boolean = true,
    private val wrap: (MockProducer<ByteArray, ByteArray>) -> Producer<ByteArray, ByteArray> = { it },
) : ProducerFactory {
    val createdProducers = mutableListOf<MockProducer<ByteArray, ByteArray>>()
    val createdWithProperties = mutableListOf<Map<String, String>>()

    override fun create(properties: Map<String, String>): Producer<ByteArray, ByteArray> {
        val mock = MockProducer(autoComplete, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
        createdProducers += mock
        createdWithProperties += properties
        return wrap(mock)
    }
}

/**
 * Stateless encoder: the formatted message as UTF-8 bytes. Safe for
 * concurrent appends. Explicitly open for [RecordingEncoder] - the
 * build has no all-open plugin, so every subclassable class says so.
 */
internal open class MessageBytesEncoder : EncoderBase<ILoggingEvent>() {
    override fun encode(event: ILoggingEvent): ByteArray = event.formattedMessage.toByteArray(Charsets.UTF_8)

    override fun headerBytes(): ByteArray = ByteArray(0)

    override fun footerBytes(): ByteArray = ByteArray(0)
}

/**
 * [MessageBytesEncoder] that additionally records every encoded event.
 * The list is a plain `ArrayList`: encoding runs on the caller's
 * thread, so use it only from single-threaded tests.
 */
internal class RecordingEncoder : MessageBytesEncoder() {
    val encodedEvents = mutableListOf<ILoggingEvent>()

    override fun encode(event: ILoggingEvent): ByteArray {
        encodedEvents += event
        return super.encode(event)
    }
}

/** Encoder whose `encode` always throws - the hot-path failure injector. */
internal class ThrowingEncoder : EncoderBase<ILoggingEvent>() {
    override fun encode(event: ILoggingEvent): ByteArray = throw RuntimeException("simulated encoder failure")

    override fun headerBytes(): ByteArray = ByteArray(0)

    override fun footerBytes(): ByteArray = ByteArray(0)
}

// region Meter lookups

/*
 * Rationale: one lookup form for every test that reads a meter. A
 * missing meter fails with its name and tags instead of a bare NPE
 * after `isNotNull` plus a non-null assertion, or an "expected 1.0
 * but was null" from a `?.count()` passed into `assertThat`
 * (CODE_STYLE-2026-10-10T16-38-23, findings 1 and 2). Tests that
 * assert a meter's absence keep using `find(...)` directly.
 */

private fun <M : Meter> MeterRegistry.requireMeter(
    kind: String,
    name: String,
    tags: Array<out Pair<String, String>>,
    lookup: (Search) -> M?,
): M = lookup(find(name).tags(tags.map { Tag.of(it.first, it.second) })) ?: error("$kind $name with tags ${tags.toList()} not registered")

/** The counter [name] carrying (at least) [tags]; fails naming both when absent. */
internal fun MeterRegistry.requireCounter(
    name: String,
    vararg tags: Pair<String, String>,
): Counter = requireMeter("counter", name, tags) { it.counter() }

/** The function counter [name] carrying (at least) [tags]; fails naming both when absent. */
internal fun MeterRegistry.requireFunctionCounter(
    name: String,
    vararg tags: Pair<String, String>,
): FunctionCounter = requireMeter("function counter", name, tags) { it.functionCounter() }

/** The gauge [name] carrying (at least) [tags]; fails naming both when absent. */
internal fun MeterRegistry.requireGauge(
    name: String,
    vararg tags: Pair<String, String>,
): Gauge = requireMeter("gauge", name, tags) { it.gauge() }

/** The time gauge [name] carrying (at least) [tags]; fails naming both when absent. */
internal fun MeterRegistry.requireTimeGauge(
    name: String,
    vararg tags: Pair<String, String>,
): TimeGauge = requireMeter("time gauge", name, tags) { it.timeGauge() }

/** The timer [name] carrying (at least) [tags]; fails naming both when absent. */
internal fun MeterRegistry.requireTimer(
    name: String,
    vararg tags: Pair<String, String>,
): Timer = requireMeter("timer", name, tags) { it.timer() }

/** The distribution summary [name] carrying (at least) [tags]; fails naming both when absent. */
internal fun MeterRegistry.requireSummary(
    name: String,
    vararg tags: Pair<String, String>,
): DistributionSummary = requireMeter("summary", name, tags) { it.summary() }

// endregion

// region Appender fixture

/**
 * An unstarted [KafkaAppender] with every mandatory property set: the
 * one place a newly mandatory property is added
 * (CODE_STYLE-2026-10-10T16-38-23, finding 21). Callers override only
 * what their test is about and own the lifecycle (start, stop, cleanup
 * lists).
 *
 * @param name Appender name; `null` keeps the appender's own default.
 * @param fallback Filled via `addAppender`, the path Joran's
 *                 `<appender-ref>` takes.
 */
internal fun newTestAppender(
    context: LoggerContext = LoggerContext(),
    name: String? = null,
    encoder: Encoder<ILoggingEvent>? = MessageBytesEncoder(),
    component: String = "test-service",
    cmdbId: String = "CMDB-TEST",
    environment: String = "test",
    kafkaProducerProperties: String = "${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092",
    topicMapping: TopicMappingConfig = TopicMappingConfig().apply { defaultTopic = "default.topic" },
    producerFactory: ProducerFactory = RecordingProducerFactory(),
    fallback: Appender<ILoggingEvent>? = null,
): KafkaAppender =
    KafkaAppender().apply {
        this.context = context
        name?.let { this.name = it }
        this.encoder = encoder
        this.component = component
        this.cmdbId = cmdbId
        this.environment = environment
        this.kafkaProducerProperties = kafkaProducerProperties
        this.topicMapping = topicMapping
        this.producerFactory = producerFactory
        fallback?.let { addAppender(it) }
    }

// endregion
