package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.encoder.EncoderBase
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
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
