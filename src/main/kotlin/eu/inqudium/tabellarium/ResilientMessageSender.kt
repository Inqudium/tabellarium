package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import eu.inqudium.tabellarium.ResilientMessageSender.Companion.DEFAULT_HALF_OPEN_PROBE_GAP
import eu.inqudium.tabellarium.ResilientMessageSender.Companion.circuitBreakerName
import eu.inqudium.tabellarium.ResilientMessageSender.Companion.defaultCircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * Sends encoded log events to Kafka through per-topic-class circuit
 * breakers, with an optional fallback Logback appender for when the
 * breaker is open or the producer call fails.
 *
 * Marked `internal`: this class is an implementation detail of
 * [KafkaAppender] and is not intended for direct use from outside the
 * module. The constructor takes a [FallbackDispatcher], which is also
 * internal - exposing both as public API would commit to maintaining a
 * Java-callable surface for which there is no current use case.
 *
 * ## Resilience model
 *
 * One [CircuitBreaker] is acquired from [circuitBreakerRegistry] per
 * active topic class in [producerRegistry]. Audit, functional, technical,
 * and performance topics fail independently of each other - a stuck
 * audit-topic broker does not throttle technical-log delivery and vice
 * versa.
 *
 * For each call to [send] the sender:
 *
 * 1. Asks its circuit breaker for permission via
 *    [CircuitBreaker.tryAcquirePermission]. Denied → routes the original
 *    [ILoggingEvent] to [fallbackAppender]; never invokes the producer.
 * 2. Builds the [ProducerRecord] (topic + key + value + headers) and
 *    invokes `producer.send` with a callback.
 *      - Callback success → [CircuitBreaker.onSuccess].
 *      - Callback error → [CircuitBreaker.onError], then route to fallback.
 *      - Synchronous throw from `producer.send` (e.g. buffer full after
 *        `max.block.ms`, or producer closed) → [CircuitBreaker.onError],
 *        then route to fallback.
 *
 * The Future returned by `producer.send` is deliberately not retained:
 * delivery outcome is reported exclusively through the callback, so
 * delivery failures are never silent.
 *
 * ## Half-open throttling
 *
 * In addition to the per-class breakers, the sender wraps each breaker
 * in a [HalfOpenThrottle] that spreads probe permissions over time
 * during the HALF_OPEN state. Without throttling, all probes are
 * dispatched within microseconds at high logging volume, causing all
 * further events to be routed to the fallback for the duration of the
 * Kafka round-trip even though the cluster may have already recovered.
 * The throttle admits one probe per [halfOpenProbeGap] in HALF_OPEN
 * state and is transparent in CLOSED or OPEN. Set
 * [halfOpenProbeGap] to [Duration.ZERO] to disable.
 *
 * ## Fallback
 *
 * [fallbackDispatcher] is a queue-and-worker decoupling between this
 * sender and the actual fallback `Appender` (typically a `FileAppender`
 * configured in the user's `logback-spring.xml` and passed in via
 * `<appender-ref>`). The dispatcher must be queue-based, not synchronous,
 * because the Kafka send callback runs on the producer's I/O thread
 * (see Threading below). When the dispatcher is null, events for which
 * Kafka delivery is unavailable are silently dropped - the deliberate
 * operator choice: configuring a fallback is the operator's way of
 * saying "loss is unacceptable here"; leaving it null is the operator's
 * way of saying "best-effort is fine".
 *
 * ## Threading
 *
 * The callback runs on the Kafka producer's I/O thread
 * (`kafka-producer-network-thread`). This thread is shared across all
 * in-flight requests of the producer; blocking it stalls all subsequent
 * callbacks. Therefore the sender **must not** call a potentially-
 * blocking fallback appender from the callback. [FallbackDispatcher]
 * solves this by accepting the event into a bounded queue in O(1) and
 * draining it from a dedicated daemon thread, so the Kafka I/O thread
 * returns immediately even if the fallback appender (e.g. `FileAppender`
 * under slow disk) is blocked. Events that overflow the dispatcher
 * queue are dropped, with the count exposed for operator diagnostics.
 *
 * @param producerRegistry The registry holding one producer per active
 *                         topic class. Must outlive this sender.
 * @param circuitBreakerRegistry Resilience4j registry from which one
 *                               breaker per active topic class is
 *                               acquired by name. The naming convention
 *                               is exposed via [circuitBreakerName]
 *                               so operators can override the default
 *                               configuration for individual breakers.
 * @param fallbackDispatcher Asynchronous bridge to the fallback
 *                           [ch.qos.logback.core.Appender]. Null means
 *                           "drop events on Kafka delivery failure".
 * @param halfOpenProbeGap Minimum time between two probe admissions
 *                         per topic class while the corresponding
 *                         circuit breaker is in HALF_OPEN state.
 *                         Defaults to [DEFAULT_HALF_OPEN_PROBE_GAP];
 *                         set to [Duration.ZERO] to disable the
 *                         throttle entirely (every event reaches the
 *                         breaker). See [HalfOpenThrottle] for the
 *                         rationale.
 * @param nanoTimeSource Monotonic time source used by the per-class
 *                       [HalfOpenThrottle] instances. Defaults to
 *                       [System.nanoTime]; tests inject a deterministic
 *                       source.
 */
internal class ResilientMessageSender(
    private val producerRegistry: ProducerRegistry,
    val circuitBreakerRegistry: CircuitBreakerRegistry,
    private val fallbackDispatcher: FallbackDispatcher?,
    halfOpenProbeGap: Duration = DEFAULT_HALF_OPEN_PROBE_GAP,
    nanoTimeSource: () -> Long = System::nanoTime,
) {
    private val circuitBreakersByClass: Map<TopicClass, CircuitBreaker> =
        producerRegistry.activeTopicClasses.associateWith { topicClass ->
            circuitBreakerRegistry.circuitBreaker(circuitBreakerName(topicClass))
        }

    private val throttlesByClass: Map<TopicClass, HalfOpenThrottle> =
        circuitBreakersByClass.mapValues { (_, breaker) ->
            HalfOpenThrottle(breaker, halfOpenProbeGap, nanoTimeSource)
        }

    /**
     * Pluggable metrics hook. Defaults to [KafkaAppenderMetrics.NO_OP];
     * replaced by [setMetrics] when an operator wires the appender to
     * a Micrometer registry. The reference is `@Volatile` because the
     * setter may be called from a Spring bootstrap thread while the
     * Kafka I/O callback thread is concurrently reading it for the
     * send-completion timer.
     */
    @Volatile
    private var metrics: KafkaAppenderMetrics = KafkaAppenderMetrics.NO_OP

    /**
     * Replaces the [KafkaAppenderMetrics] implementation. Called by
     * [KafkaAppender.bindMeterRegistry] after the Logback context has
     * been wired up. Idempotent - multiple calls simply replace the
     * previous instance.
     */
    fun setMetrics(metrics: KafkaAppenderMetrics) {
        this.metrics = metrics
    }

    /**
     * Sends the given payload to Kafka via the producer assigned to
     * [topicClass]. If the circuit is open or the send fails, [originalEvent]
     * is enqueued for asynchronous delivery to the fallback appender.
     *
     * @throws IllegalStateException if [topicClass] is not active in the
     *                               registry. This is a programming error
     *                               (configuration drift), not a runtime
     *                               condition.
     */
    fun send(
        topicClass: TopicClass,
        topicName: String,
        payload: ByteArray,
        enrichment: EnrichedRecord,
        originalEvent: ILoggingEvent,
    ) {
        val circuitBreaker =
            circuitBreakersByClass[topicClass]
                ?: error("Topic class $topicClass is not active in this registry")
        val throttle = throttlesByClass.getValue(topicClass)
        // Snapshot the volatile reference once per call so all metric
        // hooks for this event use the same implementation, even if
        // setMetrics is called concurrently mid-send.
        val m = metrics

        // Throttle first: when HALF_OPEN and the gap has not elapsed,
        // route to the fallback without consuming a Resilience4j
        // permission. See HalfOpenThrottle KDoc for the rationale.
        if (!throttle.mayAttemptProbe()) {
            m.eventFallback(topicClass, KafkaAppenderMetrics.FallbackReason.THROTTLE)
            sendToFallback(originalEvent)
            return
        }

        if (!circuitBreaker.tryAcquirePermission()) {
            // Breaker is OPEN, or HALF_OPEN with no further permitted calls.
            m.eventFallback(topicClass, KafkaAppenderMetrics.FallbackReason.BREAKER_OPEN)
            sendToFallback(originalEvent)
            return
        }

        val producer = producerRegistry.producerFor(topicClass)
        val record = buildRecord(topicName, payload, enrichment)
        val startNanos = System.nanoTime()

        try {
            // The Future returned here is intentionally discarded; the callback
            // is the single source of truth for delivery outcome. The callback
            // runs on the Kafka producer's I/O thread - sendToFallback must
            // therefore be non-blocking (handled by FallbackDispatcher).
            producer.send(record) { _, exception ->
                val elapsed = System.nanoTime() - startNanos
                val elapsedDuration = Duration.ofNanos(elapsed)
                if (exception != null) {
                    circuitBreaker.onError(elapsed, TimeUnit.NANOSECONDS, exception)
                    m.sendCompleted(topicClass, KafkaAppenderMetrics.SendOutcome.ERROR, elapsedDuration)
                    m.eventFallback(topicClass, KafkaAppenderMetrics.FallbackReason.SEND_ERROR)
                    sendToFallback(originalEvent)
                } else {
                    circuitBreaker.onSuccess(elapsed, TimeUnit.NANOSECONDS)
                    m.sendCompleted(topicClass, KafkaAppenderMetrics.SendOutcome.SUCCESS, elapsedDuration)
                }
            }
            // Count "handed to producer.send successfully" only after the
            // call returns. A synchronous throw below means the dispatch
            // did not happen.
            m.eventDispatched(topicClass)
        } catch (e: Exception) {
            // Synchronous failure from producer.send: closed producer, buffer
            // exhaustion after max.block.ms elapsed, illegal record, etc.
            val elapsed = System.nanoTime() - startNanos
            circuitBreaker.onError(elapsed, TimeUnit.NANOSECONDS, e)
            m.sendCompleted(topicClass, KafkaAppenderMetrics.SendOutcome.ERROR, Duration.ofNanos(elapsed))
            m.eventFallback(topicClass, KafkaAppenderMetrics.FallbackReason.SEND_ERROR)
            sendToFallback(originalEvent)
        }
    }

    private fun sendToFallback(event: ILoggingEvent) {
        // Null dispatcher means "drop": operator's explicit choice.
        // Non-null dispatcher is enqueue-only - the actual doAppend
        // runs on the dispatcher's own worker thread, decoupling the
        // Kafka I/O thread from a potentially-blocking fallback
        // appender.
        fallbackDispatcher?.enqueue(event)
    }

    private fun buildRecord(
        topicName: String,
        payload: ByteArray,
        enrichment: EnrichedRecord,
    ): ProducerRecord<ByteArray, ByteArray> {
        val key = enrichment.partitioningKey?.toByteArray(Charsets.UTF_8)
        val record = ProducerRecord<ByteArray, ByteArray>(topicName, null, null, key, payload)
        // Header values are already UTF-8-encoded by the MessageEnricher
        // at construction time. We pass the arrays by reference - Kafka
        // does not defensive-copy them, so the enricher and downstream
        // code treat them as read-only by convention.
        enrichment.headers.forEach { (name, valueBytes) ->
            record.headers().add(name, valueBytes)
        }
        return record
    }

    companion object {
        /**
         * Returns the Resilience4j circuit-breaker name used for the given
         * topic class. Exposed so operators can register a class-specific
         * configuration on the [CircuitBreakerRegistry] before constructing
         * the sender.
         */
        fun circuitBreakerName(topicClass: TopicClass): String = "kafka-appender-${topicClass.name.lowercase()}"

        /**
         * Default circuit-breaker configuration tuned for logging traffic.
         *
         * Defaults differ from Resilience4j's out-of-the-box config: the
         * 100-call minimum window is too large for logging, where we want
         * to trip the breaker quickly once Kafka starts failing. The values
         * here trip after roughly 10 failures in a 20-call sliding window,
         * stay open for 30 seconds, and then admit 10 probe calls in
         * half-open before deciding.
         *
         * The half-open count of 10 is calibrated for asynchronous I/O:
         * the producer.send callback completes after a network round trip
         * (typically 10-50 ms with a healthy Kafka cluster, longer on
         * congested links). During the probe window all incoming events
         * beyond the permitted count are routed to the fallback. A
         * too-small count would over-route to the fallback whenever the
         * breaker recovered; a too-large count would prolong the period
         * of uncertainty if the cluster is still degraded. Ten is a
         * compromise that operators may want to tune per topic class -
         * see [circuitBreakerName] for the per-class override path.
         *
         * ## Ignored exceptions
         *
         * Client-side, deterministically payload-dependent exceptions are
         * registered as `ignoreExceptions` - they do not count toward the
         * failure rate. The rationale: the circuit breaker is an
         * **infrastructure-health** signal ("is Kafka reachable?"), not a
         * **payload-validation** filter. A buggy application that suddenly
         * logs 2 MB stacktraces would otherwise produce a stream of
         * [org.apache.kafka.common.errors.RecordTooLargeException]s that
         * open the breaker for the entire topic class - silencing
         * legitimate logs from the same service even though the Kafka
         * cluster is perfectly healthy. The same logic applies to
         * [org.apache.kafka.common.errors.InvalidTopicException],
         * [org.apache.kafka.common.errors.SerializationException], and
         * [org.apache.kafka.common.errors.TopicAuthorizationException]:
         * all are deterministic, all are insensitive to retry, and all
         * would survive a breaker recovery cycle anyway. The individual
         * failed events still go to the fallback appender (the operator's
         * configured escape hatch for delivery failures), so no log is
         * lost; only the breaker statistics are spared.
         *
         * Transient infrastructure exceptions -
         * [org.apache.kafka.common.errors.TimeoutException],
         * [org.apache.kafka.common.errors.NetworkException],
         * [org.apache.kafka.common.errors.LeaderNotAvailableException],
         * [org.apache.kafka.common.errors.NotEnoughReplicasException] -
         * are NOT ignored. These are exactly the conditions the breaker
         * exists to react to.
         */
        fun defaultCircuitBreakerConfig(): CircuitBreakerConfig =
            CircuitBreakerConfig
                .custom()
                .failureRateThreshold(50f)
                .slidingWindowSize(20)
                .minimumNumberOfCalls(10)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(10)
                .ignoreExceptions(
                    org.apache.kafka.common.errors.RecordTooLargeException::class.java,
                    org.apache.kafka.common.errors.InvalidTopicException::class.java,
                    org.apache.kafka.common.errors.SerializationException::class.java,
                    org.apache.kafka.common.errors.TopicAuthorizationException::class.java,
                ).build()

        /**
         * Convenience factory for a registry preconfigured with
         * [defaultCircuitBreakerConfig].
         */
        fun defaultCircuitBreakerRegistry(): CircuitBreakerRegistry = CircuitBreakerRegistry.of(defaultCircuitBreakerConfig())

        /**
         * Default minimum gap between two probe admissions in HALF_OPEN
         * state. Five milliseconds is calibrated against the typical
         * Kafka send round-trip (10-50 ms): the gap is short enough that
         * 10 probes complete within ~50 ms of wall time, long enough that
         * a busy logger producing thousands of events per second is not
         * starved of fallback routing during the probe window.
         */
        val DEFAULT_HALF_OPEN_PROBE_GAP: Duration = Duration.ofMillis(5)
    }
}
