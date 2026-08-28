package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.UnsynchronizedAppenderBase
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.spi.AppenderAttachable
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import org.apache.kafka.clients.CommonClientConfigs
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Logback appender that ships log events to Kafka with per-topic-class
 * circuit breakers, compliance-driven producer configuration, and an
 * optional fallback appender.
 *
 * This is the orchestrator: it wires together the individual components
 * ([TopicRouter], [TopicTable], [MessageEnricher], [ProducerRegistry],
 * [SendDispatcher], [ResilientMessageSender]), exposes the XML
 * configuration surface to Joran, and runs the per-event hot path.
 *
 * ## Configuration surface
 *
 * ```xml
 * <appender name="KAFKA" class="eu.inqudium.tabellarium.KafkaAppender">
 *   <encoder class="net.logstash.logback.encoder.LogstashEncoder">...</encoder>
 *   <kafkaProducerProperties>
 *     bootstrap.servers=broker:9092
 *     security.protocol=SSL
 *     ...
 *   </kafkaProducerProperties>
 *   <topicMapping>
 *     <defaultTopic>my.application.logs</defaultTopic>
 *   </topicMapping>
 *   <environment>${STAGE}</environment>
 *   <component>${ARTIFACT_ID}</component>
 *   <cmdbId>MyApplication</cmdbId>
 *   <debug>false</debug>                     <!-- optional -->
 *   <sendQueueCapacity>1024</sendQueueCapacity>       <!-- optional -->
 *   <includeCallerData>false</includeCallerData>      <!-- optional -->
 *   <appender-ref ref="FALLBACK_FILE"/>      <!-- optional -->
 * </appender>
 * ```
 *
 * The Joran round-trip test (`JoranXmlConfigurationTest`) binds every
 * element of this example; a new setter belongs in both.
 *
 * [debug] affects **startup diagnostics only** - it has no per-event
 * effect. A note to that effect is emitted to the status manager when
 * [debug] is `true`.
 *
 * ## Lifecycle
 *
 * - **[start]** validates configuration eagerly, builds the pipeline,
 *   and surfaces any [MandatoryOverrideViolation] from
 *   [ProducerPropertiesBuilder] as warnings on the Logback status
 *   manager. Misconfiguration causes `addError` plus refusal to start;
 *   the appender stays `isStarted=false` and downstream `doAppend` calls
 *   are no-ops.
 * - **[append]** runs only CPU-bound work on the caller's thread:
 *   routing, encoding, enrichment. The potentially-blocking
 *   `producer.send` (up to the per-class `max.block.ms` cap while
 *   Kafka metadata or buffer space is missing) happens on a
 *   per-topic-class [SendDispatcher] worker - the caller enqueues in
 *   O(1) and returns; a full queue diverts to the fallback instead of
 *   blocking. No synchronization, no per-event allocation outside
 *   what the encoder and sender already require. Hot-path exceptions
 *   are logged **once** (via [AtomicBoolean]-guarded `addError`) and
 *   route to [fallbackAppender] if configured; subsequent errors are
 *   suppressed to prevent log storms.
 * - **[stop]** first closes Logback's ingress gate (`isStarted`) so no
 *   new event enters the teardown, then closes the [SendDispatcher]s
 *   (their drain still sends through the open producers; the remainder
 *   diverts to the fallback), then the [ProducerRegistry] with its configured
 *   timeout, then the fallback dispatcher, stops the encoder, and
 *   completes via `super.stop()`. Per-resource close failures are
 *   recorded as warnings but do not prevent the rest of the shutdown
 *   sequence.
 *
 * ## Why UnsynchronizedAppenderBase
 *
 * `AppenderBase`'s `doAppend` is `synchronized`. In a Reactor Netty /
 * virtual-thread environment that lock causes carrier-thread pinning
 * and ripples up the call chain as
 * back-pressure. The components used in [append]
 * are all thread-safe (Kafka `Producer.send` is documented thread-safe;
 * the [TopicRouter] / [TopicTable] / [MessageEnricher] are pure
 * functions; Resilience4j `CircuitBreaker` is thread-safe), so the
 * lock is unnecessary.
 */
class KafkaAppender :
    UnsynchronizedAppenderBase<ILoggingEvent>(),
    AppenderAttachable<ILoggingEvent> {
    // -- Joran-populated configuration ---------------------------------

    /** Encoder turning the log event into a Kafka record payload. */
    var encoder: Encoder<ILoggingEvent>? = null

    /** Raw text of the `<kafkaProducerProperties>` element. */
    var kafkaProducerProperties: String = ""

    /** Nested `<topicMapping>` configuration. */
    var topicMapping: TopicMappingConfig = TopicMappingConfig()

    /** Deployment environment (e.g. `prod`, `staging`). Trimmed on set. */
    var environment: String = ""
        set(value) {
            field = value.trim()
        }

    /** Service component identifier. Trimmed on set. */
    var component: String = ""
        set(value) {
            field = value.trim()
        }

    /** CMDB identifier of the deploying instance. Trimmed on set. */
    var cmdbId: String = ""
        set(value) {
            field = value.trim()
        }

    /**
     * Enables additional startup diagnostics in the status manager.
     *
     * The flag affects **startup only** - it has no per-event effect.
     * Operators should consider removing `<debug>true</debug>` from
     * their configuration.
     */
    var debug: Boolean = false

    /**
     * When `true`, the caller data (class, method, line of the logging
     * site) is captured on the caller's thread before the event crosses
     * to the asynchronous send/fallback workers - the same opt-in
     * contract as Logback's own `AsyncAppender`. Off by default because
     * the stack walk is expensive relative to the rest of the hot path.
     * Only relevant when a fallback appender's layout consumes
     * `%caller`; without the flag, caller data computed on a worker
     * thread would point at the worker, not the logging site.
     */
    var includeCallerData: Boolean = false

    /**
     * Optional Logback appender invoked when the circuit is open or a
     * send fails. Typically configured in the XML via the standard
     * `<appender-ref ref="FALLBACK_FILE"/>` element pointing at a file
     * appender. Null means "drop events on failure".
     *
     * The setter is private; the slot is filled either via
     * [addAppender] (which is what Joran's `AppenderRefAction` calls
     * when it encounters `<appender-ref>`) or, in tests, by calling
     * [addAppender] directly.
     */
    var fallbackAppender: Appender<ILoggingEvent>? = null
        private set

    // -- Internally injectable for tests --------------------------------

    /**
     * Producer factory. Default builds real [org.apache.kafka.clients.producer.KafkaProducer]
     * instances. Tests replace this with a factory returning `MockProducer`.
     */
    internal var producerFactory: ProducerFactory = ProducerFactory.default()

    /**
     * Resilience4j circuit-breaker registry. Default uses
     * [ResilientMessageSender.defaultCircuitBreakerRegistry] which is
     * tuned for logging traffic.
     */
    internal var circuitBreakerRegistry: CircuitBreakerRegistry =
        ResilientMessageSender.defaultCircuitBreakerRegistry()

    /**
     * Capacity of each per-topic-class [SendDispatcher] queue - the
     * bounded hand-off between the logging caller and the worker that
     * performs `producer.send`. Configurable via
     * `<sendQueueCapacity>` in the XML. When the queue is full, events
     * divert to the fallback (reason `queue.full`) instead of blocking
     * the caller.
     */
    var sendQueueCapacity: Int = SendDispatcher.DEFAULT_QUEUE_CAPACITY

    // -- Pipeline state, built in start() -------------------------------

    private lateinit var topicRouter: TopicRouter
    private lateinit var topicTable: TopicTable
    private lateinit var messageEnricher: MessageEnricher
    private lateinit var producerRegistry: ProducerRegistry
    private lateinit var messageSender: ResilientMessageSender

    /**
     * One asynchronous send hand-off per active topic class - the
     * component that keeps `producer.send` off the logging caller's
     * thread. Built in [buildPipeline], closed FIRST in [stop] (before
     * the producer registry, so the drain can still send).
     */
    private var sendDispatchers: Map<TopicClass, SendDispatcher> = emptyMap()

    /**
     * Asynchronous dispatcher between the Kafka callback / synchronous
     * failure paths and the fallback appender. Null when no
     * [fallbackAppender] is configured. Built in [start], closed in
     * [stop]. See [FallbackDispatcher] for the rationale.
     */
    private var fallbackDispatcher: FallbackDispatcher? = null

    /**
     * Guard against hot-path log storms: only the first error gets
     * `addError`-logged, subsequent errors fall back silently. Atomic
     * because [append] may run concurrently on multiple threads.
     */
    private val firstHotPathErrorLogged = AtomicBoolean(false)

    /**
     * The effective `client.id` values of this appender's producers,
     * snapshot from the [ProducerRegistry] in [buildPipeline]. Used by
     * the self-logging guard in [append]; see there.
     */
    private var producerClientIds: Set<String> = emptySet()

    /**
     * Pluggable metrics hook. Defaults to [KafkaAppenderMetrics.NO_OP].
     * Replaced by [bindMeterRegistry] when an operator wires the
     * appender to a Micrometer registry - typically from a Spring
     * `@PostConstruct` after the application context is ready.
     * Volatile because the setter may be called from a different
     * thread than the hot path.
     */
    @Volatile
    private var metrics: KafkaAppenderMetrics = KafkaAppenderMetrics.NO_OP

    /**
     * Owns the Micrometer bind/unbind lifecycle (appender meters,
     * per-producer Kafka client metrics, circuit-breaker metrics).
     * See [MetricsBindings] for the probe pattern and the teardown
     * rationale.
     */
    private val metricsBindings = MetricsBindings(this)

    /**
     * Per-thread reentry guard for [append]. Logback's
     * `UnsynchronizedAppenderBase` ships only a no-op guard, so a log
     * event emitted *synchronously from inside the append path itself*
     * would re-enter [append] on the same thread. That is not
     * hypothetical: the Kafka 4.x client logs `ApiException`s at DEBUG
     * on the **caller's** thread in `KafkaProducer.doSend`'s synchronous
     * failure path - before the send callback runs. With
     * `org.apache.kafka` at DEBUG and the appender attached at the root
     * logger, each such log would recursively invoke `producer.send`
     * again (the network-thread-name guard below cannot catch it, the
     * event carries the application thread's name), stacking up repeated
     * `max.block.ms` waits and ultimately a `StackOverflowError`.
     * Reentrant events are dropped entirely - same policy as the
     * network-thread guard: no metrics, no fallback.
     */
    private val inAppend = ThreadLocal.withInitial { false }

    /**
     * Guards the teardown in [stop] so a repeated stop (Logback may call
     * it more than once during context teardown) does not re-run the
     * close sequence - re-closing the dispatcher would double-count its
     * remaining queue as dropped and re-emit the drop warning. Reset in
     * [start] in case the appender is ever restarted.
     */
    private val stopExecuted = AtomicBoolean(false)

    // -- Lifecycle ------------------------------------------------------

    override fun start() {
        if (isStarted) {
            // Idempotence guard: a second start() would rebuild the whole
            // pipeline and overwrite the references to the running one -
            // orphaning producers (network threads, buffers, MBeans) and
            // a fallback worker that no later stop() could ever reach.
            addWarn("KafkaAppender is already started; ignoring repeated start().")
            return
        }
        if (!validateConfiguration()) {
            return // addError was already called for each failure
        }
        stopExecuted.set(false)

        // Start the encoder BEFORE the pipeline exists: encoders are
        // self-contained, so a failing encoder.start() aborts the
        // startup while there is nothing to roll back yet. Logback
        // start() methods are idempotent, so starting an already-started
        // encoder is safe - we start it ourselves to handle the case
        // where Logback's outer initialization order hasn't done so.
        try {
            checkNotNull(encoder) { "encoder was validated non-null in validateConfiguration" }.start()
        } catch (e: Exception) {
            addError("Failed to start encoder (${e.javaClass.name}): ${e.message}", e)
            return
        }

        try {
            buildPipeline()
        } catch (e: Exception) {
            // buildPipeline rolled its own resources back; the encoder
            // started above is the only thing left to release.
            runCatching { encoder?.stop() }
            // The exception text originates in the Kafka client and is
            // built from credential-bearing configuration. Kafka masks
            // Password-typed values in its own output, but that text is
            // not under this appender's control - so the default path
            // reports only the exception type, and the message plus the
            // stack trace stay behind <debug>. See SECURITY.md on
            // credential leakage through status output.
            if (debug) {
                addError("Failed to build KafkaAppender pipeline: ${e.message}", e)
            } else {
                addError(
                    "Failed to build KafkaAppender pipeline (${e.javaClass.name}). " +
                        "Set <debug>true</debug> to include the cause and stack trace; " +
                        "the details are withheld here because they may echo producer " +
                        "configuration values.",
                )
            }
            return
        }

        producerRegistry.mandatoryOverrideViolations.forEach { violation ->
            addWarn(buildViolationMessage(violation))
        }

        warnOnCleartextTransportForGradedClasses()

        if (debug) {
            emitDebugDiagnostics()
        }

        super.start()
    }

    private fun validateConfiguration(): Boolean {
        var ok = true
        if (encoder == null) {
            addError("No <encoder> configured for KafkaAppender")
            ok = false
        }
        if (component.isBlank()) {
            addError("<component> must not be blank")
            ok = false
        }
        if (cmdbId.isBlank()) {
            addError("<cmdbId> must not be blank")
            ok = false
        }
        if (environment.isBlank()) {
            addError("<environment> must not be blank")
            ok = false
        }
        if (sendQueueCapacity <= 0) {
            addError("<sendQueueCapacity> must be positive (was $sendQueueCapacity)")
            ok = false
        }
        return ok
    }

    private fun buildPipeline() {
        val baseProperties = parseKafkaProducerProperties(kafkaProducerProperties)
        topicRouter = topicMapping.toTopicRouter()
        topicTable = topicMapping.toTopicTable()
        messageEnricher =
            MessageEnricher(
                component = component,
                cmdbId = cmdbId,
                environment = environment,
            )
        val registry =
            ProducerRegistry.create(
                propertiesBuilder =
                    ProducerPropertiesBuilder(
                        baseProperties,
                        defaultClientIdPrefix = "tabellarium-${jmxSafe(component)}",
                    ),
                activeTopicClasses = topicTable.activeTopicClasses,
                producerFactory = producerFactory,
            )
        // From here on real resources exist (producers, worker threads).
        // Any later construction failure rolls them back in reverse
        // ownership order - mirroring stop() - so a failed or reloaded
        // configuration never leaks producers or daemon workers that
        // only an external stop() call could reach. The fields are
        // published only on full success.
        var newFallbackDispatcher: FallbackDispatcher? = null
        val newSendDispatchers = LinkedHashMap<TopicClass, SendDispatcher>()
        try {
            // Wrap the fallback appender in a dispatcher so the Kafka I/O
            // thread is never blocked on the fallback's downstream I/O.
            // See FallbackDispatcher KDoc for the rationale.
            newFallbackDispatcher =
                fallbackAppender?.let {
                    FallbackDispatcher(
                        it,
                        onWorkerDeath = { t ->
                            addWarn(
                                "Fallback dispatcher worker died from ${t.javaClass.name}; " +
                                    "queued fallback events will be dropped and counted.",
                                t,
                            )
                        },
                    )
                }
            val sender =
                ResilientMessageSender(
                    producerRegistry = registry,
                    circuitBreakerRegistry = circuitBreakerRegistry,
                    fallbackDispatcher = newFallbackDispatcher,
                )
            // One send dispatcher per active class: producer.send runs on
            // the dispatcher's worker, never on the logging caller. The
            // per-class split mirrors the producer/breaker isolation - a
            // stalled AUDIT send cannot delay TECHNICAL delivery.
            registry.activeTopicClasses.forEach { topicClass ->
                newSendDispatchers[topicClass] =
                    SendDispatcher(
                        topicClass = topicClass,
                        sendAction = { pending ->
                            // claimDiversion shares the per-item exactly-once
                            // guard with the dispatcher, so a forced-shutdown
                            // divert and the sender's own error routing can
                            // never both deliver the same event.
                            sender.send(
                                topicClass,
                                pending.topicName,
                                pending.payload,
                                pending.enrichment,
                                pending.originalEvent,
                                claimDiversion = pending::tryClaimDiversion,
                            )
                        },
                        fallbackDispatcher = newFallbackDispatcher,
                        reentryGuard = inAppend,
                        queueCapacity = sendQueueCapacity,
                        onWorkerDeath = { t ->
                            addWarn(
                                "Send dispatcher worker for $topicClass died from ${t.javaClass.name}; " +
                                    "queued and further $topicClass events divert to the fallback " +
                                    "(reason send.error).",
                                t,
                            )
                        },
                    )
            }
            producerRegistry = registry
            producerClientIds = registry.clientIds
            fallbackDispatcher = newFallbackDispatcher
            messageSender = sender
            sendDispatchers = newSendDispatchers
        } catch (e: Exception) {
            newSendDispatchers.values.forEach { dispatcher -> runCatching { dispatcher.close() } }
            runCatching { registry.close() }
            newFallbackDispatcher?.let { dispatcher -> runCatching { dispatcher.close() } }
            throw e
        }
    }

    /**
     * The client.id ends up in JMX object names and metric tags, where
     * characters outside this set break registration or make tags
     * unusable, so anything else in the component name is mapped to '-'.
     */
    private fun jmxSafe(value: String): String = value.replace(Regex("[^a-zA-Z0-9._-]"), "-")

    /**
     * Warns when a compliance-graded topic class ships over cleartext.
     *
     * The appender enforces durability for AUDIT/FUNCTIONAL through
     * mandatory overrides and says so loudly when an operator value is
     * overruled. Transport confidentiality is the operator's decision -
     * forcing TLS here would over-reach, and the appender has no way to
     * supply certificates - but staying silent about it would be
     * inconsistent: compliance-graded records traversing the network in
     * the clear are readable and tamperable by anyone on the path. So
     * the gap is closed with a signal, not with enforcement.
     *
     * Kafka's own default for `security.protocol` is `PLAINTEXT`, so an
     * absent setting is treated exactly like an explicit one.
     */
    private fun warnOnCleartextTransportForGradedClasses() {
        val gradedClasses =
            producerRegistry.activeTopicClasses
                .filter { it.mandatoryOverrides.isNotEmpty() }
                .filter { topicClass ->
                    val protocol =
                        producerRegistry.effectiveProperties
                            .getValue(topicClass)[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG]
                            ?.trim()
                    protocol == null || protocol.equals(CLEARTEXT_SECURITY_PROTOCOL, ignoreCase = true)
                }
        if (gradedClasses.isEmpty()) return
        addWarn(
            "Compliance-graded topic class(es) ${gradedClasses.joinToString()} are configured " +
                "for cleartext transport (${CommonClientConfigs.SECURITY_PROTOCOL_CONFIG} is unset " +
                "or $CLEARTEXT_SECURITY_PROTOCOL). Their records are enforced to be durable " +
                "(acks/idempotence) but travel unencrypted and unauthenticated - anyone on the " +
                "network path can read or tamper with them. Configure SSL or SASL_SSL in " +
                "<kafkaProducerProperties> unless the transport is secured below the application.",
        )
    }

    private fun buildViolationMessage(violation: MandatoryOverrideViolation): String =
        "Mandatory override applied for ${violation.topicClass}: " +
            "${violation.propertyKey} forced from '${violation.userValue}' to " +
            "'${violation.enforcedValue}'. This is a non-negotiable topic-class " +
            "requirement; see TopicClass.${violation.topicClass} for rationale."

    private fun emitDebugDiagnostics() {
        addInfo(
            "Debug mode enabled. Note: <debug> affects only startup " +
                "diagnostics and has no per-event effect. Consider removing " +
                "<debug>true</debug> from your logback configuration.",
        )
        addInfo("Active topic classes: ${producerRegistry.activeTopicClasses.joinToString()}")
        addInfo(
            "Fallback appender: " +
                (
                    fallbackAppender?.let { "configured (${it.javaClass.simpleName})" }
                        ?: "none - events will be silently dropped on send failure"
                ),
        )
        // Per active class, the producer settings the appender GENERATED on
        // top of the operator's own configuration: the derived client.id
        // plus the class's default and mandatory overrides that actually
        // took effect. Deliberately a diff against the operator's base
        // properties - their own values (including credentials) are never
        // repeated here, which keeps this output credential-safe by
        // construction (see SECURITY.md on status-message leakage).
        val baseProperties = parseKafkaProducerProperties(kafkaProducerProperties)
        producerRegistry.activeTopicClasses.forEach { topicClass ->
            val generated =
                producerRegistry.effectiveProperties
                    .getValue(topicClass)
                    .filter { (key, value) -> baseProperties[key] != value }
                    .toSortedMap()
                    .entries
                    .joinToString(", ") { (key, value) -> "$key=$value" }
            addInfo("Generated producer settings [${topicClass.tag}]: $generated")
        }
    }

    // -- Hot path -------------------------------------------------------

    override fun append(event: ILoggingEvent) {
        // Reentry guard: a log event created synchronously from inside
        // this very append path (most relevantly the Kafka client's
        // caller-thread DEBUG logging in its synchronous send-failure
        // path) must not recurse into the producer. See the field KDoc.
        if (inAppend.get()) {
            return
        }
        // Self-logging guard: the Kafka client names its producer network
        // thread "kafka-producer-network-thread | <client.id>". Log events
        // from those threads are the producer's own logging; routing them
        // back into this appender would feed the producer its own output -
        // a feedback loop that amplifies exactly when the producer logs
        // most (broker trouble). Such events are ignored entirely: no
        // metrics, no fallback. The match is anchored to the exact thread-
        // naming scheme (prefix + full client.id), so an operator-supplied
        // short client.id can never swallow events from unrelated
        // application threads whose names merely contain it.
        val threadName = event.threadName
        if (threadName != null &&
            threadName.startsWith(PRODUCER_NETWORK_THREAD_PREFIX) &&
            threadName.removePrefix(PRODUCER_NETWORK_THREAD_PREFIX) in producerClientIds
        ) {
            return
        }
        // Snapshot once so all hooks for this event use the same instance.
        val m = metrics
        // Determine topic class before the try so we can use it in both
        // the success and the failure metric. Routing exceptions go to
        // the catch with topicClass=null and we report the failure
        // without a class tag (rare; only on malformed marker input).
        var topicClassForFailure: TopicClass? = null
        inAppend.set(true)
        try {
            // Freeze the event's lazy state (formatted message, thread
            // name, MDC snapshot) on the caller's thread: the event
            // crosses to the send worker and potentially to the fallback
            // worker, and Logback's deferred-processing contract requires
            // materializing those fields before any asynchronous hand-off
            // - otherwise a fallback layout could observe late-mutated
            // arguments or another thread's context. Caller data is
            // deliberately opt-in (see includeCallerData).
            try {
                event.prepareForDeferredProcessing()
            } catch (_: RuntimeException) {
                // A LoggerContext without a bound MDC adapter (possible
                // in embedded setups) throws from the MDC
                // materialization; deliver the event as-is rather than
                // failing the hot path.
            }
            if (includeCallerData) {
                event.callerData
            }
            // Non-null by the start() gate: append only runs on a started
            // appender, and start() refuses without an encoder.
            val payload = checkNotNull(encoder).encode(event)
            val markers = event.markerList ?: emptyList()
            val topicName = topicRouter.route(markers)
            val topicClass = topicTable.classFor(topicName)
            topicClassForFailure = topicClass
            m.eventAccepted(topicClass)
            val enrichment = messageEnricher.enrich(event)
            // Hand-off point: everything up to here was CPU-bound work
            // on the caller; the potentially-blocking producer.send
            // happens on the dispatcher's worker thread.
            sendDispatchers.getValue(topicClass).dispatch(topicName, payload, enrichment, event)
        } catch (e: Exception) {
            // Hot-path failure (encoder bug, OOM, etc. - should be rare).
            // Log the first occurrence so operators notice, then suppress
            // to prevent log storms; route the event to fallback
            // regardless.
            if (firstHotPathErrorLogged.compareAndSet(false, true)) {
                addError(
                    "Hot path error in KafkaAppender. Further errors will " +
                        "be suppressed to prevent log storms. First error: " +
                        "${e.message}",
                    e,
                )
            }
            // If routing succeeded but encoding/sending failed, we know the
            // class. If routing itself failed, we have no class - fall back
            // to TECHNICAL as the closest default. (The metric tag is for
            // diagnostics, not correctness; using TECHNICAL keeps the
            // dimensionality stable instead of introducing a null/unknown
            // category that would split series.)
            val cls = topicClassForFailure ?: TopicClass.TECHNICAL
            if (topicClassForFailure == null) {
                // The failure hit before routing resolved a class, so
                // eventAccepted was not recorded yet. Record it here (with
                // the same TECHNICAL default as the failure metric) so the
                // accepted counter keeps its "every event entering append"
                // contract and accepted = dispatched + fallback stays
                // conserved on this path too.
                m.eventAccepted(cls)
            }
            m.eventFallback(cls, KafkaAppenderMetrics.FallbackReason.ENCODER_ERROR)
            // Async via dispatcher: even from the hot path, we avoid
            // blocking the caller thread (typically a Logback AsyncAppender
            // worker) on the fallback appender's downstream I/O.
            fallbackDispatcher?.enqueue(event)
        } finally {
            inAppend.set(false)
        }
    }

    // -- Shutdown -------------------------------------------------------

    override fun stop() {
        if (!stopExecuted.compareAndSet(false, true)) {
            // Teardown already ran; just keep Logback's state machine happy.
            super.stop()
            return
        }
        // Close Logback's ingress gate FIRST: super.stop() flips the
        // volatile isStarted that doAppend checks, so no new event can
        // enter append() while the teardown below closes dispatchers,
        // producers, fallback, and encoder. The teardown is bounded but
        // can take seconds; with the gate still open, concurrent logging
        // would target progressively closed resources. (An append that
        // already passed the gate can still overlap the teardown for
        // microseconds; the dispatchers' own post-close accounting
        // covers that residual window.)
        super.stop()
        metricsBindings.unbind()
        metrics = KafkaAppenderMetrics.NO_OP
        // Close the send dispatchers BEFORE the producer registry: their
        // graceful drain delivers the queued events through the still-
        // open producers; whatever cannot be sent in time diverts to the
        // fallback dispatcher (which closes later for exactly that
        // reason). Closed IN PARALLEL so the per-dispatcher budgets
        // (drain plus interrupt grace) do not stack across topic classes
        // - the same single-overall-budget principle the producer
        // registry applies to its close.
        closeSendDispatchersInParallel()
        if (this::producerRegistry.isInitialized) {
            try {
                producerRegistry.close()
            } catch (e: Exception) {
                addWarn("Error closing producer registry: ${e.message}", e)
            }
        }
        // Close the dispatcher AFTER the producer registry: the registry
        // may still trigger fallback events during its own close-path
        // drain. Once the registry is gone, no more events can land in
        // the dispatcher; we can drain and shut it down.
        try {
            fallbackDispatcher?.let { dispatcher ->
                dispatcher.close()
                if (dispatcher.droppedEventCount > 0) {
                    addWarn(
                        "Fallback dispatcher dropped ${dispatcher.droppedEventCount} " +
                            "event(s) during the lifetime of this appender",
                    )
                }
            }
        } catch (e: Exception) {
            addWarn("Error closing fallback dispatcher: ${e.message}", e)
        }
        // Stop the attached fallback appender(s). Logback may or may not
        // hold its own reference to the fallback appender; calling stop
        // here guarantees its file handles and worker threads are
        // released even if no other path closes it.
        try {
            detachAndStopAllAppenders()
        } catch (e: Exception) {
            addWarn("Error stopping fallback appender(s): ${e.message}", e)
        }
        try {
            encoder?.stop()
        } catch (e: Exception) {
            addWarn("Error stopping encoder: ${e.message}", e)
        }
    }

    /**
     * Closes all send dispatchers concurrently and waits for them within
     * one shared budget. Each [SendDispatcher.close] is itself bounded
     * (drain timeout plus interrupt grace), so the closer threads always
     * finish; the join budget only adds scheduling margin. An interrupt
     * of the stopping thread ends the wait early - the daemon closer
     * threads complete on their own - and is restored before returning.
     */
    private fun closeSendDispatchersInParallel() {
        if (sendDispatchers.isEmpty()) return
        val closers =
            sendDispatchers.map { (topicClass, dispatcher) ->
                Thread({
                    try {
                        dispatcher.close()
                    } catch (e: Exception) {
                        addWarn("Error closing send dispatcher for $topicClass: ${e.message}", e)
                    }
                }, "tabellarium-send-dispatcher-close-${topicClass.tag}").apply {
                    isDaemon = true
                    start()
                }
            }
        var interrupted = false
        val deadlineNanos = System.nanoTime() + SEND_DISPATCHER_CLOSE_BUDGET_MS * 1_000_000
        for (closer in closers) {
            val remainingMs = (deadlineNanos - System.nanoTime()) / 1_000_000
            if (remainingMs <= 0) break
            try {
                closer.join(remainingMs)
            } catch (_: InterruptedException) {
                interrupted = true
                break
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt()
        }
    }

    // -- Public API: metrics integration --------------------------------

    /**
     * Wires the appender to a Micrometer [MeterRegistry].
     *
     * After this call, the appender publishes counters, timers and
     * gauges for hot-path events; the [ResilientMessageSender] reports
     * dispatch outcomes; the [FallbackDispatcher] reports queue depth
     * and dropped events. See [MicrometerKafkaAppenderMetrics] for the
     * complete metric inventory.
     *
     * **Additional bindings:** the circuit-breaker state and
     * call-outcome metrics are bound by the appender's own binder
     * (mirroring `resilience4j-micrometer`'s metric names, with an
     * additional `appender` tag so multiple appender instances on one
     * registry never collide). If the Micrometer Kafka binder is on
     * the classpath, the underlying Kafka producers' internal metrics
     * are bound as well, carrying the same `appender` tag. A binding
     * failing (missing classpath, registry error) is non-fatal and
     * reported via Logback's status manager.
     *
     * **When to call:** typically from a Spring `@PostConstruct` or
     * an `ApplicationReadyEvent` handler, after the application's
     * [MeterRegistry] bean is available.
     * Pre-Spring log events are not captured (they happen before
     * the registry exists), which is acceptable for almost all
     * monitoring needs.
     *
     * Calling this method on a stopped appender is a no-op.
     *
     * @param registry The Micrometer registry to publish to.
     * @param commonTags Tags attached to every metric. Use sparingly.
     *                   The registry's own common tags are typically
     *                   enough; pass [Tags.empty]
     *                   for the no-extra-tags case.
     */
    fun bindMeterRegistry(
        registry: MeterRegistry,
        commonTags: Iterable<Tag> = Tags.empty(),
    ) {
        if (!isStarted) {
            addWarn("bindMeterRegistry called on a stopped/uninitialized appender; ignored.")
            return
        }
        // A repeated bind (context refresh, manual re-wiring) replaces the
        // previous registration - MetricsBindings tears it down first.
        val impl =
            metricsBindings.bind(
                registry = registry,
                commonTags = commonTags,
                appenderName = this.name,
                circuitBreakerRegistry = messageSender.circuitBreakerRegistry,
                producerRegistry = producerRegistry,
            )
        metrics = impl
        messageSender.setMetrics(impl)
        sendDispatchers.values.forEach { it.setMetrics(impl) }
        fallbackDispatcher?.setMetrics(impl)
    }

    // -- AppenderAttachable<ILoggingEvent> ------------------------------

    /**
     * Called by Joran's `AppenderRefAction` when an
     * `<appender-ref ref="..."/>` element is encountered inside the
     * `<appender>` configuration. Stores the referenced appender in
     * the single fallback slot. Additional `<appender-ref>` elements
     * are ignored with a status warning - the KafkaAppender has only
     * one fallback slot and the first one wins.
     *
     * **Ownership:** the KafkaAppender assumes it owns the attached
     * fallback appender's lifecycle - [stop] stops it (via
     * [detachAndStopAllAppenders]) to release file handles and worker
     * threads. Do not attach an appender that is simultaneously
     * referenced by other loggers unless a full-context shutdown is
     * the only stop path in your deployment; a selective stop of this
     * appender would silence the shared appender for everyone.
     */
    override fun addAppender(newAppender: Appender<ILoggingEvent>) {
        if (fallbackAppender != null) {
            addWarn(
                "KafkaAppender supports only a single fallback appender; " +
                    "ignoring additional <appender-ref ref=\"${newAppender.name}\"/>.",
            )
            return
        }
        fallbackAppender = newAppender
    }

    override fun iteratorForAppenders(): Iterator<Appender<ILoggingEvent>> = listOfNotNull(fallbackAppender).iterator()

    override fun getAppender(name: String?): Appender<ILoggingEvent>? = fallbackAppender?.takeIf { it.name == name }

    override fun isAttached(appender: Appender<ILoggingEvent>?): Boolean = appender != null && fallbackAppender === appender

    override fun detachAndStopAllAppenders() {
        fallbackAppender?.let {
            it.stop()
            fallbackAppender = null
        }
    }

    override fun detachAppender(appender: Appender<ILoggingEvent>?): Boolean {
        if (appender != null && fallbackAppender === appender) {
            fallbackAppender = null
            return true
        }
        return false
    }

    override fun detachAppender(name: String?): Boolean {
        if (name != null && fallbackAppender?.name == name) {
            fallbackAppender = null
            return true
        }
        return false
    }

    private companion object {
        /**
         * Overall wait budget for the parallel send-dispatcher close:
         * one dispatcher's own bounded close (drain timeout plus
         * interrupt grace) plus scheduling margin. Shared across all
         * dispatchers because they close concurrently.
         */
        private const val SEND_DISPATCHER_CLOSE_BUDGET_MS: Long =
            SendDispatcher.DEFAULT_DRAIN_TIMEOUT_MS + 1000

        /**
         * Kafka's fixed naming scheme for the producer's network thread;
         * the client.id follows verbatim after this prefix. See
         * `org.apache.kafka.clients.producer.KafkaProducer` (NETWORK_THREAD_PREFIX).
         */
        private const val PRODUCER_NETWORK_THREAD_PREFIX = "kafka-producer-network-thread | "

        /** Kafka's cleartext security protocol - also its default when unset. */
        private const val CLEARTEXT_SECURITY_PROTOCOL = "PLAINTEXT"
    }
}
