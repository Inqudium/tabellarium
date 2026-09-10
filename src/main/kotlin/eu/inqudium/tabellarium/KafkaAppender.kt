package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.UnsynchronizedAppenderBase
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.spi.AppenderAttachable
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Logback appender that ships log events to Kafka with per-topic-class
 * circuit breakers, compliance-driven producer configuration, and an
 * optional fallback appender.
 *
 * This is the composition root: it exposes the XML configuration
 * surface to Joran, runs the per-event hot path, and drives the Logback
 * lifecycle. It composes two halves with one concern each: the
 * [RecordPlan] turns an event into a record (routing, encoding,
 * enrichment - pure, fixed for the appender's lifetime, nothing to
 * close) and the [KafkaTransport] carries records to Kafka (producers,
 * breakers, send queues, fallback dispatcher - stateful, opened in
 * [start] and closed in [stop]). The start-up messages it
 * reports come from [StartupDiagnostics].
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
 * - **[start]** validates configuration eagerly, builds the
 *   [RecordPlan], opens the [KafkaTransport] for the plan's active
 *   classes, and surfaces any [MandatoryOverrideViolation] from
 *   [ProducerPropertiesBuilder] as warnings on the Logback status
 *   manager. Misconfiguration causes `addError` plus refusal to start;
 *   the appender stays `isStarted=false` and downstream `doAppend` calls
 *   are no-ops.
 * - **[append]** runs only CPU-bound work on the caller's thread:
 *   [RecordPlan.route] and [RecordPlan.materialize] (routing, encoding,
 *   enrichment). The potentially-blocking
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
 *   timeout, then the fallback dispatcher; only then are the metrics
 *   unbound, so a scrape during the teardown still sees the shutdown
 *   diversions and drops. Finally the fallback appender is stopped
 *   (but stays attached, see [addAppender]) and the encoder is
 *   stopped. Per-resource close failures are recorded as warnings but
 *   do not prevent the rest of the shutdown sequence. CAUTION: the
 *   send dispatchers' shutdown remainder (up to one queue capacity per
 *   active class) drains into the single fallback queue of the same
 *   default capacity; on a stop during an outage, overflow beyond that
 *   is dropped and counted.
 * - **No restart.** `start()` after `stop()` is refused with an error
 *   (ADR-0004): Logback never restarts an appender instance - a
 *   reconfiguration stops the old ones and builds new ones - and the
 *   appender follows that lifecycle instead of carrying every per-life
 *   resource (fallback, breakers, metrics binding, error guard) across
 *   a second start.
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
     * Chooses the [SelfLoggingGuard] implementation. Default builds a
     * [ClientIdSelfLoggingGuard] over the producers' client ids; the
     * transport calls the factory once in [start], after the producers
     * exist. Tests substitute a guard with recorded decisions.
     */
    internal var selfLoggingGuardFactory: SelfLoggingGuardFactory = SelfLoggingGuardFactory.default()

    /**
     * Capacity of each per-topic-class [SendDispatcher] queue - the
     * bounded hand-off between the logging caller and the worker that
     * performs `producer.send`. Configurable via
     * `<sendQueueCapacity>` in the XML. When the queue is full, events
     * divert to the fallback (reason `queue.full`) instead of blocking
     * the caller.
     */
    var sendQueueCapacity: Int = SendDispatcher.DEFAULT_QUEUE_CAPACITY

    // -- Running state, built in start() --------------------------------

    /**
     * The per-event-invariant half: how an event becomes a record
     * ([RecordPlan.route], [RecordPlan.materialize]). Built first in
     * [start], before any resource exists; null until then. Nothing to
     * close.
     */
    private var plan: RecordPlan? = null

    /**
     * The stateful half: producers, breakers, send queues, fallback
     * dispatcher. Opened in [start] after the plan, closed as one unit
     * in [stop]; null until [start] succeeded.
     *
     * Both fields are published to other threads through Logback's
     * volatile `started` flag, which [start] sets after them and which
     * `doAppend` checks before calling [append].
     */
    private var transport: KafkaTransport? = null

    /**
     * Guard against hot-path log storms: only the first error gets
     * `addError`-logged, subsequent errors fall back silently. Atomic
     * because [append] may run concurrently on multiple threads.
     */
    private val firstHotPathErrorLogged = AtomicBoolean(false)

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
     * Serializes [bindMeterRegistry] against the unbind in [stop]: a
     * bind that passed the `isStarted` gate just before a stop must
     * either complete before the unbind (which then removes its meters)
     * or observe the stopped state and do nothing - never register
     * meters after the unbind ran, which nothing would ever remove
     * again. A lock rather than an atomic because both sides mutate the
     * bindings' lists; it is never touched on the hot path.
     */
    private val bindLock = ReentrantLock()

    /**
     * Guards the teardown in [stop] so a repeated stop (Logback may call
     * it more than once during context teardown) does not re-run the
     * close sequence - re-closing the dispatcher would double-count its
     * remaining queue as dropped and re-emit the drop warning. Never
     * reset: once stopped, [start] refuses (ADR-0004).
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
        if (stopExecuted.get()) {
            // Rationale: a stopped appender has released its fallback,
            // its breakers' history, its metrics binding and its one-shot
            // error guard; making all of that come back symmetrically is
            // a lifecycle nobody asked for - Logback itself replaces
            // instances instead of restarting them (ADR-0004).
            addError(
                "KafkaAppender cannot be started again after stop() (ADR-0004): Logback replaces " +
                    "appender instances on reconfiguration - create a new instance instead.",
            )
            return
        }
        if (!validateConfiguration()) {
            return // addError was already called for each failure
        }

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

        // Stage one, pure: how an event becomes a record. Fails only for
        // configuration reasons, and before any resource exists.
        val plan =
            try {
                RecordPlan.from(
                    topicMapping = topicMapping,
                    component = component,
                    cmdbId = cmdbId,
                    environment = environment,
                    encoder = checkNotNull(encoder) { "encoder was validated non-null in validateConfiguration" },
                )
            } catch (e: Exception) {
                failStartup(e)
                return
            }
        // Stage two, stateful: the transport for the plan's active classes.
        // KafkaTransport.open rolls its own resources back on failure.
        val transport =
            try {
                KafkaTransport.open(
                    settings =
                        TransportSettings(
                            kafkaProducerProperties = kafkaProducerProperties,
                            component = component,
                            sendQueueCapacity = sendQueueCapacity,
                        ),
                    activeTopicClasses = plan.activeTopicClasses,
                    fallbackAppender = fallbackAppender,
                    producerFactory = producerFactory,
                    circuitBreakerRegistry = circuitBreakerRegistry,
                    selfLoggingGuardFactory = selfLoggingGuardFactory,
                    warn = { message, cause -> addWarn(message, cause) },
                )
            } catch (e: Exception) {
                failStartup(e)
                return
            }
        this.plan = plan
        this.transport = transport

        transport.producerRegistry.mandatoryOverrideViolations.forEach { violation ->
            addWarn(StartupDiagnostics.mandatoryOverrideWarning(violation))
        }
        StartupDiagnostics.cleartextTransportWarning(transport.producerRegistry)?.let(::addWarn)
        if (debug) {
            StartupDiagnostics
                .debugMessages(transport.producerRegistry, fallbackAppender, kafkaProducerProperties)
                .forEach(::addInfo)
        }

        super.start()
    }

    /**
     * Reports a failed [start] after the encoder was started: releases
     * the encoder (the plan holds nothing, the transport rolled itself
     * back) and records the failure at the level of detail `<debug>`
     * allows.
     */
    private fun failStartup(e: Exception) {
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

    // -- Hot path -------------------------------------------------------

    override fun append(event: ILoggingEvent) {
        // Both non-null whenever doAppend lets an event through: start()
        // sets them before it flips the started flag that doAppend checks.
        val plan = this.plan ?: return
        val transport = this.transport ?: return
        // Self-logging guard: an event that is this appender's own echo -
        // logged synchronously from inside this append path on this very
        // thread, or by the network thread of one of its own producers -
        // is ignored entirely: no metrics, no fallback. Both cases and
        // their rationale live on SelfLoggingGuard.
        val guard = transport.selfLoggingGuard
        if (guard.shouldDrop(event)) {
            return
        }
        // Snapshot once so all hooks for this event use the same instance.
        val m = metrics
        // Determine topic class before the try so we can use it in both
        // the success and the failure metric. Routing exceptions go to
        // the catch with topicClass=null and we report the failure
        // without a class tag (rare; only on malformed marker input).
        var topicClassForFailure: TopicClass? = null
        guard.enter()
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
                // materialization. Safety: pin an empty MDC snapshot on
                // the event so that no later reader - the encoder, the
                // partitioning-key extractor, a fallback layout - runs
                // into the same failure again; only then can the event
                // be delivered as-is instead of failing the hot path
                // (docs/assessment/CODE_ANALYSIS-2026-09-07T19-09-00.R2.md,
                // finding R2-1). The setter
                // refuses an already-materialized map; that case cannot
                // be the one that just failed, so the refusal is ignored.
                (event as? LoggingEvent)?.let { mutable ->
                    runCatching { mutable.mdcPropertyMap = emptyMap() }
                }
            }
            if (includeCallerData) {
                event.callerData
            }
            // Route first, then materialize: a failure in encoding or
            // enrichment is then attributed to the class the event was
            // routed to instead of to the TECHNICAL default.
            val route = plan.route(event)
            topicClassForFailure = route.topicClass
            m.eventAccepted(route.topicClass)
            val record = plan.materialize(route, event)
            // Hand-off point: everything up to here was CPU-bound work
            // on the caller; the potentially-blocking producer.send
            // happens on the dispatcher's worker thread.
            transport.dispatch(record, event)
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
            // Asynchronously, so the caller never blocks on the fallback
            // appender's downstream I/O.
            transport.divertToFallback(event)
        } finally {
            guard.exit()
        }
    }

    // -- Shutdown -------------------------------------------------------

    override fun stop() {
        if (!stopExecuted.compareAndSet(false, true)) {
            // Teardown already ran; just keep Logback's state machine happy.
            super.stop()
            return
        }
        // Safety: close Logback's ingress gate first (the volatile
        // isStarted that doAppend checks), so no new event enters a
        // multi-second teardown; see the lifecycle section of the class
        // KDoc for the order. An append that already passed the gate can
        // overlap for microseconds - the dispatchers' post-close
        // accounting covers that residual window.
        super.stop()
        // The transport closes its components in reverse ownership order
        // (send dispatchers, producer registry, fallback dispatcher); the
        // order and its rationale live in KafkaTransport. Null when
        // start() never succeeded. The plan has nothing to close.
        transport?.close { message, cause ->
            if (cause != null) addWarn(message, cause) else addWarn(message)
        }
        // Unbind the metrics only now: the dispatcher closes are where
        // the shutdown diversions and drops are counted, and a
        // scrape during the (multi-second) teardown should still see
        // them. Under the bind lock - see bindLock.
        bindLock.withLock {
            metricsBindings.unbind()
            metrics = KafkaAppenderMetrics.NO_OP
        }
        // Stop the attached fallback appender. Logback may or may not
        // hold its own reference to it; calling stop here guarantees its
        // file handles and worker threads are released even if no other
        // path closes it. Not detached: the stopped appender stays
        // inspectable through the AppenderAttachable accessors, and
        // detachAndStopAllAppenders remains available to callers who
        // want the slot cleared.
        try {
            fallbackAppender?.stop()
        } catch (e: Exception) {
            addWarn("Error stopping fallback appender: ${e.message}", e)
        }
        try {
            encoder?.stop()
        } catch (e: Exception) {
            addWarn("Error stopping encoder: ${e.message}", e)
        }
    }

    // -- Public API: metrics integration --------------------------------

    /**
     * Whether a [bindMeterRegistry] binding is currently in place. The
     * [KafkaAppenderMetricsBinding] decides on this - not on appender
     * identity - so an appender whose earlier bind failed is bound on
     * the next `bindAppenders()` call and a bound one is never bound
     * twice.
     */
    internal val isMeterRegistryBound: Boolean
        get() = bindLock.withLock { metricsBindings.isBound }

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
        bindLock.withLock {
            // Safety: re-checked under the lock - stop() flips isStarted
            // before it takes the lock for the unbind, so a bind that
            // arrives after that observes the stopped state here instead
            // of registering meters nothing would remove.
            if (!isStarted) {
                addWarn("bindMeterRegistry called on a stopped/uninitialized appender; ignored.")
                return
            }
            // A repeated bind (context refresh, manual re-wiring) replaces the
            // previous registration - MetricsBindings tears it down first.
            val transport = checkNotNull(transport) { "a started appender always has a transport" }
            val impl =
                metricsBindings.bind(
                    registry = registry,
                    commonTags = commonTags,
                    appenderName = this.name,
                    circuitBreakerRegistry = transport.circuitBreakerRegistry,
                    producerRegistry = transport.producerRegistry,
                )
            metrics = impl
            transport.setMetrics(impl)
        }
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
     * fallback appender's lifecycle - [stop] stops it (it stays attached
     * and inspectable; the appender itself is not restarted, ADR-0004)
     * to release file handles and worker threads. Do not attach an appender that is
     * simultaneously referenced by other loggers unless a full-context
     * shutdown is the only stop path in your deployment; a selective
     * stop of this appender would silence the shared appender for
     * everyone.
     *
     * **Self-logging:** the fallback appender must not log through
     * SLF4J per delivered event. Its `doAppend` runs on the fallback
     * dispatcher's worker, which carries this appender's reentry guard:
     * such log events are dropped by [append] (no metrics, no fallback)
     * instead of looping back into the pipeline. Logback's own file
     * appenders report through the status manager and are unaffected.
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
}
