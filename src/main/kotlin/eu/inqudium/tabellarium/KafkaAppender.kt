package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.UnsynchronizedAppenderBase
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.spi.AppenderAttachable
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Logback appender that ships log events to Kafka with per-topic-class
 * circuit breakers, compliance-driven producer configuration, and an
 * optional fallback appender.
 *
 * This is the orchestrator: it wires together the individual components
 * ([TopicRouter], [TopicTable], [MessageEnricher], [ProducerRegistry],
 * [ResilientMessageSender]), exposes the legacy XML configuration
 * surface to Joran, and runs the per-event hot path.
 *
 * ## Configuration surface (Drop-In to the legacy appender)
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
 *   <debug>false</debug>
 *   <appender-ref ref="FALLBACK_FILE"/>  <!-- new, optional -->
 * </appender>
 * ```
 *
 * Every element of the legacy XML is supported with the same semantics
 * - except [debug], whose former per-event hot-path behavior (audit
 * finding F-011) has been removed: the flag now affects **startup
 * diagnostics only**. A migration note is emitted to the status manager
 * when [debug] is `true`.
 *
 * ## Lifecycle
 *
 * - **[start]** validates configuration eagerly, builds the pipeline,
 *   and surfaces any [MandatoryOverrideViolation] from
 *   [ProducerPropertiesBuilder] as warnings on the Logback status
 *   manager. Misconfiguration causes `addError` plus refusal to start;
 *   the appender stays `isStarted=false` and downstream `doAppend` calls
 *   are no-ops.
 * - **[append]** runs the hot path on the caller's thread (typically a
 *   Logback `AsyncAppender` worker, given how AppenderBase is normally
 *   used). No synchronization, no per-event allocation outside what
 *   the encoder and sender already require. Hot-path exceptions are
 *   logged **once** (via [AtomicBoolean]-guarded `addError`) and route
 *   to [fallbackAppender] if configured; subsequent errors are
 *   suppressed to prevent log storms.
 * - **[stop]** closes the [ProducerRegistry] with its configured
 *   timeout, stops the encoder, and completes via `super.stop()`.
 *   Per-resource close failures are recorded as warnings but do not
 *   prevent the rest of the shutdown sequence.
 *
 * ## Why UnsynchronizedAppenderBase
 *
 * The legacy appender extended `AppenderBase`, whose `doAppend` is
 * `synchronized`. In a Reactor Netty / virtual-thread environment that
 * lock causes carrier-thread pinning and ripples up the call chain as
 * back-pressure (audit finding F-001). The components used in [append]
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
     * **Important behavior change:** in the legacy appender, this flag
     * also injected per-record debug output into the hot path (audit
     * finding F-011: `formatter.format(loggingEvent)` was invoked on
     * every event regardless of whether the debug output was actually
     * consumed). That behavior has been removed. The flag now affects
     * **startup only**; operators should consider removing
     * `<debug>true</debug>` from their configuration.
     */
    var debug: Boolean = false

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
     * Test-only hook. When true, [FallbackDispatcher] runs in
     * synchronous mode so test assertions on the fallback appender
     * do not need to poll. Must remain false in production.
     */
    internal var useSynchronousFallbackForTests: Boolean = false

    // -- Pipeline state, built in start() -------------------------------

    private lateinit var topicRouter: TopicRouter
    private lateinit var topicTable: TopicTable
    private lateinit var messageEnricher: MessageEnricher
    private lateinit var producerRegistry: ProducerRegistry
    private lateinit var messageSender: ResilientMessageSender

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
     * Pluggable metrics hook. Defaults to [KafkaAppenderMetrics.NO_OP].
     * Replaced by [bindMeterRegistry] when an operator wires the
     * appender to a Micrometer registry - typically from a Spring
     * `@PostConstruct` after the application context is ready.
     * Volatile because the setter may be called from a different
     * thread than the hot path.
     */
    @Volatile
    private var metrics: KafkaAppenderMetrics = KafkaAppenderMetrics.NO_OP

    // -- Lifecycle ------------------------------------------------------

    override fun start() {
        if (!validateConfiguration()) {
            return // addError was already called for each failure
        }

        try {
            buildPipeline()
        } catch (e: Exception) {
            addError("Failed to build KafkaAppender pipeline: ${e.message}", e)
            return
        }

        producerRegistry.mandatoryOverrideViolations.forEach { violation ->
            addWarn(buildViolationMessage(violation))
        }

        if (debug) {
            emitDebugDiagnostics()
        }

        // Logback start() methods are idempotent, so starting an already-
        // started encoder is safe - we start it ourselves to handle the
        // case where Logback's outer initialization order hasn't done so.
        encoder!!.start()

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
        producerRegistry =
            ProducerRegistry.create(
                propertiesBuilder = ProducerPropertiesBuilder(baseProperties),
                activeTopicClasses = topicTable.activeTopicClasses,
                producerFactory = producerFactory,
            )
        // Wrap the fallback appender in a dispatcher so the Kafka I/O
        // thread is never blocked on the fallback's downstream I/O.
        // See FallbackDispatcher KDoc for the rationale.
        fallbackDispatcher =
            fallbackAppender?.let {
                FallbackDispatcher(it, synchronous = useSynchronousFallbackForTests)
            }
        messageSender =
            ResilientMessageSender(
                producerRegistry = producerRegistry,
                circuitBreakerRegistry = circuitBreakerRegistry,
                fallbackDispatcher = fallbackDispatcher,
            )
    }

    private fun buildViolationMessage(violation: MandatoryOverrideViolation): String =
        "Mandatory override applied for ${violation.topicClass}: " +
            "${violation.propertyKey} forced from '${violation.userValue}' to " +
            "'${violation.enforcedValue}'. This is a compliance requirement; " +
            "see TopicClass.${violation.topicClass} for rationale."

    private fun emitDebugDiagnostics() {
        addInfo(
            "Debug mode enabled. Note: <debug> now affects only startup " +
                "diagnostics; per-record debug output has been removed for " +
                "performance (audit finding F-011). Consider removing " +
                "<debug>true</debug> from your logback configuration.",
        )
        addInfo("Active topic classes: ${producerRegistry.activeTopicClasses.joinToString()}")
        addInfo(
            "Fallback appender: " +
                if (fallbackAppender != null) {
                    "configured (${fallbackAppender!!.javaClass.simpleName})"
                } else {
                    "none - events will be silently dropped on send failure"
                },
        )
    }

    // -- Hot path -------------------------------------------------------

    override fun append(event: ILoggingEvent) {
        // Snapshot once so all hooks for this event use the same instance.
        val m = metrics
        // Determine topic class before the try so we can use it in both
        // the success and the failure metric. Routing exceptions go to
        // the catch with topicClass=null and we report the failure
        // without a class tag (rare; only on malformed marker input).
        var topicClassForFailure: TopicClass? = null
        try {
            val payload = encoder!!.encode(event)
            val markers = event.markerList ?: emptyList()
            val topicName = topicRouter.route(markers)
            val topicClass = topicTable.classFor(topicName)
            topicClassForFailure = topicClass
            m.eventAccepted(topicClass)
            val enrichment = messageEnricher.enrich(event)
            messageSender.send(topicClass, topicName, payload, enrichment, event)
        } catch (e: Exception) {
            // Hot-path failure (encoder bug, OOM, etc. - should be rare).
            // Log the first occurrence so operators notice, then suppress
            // to prevent log storms (audit finding F-037); route the event
            // to fallback regardless.
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
            m.eventFallback(cls, KafkaAppenderMetrics.FallbackReason.ENCODER_ERROR)
            // Async via dispatcher: even from the hot path, we avoid
            // blocking the caller thread (typically a Logback AsyncAppender
            // worker) on the fallback appender's downstream I/O.
            fallbackDispatcher?.enqueue(event)
        }
    }

    // -- Shutdown -------------------------------------------------------

    override fun stop() {
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
        super.stop()
    }

    // -- Public API: metrics integration --------------------------------

    /**
     * Wires the appender to a Micrometer [io.micrometer.core.instrument.MeterRegistry].
     *
     * After this call, the appender publishes counters, timers and
     * gauges for hot-path events; the [ResilientMessageSender] reports
     * dispatch outcomes; the [FallbackDispatcher] reports queue depth
     * and dropped events. See [MicrometerKafkaAppenderMetrics] for the
     * complete metric inventory.
     *
     * **Optional bindings:** if `resilience4j-micrometer` is on the
     * classpath, the circuit-breaker state and call-outcome metrics are
     * additionally bound. If `micrometer-kafka` is on the classpath,
     * the underlying Kafka producers' internal metrics are bound as
     * well. Either binding failing (missing classpath, registry error)
     * is non-fatal and reported via Logback's status manager.
     *
     * **When to call:** typically from a Spring `@PostConstruct` or
     * an `ApplicationReadyEvent` handler, after the application's
     * [io.micrometer.core.instrument.MeterRegistry] bean is available.
     * Pre-Spring log events are not captured (they happen before
     * the registry exists), which is acceptable for almost all
     * monitoring needs.
     *
     * Calling this method on a stopped appender is a no-op.
     *
     * @param registry The Micrometer registry to publish to.
     * @param commonTags Tags attached to every metric. Use sparingly.
     *                   The registry's own common tags are typically
     *                   enough; pass [io.micrometer.core.instrument.Tags.empty]
     *                   for the no-extra-tags case.
     */
    fun bindMeterRegistry(
        registry: io.micrometer.core.instrument.MeterRegistry,
        commonTags: Iterable<io.micrometer.core.instrument.Tag> =
            io.micrometer.core.instrument.Tags.empty(),
    ) {
        if (!isStarted) {
            addWarn("bindMeterRegistry called on a stopped/uninitialized appender; ignored.")
            return
        }
        val impl = MicrometerKafkaAppenderMetrics(registry, commonTags, appenderName = this.name)
        metrics = impl
        messageSender.setMetrics(impl)
        fallbackDispatcher?.setMetrics(impl)

        bindResilience4jMetrics(registry, commonTags)
        bindKafkaProducerMetrics(registry, commonTags)
    }

    /**
     * Best-effort binding of Resilience4j circuit-breaker metrics.
     * Requires `io.github.resilience4j:resilience4j-micrometer` on the
     * classpath. Silently skipped if the class is missing; reported
     * via status manager if the call itself fails.
     *
     * ## Lazy class-loading pattern
     *
     * The first step is a [Class.forName] **probe** that succeeds
     * only when `TaggedCircuitBreakerMetrics` is on the classpath. If
     * the probe throws [ClassNotFoundException], we never enter
     * [doBindResilience4jMetrics] and the JVM never has to resolve
     * the symbols that method references - so the appender works
     * fine without resilience4j-micrometer in the dependency tree.
     *
     * The actual binding logic lives in [doBindResilience4jMetrics],
     * which uses the bridge class **directly** (no reflection). This
     * is safe because Kotlin compiles `private fun` to a regular
     * private static JVM method; the JVM resolves the referenced
     * types only when the method is first invoked, not when the
     * containing class is loaded. The `Class.forName` probe gates
     * that invocation.
     */
    private fun bindResilience4jMetrics(
        registry: io.micrometer.core.instrument.MeterRegistry,
        commonTags: Iterable<io.micrometer.core.instrument.Tag>,
    ) {
        try {
            Class.forName("io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics")
        } catch (_: ClassNotFoundException) {
            // resilience4j-micrometer not on classpath; expected when
            // operators opt out. Stay silent.
            return
        }
        try {
            doBindResilience4jMetrics(registry)
        } catch (e: Exception) {
            addInfo(
                "Failed to bind Resilience4j metrics to MeterRegistry " +
                    "(circuit-breaker state metrics will be unavailable): ${e.message}",
            )
        }
    }

    /**
     * Performs the actual Resilience4j binding using direct typed
     * references. Only ever called from [bindResilience4jMetrics]
     * after the [Class.forName] probe has confirmed the bridge
     * class is available - so the JVM symbol resolution that
     * happens on first method entry will succeed.
     */
    private fun doBindResilience4jMetrics(registry: io.micrometer.core.instrument.MeterRegistry) {
        io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
            .ofCircuitBreakerRegistry(messageSender.circuitBreakerRegistry)
            .bindTo(registry)
    }

    /**
     * Best-effort binding of Kafka producer-internal metrics. Requires
     * `io.micrometer:micrometer-core` with the Kafka binder on the
     * classpath. Silently skipped if the class is missing.
     *
     * See [bindResilience4jMetrics] for the rationale of the
     * probe-then-direct-call pattern.
     */
    private fun bindKafkaProducerMetrics(
        registry: io.micrometer.core.instrument.MeterRegistry,
        commonTags: Iterable<io.micrometer.core.instrument.Tag>,
    ) {
        try {
            Class.forName("io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics")
        } catch (_: ClassNotFoundException) {
            // micrometer-core kafka binder not on classpath; silent.
            return
        }
        try {
            doBindKafkaProducerMetrics(registry, commonTags)
        } catch (e: Exception) {
            addInfo(
                "Failed to bind Kafka producer metrics to MeterRegistry " +
                    "(producer-internal metrics will be unavailable): ${e.message}",
            )
        }
    }

    /**
     * Performs the actual Kafka producer binding using direct typed
     * references. Only ever called from [bindKafkaProducerMetrics]
     * after the probe has confirmed the binder class is available.
     */
    private fun doBindKafkaProducerMetrics(
        registry: io.micrometer.core.instrument.MeterRegistry,
        commonTags: Iterable<io.micrometer.core.instrument.Tag>,
    ) {
        for (topicClass in producerRegistry.activeTopicClasses) {
            val producer = producerRegistry.producerFor(topicClass)
            val tagsForClass =
                io.micrometer.core.instrument.Tags.of(commonTags)
                    .and(MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS, topicClass.tag)
            io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics(producer, tagsForClass)
                .bindTo(registry)
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
