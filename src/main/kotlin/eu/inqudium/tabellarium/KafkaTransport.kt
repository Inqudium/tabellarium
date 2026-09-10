package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry

/**
 * The transport part of a [KafkaAppender]'s configuration: what the
 * producers and the send queues are built from.
 *
 * @param kafkaProducerProperties Raw text of `<kafkaProducerProperties>`.
 * @param component Service component identifier; feeds the per-class
 *                  `client.id` default (`tabellarium-<component>-<class>`).
 * @param sendQueueCapacity Capacity of each per-class send queue.
 */
internal data class TransportSettings(
    val kafkaProducerProperties: String,
    val component: String,
    val sendQueueCapacity: Int,
)

/**
 * The stateful half of a started [KafkaAppender]: the producers
 * ([ProducerRegistry]), the breakers and the send path
 * ([ResilientMessageSender]), one [SendDispatcher] per active topic
 * class, and the optional [FallbackDispatcher]. Everything here changes
 * with every event - queues fill, breaker windows move, producers buffer
 * - and everything here is what [close] tears down. The per-event
 * functions that do not change live in the [RecordPlan]; the appender
 * composes the two.
 *
 * The appender talks to the transport in two verbs: [dispatch] a
 * materialized [Record] into its class's queue and [divertToFallback] an
 * event the hot path could not turn into a record to the fallback - after
 * asking the [selfLoggingGuard] whether the event is the appender's own
 * echo. The components behind them are private; the exposed queries
 * ([producerRegistry], [circuitBreakerRegistry], [selfLoggingGuard]) exist
 * for the start-up diagnostics, the metrics binding and the hot path.
 *
 * ## Why one owner
 *
 * Before the extraction the appender held these components in separate
 * fields, and the reverse-ownership close order was written three times
 * (the rollback inside the build, `stop()`, and the parallel dispatcher
 * close) and kept in step by hand; the 2026-09-07 architecture review
 * ranked the appender as the unit where the lifecycle findings
 * clustered. Here the order exists once, in [closeAll], and both the
 * rollback in [open] and [close] run it.
 *
 * ## Open as a transaction
 *
 * Invariant: a construction failure in [open] after the first real
 * resource exists closes everything created so far, in the order
 * [close] uses, so a failed or reloaded configuration never leaks
 * producers or daemon workers that only an external `stop()` could
 * reach. The encoder is not owned here: the appender starts it before
 * [open] and releases it if [open] throws.
 *
 * ## Close order
 *
 * [close] runs the send dispatchers first and in parallel (their drain
 * still sends through the open producers; the remainder diverts to the
 * fallback), then the producer registry, then the fallback dispatcher,
 * which the registry's close-path drain may still feed. Metrics are
 * not this class's concern: the appender unbinds them after [close] so
 * a scrape during the teardown still sees the shutdown diversions and
 * drops.
 */
internal class KafkaTransport private constructor(
    val producerRegistry: ProducerRegistry,
    /**
     * The feedback-loop guard for this transport's producers and
     * workers; the appender consults it first thing on the hot path.
     * Built here because its client ids exist only once the producers
     * do, and the workers that carry its mark are created right after.
     */
    val selfLoggingGuard: SelfLoggingGuard,
    private val messageSender: ResilientMessageSender,
    private val sendDispatchers: Map<TopicClass, SendDispatcher>,
    private val fallbackDispatcher: FallbackDispatcher?,
) {
    /** The breaker registry the sender draws from, for the metrics binding. */
    val circuitBreakerRegistry: CircuitBreakerRegistry
        get() = messageSender.circuitBreakerRegistry

    /**
     * Hands a materialized record to its class's send queue in O(1).
     * Everything before this call was CPU-bound work on the caller; the
     * potentially-blocking `producer.send` happens on the dispatcher's
     * worker thread. A full queue diverts to the fallback (reason
     * `queue.full`) instead of blocking.
     */
    fun dispatch(
        record: Record,
        event: ILoggingEvent,
    ) {
        sendDispatchers
            .getValue(record.route.topicClass)
            .dispatch(record.route.topicName, record.payload, record.enrichment, event)
    }

    /**
     * Routes an event the hot path could not turn into a record to the
     * fallback appender, asynchronously through the fallback dispatcher
     * so the caller never blocks on the fallback's downstream I/O. No-op
     * when the operator configured no fallback (drop policy).
     */
    fun divertToFallback(event: ILoggingEvent) {
        fallbackDispatcher?.enqueue(event)
    }

    /** Fans a metrics implementation out to every component that reports. */
    fun setMetrics(metrics: KafkaAppenderMetrics) {
        messageSender.setMetrics(metrics)
        sendDispatchers.values.forEach { it.setMetrics(metrics) }
        fallbackDispatcher?.setMetrics(metrics)
    }

    /**
     * Closes every component in reverse ownership order (see the class
     * KDoc). Per-resource failures are reported through [warn] and do
     * not prevent the rest of the sequence.
     */
    fun close(warn: (message: String, cause: Throwable?) -> Unit) {
        closeAll(sendDispatchers, producerRegistry, fallbackDispatcher, warn)
    }

    companion object {
        /**
         * Overall wait budget for the parallel send-dispatcher close:
         * one dispatcher's own bounded close (drain timeout plus
         * interrupt grace) plus scheduling margin. Shared across all
         * dispatchers because they close concurrently.
         */
        private const val SEND_DISPATCHER_CLOSE_BUDGET_MS: Long =
            SendDispatcher.DEFAULT_DRAIN_TIMEOUT_MS + 1000

        /**
         * Opens the transport for [activeTopicClasses]. See the class
         * KDoc for the transaction contract. [settings] is the value the
         * producers and queues derive from; [fallbackAppender],
         * [producerFactory] and [circuitBreakerRegistry] are
         * collaborators with state of their own; [warn] is the
         * appender's hook. The [SelfLoggingGuard] is created here by
         * [selfLoggingGuardFactory] from the registry's client ids; every
         * worker created here marks itself with it for its whole
         * lifetime.
         *
         * @param warn Sink for asynchronous worker-death reports; the
         *             appender routes it to its status manager.
         */
        fun open(
            settings: TransportSettings,
            activeTopicClasses: Set<TopicClass>,
            fallbackAppender: Appender<ILoggingEvent>?,
            producerFactory: ProducerFactory,
            circuitBreakerRegistry: CircuitBreakerRegistry,
            selfLoggingGuardFactory: SelfLoggingGuardFactory,
            warn: (message: String, cause: Throwable) -> Unit,
        ): KafkaTransport {
            val registry =
                ProducerRegistry.create(
                    propertiesBuilder =
                        ProducerPropertiesBuilder(
                            parseKafkaProducerProperties(settings.kafkaProducerProperties),
                            defaultClientIdPrefix = "tabellarium-${jmxSafe(settings.component)}",
                        ),
                    activeTopicClasses = activeTopicClasses,
                    producerFactory = producerFactory,
                )
            // The guard needs the producers' client ids, so it comes right
            // after the registry and before the workers that carry its mark.
            val selfLoggingGuard = selfLoggingGuardFactory.create(registry.clientIds)
            // From here on real resources exist; the catch below implements
            // the rollback contract from the class KDoc.
            var fallbackDispatcher: FallbackDispatcher? = null
            val sendDispatchers = LinkedHashMap<TopicClass, SendDispatcher>()
            try {
                // Wrap the fallback appender in a dispatcher so the Kafka I/O
                // thread is never blocked on the fallback's downstream I/O.
                // See FallbackDispatcher KDoc for the rationale.
                fallbackDispatcher =
                    fallbackAppender?.let {
                        FallbackDispatcher(
                            it,
                            reentryGuard = selfLoggingGuard,
                            onWorkerDeath = { t ->
                                warn(
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
                        fallbackDispatcher = fallbackDispatcher,
                    )
                // One send dispatcher per active class: producer.send runs on
                // the dispatcher's worker, never on the logging caller. The
                // per-class split mirrors the producer/breaker isolation - a
                // stalled AUDIT send cannot delay TECHNICAL delivery.
                registry.activeTopicClasses.forEach { topicClass ->
                    sendDispatchers[topicClass] =
                        SendDispatcher(
                            topicClass = topicClass,
                            sendAction = { pending ->
                                // Invariant: claimDiversion shares the per-item
                                // exactly-once guard with the dispatcher, so a
                                // forced-shutdown divert and the sender's own
                                // error routing never both deliver the same
                                // event. Rationale: the detached claim object
                                // (not the PendingSend) is what the Kafka
                                // callback retains - see DiversionClaim.
                                sender.send(
                                    topicClass,
                                    pending.topicName,
                                    pending.payload,
                                    pending.enrichment,
                                    pending.originalEvent,
                                    claimDiversion = pending.claim::tryClaim,
                                )
                            },
                            fallbackDispatcher = fallbackDispatcher,
                            reentryGuard = selfLoggingGuard,
                            queueCapacity = settings.sendQueueCapacity,
                            onWorkerDeath = { t ->
                                warn(
                                    "Send dispatcher worker for $topicClass died from ${t.javaClass.name}; " +
                                        "queued and further $topicClass events divert to the fallback " +
                                        "(reason send.error).",
                                    t,
                                )
                            },
                        )
                }
                return KafkaTransport(
                    producerRegistry = registry,
                    selfLoggingGuard = selfLoggingGuard,
                    messageSender = sender,
                    sendDispatchers = sendDispatchers,
                    fallbackDispatcher = fallbackDispatcher,
                )
            } catch (e: Exception) {
                // Rationale: the rollback is silent - the caller reports the
                // construction failure itself, and the fresh, empty workers
                // have nothing to drop or to warn about.
                closeAll(sendDispatchers, registry, fallbackDispatcher) { _, _ -> }
                throw e
            }
        }

        /**
         * The one place that knows the reverse ownership order: send
         * dispatchers (in parallel, within one shared budget; each
         * [SendDispatcher.close] is itself bounded, so the closer
         * threads always finish and the join budget only adds
         * scheduling margin), then the producer registry, then the
         * fallback dispatcher.
         */
        private fun closeAll(
            sendDispatchers: Map<TopicClass, SendDispatcher>,
            producerRegistry: ProducerRegistry,
            fallbackDispatcher: FallbackDispatcher?,
            warn: (message: String, cause: Throwable?) -> Unit,
        ) {
            // Close the send dispatchers BEFORE the producer registry: their
            // graceful drain delivers the queued events through the still-
            // open producers; whatever cannot be sent in time diverts to the
            // fallback dispatcher (which closes later for exactly that
            // reason). Closed IN PARALLEL so the per-dispatcher budgets
            // (drain plus interrupt grace) do not stack across topic classes
            // - the same single-overall-budget principle the producer
            // registry applies to its close.
            closeInParallel(
                budgetMs = SEND_DISPATCHER_CLOSE_BUDGET_MS,
                tasks =
                    sendDispatchers.map { (topicClass, dispatcher) ->
                        "tabellarium-send-dispatcher-close-${topicClass.tag}" to {
                            try {
                                dispatcher.close()
                            } catch (e: Exception) {
                                warn("Error closing send dispatcher for $topicClass: ${e.message}", e)
                            }
                        }
                    },
            )
            try {
                producerRegistry.close()
            } catch (e: Exception) {
                warn("Error closing producer registry: ${e.message}", e)
            }
            // Close the dispatcher AFTER the producer registry: the registry
            // may still trigger fallback events during its own close-path
            // drain. Once the registry is gone, no more events can land in
            // the dispatcher; we can drain and shut it down.
            try {
                fallbackDispatcher?.let { dispatcher ->
                    dispatcher.close()
                    if (dispatcher.droppedEventCount > 0) {
                        warn(
                            "Fallback dispatcher dropped ${dispatcher.droppedEventCount} " +
                                "event(s) during the lifetime of this appender",
                            null,
                        )
                    }
                }
            } catch (e: Exception) {
                warn("Error closing fallback dispatcher: ${e.message}", e)
            }
        }

        /**
         * The client.id ends up in JMX object names and metric tags, where
         * characters outside this set break registration or make tags
         * unusable, so anything else in the component name is mapped to '-'.
         */
        private fun jmxSafe(value: String): String = value.replace(Regex("[^a-zA-Z0-9._-]"), "-")
    }
}
