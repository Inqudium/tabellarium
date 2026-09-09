package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry

/**
 * The running components of a started [KafkaAppender], owned as one
 * unit: router, table, enricher, producer registry, sender, one
 * [SendDispatcher] per active topic class, and the optional
 * [FallbackDispatcher].
 *
 * [KafkaAppender] stays the composition root - it carries the Joran
 * surface, the hot path and the Logback lifecycle - and delegates what
 * `start()` builds and `stop()` tears down to this class, so that the
 * ownership order exists in exactly one place ([closeAll]). Before the
 * extraction the reverse-ownership close order was written three times
 * (the rollback inside the build, `stop()`, and the parallel dispatcher
 * close) and kept in step by hand; the 2026-09-07 architecture review
 * ranked the appender as the unit where the lifecycle findings
 * clustered.
 *
 * ## Build as a transaction
 *
 * [build] parses, routes and classifies first (pure, nothing to roll
 * back), then creates the real resources. Invariant: a construction
 * failure after the first real resource exists closes everything
 * created so far, in the same order [close] uses, so a failed or
 * reloaded configuration never leaks producers or daemon workers that
 * only an external `stop()` could reach. The encoder is not owned here:
 * the appender starts it before [build] and releases it if [build]
 * throws.
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
internal class AppenderPipeline private constructor(
    val topicRouter: TopicRouter,
    val topicTable: TopicTable,
    val messageEnricher: MessageEnricher,
    val producerRegistry: ProducerRegistry,
    val messageSender: ResilientMessageSender,
    val sendDispatchers: Map<TopicClass, SendDispatcher>,
    val fallbackDispatcher: FallbackDispatcher?,
) {
    /**
     * The effective `client.id` values of the producers, for the
     * appender's self-logging guard (see [KafkaAppender.append]).
     */
    val producerClientIds: Set<String> = producerRegistry.clientIds

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
         * Builds the pipeline from the appender's validated
         * configuration. See the class KDoc for the transaction
         * contract.
         *
         * @param reentryGuard The appender's per-thread reentry guard;
         *                     every worker created here marks itself
         *                     with it for its whole lifetime.
         * @param warn Sink for asynchronous worker-death reports; the
         *             appender routes it to its status manager.
         */
        fun build(
            kafkaProducerProperties: String,
            topicMapping: TopicMappingConfig,
            component: String,
            cmdbId: String,
            environment: String,
            fallbackAppender: Appender<ILoggingEvent>?,
            producerFactory: ProducerFactory,
            circuitBreakerRegistry: CircuitBreakerRegistry,
            sendQueueCapacity: Int,
            reentryGuard: ThreadLocal<Boolean>,
            warn: (message: String, cause: Throwable) -> Unit,
        ): AppenderPipeline {
            val baseProperties = parseKafkaProducerProperties(kafkaProducerProperties)
            val topicRouter = topicMapping.toTopicRouter()
            val topicTable = topicMapping.toTopicTable()
            val messageEnricher =
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
                            reentryGuard = reentryGuard,
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
                            reentryGuard = reentryGuard,
                            queueCapacity = sendQueueCapacity,
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
                return AppenderPipeline(
                    topicRouter = topicRouter,
                    topicTable = topicTable,
                    messageEnricher = messageEnricher,
                    producerRegistry = registry,
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
