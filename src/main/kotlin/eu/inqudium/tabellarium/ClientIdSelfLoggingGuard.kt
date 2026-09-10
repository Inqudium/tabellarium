package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import org.apache.kafka.clients.producer.KafkaProducer

/**
 * The default [SelfLoggingGuard]: recognizes the appender's own echo by
 * the names of the threads that produce it and, for the one case a name
 * cannot tell, by a per-thread mark. Three echoes exist, and this class
 * answers all of them with one question ([shouldDrop]):
 *
 * - **The producers' own logging.** The Kafka client logs its
 *   connection warnings and errors on each producer's network thread,
 *   which it names `"kafka-producer-network-thread | <client.id>"`.
 *   Routed back into the appender, those events would be sent through
 *   the producer whose logging they are - a loop that amplifies exactly
 *   when the producer logs most, during broker trouble. The match is
 *   anchored to the exact scheme (prefix plus one of this appender's
 *   full client ids, see [ownThreadNames]), so an operator-supplied
 *   short client id can never match unrelated application threads whose
 *   names merely contain it, and another appender instance's producers
 *   are not matched either - that is the limit this implementation's
 *   name states, and the gap the README's "Cross-instance guards" names
 *   together with its future shape: a process-wide client-id registry,
 *   as a second [SelfLoggingGuard] implementation.
 * - **The workers' own logging.** `producer.send` runs on the send
 *   workers, and the Kafka 4.x client logs `ApiException`s at DEBUG
 *   synchronously on the `send` caller in its failure path; the fallback
 *   appender's `doAppend` runs on the fallback worker, and an appender
 *   that logs through SLF4J per event would feed each such log back into
 *   the pipeline. Both events carry the worker's thread name, and the
 *   workers have fixed names ([SendDispatcher.threadNameFor],
 *   [FallbackDispatcher.THREAD_NAME]) that belong to this library alone,
 *   so they are matched exactly like the producer threads - no mark, no
 *   thread state on the workers. Because the names are the same in every
 *   appender instance, this also drops the worker echoes of *another*
 *   instance in the same JVM, which is safe (the `kafka-appender-`
 *   scheme is nobody else's) and closes the worker half of the
 *   cross-instance gap.
 * - **Reentry on an application thread.** `UnsynchronizedAppenderBase`
 *   ships only a no-op guard, so a log event emitted *synchronously from
 *   inside the append path itself* re-enters `append` on the same
 *   thread: the remaining synchronous work (`encoder.encode`, metric
 *   hooks) can itself log through SLF4J, and without a guard that is
 *   unbounded recursion ending in a `StackOverflowError`. Such an event
 *   carries the application thread's name, which no name set can know,
 *   so the appender brackets its hot path with [enter]/[exit] and the
 *   mark is a `ThreadLocal` - the smallest state that says "this thread
 *   is already inside".
 *
 * The mark is a `ThreadLocal`, not an instance field, because the hot
 * path is lock-free: several application threads are in `append` at the
 * same time and each must see only its own state. **Deliberately also
 * active on virtual threads.** Skipping the mark for virtual callers was
 * considered and rejected: the recursion protection is needed exactly
 * where virtual threads occur - safety must not depend on the thread
 * type. The cost is one `ThreadLocalMap` per thread that ever logs
 * (allocated on its first append, gone with the thread, holding a
 * cached `Boolean`) - per pooled platform thread once, per
 * thread-per-request virtual thread once per request; the derivation
 * and the `ScopedValue` outlook (JDK 25+) are in
 * `docs/assessment/PERF_ANALYSIS-2026-08-29T11-01-08.md`, finding 6.
 *
 * Owned by the [KafkaTransport]: the client ids exist only once the
 * producers do.
 *
 * @param producerClientIds The effective `client.id` values of this
 *                          appender's producers. Blank ids are ignored -
 *                          a blank id would match the bare prefix.
 */
internal class ClientIdSelfLoggingGuard(
    producerClientIds: Set<String>,
) : SelfLoggingGuard {
    /**
     * The complete names of every thread whose log events are this
     * appender's echo: the producers' network threads, derived once
     * from the client ids (the scheme is fully known without looking at
     * any live thread), and the library's own worker threads. One set,
     * so the hot path needs a single lookup instead of a prefix check
     * plus a substring and a second lookup. A `HashSet` for O(1)
     * membership; `Thread.getName` returns the same `String` instance
     * per thread, whose hash is cached after the first computation.
     */
    private val ownThreadNames: Set<String> =
        HashSet<String>().apply {
            producerClientIds.filter { it.isNotBlank() }.mapTo(this) { PRODUCER_NETWORK_THREAD_PREFIX + it }
            TopicClass.entries.mapTo(this) { SendDispatcher.threadNameFor(it) }
            add(FallbackDispatcher.THREAD_NAME)
        }

    /** True on an application thread that is inside the append path. */
    private val inAppend: ThreadLocal<Boolean> = ThreadLocal.withInitial { false }

    /**
     * Whether [event] is this appender's own echo: logged on a thread
     * that is inside the append path, or logged by the network thread
     * of one of the appender's producers or by one of its workers.
     * Called first thing on the hot path; reads one `ThreadLocal` and,
     * only for events from other threads, does one set lookup on the
     * event's thread name.
     */
    override fun shouldDrop(event: ILoggingEvent): Boolean {
        if (inAppend.get()) {
            return true
        }
        val threadName = event.threadName ?: return false
        return threadName in ownThreadNames
    }

    override fun enter() {
        inAppend.set(true)
    }

    override fun exit() {
        inAppend.set(false)
    }

    companion object {
        /**
         * Kafka's naming scheme for the producer's network thread: the
         * public constant [KafkaProducer.NETWORK_THREAD_PREFIX], a
         * separator, then the client.id verbatim.
         *
         * Compatibility: the prefix comes from the client's public API,
         * so a rename fails compilation instead of silently disabling
         * the guard; the separator is a literal in the KafkaProducer
         * constructor with no constant to reference, so
         * `KafkaProducerThreadNamingContractTest` checks the whole
         * scheme against a real producer of the built client version -
         * a client upgrade that changes it turns the build red.
         */
        internal const val PRODUCER_NETWORK_THREAD_PREFIX: String = KafkaProducer.NETWORK_THREAD_PREFIX + " | "
    }
}
