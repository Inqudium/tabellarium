package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import org.apache.kafka.clients.producer.KafkaProducer

/**
 * The default [SelfLoggingGuard]: recognizes the appender's own echo by
 * the client ids of its own producers and by a per-thread mark. Two
 * echoes exist, on two kinds of thread, and this class answers both
 * with one question ([shouldDrop]):
 *
 * - **The producers' own logging.** The Kafka client logs its
 *   connection warnings and errors on each producer's network thread,
 *   which it names `"kafka-producer-network-thread | <client.id>"`.
 *   Routed back into the appender, those events would be sent through
 *   the producer whose logging they are - a loop that amplifies exactly
 *   when the producer logs most, during broker trouble. The match is
 *   anchored to the exact scheme (prefix plus one of this appender's
 *   full client ids, see [isOwnProducerThread]), so an operator-supplied
 *   short client id can never match unrelated application threads whose
 *   names merely contain it, and another appender instance's producers
 *   are not matched either - that is the limit this implementation's
 *   name states, and the gap the README's "Cross-instance guards" names
 *   together with its future shape: a process-wide client-id registry,
 *   as a second [SelfLoggingGuard] implementation.
 * - **Reentry on the same thread.** `UnsynchronizedAppenderBase` ships
 *   only a no-op guard, so a log event emitted *synchronously from inside
 *   the append path itself* re-enters `append` on the same thread. That
 *   happens on two kinds of thread: the send workers - `producer.send`
 *   runs there, and the Kafka 4.x client logs `ApiException`s at DEBUG
 *   synchronously on the `send` caller in its failure path (the
 *   network-thread match cannot catch it, the event carries the worker's
 *   name) - and the application threads, where the remaining synchronous
 *   work (`encoder.encode`, metric hooks) can itself log through SLF4J;
 *   without the mark that is unbounded recursion ending in a
 *   `StackOverflowError`. The appender brackets its hot path with
 *   [enter]/[exit]; every worker marks itself once for its lifetime with
 *   [markCurrentThreadForLife], because nothing a worker logs is ever
 *   legitimate output.
 *
 * The mark is a `ThreadLocal`, not an instance field, because the hot
 * path is lock-free: several application threads are in `append` at the
 * same time and each must see only its own state. **Deliberately also
 * active on virtual threads.** Skipping the mark for virtual callers
 * (the workers are always platform threads) was considered and
 * rejected: the recursion protection is needed exactly where virtual
 * threads occur - safety must not depend on the thread type. The cost
 * is one cached-Boolean `ThreadLocal` entry per thread that ever logs;
 * the derivation and the `ScopedValue` outlook (JDK 25+) are in
 * `docs/assessment/PERF_ANALYSIS-2026-08-29T11-01-08.md`, finding 6.
 *
 * Owned by the [KafkaTransport]: the client ids exist only once the
 * producers do, and the workers that carry the mark are created right
 * after them.
 *
 * @param producerClientIds The effective `client.id` values of this
 *                          appender's producers. Blank ids are ignored -
 *                          a blank id would match the bare prefix.
 */
internal class ClientIdSelfLoggingGuard(
    producerClientIds: Set<String>,
) : SelfLoggingGuard {
    private val producerClientIds: Set<String> = producerClientIds.filterTo(HashSet()) { it.isNotBlank() }

    /** True on a thread that is inside the append path, or is a worker of this appender. */
    private val inAppend: ThreadLocal<Boolean> = ThreadLocal.withInitial { false }

    /**
     * Whether [event] is this appender's own echo: logged on a thread
     * that is inside the append path (or is one of the appender's
     * workers), or logged by the network thread of one of the
     * appender's producers. Called first thing on the hot path; reads
     * one `ThreadLocal` and, only for events from other threads, does
     * one prefix check plus one set lookup.
     */
    override fun shouldDrop(event: ILoggingEvent): Boolean {
        if (inAppend.get()) {
            return true
        }
        val threadName = event.threadName ?: return false
        return isOwnProducerThread(threadName)
    }

    /**
     * Whether [threadName] is the network thread of one of this
     * appender's producers: the exact scheme
     * [PRODUCER_NETWORK_THREAD_PREFIX] followed by one full client id.
     */
    fun isOwnProducerThread(threadName: String): Boolean =
        threadName.startsWith(PRODUCER_NETWORK_THREAD_PREFIX) &&
            threadName.removePrefix(PRODUCER_NETWORK_THREAD_PREFIX) in producerClientIds

    override fun enter() {
        inAppend.set(true)
    }

    override fun exit() {
        inAppend.set(false)
    }

    override fun markCurrentThreadForLife() {
        inAppend.set(true)
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
