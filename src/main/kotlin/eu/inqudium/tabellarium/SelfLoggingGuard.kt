package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Keeps the appender out of its own feedback loops: decides, for every
 * event that reaches [KafkaAppender.append], whether it is the library's
 * own echo and must be dropped - no metrics, no fallback - instead of
 * being shipped through the very pipeline that produced it. The echo is
 * recognized by the names of the threads that produce it and, for the
 * one case a name cannot tell, by a per-thread mark. Three echoes exist,
 * and this class answers all of them with one question ([shouldDrop]):
 *
 * - **The producers' own logging.** The Kafka client logs its
 *   connection warnings and errors on each producer's network thread,
 *   which it names `"kafka-producer-network-thread | <client.id>"`.
 *   Routed back into an appender, those events would be sent through
 *   a producer whose logging they are - a loop that amplifies exactly
 *   when the producer logs most, during broker trouble. The match is
 *   anchored to the exact scheme (prefix plus one full client id), so
 *   an operator-supplied short client id can never match unrelated
 *   application threads whose names merely contain it. The client ids
 *   are those of **every live instance in the JVM**, not only this
 *   one's: two appenders attached to the same logger would otherwise
 *   ship each other's producer logging, each through a producer whose
 *   logging the other ships in turn - the cross-instance loop of
 *   `DEFECT_ANALYSIS-2026-09-15T22-05-50`, M-3. See
 *   [liveEchoThreadNames] for the process-wide registry that closes it.
 * - **The workers' own logging.** `producer.send` runs on the send
 *   workers, and the Kafka 4.x client logs `ApiException`s at DEBUG
 *   synchronously on the `send` caller in its failure path; the fallback
 *   appender's `doAppend` runs on the fallback worker, and an appender
 *   that logs through SLF4J per event would feed each such log back into
 *   the pipeline. Both events carry the worker's thread name, and the
 *   workers have fixed names ([SendDispatcher.threadNameFor],
 *   [FallbackDispatcher.THREAD_NAME]) that belong to this library alone,
 *   so they are matched exactly like the producer threads - no mark, no
 *   thread state on the workers. The names are the same in every
 *   appender instance, so they are registered once per instance and
 *   drop the worker echoes of every instance, which is safe (the
 *   `kafka-appender-` scheme is nobody else's).
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
 * `PERF_ANALYSIS-2026-08-29T11-01-08`, finding 6.
 *
 * ## Contract
 *
 * - [shouldDrop] is the first thing the hot path calls: it is cheap,
 *   never throws, and is safe to call from any number of threads at
 *   once (the append path is lock-free).
 * - [enter] and [exit] bracket the synchronous work of one append on
 *   the calling thread; an event logged from inside that bracket, on
 *   the same thread, is the appender's own echo. The pair is always
 *   used in a `try`/`finally`.
 * - [close] ends the guard's life: the names it entered leave the
 *   process-wide registry. Idempotent.
 *
 * Owned by the [KafkaTransport], which constructs the guard right after
 * the producer registry - the client ids exist only once the producers
 * do - and whose close is what [close]s the guard: last, after the
 * producers, so their logging during the close is still recognized.
 * One class, no interface: the interface that used to sit in front of
 * it was extracted for a second, process-wide implementation, which was
 * then built into this one (the registry below); the seam that remained
 * served only test substitutions
 * (`ARCHITECTURE_REVIEW-2026-10-10T22-15-57`, finding 1).
 *
 * @param producerClientIds The effective `client.id` values of this
 *                          appender's producers. Blank ids are ignored -
 *                          a blank id would match the bare prefix.
 */
internal class SelfLoggingGuard(
    producerClientIds: Set<String>,
) {
    /**
     * The names this instance entered into [liveEchoThreadNames]: the
     * producers' network threads, derived once from the client ids (the
     * scheme is fully known without looking at any live thread), and the
     * library's own worker threads. Kept so [close] can leave exactly
     * what this instance entered.
     */
    private val registeredThreadNames: Set<String> =
        HashSet<String>().apply {
            producerClientIds.filter { it.isNotBlank() }.mapTo(this) { PRODUCER_NETWORK_THREAD_PREFIX + it }
            TopicClass.entries.mapTo(this) { SendDispatcher.threadNameFor(it) }
            add(FallbackDispatcher.THREAD_NAME)
        }

    /** Guards [close] against a second unregistration of the same names. */
    private val closed = AtomicBoolean(false)

    init {
        registeredThreadNames.forEach { name -> liveEchoThreadNames.merge(name, 1, Int::plus) }
    }

    /** True on an application thread that is inside the append path. */
    private val inAppend: ThreadLocal<Boolean> = ThreadLocal.withInitial { false }

    /**
     * Whether [event] is the library's own echo: logged on a thread
     * that is inside the append path, or logged by the network thread
     * of any live instance's producer or by one of the workers. Called
     * first thing on the hot path; reads one `ThreadLocal` and, only
     * for events from other threads, does one map lookup on the event's
     * thread name (`Thread.getName` returns the same `String` instance
     * per thread, whose hash is cached after the first computation).
     */
    fun shouldDrop(event: ILoggingEvent): Boolean {
        if (inAppend.get()) {
            return true
        }
        val threadName = event.threadName ?: return false
        return liveEchoThreadNames.containsKey(threadName)
    }

    /** Marks the current thread as inside the append path; paired with [exit] in a `finally`. */
    fun enter() {
        inAppend.set(true)
    }

    /** Clears the mark set by [enter]. */
    fun exit() {
        inAppend.set(false)
    }

    /**
     * Leaves the process-wide registry: every name this instance
     * entered is counted down once, and removed when no other live
     * instance holds it. Idempotent - a second call changes nothing.
     */
    fun close() {
        if (!closed.compareAndSet(false, true)) return
        registeredThreadNames.forEach { name ->
            liveEchoThreadNames.computeIfPresent(name) { _, count -> if (count <= 1) null else count - 1 }
        }
    }

    companion object {
        /**
         * The process-wide registry: the names of every thread whose log
         * events are some live instance's echo, with a count of the
         * instances that entered each name. Entered by the constructor
         * of every instance, left by its [close]; a name stays as long as
         * one instance still holds it, so two appenders that share an
         * operator-supplied `client.id` (or the fixed worker names) keep
         * dropping the echo until the last of them closes. Static by
         * design: the loop it prevents runs across instances, so the
         * registry must be visible to all of them - a per-instance set
         * knew only its own producers (`DEFECT_ANALYSIS-2026-09-15T22-05-50`,
         * M-3). A `ConcurrentHashMap` because entries and exits happen on
         * lifecycle threads while every logging thread reads; the hot
         * path is one lock-free `containsKey`.
         */
        private val liveEchoThreadNames = ConcurrentHashMap<String, Int>()

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

        /**
         * Those of [clientIds] that more than one live instance holds -
         * asked by a started appender about its own client ids, so a
         * hit is another live appender (two instances sharing a
         * `<component>` derive identical ids). Within one instance the
         * names are a set, so its own shared explicit `client.id` counts
         * once. Read for the start-up diagnostic only.
         */
        internal fun clientIdsHeldByOtherInstances(clientIds: Set<String>): Set<String> =
            clientIds.filterTo(sortedSetOf()) { id ->
                id.isNotBlank() && (liveEchoThreadNames[PRODUCER_NETWORK_THREAD_PREFIX + id] ?: 0) > 1
            }
    }
}
