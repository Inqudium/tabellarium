package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent

/**
 * Keeps the appender out of its own feedback loops: decides, for every
 * event that reaches [KafkaAppender.append], whether it is the
 * appender's own echo and must be dropped - no metrics, no fallback -
 * instead of being shipped through the very pipeline that produced it.
 *
 * ## Contract
 *
 * - [shouldDrop] is the first thing the hot path calls: it must be
 *   cheap, must never throw, and must be safe to call from any number
 *   of threads at once (the append path is lock-free).
 * - [enter] and [exit] bracket the synchronous work of one append on
 *   the calling thread; an event logged from inside that bracket, on
 *   the same thread, is the appender's own echo. The pair is always
 *   used in a `try`/`finally`.
 *
 * The appender's own worker threads need no marking: they have fixed
 * names, and an implementation recognizes their echoes the same way it
 * recognizes the producers' - by the event's thread name.
 *
 * Rationale: the interface is the seam between the hot path and the
 * one place an implementation is chosen ([SelfLoggingGuardFactory]).
 * [ClientIdSelfLoggingGuard] is the only production implementation;
 * it keeps a process-wide registry of every live instance's producer
 * threads, so a guard has a lifecycle of its own ([close]) that the
 * owning [KafkaTransport] ends after the producers are gone. Tests
 * substitute a guard with recorded decisions through the factory.
 */
internal interface SelfLoggingGuard {
    /**
     * Whether [event] is this appender's own echo and must be dropped
     * entirely: no metrics, no fallback.
     */
    fun shouldDrop(event: ILoggingEvent): Boolean

    /** Marks the current thread as inside the append path; paired with [exit] in a `finally`. */
    fun enter()

    /** Clears the mark set by [enter]. */
    fun exit()

    /**
     * Ends the guard's life: an implementation that entered shared state
     * (the process-wide registry) leaves it here. Called once by the
     * owning transport after every producer is closed; must be safe to
     * call more than once. The default does nothing, for guards without
     * shared state.
     */
    fun close() = Unit
}

/**
 * The single place a [SelfLoggingGuard] implementation is chosen.
 * [KafkaTransport.open] calls [create] once per started appender, right
 * after the producer registry exists, with the effective client ids of
 * that appender's producers; the hot path then uses the returned guard.
 * The appender holds the factory as an internal seam (like
 * [ProducerFactory]): tests substitute a guard with recorded decisions
 * without touching any caller.
 */
internal fun interface SelfLoggingGuardFactory {
    /**
     * Creates the guard for one appender.
     *
     * @param producerClientIds The effective `client.id` values of the
     *                          appender's producers; blank ids may be
     *                          present and must be ignored.
     */
    fun create(producerClientIds: Set<String>): SelfLoggingGuard

    companion object {
        /** The default: a [ClientIdSelfLoggingGuard] over the given client ids. */
        fun default(): SelfLoggingGuardFactory = SelfLoggingGuardFactory { clientIds -> ClientIdSelfLoggingGuard(clientIds) }
    }
}
