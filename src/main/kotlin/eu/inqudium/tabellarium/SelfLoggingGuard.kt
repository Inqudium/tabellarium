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
 * - [markCurrentThreadForLife] is what the appender's own workers call
 *   once at start: nothing a worker logs is ever legitimate output, so
 *   the mark is never cleared.
 *
 * Rationale: the interface exists for one named second implementation,
 * not for mockability - the process-wide client-id registry that the
 * README's "Cross-instance guards" describes, which would answer
 * [shouldDrop] for every appender instance's producers instead of only
 * this one's. Until it exists, [ClientIdSelfLoggingGuard] is the only
 * implementation and the default of [SelfLoggingGuardFactory] - the one
 * place an implementation is chosen, so swapping it touches nothing on
 * the hot path or in the transport.
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
     * Marks the current thread for its whole lifetime: for the
     * appender's own workers, which never legitimately log through the
     * appender, so everything raised on them is a loop.
     */
    fun markCurrentThreadForLife()
}

/**
 * The single place a [SelfLoggingGuard] implementation is chosen.
 * [KafkaTransport.open] calls [create] once per started appender, right
 * after the producer registry exists, with the effective client ids of
 * that appender's producers; every worker and the hot path then use the
 * returned guard. The appender holds the factory as an internal seam
 * (like [ProducerFactory]): tests substitute a guard with recorded
 * decisions, and a future process-wide implementation is plugged in
 * here without touching any caller.
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
