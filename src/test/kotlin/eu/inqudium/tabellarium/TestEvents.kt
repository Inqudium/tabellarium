package eu.inqudium.tabellarium

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import java.util.concurrent.CountDownLatch

/**
 * Shared test-helper for constructing [ILoggingEvent] instances in unit
 * tests. Always sets [LoggingEvent.setMDCPropertyMap] explicitly: a
 * freshly constructed [LoggerContext] has no MDC adapter bound, so
 * Logback's lazy initialization in `getMDCPropertyMap()` would otherwise
 * throw NPE on first read.
 */
internal fun newTestLoggingEvent(
    message: String = "test message",
    mdc: Map<String, String> = emptyMap(),
    level: Level = Level.INFO,
    loggerName: String = "test-logger",
    threadName: String? = null,
): ILoggingEvent {
    val context = LoggerContext()
    val logger = context.getLogger(loggerName)
    return LoggingEvent("fqcn.dummy", logger, level, message, null, null).apply {
        mdcPropertyMap = mdc
        threadName?.let { this.threadName = it }
    }
}

/**
 * Bounded-deadline polling for asynchronous assertions - the shared
 * alternative to naked sleeps and to pulling in awaitility for the few
 * places that need it. Fails with an [AssertionError] when [condition]
 * does not become true within [timeoutMs].
 */
internal fun pollUntil(
    timeoutMs: Long = 2000,
    intervalMs: Long = 10,
    condition: () -> Boolean,
) {
    val deadline = System.nanoTime() + timeoutMs * 1_000_000
    while (System.nanoTime() < deadline) {
        if (condition()) return
        Thread.sleep(intervalMs)
    }
    throw AssertionError("Condition did not become true within ${timeoutMs}ms")
}

/**
 * Waits until [latch] reaches zero and swallows interrupts while doing so
 * - the shared blocking body of the fakes that model a producer or
 * appender stuck in a call that ignores interruption (a hung broker
 * round-trip, a slow fallback appender). Once the wait is over, a
 * swallowed interrupt is restored on the current thread so the code under
 * test still observes it.
 *
 * Rationale: the fakes must outlast the dispatcher's interrupt grace, so
 * a plain `await()` (which ends on the first interrupt) would model a
 * different, cooperative callee.
 */
internal fun awaitUninterruptibly(latch: CountDownLatch) {
    var wasInterrupted = false
    while (latch.count > 0) {
        try {
            latch.await()
        } catch (_: InterruptedException) {
            wasInterrupted = true
        }
    }
    if (wasInterrupted) {
        Thread.currentThread().interrupt()
    }
}
