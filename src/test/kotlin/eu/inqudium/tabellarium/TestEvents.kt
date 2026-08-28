package eu.inqudium.tabellarium

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent

/**
 * Shared test-helper for constructing [ILoggingEvent] instances in unit
 * tests. Always sets [LoggingEvent.setMDCPropertyMap] explicitly: a
 * freshly constructed [LoggerContext] has no MDC adapter bound, so
 * Logback's lazy initialization in `getMDCPropertyMap()` would otherwise
 * throw NPE on first read.
 *
 * Marked `internal` so it lives only in the test module's compilation
 * unit.
 */
internal fun newTestLoggingEvent(
    message: String = "test message",
    mdc: Map<String, String> = emptyMap(),
    level: Level = Level.INFO,
    loggerName: String = "test-logger",
): ILoggingEvent {
    val context = LoggerContext()
    val logger = context.getLogger(loggerName)
    return LoggingEvent("fqcn.dummy", logger, level, message, null, null).apply {
        mdcPropertyMap = mdc
    }
}
