package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Test-support fallback appender with a thread-safe event list. Used
 * instead of Logback's `ListAppender` wherever a test thread reads the
 * recorded events while a dispatcher worker appends concurrently:
 * `ListAppender` backs onto a plain `ArrayList`, so a reader on another
 * thread has neither mutual exclusion nor a happens-before edge on it.
 * The copy-on-write list makes every read observe a fully published
 * snapshot.
 *
 * Instantiated reflectively by Joran in the XML round-trip tests, so
 * the class needs its public no-arg constructor.
 */
internal class ThreadSafeListAppender : AppenderBase<ILoggingEvent>() {
    val events = CopyOnWriteArrayList<ILoggingEvent>()

    override fun append(event: ILoggingEvent) {
        events.add(event)
    }
}
