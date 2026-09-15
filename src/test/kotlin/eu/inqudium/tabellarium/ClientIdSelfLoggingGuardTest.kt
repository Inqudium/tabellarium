package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class ClientIdSelfLoggingGuardTest {
    // -- Test fixtures --------------------------------------------------

    private val clientId = "tabellarium-payments-audit"

    /**
     * Every guard a test built; closed after the test so its names
     * leave the process-wide registry and cannot make another test's
     * "foreign" thread name an echo.
     */
    private val createdGuards = mutableListOf<ClientIdSelfLoggingGuard>()

    @AfterEach
    fun closeGuards() {
        createdGuards.forEach { it.close() }
        createdGuards.clear()
    }

    private fun guard(vararg clientIds: String) = ClientIdSelfLoggingGuard(clientIds.toSet()).also { createdGuards += it }

    private fun networkThreadOf(clientId: String) = ClientIdSelfLoggingGuard.PRODUCER_NETWORK_THREAD_PREFIX + clientId

    private fun eventFrom(threadName: String) = newTestLoggingEvent(message = "probe", threadName = threadName)

    // -- Tests ----------------------------------------------------------

    @Nested
    inner class `Own producer threads` {
        @Test
        fun `should drop an event from the network thread of one of its own producers`() {
            // What is to be tested? Whether an event whose thread name is
            //   exactly the Kafka network-thread scheme with one of the
            //   guard's client ids is recognized as the producer's own
            //   logging.
            // How will the test case be deemed successful and why? Successful
            //   if shouldDrop returns true for prefix plus client id. This
            //   is the half of the guard that stops the feedback loop
            //   through the producer's own connection warnings.
            // Why is it important to test this test case? The loop it
            //   prevents amplifies exactly during broker trouble; a guard
            //   that stopped matching would fail silently until an outage.

            // Given
            val guard = guard(clientId)

            // When / Then
            assertThat(guard.shouldDrop(eventFrom(ClientIdSelfLoggingGuard.PRODUCER_NETWORK_THREAD_PREFIX + clientId))).isTrue()
        }

        @Test
        fun `should deliver events from threads whose name merely contains a client id`() {
            // What is to be tested? Whether the match is anchored to the
            //   exact scheme rather than a substring search: an
            //   application thread that happens to carry the client id in
            //   its name, or the prefix followed by a foreign client id,
            //   must not be dropped.
            // How will the test case be deemed successful and why? Successful
            //   if shouldDrop returns false for a substring-only match, for
            //   a foreign id behind the prefix, and for the bare prefix.
            // Why is it important to test this test case? An
            //   operator-supplied short client id (say "app") would
            //   otherwise silence every application thread whose name
            //   contains those letters - a data-loss bug that looks like
            //   a healthy pipeline.

            // Given
            val guard = guard(clientId)

            // When / Then
            assertThat(guard.shouldDrop(eventFrom("app-worker-$clientId"))).isFalse()
            assertThat(guard.shouldDrop(eventFrom(ClientIdSelfLoggingGuard.PRODUCER_NETWORK_THREAD_PREFIX + "other-instance-audit"))).isFalse()
            assertThat(guard.shouldDrop(eventFrom(ClientIdSelfLoggingGuard.PRODUCER_NETWORK_THREAD_PREFIX))).isFalse()
            assertThat(guard.shouldDrop(eventFrom("main"))).isFalse()
        }

        @Test
        fun `should ignore blank client ids`() {
            // What is to be tested? Whether a blank client id is excluded
            //   from the match set, since a blank id would match the bare
            //   prefix and thereby every producer network thread in the
            //   JVM, including the application's own producers.
            // How will the test case be deemed successful and why? Successful
            //   if a guard built with a blank id does not drop an event
            //   from a thread named exactly the prefix.
            // Why is it important to test this test case? An operator can
            //   set client.id to an empty value; the registry filters it,
            //   and this pins that the guard does too, independently.

            // Given
            val guard = guard("", "  ")

            // When / Then
            assertThat(guard.shouldDrop(eventFrom(ClientIdSelfLoggingGuard.PRODUCER_NETWORK_THREAD_PREFIX))).isFalse()
            assertThat(guard.shouldDrop(eventFrom(ClientIdSelfLoggingGuard.PRODUCER_NETWORK_THREAD_PREFIX + "  "))).isFalse()
        }
    }

    @Nested
    inner class `Reentry mark` {
        @Test
        fun `should drop events only between enter and exit on the marking thread`() {
            // What is to be tested? Whether the reentry mark is set by
            //   enter(), cleared by exit(), and visible only on the thread
            //   that set it - the append path brackets its synchronous
            //   work with the pair, and an event logged from inside that
            //   bracket must be dropped while a concurrent thread's events
            //   must not.
            // How will the test case be deemed successful and why? Successful
            //   if shouldDrop is false before enter(), true between
            //   enter() and exit(), false again after exit(), and false on
            //   another thread while this one is inside.
            // Why is it important to test this test case? A mark that
            //   leaked across threads would drop unrelated application
            //   events (the hot path is lock-free, many threads are inside
            //   at once); a mark that was not cleared would silence the
            //   thread forever after its first log call.

            // Given
            val guard = guard(clientId)
            val event = eventFrom("caller")
            assertThat(guard.shouldDrop(event)).isFalse()

            // When: inside the bracket on this thread
            guard.enter()
            try {
                // Then: dropped here ...
                assertThat(guard.shouldDrop(event)).isTrue()

                // ... but not on another thread
                val seenOnOtherThread = AtomicReference<Boolean?>()
                val done = CountDownLatch(1)
                Thread {
                    seenOnOtherThread.set(guard.shouldDrop(eventFrom("other")))
                    done.countDown()
                }.start()
                assertThat(done.await(2, TimeUnit.SECONDS)).isTrue()
                assertThat(seenOnOtherThread.get()).isFalse()
            } finally {
                guard.exit()
            }

            // Then: cleared again
            assertThat(guard.shouldDrop(event)).isFalse()
        }

        @Test
        fun `should drop events from its own worker threads`() {
            // What is to be tested? Whether the guard recognizes the
            //   library's own worker threads by their fixed names - the
            //   send dispatcher of every topic class and the fallback
            //   dispatcher - without any mark on the worker. The Kafka
            //   client logs synchronously on the send caller (the worker)
            //   in its failure path, and a fallback appender may log from
            //   doAppend on the fallback worker.
            // How will the test case be deemed successful and why? Successful
            //   if events named after every send worker and the fallback
            //   worker are dropped, while a similar but foreign name is not.
            // Why is it important to test this test case? Without the
            //   worker names in the set, an outage with org.apache.kafka at
            //   DEBUG would feed the client's own send failures back into
            //   the queue from the worker - the loop that used to need a
            //   ThreadLocal mark on every worker.

            // Given
            val guard = guard(clientId)

            // When / Then: every worker name is dropped ...
            TopicClass.entries.forEach { topicClass ->
                assertThat(guard.shouldDrop(eventFrom(SendDispatcher.threadNameFor(topicClass))))
                    .describedAs("send worker of %s", topicClass)
                    .isTrue()
            }
            assertThat(guard.shouldDrop(eventFrom(FallbackDispatcher.THREAD_NAME))).isTrue()
            // ... a look-alike is not
            assertThat(guard.shouldDrop(eventFrom("kafka-appender-send-dispatcher-unknown"))).isFalse()
            assertThat(guard.shouldDrop(eventFrom("kafka-appender-fallback-dispatcher-2"))).isFalse()
        }
    }

    @Nested
    inner class `Cross-instance echoes` {
        @Test
        fun `should drop an event from the producer thread of another live instance`() {
            // What is to be tested? Whether the guard recognizes the
            //   producer logging of ANOTHER appender instance in the same
            //   JVM: the registry of live producer thread names is
            //   process-wide, not per instance.
            // How will the test case be deemed successful and why? Successful
            //   if a guard built for client id A drops an event from the
            //   network thread of client id B while a guard for B exists,
            //   and vice versa. Before the fix each guard knew only its
            //   own ids (docs/assessment/DEFECT_ANALYSIS-2026-09-15T22-05-50.md,
            //   M-3).
            // Why is it important to test this test case? Two appenders on
            //   one logger shipped each other's producer logging through
            //   their own producers - a feedback loop that amplified
            //   exactly during broker trouble, when the degraded path must
            //   stay bounded.

            // Given: two live guards with distinct client ids
            val guardA = guard("tabellarium-a-audit")
            val guardB = guard("tabellarium-b-audit")

            // When / Then: each drops the other's producer echo
            assertThat(guardA.shouldDrop(eventFrom(networkThreadOf("tabellarium-b-audit")))).isTrue()
            assertThat(guardB.shouldDrop(eventFrom(networkThreadOf("tabellarium-a-audit")))).isTrue()
            // And: a client id no live instance owns is still delivered
            assertThat(guardA.shouldDrop(eventFrom(networkThreadOf("application-producer")))).isFalse()
        }

        @Test
        fun `should stop dropping another instance's producer thread once that instance closed`() {
            // What is to be tested? Whether close() leaves the process-wide
            //   registry: after an instance closed, its producer thread
            //   names are no longer anyone's echo.
            // How will the test case be deemed successful and why? Successful
            //   if guard A drops B's producer thread while B is live and
            //   delivers it after B closed, while A's own producer thread
            //   is still dropped. This is the leave half of the
            //   enter-at-start/leave-at-stop contract.
            // Why is it important to test this test case? A registry that
            //   only grows would keep dropping events from a thread name
            //   an application producer might legitimately reuse after a
            //   Logback reconfiguration - silent loss disguised as loop
            //   prevention.

            // Given
            val guardA = guard("tabellarium-a-audit")
            val guardB = guard("tabellarium-b-audit")
            assertThat(guardA.shouldDrop(eventFrom(networkThreadOf("tabellarium-b-audit")))).isTrue()

            // When: B ends its life (twice - close is idempotent)
            guardB.close()
            guardB.close()

            // Then: B's producer echo is delivered again, A's own still dropped
            assertThat(guardA.shouldDrop(eventFrom(networkThreadOf("tabellarium-b-audit")))).isFalse()
            assertThat(guardA.shouldDrop(eventFrom(networkThreadOf("tabellarium-a-audit")))).isTrue()
        }

        @Test
        fun `should keep a shared client id registered until the last instance holding it closed`() {
            // What is to be tested? Whether the registry counts instances
            //   per name: two appenders whose operator gave them the same
            //   client.id (or the fixed worker names every instance
            //   shares) must keep dropping the echo until BOTH closed.
            // How will the test case be deemed successful and why? Successful
            //   if after closing the first of two guards with the same
            //   client id the producer thread and the worker thread are
            //   still dropped, and after closing the second they are not.
            // Why is it important to test this test case? A plain set
            //   would lose the name on the first close and open the loop
            //   for the surviving instance - the exact case the shared
            //   worker names would hit on every stop of any instance.

            // Given: two instances sharing one client id
            val first = guard("shared-id")
            val second = guard("shared-id")
            val workerThread = SendDispatcher.threadNameFor(TopicClass.AUDIT)

            // When: the first closes
            first.close()

            // Then: the second still recognizes both echoes
            assertThat(second.shouldDrop(eventFrom(networkThreadOf("shared-id")))).isTrue()
            assertThat(second.shouldDrop(eventFrom(workerThread))).isTrue()

            // When: the last holder closes
            second.close()

            // Then: the shared client id is gone (the worker names are not
            //   asserted here: every appender alive anywhere in the test JVM
            //   holds them too, so their absence is not this test's to claim)
            assertThat(second.shouldDrop(eventFrom(networkThreadOf("shared-id")))).isFalse()
        }
    }
}
