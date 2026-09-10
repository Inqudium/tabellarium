package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class ClientIdSelfLoggingGuardTest {
    // -- Test fixtures --------------------------------------------------

    private val clientId = "tabellarium-payments-audit"

    private fun guard(vararg clientIds: String) = ClientIdSelfLoggingGuard(clientIds.toSet())

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
            assertThat(guard.isOwnProducerThread(ClientIdSelfLoggingGuard.PRODUCER_NETWORK_THREAD_PREFIX + clientId)).isTrue()
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
        fun `should keep a thread marked for life`() {
            // What is to be tested? Whether markCurrentThreadForLife sets
            //   the mark without a matching exit - the contract the
            //   dispatcher workers rely on, which mark themselves once and
            //   never log legitimately through the appender.
            // How will the test case be deemed successful and why? Successful
            //   if, on a thread that marked itself, shouldDrop stays true
            //   across repeated calls.
            // Why is it important to test this test case? The Kafka client
            //   logs synchronously on the send caller - the worker - in its
            //   failure path; a mark that expired after one call would let
            //   the second such event back into the queue during an outage.

            // Given
            val guard = guard(clientId)
            val results = AtomicReference<List<Boolean>>()
            val done = CountDownLatch(1)

            // When: a worker marks itself and then "logs" twice
            Thread {
                guard.markCurrentThreadForLife()
                results.set(listOf(guard.shouldDrop(eventFrom("worker")), guard.shouldDrop(eventFrom("worker"))))
                done.countDown()
            }.start()

            // Then
            assertThat(done.await(2, TimeUnit.SECONDS)).isTrue()
            assertThat(results.get()).containsExactly(true, true)
            // ... and the test thread is unaffected
            assertThat(guard.shouldDrop(eventFrom("test"))).isFalse()
        }
    }
}
