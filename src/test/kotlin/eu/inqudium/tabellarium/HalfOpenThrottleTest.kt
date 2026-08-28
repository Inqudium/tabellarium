package eu.inqudium.tabellarium

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class HalfOpenThrottleTest {
    // -- Test fixtures --------------------------------------------------

    /**
     * Deterministic monotonic time source backed by an AtomicLong so
     * tests can advance time by arbitrary increments. Always returns
     * the current value in nanoseconds.
     */
    private class FakeNanoClock(
        initialNanos: Long = 0L,
    ) {
        private val nanos = AtomicLong(initialNanos)

        fun now(): Long = nanos.get()

        fun advance(duration: Duration) {
            nanos.addAndGet(duration.toNanos())
        }
    }

    /**
     * Builds a circuit breaker whose state we can drive manually via
     * [CircuitBreaker.transitionToOpenState] etc. - much cleaner than
     * waiting for the breaker to transition based on success/failure
     * counters.
     */
    private fun newBreaker(name: String = "test"): CircuitBreaker {
        // A fresh breaker starts in CLOSED and accepts
        // transitionToOpenState directly - no recorded calls needed.
        return CircuitBreaker.of(
            name,
            CircuitBreakerConfig
                .custom()
                .slidingWindowSize(10)
                .minimumNumberOfCalls(10)
                .permittedNumberOfCallsInHalfOpenState(10)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .build(),
        )
    }

    private fun newThrottle(
        breaker: CircuitBreaker = newBreaker(),
        gap: Duration = Duration.ofMillis(5),
        clock: FakeNanoClock = FakeNanoClock(),
    ): Pair<HalfOpenThrottle, FakeNanoClock> = HalfOpenThrottle(breaker, gap, clock::now) to clock

    // -- Tests ----------------------------------------------------------

    @Nested
    inner class `State-based behavior` {
        @Test
        fun `should always allow probes when the breaker is CLOSED`() {
            // What is to be tested? Whether the throttle is transparent
            //   for normal traffic (CLOSED state). Throttling normal
            //   traffic would be a regression - it would silently rate-
            //   limit production logging.
            // How will the test case be deemed successful and why? Successful
            //   if a hundred sequential calls all return true, with the
            //   clock not advancing. The throttle must not gate CLOSED
            //   traffic at all.
            // Why is it important to test this test case? The throttle's
            //   value depends on being a no-op outside HALF_OPEN; a
            //   regression here would silently reduce logging throughput
            //   in production.

            // Given: a breaker in CLOSED state (default after construction)
            val (throttle, _) = newThrottle()

            // When / Then
            repeat(100) {
                assertThat(throttle.mayAttemptProbe()).isTrue()
            }
        }

        @Test
        fun `should always allow probes when the breaker is OPEN`() {
            // Given
            val breaker = newBreaker()
            breaker.transitionToOpenState()
            val (throttle, _) = newThrottle(breaker = breaker)

            // When / Then: throttle does not add gating; the breaker
            //   will deny permission downstream anyway.
            repeat(100) {
                assertThat(throttle.mayAttemptProbe()).isTrue()
            }
        }

        @Test
        fun `should gate probes when the breaker is HALF_OPEN`() {
            // Given
            val breaker = newBreaker()
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()
            val (throttle, _) = newThrottle(breaker = breaker)

            // When: first probe within the gap - should pass
            assertThat(throttle.mayAttemptProbe()).isTrue()

            // Then: subsequent probes within the gap - should be gated
            repeat(5) {
                assertThat(throttle.mayAttemptProbe()).isFalse()
            }
        }
    }

    @Nested
    inner class `Gap timing` {
        @Test
        fun `should allow a new probe after the gap has elapsed`() {
            // What is to be tested? Whether the throttle correctly
            //   admits a second probe once minProbeGap has elapsed
            //   since the first. This is the "spread the probes over
            //   time" core property.
            // How will the test case be deemed successful and why? Successful
            //   if probe N+1 is denied just before the gap elapses and
            //   admitted just after. Pins the time threshold precisely
            //   using the injected clock.
            // Why is it important to test this test case? Without this
            //   guarantee, the throttle would either over-restrict (no
            //   probes ever reach the cluster again) or under-restrict
            //   (the gap is ignored).

            // Given
            val breaker = newBreaker()
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()
            val gap = Duration.ofMillis(5)
            val (throttle, clock) = newThrottle(breaker = breaker, gap = gap)

            // When: first probe at t=0
            assertThat(throttle.mayAttemptProbe()).isTrue()

            // And: clock advances to just before the gap elapses
            clock.advance(gap.minusNanos(1))

            // Then: second probe is still denied
            assertThat(throttle.mayAttemptProbe()).isFalse()

            // When: clock advances past the gap
            clock.advance(Duration.ofNanos(1))

            // Then: second probe is admitted
            assertThat(throttle.mayAttemptProbe()).isTrue()
        }

        @Test
        fun `should spread ten probes evenly when called at maximum rate`() {
            // What is to be tested? The intended use case: many
            //   incoming events at high rate, throttle spaces probes
            //   over time so the breaker can make its decision while
            //   probes are still being dispatched.
            // How will the test case be deemed successful and why? Successful
            //   if exactly 10 probes are admitted across 50ms of
            //   simulated time with a 5ms gap - one probe per 5ms slot.
            //   This is the actual contract.
            // Why is it important to test this test case? Pins down the
            //   real-world behavior under load, which is the entire
            //   motivation for the class.

            // Given: HALF_OPEN breaker with 5ms gap
            val breaker = newBreaker()
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()
            val gap = Duration.ofMillis(5)
            val (throttle, clock) = newThrottle(breaker = breaker, gap = gap)

            // When: 10 probe attempts every 5ms over 50ms
            var admitted = 0
            repeat(10) {
                if (throttle.mayAttemptProbe()) admitted++
                clock.advance(gap)
            }

            // Then: every attempt was admitted because each came
            //   exactly one gap after the previous one
            assertThat(admitted).isEqualTo(10)
        }

        @Test
        fun `should admit only one probe per gap window under high event rate`() {
            // What is to be tested? Whether the throttle correctly
            //   gates when events arrive faster than the gap allows.
            // How will the test case be deemed successful and why? Successful
            //   if calling mayAttemptProbe 100 times within one gap
            //   window yields exactly one admission. Pins the
            //   "one probe per slot" behavior.
            // Why is it important to test this test case? Without this,
            //   a busy-loop of failing sends could exhaust the breaker's
            //   permittedNumberOfCallsInHalfOpenState in microseconds -
            //   the exact scenario the throttle exists to prevent.

            // Given
            val breaker = newBreaker()
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()
            val (throttle, _) = newThrottle(breaker = breaker, gap = Duration.ofMillis(5))

            // When: 100 rapid-fire calls
            val admitted = (1..100).count { throttle.mayAttemptProbe() }

            // Then: only the first wins
            assertThat(admitted).isEqualTo(1)
        }
    }

    @Nested
    inner class `Concurrency` {
        @Test
        fun `should admit at most one probe per gap when threads race`() {
            // What is to be tested? Whether the CAS-based slot
            //   acquisition correctly serializes concurrent callers
            //   in HALF_OPEN state.
            // How will the test case be deemed successful and why? Successful
            //   if N threads racing on mayAttemptProbe() at simulated
            //   t=0 yield exactly 1 admission. The contract is "at most
            //   one probe per gap window" - concurrent callers must not
            //   sneak through.
            // Why is it important to test this test case? The throttle
            //   sits in the hot path of every log event of a
            //   high-volume service. A race that admits 2 or 3 probes
            //   per gap would mean the throttle silently loses its
            //   guarantee under load - the precise condition (high
            //   load) where it matters most.

            // Given
            val breaker = newBreaker()
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()
            val (throttle, _) = newThrottle(breaker = breaker, gap = Duration.ofMillis(5))

            // When: 16 threads race
            val threadCount = 16
            val admitted = AtomicInteger(0)
            val startLatch = CountDownLatch(1)
            val executor = Executors.newFixedThreadPool(threadCount)
            try {
                val futures =
                    (1..threadCount).map {
                        executor.submit {
                            startLatch.await()
                            if (throttle.mayAttemptProbe()) {
                                admitted.incrementAndGet()
                            }
                        }
                    }
                // Release all threads at once
                startLatch.countDown()
                // Resolving every Future is both the bounded completion
                // wait and the failure propagation: an exception or
                // assertion error in any worker rethrows here instead of
                // vanishing inside the executor.
                futures.forEach { it.get(5, TimeUnit.SECONDS) }
            } finally {
                executor.shutdownNow()
            }

            // Then: only one thread won the slot
            assertThat(admitted.get()).isEqualTo(1)
        }
    }

    @Nested
    inner class `Edge cases` {
        @Test
        fun `should reject a negative gap at construction time`() {
            // Given / When / Then
            assertThatThrownBy {
                HalfOpenThrottle(newBreaker(), Duration.ofMillis(-1))
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("non-negative")
        }

        @Test
        fun `should allow the first probe even when the monotonic clock is deeply negative`() {
            // What is to be tested? Whether the "no probe yet" state is
            //   anchored to the actual clock instead of a fixed far-past
            //   sentinel. System.nanoTime has an arbitrary origin and may
            //   itself be deeply negative; with the old sentinel
            //   (Long.MIN_VALUE / 2), a clock below the sentinel made
            //   `now - last` negative and denied every probe forever -
            //   permanently locking the breaker out of recovery.
            // How will the test case be deemed successful and why? Successful
            //   if a HALF_OPEN throttle whose clock starts near
            //   Long.MIN_VALUE admits its first probe. This pins the
            //   clock-anchored initialization.
            // Why is it important to test this test case? The failure mode
            //   is a total, permanent logging outage after the first
            //   breaker trip - invisible in any test using a small clock.

            // Given: a clock starting near the bottom of the long range
            val breaker = newBreaker()
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()
            val deepNegativeClock = FakeNanoClock(initialNanos = Long.MIN_VALUE + 1_000_000)
            val (throttle, _) =
                newThrottle(breaker = breaker, gap = Duration.ofMillis(5), clock = deepNegativeClock)

            // When / Then: the first probe is admitted
            assertThat(throttle.mayAttemptProbe()).isTrue()
        }

        @Test
        fun `should disable throttling when gap is zero`() {
            // What is to be tested? Whether gap=0 is an explicit
            //   "disable" sentinel, allowing operators to switch off
            //   the throttle without removing it from the call site.
            // How will the test case be deemed successful and why? Successful
            //   if a HALF_OPEN breaker with gap=0 admits every call.
            //   This makes the throttle opt-in by configuration.
            // Why is it important to test this test case? An operator
            //   who finds the throttle interferes with their workload
            //   needs a clean off-switch; "gap=0 = disabled" is the
            //   simplest possible UX.

            // Given
            val breaker = newBreaker()
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()
            val (throttle, _) = newThrottle(breaker = breaker, gap = Duration.ZERO)

            // When / Then
            repeat(100) {
                assertThat(throttle.mayAttemptProbe()).isTrue()
            }
        }
    }
}
