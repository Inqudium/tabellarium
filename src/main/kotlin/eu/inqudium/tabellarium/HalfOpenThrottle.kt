package eu.inqudium.tabellarium

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

/**
 * Gating primitive that spreads circuit-breaker probe calls over time
 * during the HALF_OPEN state.
 *
 * ## Why this exists
 *
 * Resilience4j's HALF_OPEN state admits the first N events as probes,
 * back-to-back. At high logging volume (many events per millisecond)
 * all N probes are dispatched to Kafka within a sub-millisecond
 * window, then for the duration of the Kafka round-trip
 * (typically 10-50 ms with a healthy cluster) all further events are
 * denied permission and routed to the fallback - even though the
 * cluster is in fact recovered.
 *
 * This throttle spaces the probes out: in HALF_OPEN state, only one
 * probe is allowed every [minProbeGap]. Subsequent events in the same
 * window bypass the breaker entirely and go to the fallback. The result
 * is that the N probes are dispatched over `N * minProbeGap` of wall
 * time, giving the cluster's response a chance to arrive (and the
 * breaker a chance to transition back to CLOSED) before the next probe
 * is needed.
 *
 * ## State semantics
 *
 * - **CLOSED:** [mayAttemptProbe] always returns true. The throttle is
 *   transparent for normal traffic - only HALF_OPEN is rate-limited.
 * - **OPEN:** also returns true. The underlying breaker will deny
 *   permission anyway; we don't add extra gating here.
 * - **HALF_OPEN:** returns true if at least [minProbeGap] has elapsed
 *   since the last probe permission was granted; otherwise false.
 *
 * ## Concurrency
 *
 * The throttle uses an [AtomicLong] for the timestamp of the last
 * permitted probe, updated with [AtomicLong.compareAndSet]. Two
 * threads racing to be "the next probe" both check the timestamp;
 * only one wins the CAS. The loser sees `mayAttemptProbe() == false`
 * and routes to the fallback. There is no lock, no blocking.
 *
 * ## Time source
 *
 * [nanoTimeSource] is injectable so unit tests can drive the clock
 * deterministically. The default is [System.nanoTime] which is the
 * correct monotonic source for measuring elapsed time on the JVM.
 *
 * @param circuitBreaker The underlying breaker; only its
 *                       [CircuitBreaker.getState] is read.
 * @param minProbeGap Minimum time between two probe permissions while
 *                    in HALF_OPEN state. Must be non-negative; a value
 *                    of zero disables the throttle (every call returns
 *                    true).
 * @param nanoTimeSource Monotonic time source in nanoseconds. Default
 *                      uses [System.nanoTime]. Tests inject a
 *                      deterministic source.
 */
internal class HalfOpenThrottle(
    private val circuitBreaker: CircuitBreaker,
    minProbeGap: Duration,
    private val nanoTimeSource: () -> Long = System::nanoTime,
) {
    private val minProbeGapNanos: Long = minProbeGap.toNanos()

    /**
     * Timestamp of the last permitted probe in nanoseconds (monotonic).
     * Initialized to construction-time-minus-one-gap so the very first
     * probe is always allowed. Deliberately NOT a fixed far-past
     * sentinel: `nanoTime` has an arbitrary origin and may itself be
     * deeply negative, and a sentinel below that origin would make
     * `now - last` negative - permanently denying every probe and
     * locking the breaker out of recovery. Anchoring to the actual
     * clock keeps the arithmetic valid for any origin (the only
     * remaining wrap case is a clock value within one gap of
     * Long.MIN_VALUE, which no JVM produces in practice).
     */
    private val lastProbeNanos: AtomicLong = AtomicLong(nanoTimeSource() - minProbeGapNanos)

    init {
        require(!minProbeGap.isNegative) {
            "minProbeGap must be non-negative, got $minProbeGap"
        }
    }

    /**
     * Returns true if the caller may attempt to acquire a permission
     * on the underlying breaker. In CLOSED or OPEN state this is
     * always true. In HALF_OPEN state, it is true only if the previous
     * probe was at least [minProbeGap] ago - and the caller wins the
     * CAS for the new probe timestamp.
     */
    fun mayAttemptProbe(): Boolean {
        if (minProbeGapNanos == 0L) {
            // Throttle disabled - fast path.
            return true
        }
        if (circuitBreaker.state != CircuitBreaker.State.HALF_OPEN) {
            // CLOSED or OPEN - throttle is transparent.
            return true
        }
        val now = nanoTimeSource()
        val last = lastProbeNanos.get()
        if (now - last < minProbeGapNanos) {
            // Too soon since the previous probe.
            return false
        }
        // Try to claim the probe slot. If a concurrent caller beats us,
        // we lose and route to the fallback.
        return lastProbeNanos.compareAndSet(last, now)
    }
}
