package eu.inqudium.tabellarium.bench;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Verifies finding 1 of PERF_ANALYSIS-2026-08-29T11-01-08: the
 * caller-side hand-off is a {@link LinkedBlockingQueue} put-lock, and
 * the open question is its behaviour under high caller fan-in.
 *
 * <p><b>Why this instrument (redesign, documented):</b> the first
 * attempt measured {@code doAppend} open-loop through the full
 * appender; the path-mix counters showed that a saturating caller
 * fills the queue and {@code offer} then bails out on the count check
 * WITHOUT touching the lock - the benchmark had silently switched to
 * measuring the overflow path. Production at the assumed load runs the
 * opposite regime: a near-empty queue whose consumer keeps up, with
 * offers contending only among themselves. This benchmark reproduces
 * exactly that regime as a worst case: an asymmetric group where one
 * consumer drains in batches ({@code drainTo}, one take-lock
 * acquisition per up-to-4096 elements, so it always outpaces the
 * producers) while N producers hammer {@code offer}. The producer
 * score is therefore the offer cost INCLUDING put-lock contention at
 * N callers - an upper bound for any real event rate, because real
 * callers do microseconds of encoding between offers.
 *
 * <p>Thread distribution is set per run via {@code -tg <producers>,1}.
 * The {@code rejected} aux counter proves the measured regime: it must
 * stay at (or very near) zero, or the queue was full and the run is
 * invalid for this finding.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
public class HandoffBenchmark {

    /** Stands in for one PendingSend hand-off unit; identity is irrelevant to the lock. */
    private static final Object ITEM = new Object();

    @State(Scope.Group)
    public static class Shared {
        final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>(1 << 16);
    }

    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class Rejections {
        public long rejected;
    }

    @State(Scope.Thread)
    public static class Sink {
        final ArrayList<Object> drained = new ArrayList<>(4096);
    }

    @Benchmark
    @Group("handoff")
    @GroupThreads(1)
    public void producer(Shared shared, Rejections rejections) {
        if (!shared.queue.offer(ITEM)) {
            rejections.rejected++;
        }
    }

    @Benchmark
    @Group("handoff")
    @GroupThreads(1)
    public void consumer(Shared shared, Sink sink, Blackhole blackhole) {
        int drained = shared.queue.drainTo(sink.drained, 4096);
        blackhole.consume(drained);
        sink.drained.clear();
        if (drained == 0) {
            Thread.onSpinWait();
        }
    }
}
