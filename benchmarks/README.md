# tabellarium-benchmarks

JMH verification benchmarks for the performance-analysis reports under
`../docs/assessment/`. This module is a **permanent regression asset**:
it exists so that the findings verified in
`BENCH_REPORT-2026-08-29T11-38-12.md` can be re-measured after any
optimization or dependency bump. It is a standalone Maven module and is
deliberately not part of the library's build.

## Build & run

```bash
# 1. install the library snapshot the benchmarks link against
mvn -DskipTests install

# 2. build the shaded benchmark jar
mvn -f benchmarks/pom.xml package

# 3. run (examples; add -prof gc for allocation, -bm sample for percentiles)
java -jar benchmarks/target/benchmarks.jar RecordHeadersBenchmark -prof gc
java -jar benchmarks/target/benchmarks.jar SenderPathBenchmark -prof gc
java -jar benchmarks/target/benchmarks.jar HandoffBenchmark -tg 1,8       # 1 consumer, 8 producers
java -jar benchmarks/target/benchmarks.jar AppendPipelineBenchmark -bm sample -t 32
```

All benchmarks use 3 forks, 5 warmup and 5 measurement iterations by
default (annotation-driven). Raw outputs of the 2026-08-29 verification
session live under `results/2026-08-29/`.

## Inventory

| Benchmark | Verifies | What it measures |
|---|---|---|
| `RecordHeadersBenchmark` | PERF_ANALYSIS-2026-08-29T11-01-08 finding 2 | Per-record header construction: production shape (5 × `headers().add`) vs. one shared pre-built header list. Primary metric: `gc.alloc.rate.norm`. |
| `SenderPathBenchmark` | finding 3 | The delivered-path worker side (`ResilientMessageSender.send` incl. breaker, record build, instant-success callback) with `metricsBound=false/true`; the param delta is the observability envelope per delivered event. |
| `HandoffBenchmark` | finding 1 | The caller-side hand-off primitive: N producers `offer` into a bounded `LinkedBlockingQueue` while one batch-draining consumer keeps it near-empty (worst-case put-lock contention; `rejected` aux counter proves the regime). Thread split via `-tg 1,<producers>`. |
| `AppendPipelineBenchmark` | secondary evidence (findings 1/3), caller-side allocation | The full production `doAppend` path open-loop against a `DiscardingProducer`. Under open load it saturates the worker and measures the shedding regime — the teardown prints the achieved path mix (delivered vs. diverted), which is part of the evidence, not a hidden variable. |

Support: `DiscardingProducer` — a `Producer` stand-in that completes
every send instantly (success callback runs, nothing is retained), so
the appender pipeline is measured rather than the Kafka client.

## Conventions

Sources are Java (JMH annotation processing is first-class under
javac); the library's internal seams are reached via their stable
mangled JVM names (Kotlin `internal` compiles to public bytecode with a
`$tabellarium` suffix) — no production code is modified for
benchmarking. Repository conventions (English, injected time sources,
no mock libraries) apply.
