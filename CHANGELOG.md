# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- Jackson 3 (`tools.jackson`) pinned to 3.1.7 via its BOM, above the
  3.1.5 the Spring Boot BOM manages: seven advisories in jackson-core and
  jackson-databind 3.1.5 (GHSA-7hhh-6rmp-j9qf, GHSA-p6pp-m3f8-5c89,
  GHSA-cxp5-3px4-pw24, GHSA-q4xh-88c3-wmh7, GHSA-wv8q-qhhj-9h54,
  GHSA-gx83-3vf8-gh7j, GHSA-wjgm-6hv5-3cvf). No exposure in tabellarium:
  all seven need Jackson to parse or deserialize untrusted JSON, the
  shipped code does not use Jackson, and Jackson 3 arrives only through
  the optional logstash-logback-encoder, so consumers' Jackson version is
  unaffected. The pin fixes the build, its tests and the SBOM the OSV
  scan reads; it is dropped once the Boot BOM manages 3.1.7 or newer.

### Added

- Distribution summary `kafka.appender.event.size` (bytes, per
  `topic.class`): the encoded payload size of every event the class's
  send worker takes up, delivered or diverted by breaker/throttle.
  Recorded on the send worker, never on the logging caller; measured at
  +21 to +31 ns per event on the worker path with metrics bound and no
  allocation (`benchmarks/results/2026-10-10/`). Four series per
  appender; percentiles opt-in via `MeterFilter`. The appender dashboard
  shows average and max per class.
- Info gauges `kafka.appender.info` and `kafka.appender.topic.info`
  (value always `1`, the Prometheus `*_info` convention), registered by
  `bindMeterRegistry()` and removed with the other meters on stop. The
  first carries the fallback appender's name and class (`none` without
  one), the tabellarium and Kafka client versions and the `debug` flag;
  the second has one series per configured topic with its topic class
  and the class's effective `acks` and `linger.ms`. Join them onto the
  other series with `group_left` - e.g. to tell whether
  `events.fallback` lands in a file or is dropped. Cardinality grows by
  one series per appender plus one per configured topic; metrics
  overview, README and configuration guide describe labels and queries.
  Measured cost: +1.2 % series and +1.3 % scrape size for one appender
  with five topics, nothing on the logging path; a Micrometer
  `MeterFilter.deny` switches them off (metrics guide, "Cost").
- Grafana dashboards use the info gauges: the appender dashboard gains
  a "Fallback events by destination" panel (diversions joined with the
  fallback appender; `none` marks dropped events the "Drop rate" panel
  does not count) and a "Configuration" row with appender and topic
  tables; the producer-internals dashboard gains the topic table.

### Changed

- **`bindMeterRegistry` is now an extension function (Java callers and
  binary compatibility):** it moved from a `KafkaAppender` member to a
  top-level extension in `KafkaAppenderMeterRegistry.kt`. Kotlin source
  is unchanged (`appender.bindMeterRegistry(registry)`); Java callers
  write `KafkaAppenderMeterRegistry.bindMeterRegistry(appender, registry)`.
  Code compiled against 1.1.x must be recompiled. It now returns
  whether the appender is bound afterwards (`false` on a stopped
  appender or a refused duplicate meter identity). See "Fixed" for why.

- **Metric names (dashboards and alerts must be updated):** the
  circuit-breaker and Kafka-producer metrics now share the appender's
  prefix, so every meter the appender publishes starts with
  `kafka.appender.`:
  `resilience4j.circuitbreaker.*` → `kafka.appender.circuitbreaker.*`
  (Prometheus `resilience4j_circuitbreaker_*` →
  `kafka_appender_circuitbreaker_*`) and `kafka.producer.*` →
  `kafka.appender.producer.*` (`kafka_producer_*` →
  `kafka_appender_producer_*`), the client's `kafka.app.info.*` →
  `kafka.appender.producer.app.info.*`; meters, types and tags are
  unchanged. The Kafka client's own JMX MBeans keep their names.
  One prefix now selects or switches off all of them, and they no longer
  mix with the application's own breakers and Kafka clients. The breaker
  binder names its meters directly; the Kafka client binder registers
  into a private registry from which a mirror publishes the renamed
  meters - no `MeterFilter` is added to the operator's registry, and
  composite registries (Spring Boot's) see the new names on the
  composite itself. The shipped dashboards, the metrics guide and the
  README use the new names. The single namespace is recorded as a
  product decision, with its costs, in ADR-0005; the mirror handles
  exactly the meter types the Kafka binder registers (gauge, time gauge,
  function counter) and reports any other type once instead of copying
  it generically.

- Internal structure (no behavior change, public API per ADR-0002
  unchanged): the `SelfLoggingGuard` interface and its
  `SelfLoggingGuardFactory` are gone. The one implementation, formerly
  `ClientIdSelfLoggingGuard`, is now the class `SelfLoggingGuard`, which
  `KafkaTransport.open` constructs directly from the producer registry's
  client ids. The interface had been extracted for a second, process-wide
  implementation that was then built into the first one (the JVM-wide
  registry of 1.1.2), leaving a seam that served three test substitutions
  only; the tests observe the guard through its behavior instead - the
  thread names it drops, the client ids it holds in the process-wide
  registry after a failed start.
- `DeliveryOwnership` has no `reset()` any more: an ownership is never
  reused in production, and the method existed for the sender-path
  benchmark alone, which now allocates a fresh ownership per operation
  (`benchmarks/README.md` states the +32 B/op). CONTRIBUTING names the
  rule for test- and benchmark-only seams in production signatures.
- ADR-0002's table of public declarations gains the `bindMeterRegistry`
  extension function (it had been public since the move above without a
  row) and records the `open` hooks of `KafkaAppenderMetricsBinding` as
  the surface's one extension point.

- Reproducible builds: the POM sets `project.build.outputTimestamp`
  (bumped in every release commit), the jar and sources manifests no
  longer carry `Created-By`/`Build-Jdk-Spec` (the build-JDK line was the
  one thing that kept a JDK 25 CI build and a JDK 26 local build of the
  same tag from matching), and the javadoc jar is rendered by Dokka but
  packaged by the jar plugin, because Dokka's `javadocJar` goal writes
  the build time and JDK into the archive. Verified: three builds of one
  commit - twice on JDK 26, once on Temurin 25 - produce byte-identical
  main, sources and javadoc jars. From the next release on, the jar
  attached to the GitHub release (SLSA-attested) and the jar deployed to
  Maven Central are therefore the same bytes; for 1.1.2 and earlier they
  differ in zip timestamps and the `Build-Jdk-Spec` line only. README
  ("Installation", "Reproducible builds") describes it; the Installation
  text no longer claims an SBOM on Maven Central - the CycloneDX SBOM
  and the provenance are GitHub-release assets.

### Fixed

- Logback configuration no longer fails without Micrometer on the
  classpath. `micrometer-core` is optional, but the `KafkaAppender`
  class exposed `MeterRegistry`/`Tag` in a public method signature, and
  Joran's bean introspection resolves those types while configuring the
  appender - a consumer without Micrometer got `NoClassDefFoundError`
  before the appender existed. The Micrometer-typed API now lives
  outside the appender class (see "Changed"); a test re-runs a Joran
  configuration in a class loader that hides Micrometer and Spring.
- A metrics bind that failed after registering some of the appender's
  meters left them behind, and the leftover `events.accepted` counter
  made every retry look like a second, same-named appender, so the bind
  was refused for good. The base meters are now registered as one
  transaction and removed again on failure.
- Removing the circuit-breaker meters on stop or unbind continues past
  a meter whose removal fails, instead of leaving all later ones
  registered.
- An `Error` (linkage, initializer, resource exhaustion) while building
  the producers now closes the producers already created and releases
  the started encoder before it propagates; only `Exception` was
  rolled back before.
- `KafkaAppenderMetricsBinding` no longer logs "Bound KafkaAppender ..."
  for a bind that was refused for a duplicate meter identity.
- The configuration guide's advice to cap `delivery.timeout.ms=10000`
  for AUDIT topics made the appender refuse to start: the Kafka producer
  rejects an explicit `delivery.timeout.ms` below `linger.ms +
  request.timeout.ms` (30 s by default), and the appender withheld that
  cause unless `<debug>` was on. The guide now recommends a consistent
  pair (`request.timeout.ms=5000`, `delivery.timeout.ms=10000`), and the
  start-up validation rejects an inconsistent pair with a named error.
- A metrics re-bind to another registry that was refused (same-named
  appender there) or failed no longer tears down the working binding
  first; a failed re-bind to the same registry stops recording into the
  removed meters.
- Two running appenders with the same `<component>` derive identical
  `client.id`s, which makes their producers' JMX metrics collide; the
  second appender now warns at start-up (the guide no longer claims the
  derived ids never collide).
- A fallback appender that is also attached to a logger ahead of the
  KafkaAppender (e.g. `CONSOLE` on root) is already stopped when a
  Logback context reset reaches the KafkaAppender, so every event its
  teardown diverts is dropped. `stop()` now names this case in a status
  warning, and the configuration guide no longer calls a shared fallback
  safe under full-context shutdown: use a dedicated fallback appender,
  or attach the KafkaAppender ahead of the shared one.
- A failing registration of the info gauges is now rolled back and
  reported as a status warning instead of being swallowed with a partial
  info set left behind.
- `stop()` on a thread without a happens-before edge to `start()` can no
  longer miss the running pipeline and skip the teardown.
- The parallel teardown (send dispatchers, producers) no longer aborts
  the whole shutdown when a closer thread cannot be created (thread
  exhaustion, `OutOfMemoryError: unable to create native thread`): the
  task without a thread runs on the closing thread instead, and the
  closes that follow still happen. The start-up transaction already
  rolled back for the same `Error`; the teardown now matches it.
- The dispatcher skeleton refuses a non-positive drain budget at
  construction. `0` would have turned the bounded worker join into
  `Thread.join(0)` - wait forever - and a negative value would have
  aborted the close before the queued remainder was accounted. No
  production caller passed such a value; the guard pins the contract.

## [1.1.2] - 2026-09-16

Public API (ADR-0002) unchanged. Behavior changes operators should read
before upgrading: the self-logging guard now recognizes the producer
threads of every `KafkaAppender` instance in the JVM, attaching a
fallback appender to a started appender is refused like detaching one,
a second appender with the same name (or both unnamed) bound to the
same `MeterRegistry` is refused as a whole instead of sharing meters,
the two `kafka.appender.fallback.queue.*` gauges exist only with a
fallback appender configured, and `events.dispatched` never counts an
event that also counts as a `shutdown` fallback. Everything else is
defect fixes, tests, internal structure and build.

### Changed

- Internal structure (no behavior change, public API per ADR-0002
  unchanged): the self-logging guard lives behind its own
  `SelfLoggingGuard` interface, whose default implementation
  `ClientIdSelfLoggingGuard` answers one question for every event on
  the hot path - is this the appender's own echo? - for both cases that
  used to be spread over three classes: the network-thread match against the appender's own
  producer client ids (previously on `KafkaTransport`) and the per-thread
  reentry mark that the append path brackets (previously a `ThreadLocal`
  on `KafkaAppender`). `KafkaTransport` builds the guard right after the
  producer registry and exposes it. The dispatcher workers carry no guard
  state any more: their fixed, library-owned thread names
  (`SendDispatcher.threadNameFor`, `FallbackDispatcher.THREAD_NAME`) are
  in the guard's name set next to the producer network threads, which
  also drops the worker echoes of another appender instance in the same
  JVM. The `ThreadLocal` remains only for reentry on application threads
  - the one case a thread name cannot tell. A dedicated unit test pins
  the exact-scheme match, the blank-id exclusion, the worker names and
  the thread locality of the mark; the appender-level and dispatcher
  tests keep exercising the guard through `doAppend` and the workers. The implementation is
  chosen in exactly one place, `SelfLoggingGuardFactory` (an internal
  seam on the appender like `ProducerFactory`, called by the transport
  once the producers exist); that is where the one named second
  implementation, a process-wide client-id registry for the
  cross-instance guards (README, "Future work"), would be plugged in.
  An appender test pins that the configured factory's guard is the one
  the hot path and the workers use.
- `ClientIdSelfLoggingGuard` derives the complete network-thread names
  of its producers once from the client ids and answers the hot-path
  question with a single set lookup, instead of a prefix check, a
  substring and a second lookup per event from a producer thread. Same
  exact-scheme semantics, same tests.
- The two `kafka.appender.fallback.queue.*` gauges are registered
  together and only when a fallback appender is configured. Previously
  `fallback.queue.size` existed unconditionally and read a constant 0
  without a fallback while `fallback.queue.capacity` did not exist -
  a healthy-looking queue where events are dropped by policy, and a
  saturation ratio dividing by an absent series. Without a fallback
  appender both series are now absent; the metrics overview says so.
- Detaching the fallback appender while the appender is started is
  refused with a status warning (`detachAppender`,
  `detachAndStopAllAppenders`): the running pipeline reads the slot
  once at `start()` and kept delivering to the detached - and in the
  second case stopped - appender, loss no counter showed, while the
  `AppenderAttachable` accessors reported "no fallback". One life, one
  slot (ADR-0004); detach before `start()` or after `stop()` as before.
- `stop()` on an instance that never started successfully no longer
  arms the ADR-0004 no-restart latch: nothing was built, so there is no
  first life to end, and a corrected configuration may still start the
  instance instead of being refused with "cannot be started again".
  Logback's state, the attached fallback appender and the encoder are
  still stopped.

- The self-logging guard is process-wide: `ClientIdSelfLoggingGuard`
  enters the network-thread names derived from its producers' `client.id`s
  (and the fixed worker names) into a JVM-wide, reference-counted
  registry when the appender starts, and leaves it through the new
  `SelfLoggingGuard.close`, which `KafkaTransport` calls last in its
  close order - after the producers whose logging it recognizes - and in
  the rollback of a failed `open`. Every instance therefore drops the
  producer logging of every live instance. Previously each guard knew
  only its own client ids, so two appenders attached to the same logger
  shipped each other's Kafka client logging through their own producers
  - a cross-instance feedback loop that amplified exactly during broker
  trouble (`DEFECT_ANALYSIS-2026-09-15T22-05-50`, M-3). The README's
  "Cross-instance guards" now lists only the topic/class exclusivity
  check as future work. Two instances sharing an operator-supplied
  `client.id` keep the entry until the last of them stops.
- `addAppender` on a started appender is refused with a status warning
  naming the refused appender and the wiring the pipeline actually has
  - the mirror image of the refused detach: the transport reads the
  fallback slot once at `start()`, so a late attach only changed the
  public slot while diversions kept being dropped, the fallback queue
  gauges stayed absent and `stop()` stopped an appender that never
  received an event (finding L-1). Joran is unaffected:
  `<appender-ref>` is processed before the enclosing `<appender>` starts.
- `bindMeterRegistry` refuses a binding whose meter identity - the
  `appender` tag plus the common tags - is already live in the registry:
  no appender, breaker or producer meters are registered, one status
  warning names the tag, and `isMeterRegistryBound` stays false. Two
  same-named (or both unnamed) instances on one registry previously got
  the same meter objects from Micrometer, mixed their counts, and the
  first `stop()` removed the series the survivor still published
  through - an outage shown as "no data" (L-2). The former
  breaker-only collision warning is folded into this check; a
  per-instance discriminator tag was rejected because it would change
  the documented inventory for every deployment. README and the metrics
  overview state the unique-name requirement.
- Benchmarks: `SenderPathBenchmark` compiles against the changed
  internal seams (the CI job that compiles the module caught the
  drift) and reuses a pre-built ring of `DeliveryOwnership` objects
  reset per op - `DeliveryOwnership.reset` is an internal,
  benchmark-only instrument - so the per-event ownership production
  allocates on the caller path does not land on the measured worker
  path. Re-measured with `-prof gc` on the same machine and JDK as the
  2026-09-07 baseline: 112 B/op unbound unchanged, +4.5 ns/op for the
  hand-off compare-and-set; raw output under
  `benchmarks/results/2026-09-15/`.

### Fixed

- Start-up validation errors raised by this library itself - blank or
  Kafka-invalid topic names, an unknown `<topicClass>` or
  `<defaultTopicClass>`, a marker mapped twice, one topic under two
  classes, a blank identity field as seen by the enricher, and the
  idempotence-compatibility checks - are reported **with their message**
  again, as README, configuration guide and this changelog promised
  ("aborts `start()` with a named error"). Since 1.1.0 (fix commit
  `1c8b048`) they were routed under the credential-withholding path
  meant for the Kafka client's construction failure, so the operator saw
  `Failed to build KafkaAppender pipeline (java.lang.IllegalArgumentException)`
  and nothing else unless `<debug>` was on. Only a `ProducerFactory`
  failure (the Kafka client composing its error from credential-bearing
  configuration) is still withheld by default; it now names the topic
  class and the cause's type.
- A fallback appender that is attached but not started - its own
  `start()` failed (an unwritable `FileAppender` path), or a foreign
  owner stopped it - is reported with a status warning at `start()`, and
  every event diverted to it is counted as a fallback-dispatcher drop
  (`kafka.appender.fallback.dropped`) instead of as delivered: Logback's
  `doAppend` on a not-started appender returns without appending after
  three warnings, so the loss showed in no counter. The `<debug>`
  diagnostics mark such an appender as `NOT STARTED`.
- `KafkaTransport.open` rolls the producers back for every failure
  after they exist: the self-logging guard factory now runs inside the
  rollback, and the rollback catches `Throwable` (thread exhaustion at
  start-up throws an `OutOfMemoryError` from the dispatcher
  constructors, which the previous `Exception` catch let through with
  the producers left behind).
- A failing Resilience4j or Kafka producer metrics binding is reported
  at WARN with its cause, like every teardown failure in the same
  class, instead of at INFO - an operator filtering the status output
  for warnings never learned that the breaker meters were missing.
- A forced `SendDispatcher` close could divert the in-flight event as a
  `shutdown` remainder after `producer.send` had already accepted the
  record: the record reached Kafka and the fallback, and
  `events.dispatched` and `events.fallback{reason="shutdown"}` both
  counted it (`DEFECT_ANALYSIS-2026-09-15T22-05-50`, M-2). The two-state
  diversion claim is replaced by `DeliveryOwnership` with the explicit
  states pending, handed off and diverted: the sender marks the hand-off
  the moment `producer.send` returns without a synchronous failure,
  every dispatcher rejection - the close's in-flight claim included -
  diverts only a pending event, and only the Kafka callback may divert
  after the hand-off. In the one residual race the state cannot close -
  the close claiming while the client is accepting the record - the
  hand-off fails and the event is counted as the shutdown fallback
  only, never additionally as dispatched. A deterministic dispatcher
  test hands off, then pins the worker across a forced close; the
  metrics overview states the exclusivity.
- With `interceptor.classes` configured, every record gets `RecordHeader`
  wrappers and value arrays of its own instead of the shared pre-built
  header instances. Kafka allows a `ProducerInterceptor` to modify the
  record it receives, and `RecordHeader.value()` exposes the array
  itself, so an interceptor writing into a header value would have
  changed `meta.component`, `meta.cmdbId` and `meta.environment` of
  every later record and of every record still waiting for
  serialization (M-1). The interceptor is the only extension that
  receives the record before serialization - the serializers are forced
  to `ByteArraySerializer` and the partitioner never sees headers - so
  the copies are paid exactly there; without interceptors the shared
  instances (the measured saving of the 2026-08-30 header fix) remain.

## [1.1.1] - 2026-09-09

Public API (ADR-0002) unchanged. Two operator-visible changes: hot-path
failures (`events.fallback{reason="encoder.error"}` and the matching
`events.accepted`) are now attributed to the topic class the event was
routed to instead of always `technical`, and the published POM no longer
declares `spring-boot-starter-parent` as its parent - `lz4-java` appears
as a direct runtime dependency, every resolved version is unchanged.
Everything else is internal structure, tests and build.

### Added

- `ParallelCloseTest` pins the shared parallel-close helper on its own:
  one overall budget for all closers, a closer that overruns it does not
  hold the caller (the `Thread.join(0)` trap), an interrupt of the caller
  ends the wait early and is restored, and an empty task list returns at
  once. Previously covered only through the registry and appender tests.
- `CircuitBreakerMetricsMirrorTest` compares the appender's own
  circuit-breaker binder against `resilience4j-micrometer`'s
  `TaggedCircuitBreakerMetrics` (meter names, types, tags without the
  `appender` tag) on every build; the official binder is a test-scoped
  dependency only. A Resilience4j upgrade that changes the official
  meters now fails the build instead of drifting the mirror.

### Changed

- Build: the POM no longer inherits from `spring-boot-starter-parent`.
  The application parent had leaked into the published POM (the
  CI-friendly flatten mode kept the `<parent>` element, so consumers
  resolved the Boot parent chain to read this library's dependency
  versions). The Spring Boot dependency BOM is now an explicit import
  (preceded by the Kotlin BOM, because an imported BOM no longer honors
  the project's `kotlin.version` override the way the parent did), the
  core Maven plugins are pinned to the versions the parent resolved, and
  the three pieces of plugin configuration it contributed (compiler
  `-parameters`, `@..@` resource delimiters, manifest implementation
  entries) are declared in `pluginManagement`. Flatten runs in `ossrh`
  mode without `dependencyManagement`, so the published POM carries no
  parent and no BOM - only resolved versions; the `lz4-java` security
  pin therefore moved from `dependencyManagement` to a direct `runtime`
  dependency so it still reaches consumers. Verified against the
  previous build: the resolved dependency tree is identical except that
  `lz4-java` now appears as that direct runtime dependency instead of
  under `kafka-clients` (same version, same scope), and the jar manifest
  is byte-identical.
- Internal structure of `KafkaAppender` (no behavior change, public API
  per ADR-0002 unchanged): the appender now composes two halves with one
  concern each. `RecordPlan` turns an event into a record (`route`:
  topic and class from the markers; `materialize`: encoded payload, key
  and headers) - pure, derived from the routing and identity
  configuration plus the encoder, nothing to close. `KafkaTransport`
  carries records (`dispatch`, `divertToFallback`, `isOwnProducerThread`) and owns
  the stateful components (producers, breakers, send queues, fallback
  dispatcher), opened from a `TransportSettings` value and closed as one
  unit, so the reverse ownership close order exists once instead of
  three times (open rollback, `stop()`, parallel dispatcher close). The
  start-up messages (mandatory-override warnings, cleartext-transport
  warning, `<debug>` diagnostics) moved to `StartupDiagnostics` as pure
  functions. `RecordPlan` and `StartupDiagnostics` carry their own unit
  tests; `KafkaTransport` (including the rollback of a failed open) is
  covered through the appender tests.
- The self-logging guard derives the producer network-thread prefix
  from the Kafka client's public `KafkaProducer.NETWORK_THREAD_PREFIX`
  instead of a duplicated literal, and a contract test checks the full
  thread-naming scheme (prefix, separator, client.id) against a real
  producer of the built client version, so a client upgrade that
  changes the scheme fails the build instead of silently disabling the
  guard.
- Metric attribution of hot-path failures: the appender now routes an
  event before it encodes it, so an encoder or enrichment failure is
  counted under the topic class the event was routed to
  (`events.accepted` and `events.fallback{reason="encoder.error"}`).
  Previously encoding ran first and such failures were always counted
  under `technical`, contrary to the documented intent. Failures before
  routing resolved a class (malformed marker input) stay under
  `technical`.

## [1.1.0] - 2026-09-07

Behavior changes operators should read before upgrading: the
`events.dispatched` metric no longer counts sends the Kafka client
rejected synchronously, `start()` after `stop()` is refused (ADR-0004),
and the fallback drain at shutdown uses its full 5 s budget. The public
API (ADR-0002) is unchanged.

### Added

- Releases are published to **Maven Central** via the Sonatype Central
  Portal (`release-central` profile: sources jar, Dokka javadoc jar,
  CycloneDX SBOM, all GPG-signed) and mirrored to the GitHub Packages
  registry (`maven.pkg.github.com/Inqudium/tabellarium`,
  `distributionManagement`); version 1.0.0 was published to both from
  the `v1.0.0` tag state, and the README documents consumption with
  Central as the zero-configuration default.
- Release workflow: on a published GitHub release it rebuilds the jar and
  the SBOM from the tag, uploads them as release assets, and attaches
  Sigstore-signed SLSA build provenance (slsa-github-generator); a
  `workflow_dispatch` variant backfills existing releases.
- The Release workflow additionally mirrors each release into the GitHub
  Packages Maven registry (the existing `distributionManagement` target),
  authenticated with the workflow token — replacing the manual
  `mvn source:jar deploy` mirror step; Maven Central remains the primary,
  deliberately manual release path.
- Daily fuzzing via Jazzer's JUnit integration (`@FuzzTest` classes under
  `src/test/java`, nightly `Fuzz` workflow with `JAZZER_FUZZ=1`): three
  targets assert the invariants of the externally influenced surfaces —
  the `<kafkaProducerProperties>` parser, topic-name validation and marker
  routing, and the MDC-derived partitioning-key bounding — and replay
  their checked-in findings as regression tests in every build.
- `DocumentationContractTest` keeps the operator documentation in step
  with the code: the configuration guide's defaults quick reference
  (queue capacities, drain budgets, producer close timeout, circuit-breaker
  thresholds, probe gap, partitioning-key source) and the metrics
  overview (metric names, tag keys, enum-derived series counts) are
  compared against the constants, so a changed constant fails the build
  until the documentation follows.
- ADR-0004: appender instances are not restartable — `start()` after
  `stop()` is refused with an error naming the ADR; Logback replaces
  appender instances on reconfiguration instead of restarting them, and
  the appender follows that lifecycle.
- Configuration guide section "What is deliberately not configurable",
  listing the behaviors fixed in code (breaker thresholds and probe gap,
  fallback capacity and drain budgets, `max.block.ms` caps, partitioning
  key source, serializers, restart) with the reason for each; linked
  from the README.
- CI compiles the benchmark module against the freshly built library
  (compile only), so the JMH regression instrument cannot rot silently
  when an internal seam it reaches changes shape; the module links
  against the library through its `tabellarium.version` property, which
  CI sets to the version it just built.

### Changed

- `events.dispatched` counts only sends the Kafka client accepted
  without a synchronous failure: an event the client rejected before
  `send()` returned (metadata timeout, buffer exhausted, record too
  large — the kafka-clients 4.x synchronous-callback path) counts as
  `events.fallback{reason="send.error"}` only, never as both.
- One `BoundedWorkerDispatcher` skeleton (bounded queue, single worker,
  in-flight ownership, death handler, two-phase close, reentry mark)
  beneath `SendDispatcher` and `FallbackDispatcher`, which now
  implement only their delivery and their rejection accounting; one
  `closeInParallel` helper replaces the two hand-rolled deadline-join
  loops of the producer registry and the appender.
- The fallback dispatcher's shutdown lets the worker drain by
  delivering for the whole `shutdownTimeoutMs` (5 s) before interrupting,
  followed by a bounded 0.5 s interrupt grace — previously the drain
  was cut off after a 200 ms graceful window and the remaining queue
  dropped although budget remained. Worst case is now 5.5 s.
- An explicit `enable.idempotence=true` on a class without the AUDIT
  mandate suppresses the class's `acks=1` default, and the idempotence
  validation now also requires `acks=all`, with a named error instead of
  the withheld Kafka `ConfigException`.
- The Kafka send callback retains a detached diversion claim instead of
  the whole pending send (payload copy); README documents the heap bound
  under a slow broker (`buffer.memory` measured in event size).
- Metrics are unbound after the dispatcher teardown, so a scrape during
  a multi-second shutdown still sees the shutdown diversions and drops;
  `bindMeterRegistry` and the unbind in `stop()` are serialized.
- `KafkaAppenderMetricsBinding` decides on the appender's own bound
  state instead of an identity set; the Logback-reconfiguration rebind
  gap is documented with `bindAppenders()` as the manual path.
- Documentation and KDoc no longer promise a per-class circuit-breaker
  override via the registry — it is an internal seam (ADR-0002); the
  README links to the guide's defaults table instead of restating the
  breaker numbers.
- Shared test support (`RecordingAppender`, `RecordingProducerFactory`,
  three encoders) replaces the per-class private fixtures; every test
  fixture closes what it starts.
- The per-send callback's synchronous-failure flag is a plain field (the
  race with an asynchronous error callback is benign in both outcomes),
  so the JIT can scalar-replace the callback wherever it does not
  escape; `SenderPathBenchmark` unbound is back at its 2026-08-30
  allocation baseline (112 B/op). The metrics-bound variant reads
  224 B/op in this session for the pre-change code as well (same JVM;
  JIT-profile variance the benchmark report already documents), so it
  carries no regression either.

### Fixed

- An event whose MDC cannot be materialized (a `LoggerContext` without
  an MDC adapter, as in embedded setups) is delivered instead of
  diverting every event as `encoder.error`: `append()` pins an empty
  MDC snapshot on the event and the default partitioning-key extractor
  treats an unreadable MDC as "no key".
- The fallback dispatcher's worker carries the appender's reentry guard,
  so a fallback appender that logs through SLF4J per delivered event no
  longer feeds those events back into the pipeline.
- The appender-level exactly-once test observes a late send failure
  through captured meters and joins the send worker, so a broken
  diversion-claim wiring turns it red.

## [1.0.0] - 2026-08-29

First stable release. The public API is the operator surface
(ADR-0002): `KafkaAppender` with its XML configuration surface,
`TopicMappingConfig`/`TopicMappingEntry`, `TopicClass`, and the
optional `KafkaAppenderMetricsBinding`.

### Added

- Initial public release of the `KafkaAppender`: resilient Logback
  appender shipping structured log events to Apache Kafka.
- Per-topic-class circuit breakers (Resilience4j) with half-open
  throttling.
- Mandatory override policy for compliance-graded topic classes
  (AUDIT, FUNCTIONAL, TECHNICAL, PERFORMANCE).
- Asynchronous fallback appender dispatch via `FallbackDispatcher`.
- Message enrichment with `meta.*` headers and MDC-`traceId`
  partitioning key.
- Opt-in Micrometer metrics with Spring binding helper
  (`KafkaAppenderMetricsBinding`).
- Per-class `client.id` default (`tabellarium-<component>-<topicclass>`)
  so producers are attributable on the broker; an operator-supplied
  `client.id` wins.
- Self-logging guard: events from the appender's own Kafka producer
  network threads are ignored, preventing producer-log feedback loops.
- Marker-based topic routing and per-topic classification via
  `<mapping>` elements in `<topicMapping>` (marker → topic →
  topicClass), activating the four-class compliance model through
  configuration; duplicate markers, conflicting classes, and unknown
  class names are rejected at startup.
- Optional `<defaultTopicClass>` element: classifies the default topic
  (and any unmapped topic) directly, so the default stream can carry a
  compliance grade without a synthetic marker mapping; conflicts with
  a `<mapping>` naming the default topic are rejected at startup.
- `<debug>true</debug>` additionally emits the generated producer
  settings per active class (derived `client.id`, applied default and
  mandatory overrides) as a diff against the operator's base
  properties — operator-supplied values, credentials included, are
  never repeated.
- Joran round-trip tests: the declarative XML surface (including
  `<mapping>` and `<appender-ref>`) is exercised end-to-end through
  `JoranConfigurator`, offline against a real Kafka producer.
- Eager validation of idempotence-incompatible producer tuning
  (`retries=0`, `max.in.flight.requests.per.connection>5`) with a
  clear startup error naming the conflict.
- Full Kafka topic-name validation at startup: reserved names (`.`,
  `..`) and names over 249 characters are rejected eagerly.
- `meta.agent.version` header is derived from the build (Maven-filtered
  resource) instead of a hardcoded constant.
- Testcontainers-based real-broker integration test (tag `integration`,
  `mvn -Pintegration test`): one successful TECHNICAL and one AUDIT
  record end-to-end against an Apache Kafka container — real
  serializers, LZ4 compression, headers, partitioning key, and the
  AUDIT acks/idempotence handshake. The default test run stays offline.
- ADR series under `docs/adr/`; ADR-0001 codifies the comment prefix
  vocabulary (`Rationale:`, `Invariant:`, `Workaround:`, `Safety:`,
  `Compatibility:`, `CAUTION:`), referenced from CONTRIBUTING; ADR-0002
  fixes the public API as the operator surface.
- Standalone JMH benchmark module (`benchmarks/`) as a permanent
  regression asset, with the measured verdicts recorded in
  `docs/assessment/BENCH_REPORT-2026-08-29T11-38-12.md`: the hand-off
  put-lock is retired for realistic loads (measured ceiling ~4-6 M
  events/s per class queue), the metrics envelope is a modest
  +211 B/+120 ns per delivered event, and the shared-header candidate
  saves a measured 160 B/event (kept on file as a tidy-up).
- Dokka runs with `failOnWarning`: an unresolved `[Symbol]` reference in
  KDoc now fails the documentation build (and thereby the Docs
  workflow), so symbolic references cannot drift silently.
- `<includeCallerData>` is documented in the configuration guide and the
  README element table; the Joran round-trip test now binds
  `<sendQueueCapacity>` and `<includeCallerData>` alongside the rest of
  the XML surface.
- CI runs the real-broker integration stage as its own job on every
  push, pull request, and the weekly schedule (`-Pintegration` with
  `-Dgroups=integration`, so only the broker tests run there) — the
  wire-level evidence can no longer rot silently.
- The `external-contract` and `integration` test profiles compose:
  each blanks only its own exclusion property, so
  `mvn -Pexternal-contract,integration test` runs both stages
  (previously the later-declared profile silently dropped the other
  requested tag group).
- The README architecture diagram shows the actual pipeline including
  the per-class `SendDispatcher` workers and the `FallbackDispatcher`
  (it previously described the pre-dispatcher architecture).
- Test-suite visibility: JaCoCo runs with every `verify` and the Docs
  workflow publishes the coverage report plus a self-hosted badge; a
  generated "Test evidence" page lists every test sentence with its
  three-question rationale, grouped by component; CI runs append a
  per-class test summary to the workflow run page; the README gained a
  "How it is tested" section. All visible numbers are generated from
  the build - none are maintained by hand.

### Fixed

- Self-logging guard matches the exact producer network-thread naming
  scheme instead of a substring, so an operator-supplied short
  `client.id` can no longer silently swallow events from unrelated
  application threads.
- `stop()` deregisters all Micrometer meters, closes the per-producer
  `KafkaClientMetrics` binders, and removes the appender's
  circuit-breaker meters; repeated binds replace instead of duplicate.
- `KafkaAppender.stop()` and `FallbackDispatcher.close()` are
  idempotent; remaining queued events are counted as dropped exactly
  once and the queue is drained.
- `HalfOpenThrottle` anchors its "no probe yet" state to the actual
  monotonic clock, removing a theoretical permanent-denial mode on
  platforms with a deeply negative `nanoTime` origin.
- Appender discovery in `KafkaAppenderMetricsBinding` is cycle-safe.
- `kafka.appender.events.fallback` is documented as "diverted from
  Kafka" (counted also when no fallback is configured and the event is
  dropped).
- Documentation drift found by the 2026-08-29 comment audit: the
  `MicrometerKafkaAppenderMetrics` KDoc inventory now lists all nine
  metrics and the correct cardinality (6 `reason` values, ~51 series
  per instance); the README states the actual breaker defaults (10
  half-open probes) and the correct fleet-level series estimate; the
  `registerFallbackQueueGauges` contract describes the real bind-time,
  repeatable call pattern; dangling KDoc references repaired.

### Security

- The partitioning key is bounded at 128 characters; a longer value is
  treated as absent. It originates in the MDC and can therefore be
  attacker-influenced, and an unbounded key inflated records past
  `max.request.size` — whose `RecordTooLargeException` the circuit
  breaker deliberately ignores, so such events flooded the fallback
  appender indefinitely instead of tripping the breaker.
- A startup warning is emitted when a compliance-graded topic class
  (`AUDIT`/`FUNCTIONAL`) is served by a producer configured for
  cleartext transport (`security.protocol` unset or `PLAINTEXT`).
- Pipeline-construction failures report the exception type only; the
  Kafka-authored message and stack trace — built from credential-bearing
  configuration — now require `<debug>true</debug>`.
- `jackson-databind` moved to `test` scope: the shipped code contains no
  Jackson reference, so consumers no longer inherit it on their runtime
  classpath.
- CI hardening: explicit least-privilege `permissions` on the CI
  workflow, and all GitHub Actions pinned to commit SHAs instead of
  mutable tags.
- CodeQL static analysis of the library sources (`java-kotlin`) and of
  the workflow definitions (`actions`), on every change and weekly,
  reporting into the repository's code-scanning view.
- CI now scans the resolved dependency graph against the OSV database
  (CycloneDX SBOM via `cyclonedx-maven-plugin` + OSV-Scanner) on every
  push and pull request and weekly, failing the build on any known
  advisory; the SBOM is retained as a build artifact.
- `lz4-java` pinned to 1.11.2, above the version `kafka-clients`
  resolves: versions up to 1.11.0 can crash the JVM through their JNI
  XXHash range handling (CVE-2026-59949). Not exploitable through this
  appender — the advisory excludes the attacker-controls-contents-only
  case, which is how Kafka's LZ4 codec uses it — but it shipped
  transitively to every consumer.

### Changed

- The Micrometer bind/unbind lifecycle moved from the appender into the
  internal `MetricsBindings` component (behavior unchanged).
- `EnrichedRecord.headers` is `internal`: the shared pre-encoded header
  arrays no longer appear on the public API, shrinking their read-only
  contract to module-internal code.
- Tests tagged `external-contract` are excluded from the default test
  run; run them with `mvn -Pexternal-contract test`.
- Configuration guide, metrics overview, and Grafana dashboards under
  `docs/config/` and `docs/metrics/`.
- Delivery guarantees are documented with their exact scope: the topic
  classes harden the Kafka **producer policy** (`acks`, idempotence),
  while the appender remains a best-effort transport through bounded
  in-memory queues with visible loss. "Audit-grade delivery" wording in
  POM, README, docs site, and KDoc was replaced accordingly, and the
  README gained a dedicated "Delivery guarantees" section.
- The test-only synchronous dispatch modes were removed from production
  code (`SendDispatcher`, `FallbackDispatcher`, and the two internal
  `KafkaAppender` hooks): appender-level tests now always exercise the
  real asynchronous worker path and assert via stop-drain or bounded
  polling.
- The Kotlin all-open/spring compiler plugin was removed from the build;
  the only Spring-proxied classes (`KafkaAppenderMetricsBinding` and
  test fixtures) are explicitly `open`.
- The public API is narrowed to the operator surface (ADR-0002):
  `KafkaAppender`, `TopicMappingConfig`/`TopicMappingEntry`,
  `TopicClass`, and `KafkaAppenderMetricsBinding` remain public;
  the eleven implementation building blocks beneath the appender
  (router, table, registry, factory, properties builder and its result
  types, enricher, enriched record, properties parser, metrics
  interface) are `internal` — they had no public composition path and
  carried an unintended compatibility commitment, including the
  credential-bearing `ProducerRegistry.effectiveProperties` map on a
  public type. Dokka's API reference now shows exactly the supported
  surface.

[Unreleased]: https://github.com/Inqudium/tabellarium/compare/v1.1.2...HEAD
[1.1.2]: https://github.com/Inqudium/tabellarium/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/Inqudium/tabellarium/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/Inqudium/tabellarium/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/Inqudium/tabellarium/releases/tag/v1.0.0
