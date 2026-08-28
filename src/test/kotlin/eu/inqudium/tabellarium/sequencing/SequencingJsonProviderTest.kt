package eu.inqudium.tabellarium.sequencing

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.classic.util.LogbackMDCAdapter
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class SequencingJsonProviderTest {
    // -- Test fixtures --------------------------------------------------

    private lateinit var loggerContext: LoggerContext
    private val json: ObjectMapper = ObjectMapper()

    @BeforeEach
    fun setUp() {
        loggerContext =
            LoggerContext().apply {
                // logback 1.5 reads the MDC adapter from the LoggerContext (LoggingEvent.getMDCPropertyMap),
                // not from the global MDC. A hand-built context has none, so serializing an event through the
                // async/encoder path NPEs. Give it a real adapter, as the SLF4J-bound context gets one.
                setMDCAdapter(LogbackMDCAdapter())
            }
    }

    private fun loggingEvent(message: String = "test message"): LoggingEvent {
        val logger = loggerContext.getLogger("test-logger")
        return LoggingEvent("fqcn.dummy", logger, Level.INFO, message, null, null)
    }

    /**
     * Encode a single event through a fully-configured
     * [SequencingLogstashEncoder] and parse the JSON result.
     */
    private fun encodeAsJson(
        event: ILoggingEvent,
        configure: SequencingLogstashEncoder.() -> Unit = {},
    ): JsonNode {
        val encoder =
            SequencingLogstashEncoder().apply {
                context = loggerContext
                configure()
                start()
            }
        val bytes = encoder.encode(event)
        return json.readTree(bytes)
    }

    // -- Tests ----------------------------------------------------------

    @Nested
    inner class `JSON output shape` {
        @Test
        fun `should emit encoder_sequence as a JSON number so Elasticsearch dynamic mapping picks long`() {
            // What is to be tested? Whether the sequence field appears
            //   in the encoded JSON as a native number (unquoted)
            //   rather than a string. Without a native JSON number,
            //   Elasticsearch's dynamic mapping infers `text` on first
            //   insert, breaking the Kibana loss-detection query.
            // How will the test case be deemed successful and why?
            //   Successful if the parsed JSON node reports its token
            //   type as numeric. Jackson's JsonNode.isNumber/isLong
            //   reflect the actual JSON token type, so this is a
            //   direct check on the wire format.
            // Why is it important to test this test case? A regression
            //   that switched to writeStringField would still produce
            //   valid JSON — with the wrong type. Kibana would fail
            //   silently in production. This test pins the contract.

            // Given
            val event = loggingEvent()

            // When
            val jsonNode = encodeAsJson(event) { instanceId = "test-instance" }

            // Then
            val seqNode = jsonNode.get("log_encoder_sequence")
            assertThat(seqNode).isNotNull
            assertThat(seqNode.isNumber)
                .describedAs("must be a JSON number so ES dynamic mapping picks long")
                .isTrue
            assertThat(seqNode.asLong()).isEqualTo(1L)
        }

        @Test
        fun `should emit encoder_instance as a JSON string`() {
            // Given
            val event = loggingEvent()

            // When
            val jsonNode = encodeAsJson(event) { instanceId = "pod-alpha" }

            // Then
            val instanceNode = jsonNode.get("log_encoder_instance")
            assertThat(instanceNode.isTextual).isTrue
            assertThat(instanceNode.asText()).isEqualTo("pod-alpha")
        }

        @Test
        fun `should preserve the standard LogstashEncoder fields alongside the sequencing fields`() {
            // Given
            val event = loggingEvent(message = "hello world")

            // When
            val jsonNode = encodeAsJson(event) { instanceId = "test-instance" }

            // Then
            assertThat(jsonNode.get("level").asText()).isEqualTo("INFO")
            assertThat(jsonNode.get("message").asText()).isEqualTo("hello world")
            assertThat(jsonNode.get("logger_name").asText()).isEqualTo("test-logger")
            assertThat(jsonNode.get("log_encoder_sequence").asLong()).isEqualTo(1L)
            assertThat(jsonNode.get("log_encoder_instance").asText()).isEqualTo("test-instance")
        }
    }

    @Nested
    inner class `Sequence numbering` {
        @Test
        fun `should assign strictly monotonic sequence numbers starting at 1`() {
            // Given
            val encoder =
                SequencingLogstashEncoder().apply {
                    context = loggerContext
                    instanceId = "test-instance"
                    start()
                }

            // When
            val jsons =
                (1..3).map { i ->
                    json.readTree(encoder.encode(loggingEvent("event-$i")))
                }

            // Then
            assertThat(jsons.map { it.get("log_encoder_sequence").asLong() })
                .containsExactly(1L, 2L, 3L)
        }

        @Test
        fun `should assign a distinct sequence stream per encoder instance`() {
            // Given
            val a =
                SequencingLogstashEncoder().apply {
                    context = loggerContext
                    instanceId = "a"
                    start()
                }
            val b =
                SequencingLogstashEncoder().apply {
                    context = loggerContext
                    instanceId = "b"
                    start()
                }

            // When
            val aFirst = json.readTree(a.encode(loggingEvent()))
            val bFirst = json.readTree(b.encode(loggingEvent()))
            val aSecond = json.readTree(a.encode(loggingEvent()))

            // Then
            assertThat(aFirst.get("log_encoder_sequence").asLong()).isEqualTo(1L)
            assertThat(aSecond.get("log_encoder_sequence").asLong()).isEqualTo(2L)
            assertThat(bFirst.get("log_encoder_sequence").asLong()).isEqualTo(1L)
        }
    }

    @Nested
    inner class `Two-point measurement` {
        @Test
        fun `should emit both async and encoder sequences independently as JSON numbers when both wrappers are active`() {
            // What is to be tested? Whether the two counters — the
            //   AsyncAppender wrapper's KeyValuePair-based sequence
            //   and the encoder provider's own sequence — coexist in
            //   the same JSON output, both as native numbers, with
            //   independent values. This is the whole two-point
            //   architecture in one assertion.
            // How will the test case be deemed successful and why?
            //   Successful if the encoded JSON has two distinct
            //   numeric fields, both readable via jsonNode.isNumber,
            //   with values matching each counter independently. A
            //   regression where the two accidentally share state
            //   (via MDC or otherwise) would produce equal or
            //   correlated values, and the assertion would catch it.
            // Why is it important to test this test case? The
            //   two-point measurement idea is the whole point of
            //   architecture change. Without a test that exercises
            //   both in the same event, we cannot claim the design
            //   works end-to-end.

            // Given: an AsyncAppender wrapper stamps sequence 1
            val asyncAppender =
                SequencingAsyncAppender().apply {
                    context = loggerContext
                    instanceId = "async-instance"
                    start()
                }
            val event = loggingEvent()
            asyncAppender.preprocess(event) // adds log.async.sequence=1 as KeyValuePair

            // And: an encoder with its own independent counter
            val encoder =
                SequencingLogstashEncoder().apply {
                    context = loggerContext
                    instanceId = "encoder-instance"
                    start()
                }

            // When
            val bytes = encoder.encode(event)
            val jsonNode = json.readTree(bytes)

            // Then: both sequences present, both as JSON numbers, both = 1
            //   (each is the first event through its own counter)
            val asyncSeq = jsonNode.get("log_async_sequence")
            val encoderSeq = jsonNode.get("log_encoder_sequence")
            assertThat(asyncSeq.isNumber).describedAs("async seq must be JSON number").isTrue
            assertThat(encoderSeq.isNumber).describedAs("encoder seq must be JSON number").isTrue
            assertThat(asyncSeq.asLong()).isEqualTo(1L)
            assertThat(encoderSeq.asLong()).isEqualTo(1L)

            // And: the instances are distinct — proving they came from
            //   different counters
            assertThat(jsonNode.get("log_async_instance").asText()).isEqualTo("async-instance")
            assertThat(jsonNode.get("log_encoder_instance").asText()).isEqualTo("encoder-instance")

            asyncAppender.stop()
        }
    }

    @Nested
    inner class `Concurrency` {
        @Test
        fun `should produce contiguous non-overlapping sequences under concurrent encoding`() {
            // Given
            val encoder =
                SequencingLogstashEncoder().apply {
                    context = loggerContext
                    instanceId = "concurrency"
                    start()
                }
            val threadCount = 16
            val eventsPerThread = 100
            val totalEvents = threadCount * eventsPerThread
            val start = CountDownLatch(1)
            val done = CountDownLatch(threadCount)
            val pool = Executors.newFixedThreadPool(threadCount)
            val collected = ConcurrentLinkedQueue<Long>()

            // When
            try {
                repeat(threadCount) {
                    pool.submit {
                        start.await()
                        repeat(eventsPerThread) {
                            val node = json.readTree(encoder.encode(loggingEvent()))
                            collected += node.get("log_encoder_sequence").asLong()
                        }
                        done.countDown()
                    }
                }
                start.countDown()
                assertThat(done.await(10, TimeUnit.SECONDS)).isTrue
            } finally {
                pool.shutdownNow()
            }

            // Then
            val sequences = collected.toList()
            assertThat(sequences).hasSize(totalEvents)
            assertThat(sequences.toSortedSet())
                .describedAs("sequence values should form a dense set 1..N")
                .isEqualTo((1L..totalEvents.toLong()).toSortedSet())
        }
    }
}
