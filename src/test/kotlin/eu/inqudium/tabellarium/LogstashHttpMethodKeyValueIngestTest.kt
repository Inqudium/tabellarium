package eu.inqudium.tabellarium

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.OutputStreamAppender
import ch.qos.logback.core.status.Status
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import net.logstash.logback.encoder.LogstashEncoder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.ResourceLock
import org.slf4j.LoggerFactory
import org.springframework.http.HttpMethod
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import ch.qos.logback.classic.Logger as LogbackLogger

/**
 * **EXTERNAL-CONTRACT CHARACTERIZATION TEST — exercises no tabellarium
 * code.** It pins the behavior of LogstashEncoder + Jackson against an
 * Elasticsearch scalar mapping for an emitter call-site that lives in
 * the consuming services, not in this library. It is kept here as
 * executable documentation of the incident below; if it fails after a
 * logstash-logback-encoder upgrade, the finding concerns the emitting
 * services' call sites — not this appender. Tagged `external-contract`
 * so it can be excluded from library-focused runs.
 *
 * Regression guard for a production incident: one structured per-exchange log event
 * ("Adapter http exchange ... -> 403") reached the pod stdout but never appeared in Kibana, while every
 * neighbouring log line on the same logger did.
 *
 * The full chain, reproduced here without any cluster access:
 *  1. The emitter attached the request method as a structured SLF4J 2 KeyValue using the raw Spring
 *     [HttpMethod] object: `addKeyValue("lap.http.request.method", method)`.
 *  2. [LogstashEncoder] serialises KeyValue values with Jackson. [HttpMethod] is not a JavaBean, so it is
 *     written as an empty JSON object: `"lap.http.request.method":{}`. The encoder does NOT fail - the
 *     event is emitted to stdout intact (so logback and the encoder are not the culprit).
 *  3. Elasticsearch then rejects the whole document on ingest, because the index maps that field as a
 *     scalar (keyword) and an object value raises a `mapper_parsing_exception`. The lap.*-free lines have
 *     no such field and ingest cleanly, which is why only this one event vanished.
 *
 * The invariant this test pins: every value handed to `addKeyValue` for a scalar-mapped field must
 * serialise to a JSON scalar, not an object. The one-line fix at the call site is to pass
 * `method.name()` (a String) instead of the [HttpMethod] object.
 */
@Tag("external-contract")
@ResourceLock("logback.global-logger-context")
class LogstashHttpMethodKeyValueIngestTest {
    /**
     * The lap.* leaf fields the log index maps as scalars (keyword / long), ECS-style. A JSON object value
     * for any of them is what Elasticsearch refuses at ingest time.
     */
    private val scalarMappedFields =
        setOf(
            "lap.http.request.method",
            "lap.url.path",
            "lap.event.outcome",
            "lap.event.duration",
            "lap.http.response.status_code",
        )

    @Nested
    inner class `Encoder behaviour` {
        @Test
        fun `should emit a fluent event with a raw HttpMethod KeyValue instead of dropping it`() {
            // What is to be tested? Whether LogstashEncoder silently drops
            //   or fails on a fluent event whose KeyValue value is a raw
            //   HttpMethod - it must emit the event and render the value as
            //   an empty JSON object.
            // How will the test case be deemed successful and why? Successful
            //   if the event appears in the encoder output with the method
            //   field rendered as {} and no encode error swallowed into the
            //   StatusManager. This rules the encoder/logback out as the
            //   cause of the incident.
            // Why is it important to test this test case? It proves the event
            //   genuinely reaches stdout, matching the raw pod log - the
            //   loss must therefore be downstream (Elasticsearch ingest).

            // Given / When: the production-shaped diary event is emitted with a raw HttpMethod KeyValue.
            val emitted = emitDiaryEvent(methodValue = HttpMethod.GET)

            // Then: the event is present, the HttpMethod is rendered as an empty object, and no encode error
            // was swallowed into the logback StatusManager.
            assertThat(emitted.json)
                .`as`("the event reaches the encoder output (it is not dropped)")
                .contains("Adapter http exchange")
            assertThat(emitted.json)
                .`as`("a raw HttpMethod is serialised by Jackson as an empty JSON object")
                .contains("\"lap.http.request.method\":{}")
            assertThat(emitted.encodeErrors)
                .`as`("LogstashEncoder reports no swallowed encode error - it does not choke on HttpMethod")
                .isZero()
        }
    }

    @Nested
    inner class `Elasticsearch ingest contract` {
        @Test
        fun `should render a raw HttpMethod KeyValue as an empty object that a scalar mapping rejects`() {
            // Given: the emitted diary JSON, parsed as Elasticsearch would receive it.
            val doc = ObjectMapper().readTree(emitDiaryEvent(methodValue = HttpMethod.GET).json)

            // When: the document is validated against an index mapping that expects scalar leaves.
            val conflict = firstScalarFieldArrivingAsObject(doc, scalarMappedFields)

            // Then: the method field is an empty object and is exactly the field that breaks ingest.
            assertThat(doc.get("lap.http.request.method").isObject)
                .`as`("raw HttpMethod renders as a JSON object")
                .isTrue()
            assertThat(conflict)
                .`as`("Elasticsearch rejects the document: a keyword-mapped field arrives as an object")
                .isEqualTo("lap.http.request.method")
        }

        @Test
        fun `should render the method name as a string that a scalar mapping accepts`() {
            // Given: the same event built with the fix - method.name() instead of the HttpMethod object.
            val doc = ObjectMapper().readTree(emitDiaryEvent(methodValue = HttpMethod.GET.name()).json)

            // When
            val conflict = firstScalarFieldArrivingAsObject(doc, scalarMappedFields)

            // Then: the method field is a string and the document ingests without a scalar/object conflict.
            assertThat(doc.get("lap.http.request.method").isTextual)
                .`as`("HttpMethod.name() renders as the JSON string \"GET\"")
                .isTrue()
            assertThat(conflict)
                .`as`("no scalar-mapped field arrives as an object, so Elasticsearch accepts the document")
                .isNull()
        }

        @Test
        fun `should omit a null method field entirely so the document still ingests cleanly`() {
            // What is to be tested? How the fixed call site behaves when
            //   diary.method is null - i.e. method?.name() is null. This
            //   happens for real when the diary exists but the request never
            //   reached the filter (e.g. a CircuitBreaker short-circuit).
            // How will the test case be deemed successful and why? Successful
            //   if the field is simply absent from the JSON (LogstashEncoder
            //   drops null KeyValues), no encode error occurred, and no
            //   scalar/object conflict arises - an absent field never
            //   conflicts with any mapping.
            // Why is it important to test this test case? The fix must not
            //   trade the object-{} bug for a different ingest break.

            // Given
            val emitted = emitDiaryEvent(methodValue = null)
            val doc = ObjectMapper().readTree(emitted.json)

            // When
            val conflict = firstScalarFieldArrivingAsObject(doc, scalarMappedFields)

            // Then: a null value is omitted (not rendered as null and not as an object), no encode error,
            // and no scalar/object conflict, so Elasticsearch accepts the document.
            assertThat(emitted.encodeErrors).`as`("no swallowed encode error for a null KeyValue").isZero()
            assertThat(emitted.json)
                .`as`("a null method is omitted entirely, not rendered as an object")
                .doesNotContain("\"lap.http.request.method\"")
            assertThat(doc.get("lap.http.request.method")).`as`("the field is absent from the document").isNull()
            assertThat(conflict)
                .`as`("an absent field never conflicts, so Elasticsearch accepts the document")
                .isNull()
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private data class Emitted(
        val json: String,
        val encodeErrors: Int,
    )

    /**
     * Emits one production-shaped diary event through a real [LogstashEncoder] into an in-memory buffer and
     * returns the single JSON line plus the number of encode errors logback swallowed into its StatusManager.
     * [methodValue] is the value under test for `lap.http.request.method` (a raw [HttpMethod] vs. its String
     * name). The logger is isolated (`additive = false`) so it is independent of any ambient logging config.
     */
    private fun emitDiaryEvent(methodValue: Any?): Emitted {
        val context = LoggerFactory.getILoggerFactory() as LoggerContext
        val out = ByteArrayOutputStream()
        val encoder =
            LogstashEncoder().apply {
                this.context = context
                start()
            }
        val appender =
            OutputStreamAppender<ILoggingEvent>().apply {
                this.context = context
                this.encoder = encoder
                this.outputStream = out
                start()
            }
        val logger =
            (context.getLogger("ingest-test.${methodValue?.javaClass?.simpleName ?: "null"}") as LogbackLogger).apply {
                level = Level.INFO
                isAdditive = false
                addAppender(appender)
            }
        val statusesBefore = context.statusManager.copyOfStatusList.size

        logger
            .atError()
            .setCause(RuntimeException("403 Forbidden"))
            .setMessage("Adapter http exchange access-profiles GET /v5/access-profiles/by-involved-party/uuid -> 403")
            .addKeyValue("lap.event.kind", "http-request")
            .addKeyValue("lap.event.outcome", "failure")
            .addKeyValue("lap.event.duration", 120_143_294L)
            .addKeyValue("lap.service.target.name", "access-profiles")
            .addKeyValue("lap.http.request.method", methodValue) // the value under test: HttpMethod vs String
            .addKeyValue("lap.url.path", "/v5/access-profiles/by-involved-party/uuid")
            .addKeyValue("lap.http.response.status_code", 403)
            .log()

        appender.stop() // flush the stream
        val encodeErrors =
            context.statusManager.copyOfStatusList
                .drop(statusesBefore)
                .count { it.level == Status.ERROR }
        return Emitted(out.toString(StandardCharsets.UTF_8).trim(), encodeErrors)
    }

    /**
     * Mimics the core Elasticsearch ingest rule: a field the index maps as a scalar (keyword / long) cannot
     * receive a JSON object value - a mismatch raises `mapper_parsing_exception` and the whole document is
     * rejected. logstash writes dotted keys and Elasticsearch expands them, but the literal dotted key carries
     * the same value node, so reading it directly is equivalent for this check. Returns the first offending
     * field path, or `null` when every scalar-mapped field carries a scalar value.
     */
    private fun firstScalarFieldArrivingAsObject(
        doc: JsonNode,
        scalarFields: Set<String>,
    ): String? = scalarFields.firstOrNull { field -> doc.get(field)?.isObject == true }
}
