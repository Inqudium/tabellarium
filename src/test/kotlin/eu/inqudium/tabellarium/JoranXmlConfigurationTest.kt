package eu.inqudium.tabellarium

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.MarkerFactory
import java.io.ByteArrayInputStream

/**
 * Round-trip evidence for the appender's DECLARATIVE contract: the
 * XML surface parsed by Joran is what operators actually consume, and
 * Joran binds it reflectively by naming convention - a setter rename
 * or a broken `<appender-ref>` wiring passes every programmatic test
 * and would otherwise surface only as a broken production
 * configuration. Each test here feeds a real XML document through
 * [JoranConfigurator] into a fresh, private [LoggerContext] (the
 * global SLF4J context is not touched).
 *
 * The end-to-end test deliberately uses the REAL producer factory: a
 * `KafkaProducer` constructs without a broker, and with a short
 * `max.block.ms` its send fails fast, driving the event through the
 * production fallback path - so the whole pipeline from XML to
 * fallback is exercised offline.
 */
class JoranXmlConfigurationTest {
    private val context = LoggerContext()

    @AfterEach
    fun tearDown() {
        // Stops all appenders (closing any real Kafka producer built by
        // a test) and releases their threads.
        context.stop()
    }

    private fun configure(xml: String) {
        val configurator = JoranConfigurator()
        configurator.context = context
        configurator.doConfigure(ByteArrayInputStream(xml.toByteArray(Charsets.UTF_8)))
    }

    private fun kafkaAppender(): KafkaAppender =
        context
            .getLogger(Logger.ROOT_LOGGER_NAME)
            .getAppender("KAFKA") as KafkaAppender

    /** The documented full configuration shape, as an operator would write it. */
    private fun fullConfigXml(maxBlockMs: Int = 100): String =
        """
        <configuration>
            <appender name="FALLBACK" class="eu.inqudium.tabellarium.ThreadSafeListAppender"/>
            <appender name="KAFKA" class="eu.inqudium.tabellarium.KafkaAppender">
                <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
                    <pattern>%msg</pattern>
                </encoder>
                <kafkaProducerProperties>
                    bootstrap.servers=localhost:1
                    max.block.ms=$maxBlockMs
                </kafkaProducerProperties>
                <topicMapping>
                    <defaultTopic>  default.topic  </defaultTopic>
                    <defaultTopicClass>functional</defaultTopicClass>
                    <mapping>
                        <marker>SECURITY</marker>
                        <topic>audit.security</topic>
                        <topicClass>AUDIT</topicClass>
                    </mapping>
                </topicMapping>
                <environment>test</environment>
                <component>joran-test-service</component>
                <cmdbId>CMDB-JORAN</cmdbId>
                <appender-ref ref="FALLBACK"/>
            </appender>
            <root level="INFO">
                <appender-ref ref="KAFKA"/>
            </root>
        </configuration>
        """.trimIndent()

    @Nested
    inner class `Declarative binding` {
        @Test
        fun `should bind every documented XML element through Joran`() {
            // What is to be tested? Whether the complete documented XML
            //   surface - encoder, kafkaProducerProperties text,
            //   topicMapping with defaultTopic and a <mapping> entry,
            //   the three identity fields, and <appender-ref> - reaches
            //   the appender through Joran's reflective binding.
            // How will the test case be deemed successful and why? Successful
            //   if the appender started and every bound value matches
            //   the XML (defaultTopic trimmed, mapping fields populated,
            //   fallback slot holding the referenced ListAppender). Any
            //   setter rename or broken adder breaks exactly here.
            // Why is it important to test this test case? The XML surface
            //   is the product's actual contract; without this round trip
            //   its correctness rests on naming conventions no compiler
            //   checks.

            // When
            configure(fullConfigXml())
            val appender = kafkaAppender()

            // Then: started, with every element bound
            assertThat(appender.isStarted).isTrue()
            assertThat(appender.topicMapping.defaultTopic).isEqualTo("default.topic")
            assertThat(appender.topicMapping.defaultTopicClass).isEqualTo("functional")
            assertThat(appender.topicMapping.mappings).hasSize(1)
            val mapping = appender.topicMapping.mappings.single()
            assertThat(mapping.marker).isEqualTo("SECURITY")
            assertThat(mapping.topic).isEqualTo("audit.security")
            assertThat(mapping.topicClass).isEqualTo("AUDIT")
            assertThat(appender.component).isEqualTo("joran-test-service")
            assertThat(appender.cmdbId).isEqualTo("CMDB-JORAN")
            assertThat(appender.environment).isEqualTo("test")
            assertThat(appender.kafkaProducerProperties).contains("bootstrap.servers=localhost:1")
            assertThat(appender.fallbackAppender)
                .isInstanceOf(ThreadSafeListAppender::class.java)
            assertThat(appender.fallbackAppender?.name).isEqualTo("FALLBACK")
        }

        @Test
        fun `should refuse to start via XML when a required element is missing`() {
            // Given: no <component>
            configure(
                """
                <configuration>
                    <appender name="KAFKA" class="eu.inqudium.tabellarium.KafkaAppender">
                        <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
                            <pattern>%msg</pattern>
                        </encoder>
                        <kafkaProducerProperties>bootstrap.servers=localhost:1</kafkaProducerProperties>
                        <topicMapping><defaultTopic>default.topic</defaultTopic></topicMapping>
                        <environment>test</environment>
                        <cmdbId>CMDB-JORAN</cmdbId>
                    </appender>
                    <root level="INFO"><appender-ref ref="KAFKA"/></root>
                </configuration>
                """.trimIndent(),
            )

            // Then: the appender refused to start, with the validation error
            //   in the context's status manager
            assertThat(kafkaAppender().isStarted).isFalse()
            assertThat(context.statusManager.copyOfStatusList.map { it.message })
                .anyMatch { it.contains("<component>") && it.contains("blank") }
        }
    }

    @Nested
    inner class `End-to-end through the real pipeline` {
        @Test
        fun `should carry an event from an XML-configured logger to the fallback when no broker is reachable`() {
            // What is to be tested? The full production path built purely
            //   from XML: logger -> appender -> encoder -> routing -> a
            //   REAL KafkaProducer whose send fails (no broker on
            //   localhost:1, max.block.ms=100) -> asynchronous
            //   FallbackDispatcher -> the XML-referenced fallback
            //   appender.
            // How will the test case be deemed successful and why? Successful
            //   if the logged event arrives in the ListAppender within the
            //   polling deadline. This proves start() assembled a working
            //   pipeline from nothing but the declarative configuration -
            //   producer construction, breaker wiring, dispatcher thread
            //   and appender-ref resolution included.
            // Why is it important to test this test case? It is the only
            //   test in the suite in which the operator-facing artifact
            //   (an XML file) is the sole input, exactly as deployed.

            // Given
            configure(fullConfigXml(maxBlockMs = 100))
            val fallback = kafkaAppender().fallbackAppender as ThreadSafeListAppender

            // When: log through the XML-configured root logger
            context
                .getLogger("joran.e2e")
                .info(MarkerFactory.getDetachedMarker("SECURITY"), "undeliverable event")

            // Then: the event surfaces in the fallback via the async
            // dispatcher; the copy-on-write list gives the polling test
            // thread a happens-before edge on the worker's write.
            pollUntil(timeoutMs = 5000) { fallback.events.size == 1 }
            assertThat(fallback.events[0].formattedMessage).isEqualTo("undeliverable event")
            assertThat(fallback.events[0].level).isEqualTo(Level.INFO)
        }
    }
}
