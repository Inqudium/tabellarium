package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import ch.qos.logback.core.status.Status
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import java.io.ByteArrayInputStream
import java.net.URL
import java.util.Collections
import java.util.Enumeration
import java.util.function.Supplier

/**
 * Consumer-classpath evidence for the optional dependencies: the
 * published POM marks `micrometer-core`, `spring-context` and
 * `logstash-logback-encoder` (with its Jackson 3) optional, so a plain
 * Logback consumer runs the appender without any of them - while the
 * test classpath always has all three, and every other test would stay
 * green if the appender class silently required one again.
 *
 * The test therefore re-loads the library, Logback and every other
 * non-JDK class in a [WithoutOptionalDependencies] class loader that
 * hides the optional packages, and drives a Joran configuration
 * entirely inside it ([JoranWithoutOptionalDependencies]).
 */
class OptionalDependencyClasspathTest {
    /**
     * Child-first class loader that defines every non-JDK class itself,
     * from the bytes of the test classpath, and reports the optional
     * packages as absent - the runtime classpath of a consumer that did
     * not add them. Child-first is essential: delegating to the
     * application class loader would resolve the hidden types there.
     */
    private class WithoutOptionalDependencies(
        private val source: ClassLoader,
    ) : ClassLoader("without-optional-dependencies", getPlatformClassLoader()) {
        override fun loadClass(
            name: String,
            resolve: Boolean,
        ): Class<*> =
            synchronized(getClassLoadingLock(name)) {
                findLoadedClass(name)
                    ?: run {
                        if (HIDDEN_PACKAGES.any { name.startsWith(it) }) throw ClassNotFoundException(name)
                        // JDK classes come from the platform loader (parent).
                        runCatching { parent.loadClass(name) }.getOrNull()
                            ?: source
                                .getResourceAsStream("${name.replace('.', '/')}.class")
                                ?.use { it.readBytes() }
                                ?.let { defineClass(name, it, 0, it.size) }
                            ?: throw ClassNotFoundException(name)
                    }
            }.also { if (resolve) resolveClass(it) }

        // The hidden artifacts' resources vanish too: their service
        // registrations (e.g. Spring Boot's Logback Configurator) would
        // otherwise name classes this loader reports as absent.
        override fun findResource(name: String): URL? = findResources(name).toList().firstOrNull()

        override fun findResources(name: String): Enumeration<URL> =
            Collections.enumeration(
                source.getResources(name).toList().filterNot { url ->
                    HIDDEN_ARTIFACTS.any { url.toString().contains(it) }
                },
            )
    }

    /**
     * The consumer side, run inside the isolated loader: configures a
     * fresh [LoggerContext] from the documented minimal XML through
     * Joran, logs one event that the unreachable broker diverts to the
     * fallback, and reports what happened as plain text (only JDK types
     * cross the loader boundary).
     */
    class JoranWithoutOptionalDependencies : Supplier<String> {
        override fun get(): String {
            val micrometerVisible =
                runCatching { Class.forName("io.micrometer.core.instrument.MeterRegistry") }.isSuccess
            val logstashVisible =
                runCatching { Class.forName("net.logstash.logback.encoder.LogstashEncoder") }.isSuccess
            val context = LoggerContext()
            val started: Boolean?
            val fallbackEvents: List<String>?
            try {
                val configurator = JoranConfigurator()
                configurator.context = context
                configurator.doConfigure(ByteArrayInputStream(XML.toByteArray(Charsets.UTF_8)))
                val appender = context.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("KAFKA")
                context.getLogger("optional.dependencies").info("delivered without micrometer")
                val fallback = (appender as? KafkaAppender)?.fallbackAppender as? ThreadSafeListAppender
                fallback?.let { pollUntil(timeoutMs = 5000) { it.events.size == 1 } }
                started = appender?.isStarted
                fallbackEvents = fallback?.events?.map { it.formattedMessage }
            } finally {
                context.stop()
            }
            // Read after stop(): the Micrometer-free teardown is part of the
            //   contract, and a warning it raised must fail the test too
            //   (docs/assessment/DEFECT_ANALYSIS-2026-10-10T15-19-31.md,
            //   finding 12).
            val problems =
                context.statusManager.copyOfStatusList
                    .filter { it.level >= Status.WARN }
                    .joinToString(" | ") { "${it.message} ${it.throwable ?: ""}" }
            return "micrometerVisible=$micrometerVisible logstashVisible=$logstashVisible " +
                "started=$started fallbackEvents=$fallbackEvents problems=[$problems]"
        }
    }

    @Test
    fun `should configure, start and deliver through Joran without the optional dependencies on the classpath`() {
        // What is to be tested? Whether a consumer that leaves out the
        //   optional micrometer-core, spring-context and
        //   logstash-logback-encoder (with its Jackson 3) can configure
        //   the appender through Joran with a plain Logback encoder, start
        //   it and log through it.
        // How will the test case be deemed successful and why? Successful
        //   if, inside a class loader that hides all four packages, the
        //   Joran run throws nothing, the appender is started, the event
        //   reaches the fallback through the real (unreachable) producer
        //   and the status manager holds no warning or error. Joran's bean
        //   introspection resolves the parameter types of every public
        //   appender method, so a single Micrometer type in a member
        //   signature fails the run with NoClassDefFoundError before the
        //   appender exists; a Logstash or Jackson reference anywhere on
        //   the hot path would fail the same way.
        // Why is it important to test this test case? It is the README's
        //   "runs without Micrometer" and "any standard Logback encoder"
        //   promise and the default consumer mode; the regular suite
        //   always has all three on the classpath and cannot notice one
        //   becoming required (DEFECT_ANALYSIS-2026-10-10T13-46-50,
        //   finding 1).

        // Given
        val loader = WithoutOptionalDependencies(javaClass.classLoader)

        // When: run with the isolated loader as context class loader -
        //   Kafka resolves its serializer classes through it, and a
        //   consumer's application has only the one loader anyway
        val thread = Thread.currentThread()
        val previousContextLoader = thread.contextClassLoader
        thread.contextClassLoader = loader
        val report =
            try {
                loader
                    .loadClass(JoranWithoutOptionalDependencies::class.java.name)
                    .getDeclaredConstructor()
                    .newInstance()
                    .let {
                        @Suppress("UNCHECKED_CAST")
                        (it as Supplier<String>).get()
                    }
            } finally {
                thread.contextClassLoader = previousContextLoader
            }

        // Then
        assertThat(report).isEqualTo(
            "micrometerVisible=false logstashVisible=false started=true " +
                "fallbackEvents=[delivered without micrometer] problems=[]",
        )
    }

    private companion object {
        val HIDDEN_PACKAGES = listOf("io.micrometer.", "org.springframework.", "net.logstash.", "tools.jackson.")

        /** Jar-path fragments of the artifacts that provide [HIDDEN_PACKAGES]. */
        val HIDDEN_ARTIFACTS = listOf("/micrometer-", "/spring-", "/logstash-logback-encoder-", "/tools/jackson/")

        val XML =
            """
            <configuration>
                <appender name="FALLBACK" class="eu.inqudium.tabellarium.ThreadSafeListAppender"/>
                <appender name="KAFKA" class="eu.inqudium.tabellarium.KafkaAppender">
                    <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
                        <pattern>%msg</pattern>
                    </encoder>
                    <kafkaProducerProperties>
                        bootstrap.servers=localhost:1
                        max.block.ms=100
                    </kafkaProducerProperties>
                    <topicMapping><defaultTopic>default.topic</defaultTopic></topicMapping>
                    <environment>test</environment>
                    <component>optional-dependencies</component>
                    <cmdbId>CMDB-OPTIONAL</cmdbId>
                    <appender-ref ref="FALLBACK"/>
                </appender>
                <root level="INFO"><appender-ref ref="KAFKA"/></root>
            </configuration>
            """.trimIndent()
    }
}
