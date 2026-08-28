package eu.inqudium.tabellarium.sequencing

import ch.qos.logback.classic.spi.ILoggingEvent
import net.logstash.logback.composite.JsonProvider

/**
 * Detects which Jackson generation the logstash-logback-encoder on the
 * classpath was built against, so [SequencingLogstashEncoder] can
 * register the matching provider implementation.
 *
 * ## What differs between the two encoder generations
 *
 * `JsonProvider.writeTo` hands the provider the encoder's own
 * `JsonGenerator`:
 *
 * ```
 * 8.x: void writeTo(com.fasterxml.jackson.core.JsonGenerator, Event) throws IOException
 * 9.x: void writeTo(tools.jackson.core.JsonGenerator, Event)
 * ```
 *
 * Jackson 2's and Jackson 3's generators share no supertype, so the two
 * signatures are different methods as far as the JVM is concerned. A
 * provider compiled for one generation does not implement the other's
 * abstract method at all.
 *
 * ## Why the interface is inspected rather than the classpath
 *
 * Probing for `tools.jackson.core.JsonGenerator` with `Class.forName`
 * answers the wrong question: Jackson 3 may sit on the classpath for
 * unrelated reasons while the encoder is still an 8.x build. The
 * parameter type of the `writeTo` the encoder will actually call is the
 * only authority, and it is always resolvable — the encoder that
 * declares it brings its own Jackson.
 *
 * ## Failure mode
 *
 * When the reflection itself fails — no `writeTo` at all, a future
 * signature change — the result is `true`, i.e. the Jackson 3 provider,
 * which is what this module builds against. A mismatch then surfaces as
 * a loud `AbstractMethodError` on the first event rather than as
 * silently missing fields.
 */
object JacksonGeneration {
    /**
     * `true` when the encoder on the classpath writes through Jackson 3
     * (`tools.jackson.core`), i.e. logstash-logback-encoder 9.0 or
     * later. Resolved once on first access.
     */
    @JvmStatic
    val isJackson3: Boolean by lazy { detectJackson3() }

    /**
     * The fully-qualified name of the Jackson 2 twin of a provider in
     * the `jackson3` package: same simple name, one package up.
     */
    @JvmStatic
    fun jackson2ClassName(jackson3ClassName: String): String {
        val simpleName = jackson3ClassName.substringAfterLast('.')
        val packageName = jackson3ClassName.substringBeforeLast('.').substringBeforeLast('.')
        return "$packageName.$simpleName"
    }

    /**
     * Instantiates the Jackson 2 twin of [jackson3ClassName], passing
     * [support] to its single-argument constructor.
     *
     * Reflection is not an optimisation here but a requirement, twice
     * over. The Jackson 2 half is compiled after the Kotlin sources by
     * a `javac` run of its own, so there is nothing to reference at
     * compile time; and loading it on a 9.x runtime would resolve a
     * class whose `writeTo` descriptor names an absent Jackson, after
     * which every reflective lookup on it — Joran's property binding
     * included — fails with `NoClassDefFoundError`. Naming it by string
     * keeps it unloaded unless it is the one that fits.
     */
    @JvmStatic
    fun <S : Any> newJackson2Provider(
        jackson3ClassName: String,
        support: S,
        supportType: Class<S>,
    ): JsonProvider<ILoggingEvent> {
        val className = jackson2ClassName(jackson3ClassName)
        val loaded = Class.forName(className, true, JacksonGeneration::class.java.classLoader)
        val instance = loaded.getConstructor(supportType).newInstance(support)

        @Suppress("UNCHECKED_CAST")
        return instance as JsonProvider<ILoggingEvent>
    }

    private fun detectJackson3(): Boolean =
        runCatching {
            JsonProvider::class.java.methods
                .firstOrNull { it.name == WRITE_TO }
                ?.parameterTypes
                ?.firstOrNull()
                ?.name
                ?.startsWith(JACKSON2_PACKAGE_PREFIX) != true
        }.getOrDefault(true)

    private const val WRITE_TO = "writeTo"
    private const val JACKSON2_PACKAGE_PREFIX = "com.fasterxml.jackson."
}
