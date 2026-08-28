package eu.inqudium.tabellarium.sequencing;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import java.beans.Introspector;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import net.logstash.logback.encoder.LogstashEncoder;

/**
 * Runs the sequencing providers against logstash-logback-encoder 8.x,
 * where the surefire suite cannot reach them: the module compiles and
 * tests against 9.x, and both encoder generations claim the same
 * package names, so they cannot share one classpath.
 *
 * <p>The module's {@code pom.xml} runs this class in the {@code verify}
 * phase on a classpath assembled from the 8.0 jars. It is a plain
 * {@code main} rather than a JUnit test for exactly that reason —
 * nothing about it may be discovered by surefire.
 *
 * <h2>What is actually at risk</h2>
 *
 * <p>Three failure modes, one check each:
 *
 * <ol>
 *   <li><strong>Signature.</strong> A provider compiled for Jackson 3
 *       does not implement 8.x's {@code writeTo} at all; the first
 *       encoded event throws {@code AbstractMethodError}. Checks 1-3
 *       encode a real event.</li>
 *   <li><strong>Reflection.</strong> A class whose method descriptors
 *       name an absent Jackson fails every reflective lookup on itself
 *       with {@code NoClassDefFoundError} — which is Joran's property
 *       binding, i.e. the failure lands during configuration. Check 4
 *       introspects the provider the way Joran does.</li>
 *   <li><strong>Selection.</strong> {@link SequencingLogstashEncoder}
 *       must pick the Jackson 2 half here — picking the other one would
 *       surface as the {@code AbstractMethodError} above. Check 1
 *       exercises the encoder shortcut, check 5 the detector behind
 *       it.</li>
 * </ol>
 */
public final class Jackson2ProviderSmokeCheck {
    private static final List<String> FAILURES = new ArrayList<>();

    private Jackson2ProviderSmokeCheck() {
    }

    public static void main(final String[] args) {
        final LoggerContext context = new LoggerContext();
        // logback 1.5 reads the MDC adapter from the LoggerContext (LoggingEvent.getMDCPropertyMap),
        // not from the global MDC. A hand-built context has none, so encoding an event NPEs.
        context.setMDCAdapter(new LogbackMDCAdapter());
        context.start();

        encoderShortcutWritesSequenceAndInstance(context);
        processStartProviderWritesStartAndUptime(context);
        processSequenceProviderWritesAllFourFields(context);
        joranStylePropertyBindingWorks();
        generationDetectorRecognisesJackson2();

        if (FAILURES.isEmpty()) {
            System.out.println("[jackson2-smoke] OK - 5 checks passed; the encoder's JsonProvider.writeTo takes "
                    + writeToParameterType());
            return;
        }
        FAILURES.forEach(failure -> System.err.println("[jackson2-smoke] FAILED: " + failure));
        throw new AssertionError(FAILURES.size() + " jackson2 smoke check(s) failed");
    }

    /** 1. The version-agnostic encoder shortcut still stamps both fields. */
    private static void encoderShortcutWritesSequenceAndInstance(final LoggerContext context) {
        final SequencingLogstashEncoder encoder = new SequencingLogstashEncoder();
        encoder.setContext(context);
        encoder.setSequenceField("log_encoder_sequence");
        encoder.start();

        final String json = encode(encoder, event(context, "first"));
        expect(json.contains("\"log_encoder_sequence\":1"), "sequence starts at 1 as a JSON number", json);
        expect(json.contains("\"log_encoder_instance\":\""), "instance is written as a JSON string", json);

        final String second = encode(encoder, event(context, "second"));
        expect(second.contains("\"log_encoder_sequence\":2"), "sequence is monotonic", second);
    }

    /** 2. The hand-registered ProcessStart twin: string start, numeric uptime. */
    private static void processStartProviderWritesStartAndUptime(final LoggerContext context) {
        final LogstashEncoder encoder = new LogstashEncoder();
        encoder.setContext(context);
        encoder.addProvider(new ProcessStartJsonProvider());
        encoder.start();

        final String json = encode(encoder, event(context, "start"));
        expect(json.matches("(?s).*\"process_start\":\"\\d{4}-.*"), "process_start is an ISO-8601 string", json);
        expect(json.matches("(?s).*\"process_uptime_ms\":-?\\d+.*"), "process_uptime_ms is a JSON number", json);
    }

    /** 3. The hand-registered ProcessSequence twin: all four fields in one pass. */
    private static void processSequenceProviderWritesAllFourFields(final LoggerContext context) {
        final LogstashEncoder encoder = new LogstashEncoder();
        encoder.setContext(context);
        encoder.addProvider(new ProcessSequenceJsonProvider());
        encoder.start();

        final String json = encode(encoder, event(context, "combined"));
        expect(json.contains("\"process_start\":\""), "process_start present", json);
        expect(json.matches("(?s).*\"process_uptime_ms\":-?\\d+.*"), "process_uptime_ms present", json);
        expect(json.contains("\"log_encoder_sequence\":1"), "log_encoder_sequence present", json);
        expect(json.contains("\"log_encoder_instance\":\""), "log_encoder_instance present", json);
    }

    /**
     * 4. Joran's property binding. This is the check the naive
     * both-overloads-on-one-class approach fails: {@code getMethods()}
     * resolves every declared parameter type, so a descriptor naming
     * the absent Jackson - here Jackson 3 - throws
     * {@code NoClassDefFoundError} here, before any event is ever logged.
     */
    private static void joranStylePropertyBindingWorks() {
        try {
            final SequencingJsonProvider provider = new SequencingJsonProvider();
            final Method[] methods = provider.getClass().getMethods();
            expect(methods.length > 0, "getMethods() resolves", "none");
            Introspector.getBeanInfo(provider.getClass());
            provider.getClass().getMethod("setSequenceField", String.class).invoke(provider, "custom_seq");
            expect("custom_seq".equals(provider.getSequenceField()), "reflective setter applies", provider.getSequenceField());
        } catch (final ReflectiveOperationException | java.beans.IntrospectionException | LinkageError e) {
            FAILURES.add("Joran-style reflection on the jackson2 provider: " + e);
        }
    }

    /**
     * 5. The detector itself, against a real 8.0 classpath — the one
     * thing checks 1-4 cannot prove on their own. Everything else here
     * would still pass if {@code isJackson3} always returned false.
     */
    private static void generationDetectorRecognisesJackson2() {
        expect(!JacksonGeneration.isJackson3(), "JacksonGeneration detects the 8.x encoder", "true");
        final String derived = JacksonGeneration.jackson2ClassName(
                "eu.inqudium.tabellarium.sequencing.jackson3.SequencingJsonProvider");
        expect("eu.inqudium.tabellarium.sequencing.SequencingJsonProvider".equals(derived),
                "the twin's name is derived correctly", derived);
    }

    /** The parameter type that makes the two encoder generations incompatible. */
    private static String writeToParameterType() {
        for (final Method method : net.logstash.logback.composite.JsonProvider.class.getMethods()) {
            if ("writeTo".equals(method.getName())) {
                return method.getParameterTypes()[0].getName();
            }
        }
        return "<no writeTo>";
    }

    private static LoggingEvent event(final LoggerContext context, final String message) {
        return new LoggingEvent("fqcn.dummy", context.getLogger("smoke"), Level.INFO, message, null, null);
    }

    private static String encode(final LogstashEncoder encoder, final ILoggingEvent event) {
        return new String(encoder.encode(event), StandardCharsets.UTF_8);
    }

    private static void expect(final boolean condition, final String what, final String actual) {
        if (!condition) {
            FAILURES.add(what + " -- actual: " + actual);
        }
    }
}
