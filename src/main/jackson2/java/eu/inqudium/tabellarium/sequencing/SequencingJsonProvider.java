package eu.inqudium.tabellarium.sequencing;

import ch.qos.logback.classic.spi.ILoggingEvent;
import net.logstash.logback.composite.AbstractJsonProvider;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;

/**
 * Jackson 2 half of
 * {@link eu.inqudium.tabellarium.sequencing.jackson3.SequencingJsonProvider}
 * — same fields, same counter, same defaults, for
 * logstash-logback-encoder <strong>8.x</strong>.
 *
 * <h2>Why a second class rather than a second method</h2>
 *
 * <p>{@code JsonProvider.writeTo} takes the encoder's own
 * {@code JsonGenerator}, and that type changed with encoder 9.0 from
 * {@code com.fasterxml.jackson.core} to {@code tools.jackson.core}. The
 * two are unrelated types, so the two signatures are different methods
 * to the JVM. Carrying both overloads on one class does not work
 * either: whichever Jackson is absent at runtime makes every reflective
 * lookup on that class fail with {@code NoClassDefFoundError}, so Joran
 * cannot even bind {@code <sequenceField>} — the failure lands during
 * configuration, not on the first event.
 *
 * <p>Everything that is not a Jackson call therefore lives in
 * {@link SequencingSupport}, which both halves share. This class is the
 * five-line adapter on top.
 *
 * <h2>Configuration</h2>
 *
 * <p>Identical to the Jackson 2 half apart from the class name:
 *
 * <pre>{@code
 * <encoder class="net.logstash.logback.encoder.LogstashEncoder">
 *   <provider class="eu.inqudium.tabellarium.sequencing.SequencingJsonProvider">
 *     <sequenceField>log_encoder_sequence</sequenceField>
 *   </provider>
 * </encoder>
 * }</pre>
 *
 * <p>{@code SequencingLogstashEncoder} needs no such declaration: it
 * detects the encoder generation and registers whichever half fits.
 *
 * <h2>Java, not Kotlin</h2>
 *
 * <p>This source is compiled by a separate {@code javac} run against
 * encoder 8.0, because a Maven module has exactly one compile classpath
 * and the 9.x jar the module builds against occupies it. See the
 * module's {@code pom.xml}.
 */
public class SequencingJsonProvider extends AbstractJsonProvider<ILoggingEvent> {
    private final SequencingSupport support;

    /** Joran's entry point: fresh, self-owned state. */
    public SequencingJsonProvider() {
        this(new SequencingSupport());
    }

    /**
     * Shares state with a caller — used by
     * {@code SequencingLogstashEncoder}, whose own setters write into
     * the same {@link SequencingSupport}.
     *
     * @param support the shared, Jackson-free state
     */
    public SequencingJsonProvider(final SequencingSupport support) {
        this.support = support;
    }

    /** @see eu.inqudium.tabellarium.sequencing.jackson3.SequencingJsonProvider */
    public void setInstanceId(final String instanceId) {
        support.setInstanceId(instanceId);
    }

    public String getInstanceId() {
        return support.getInstanceId();
    }

    public void setSequenceField(final String sequenceField) {
        support.setSequenceField(sequenceField);
    }

    public String getSequenceField() {
        return support.getSequenceField();
    }

    public void setInstanceField(final String instanceField) {
        support.setInstanceField(instanceField);
    }

    public String getInstanceField() {
        return support.getInstanceField();
    }

    @Override
    public void start() {
        final String resolvedInstance = support.resolveInstance();
        addInfo(
                "SequencingJsonProvider started, instance=" + resolvedInstance
                        + ", sequenceField=" + support.getSequenceField()
                        + ", instanceField=" + support.getInstanceField());
        super.start();
    }

    @Override
    public void writeTo(final JsonGenerator generator, final ILoggingEvent event) throws IOException {
        generator.writeNumberField(support.getSequenceField(), support.nextSequence());
        generator.writeStringField(support.getInstanceField(), support.getResolvedInstance());
    }
}
