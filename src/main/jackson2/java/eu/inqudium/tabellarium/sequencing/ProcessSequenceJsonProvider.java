package eu.inqudium.tabellarium.sequencing;

import ch.qos.logback.classic.spi.ILoggingEvent;
import net.logstash.logback.composite.AbstractJsonProvider;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;

/**
 * Jackson 2 half of
 * {@link eu.inqudium.tabellarium.sequencing.jackson3.ProcessSequenceJsonProvider}
 * — the same up-to-four fields in one pass ({@code process_start},
 * {@code process_uptime_ms}, and, unless suppressed,
 * {@code log_encoder_sequence} plus {@code log_encoder_instance}), for
 * logstash-logback-encoder <strong>8.x</strong>.
 *
 * <p>Why the split exists at all, and why the shared state lives in
 * {@link ProcessSequenceSupport}, is documented on
 * {@link SequencingJsonProvider}.
 *
 * <h2>Configuration</h2>
 *
 * <pre>{@code
 * <encoder class="net.logstash.logback.encoder.LogstashEncoder">
 *   <provider class="eu.inqudium.tabellarium.sequencing.ProcessSequenceJsonProvider">
 *     <includeSequence>true</includeSequence>
 *   </provider>
 * </encoder>
 * }</pre>
 */
public class ProcessSequenceJsonProvider extends AbstractJsonProvider<ILoggingEvent> {
    private final ProcessSequenceSupport support;

    /** Joran's entry point: fresh, self-owned state. */
    public ProcessSequenceJsonProvider() {
        this(new ProcessSequenceSupport());
    }

    /** @param support the shared, Jackson-free state */
    public ProcessSequenceJsonProvider(final ProcessSequenceSupport support) {
        this.support = support;
    }

    public void setIncludeSequence(final boolean includeSequence) {
        support.setIncludeSequence(includeSequence);
    }

    public boolean isIncludeSequence() {
        return support.getIncludeSequence();
    }

    @Override
    public void writeTo(final JsonGenerator generator, final ILoggingEvent event) throws IOException {
        // Constant per JVM run; uptime is the event's own timestamp minus start, not a fresh clock read.
        generator.writeStringField(ProcessSequenceSupport.START_FIELD, support.startIso);
        generator.writeNumberField(
                ProcessSequenceSupport.UPTIME_FIELD, support.uptimeMillis(event.getTimeStamp()));
        // nextSequence() advances the sequence once per emitted event; a gap downstream signals loss.
        if (support.getIncludeSequence()) {
            generator.writeNumberField(ProcessSequenceSupport.SEQUENCE_FIELD, support.nextSequence());
            generator.writeStringField(ProcessSequenceSupport.INSTANCE_FIELD, support.instance);
        }
    }
}
