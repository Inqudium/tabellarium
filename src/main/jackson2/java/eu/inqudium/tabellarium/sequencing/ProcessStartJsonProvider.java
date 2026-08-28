package eu.inqudium.tabellarium.sequencing;

import ch.qos.logback.classic.spi.ILoggingEvent;
import net.logstash.logback.composite.AbstractJsonProvider;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;

/**
 * Jackson 2 half of
 * {@link eu.inqudium.tabellarium.sequencing.jackson3.ProcessStartJsonProvider}
 * — same two fields, same opposite type rules (ISO-8601 string for the
 * start instant, native JSON number for the uptime), for
 * logstash-logback-encoder <strong>8.x</strong>.
 *
 * <p>Why the split exists at all, and why the shared state lives in
 * {@link ProcessStartSupport}, is documented on
 * {@link SequencingJsonProvider}.
 *
 * <h2>Configuration</h2>
 *
 * <pre>{@code
 * <encoder class="net.logstash.logback.encoder.LogstashEncoder">
 *   <provider class="eu.inqudium.tabellarium.sequencing.ProcessStartJsonProvider">
 *     <startField>process_start</startField>
 *     <uptimeField>process_uptime_ms</uptimeField>
 *   </provider>
 * </encoder>
 * }</pre>
 *
 * <h2>Finality</h2>
 *
 * <p>Deliberately {@code final}, as its Jackson 2 twin is: an
 * overridable {@code writeTo} reading a clock instead of the event
 * timestamp is exactly the defect the design prevents.
 */
public final class ProcessStartJsonProvider extends AbstractJsonProvider<ILoggingEvent> {
    private final ProcessStartSupport support;

    /** Joran's entry point: fresh, self-owned state. */
    public ProcessStartJsonProvider() {
        this(new ProcessStartSupport());
    }

    /**
     * @param support the shared, Jackson-free state, which also resolves
     *     the JVM start time
     */
    public ProcessStartJsonProvider(final ProcessStartSupport support) {
        this.support = support;
    }

    public void setStartField(final String startField) {
        support.setStartField(startField);
    }

    public String getStartField() {
        return support.getStartField();
    }

    public void setUptimeField(final String uptimeField) {
        support.setUptimeField(uptimeField);
    }

    public String getUptimeField() {
        return support.getUptimeField();
    }

    public void setIncludeStart(final boolean includeStart) {
        support.setIncludeStart(includeStart);
    }

    public boolean isIncludeStart() {
        return support.getIncludeStart();
    }

    public void setIncludeUptime(final boolean includeUptime) {
        support.setIncludeUptime(includeUptime);
    }

    public boolean isIncludeUptime() {
        return support.getIncludeUptime();
    }

    @Override
    public void start() {
        if (isStarted()) {
            addWarn("ProcessStartJsonProvider already started; ignoring repeated start()");
            return;
        }

        final ProcessStartSupport.FreezeResult result = support.freeze();
        if (result.error != null) {
            addError(result.error);
            return;
        }
        if (result.warning != null) {
            addWarn(result.warning);
        }
        if (result.info != null) {
            addInfo(result.info);
        }
        super.start();
    }

    /**
     * Contributes nothing when the provider is not started — a logging
     * subsystem that throws while logging turns a misconfiguration into
     * an application-wide outage. A negative uptime is written as-is
     * rather than clamped: a visibly negative number in Kibana beats a
     * silently plausible zero.
     */
    @Override
    public void writeTo(final JsonGenerator generator, final ILoggingEvent event) throws IOException {
        final ProcessStartSupport.Frozen config = support.getFrozen();
        if (config == null) {
            return;
        }

        if (config.includeStart) {
            generator.writeStringField(config.startField, config.startIso);
        }
        if (config.includeUptime) {
            generator.writeNumberField(config.uptimeField, event.getTimeStamp() - config.startMillis);
        }
    }
}
