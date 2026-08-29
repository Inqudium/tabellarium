import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import eu.inqudium.tabellarium.TopicRouter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.slf4j.Marker;
import org.slf4j.helpers.BasicMarkerFactory;

/**
 * Fuzzes topic-name validation and marker routing.
 *
 * Invariants under test: construction rejects with the documented
 * IllegalArgumentException exactly the names Kafka's own rules reject
 * (character set, reserved '.'/'..', length 249) - with a positive oracle,
 * so validation cannot silently tighten and reject names the broker would
 * accept; route() never throws for arbitrary marker trees and always
 * resolves to the default topic or a mapped one, with a direct marker match
 * winning.
 */
public final class TopicRouterFuzzer {
    private static final Pattern KAFKA_TOPIC = Pattern.compile("[a-zA-Z0-9._\\-]{1,249}");
    private static final String TOPIC_CHARS =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-";

    public static void fuzzerTestOneInput(FuzzedDataProvider data) {
        if (data.consumeBoolean()) {
            positiveOracle(data);
        } else {
            arbitraryNames(data);
        }
    }

    /** Structurally valid names must be accepted, and routing must honor the table. */
    private static void positiveOracle(FuzzedDataProvider data) {
        String defaultTopic = validTopic(data);
        Map<String, String> mappings = new HashMap<>();
        int count = data.consumeInt(0, 4);
        for (int i = 0; i < count; i++) {
            mappings.put("marker-" + data.consumeInt(0, 5), validTopic(data));
        }

        TopicRouter router = new TopicRouter(defaultTopic, mappings);

        BasicMarkerFactory factory = new BasicMarkerFactory();
        List<Marker> markers = new ArrayList<>();
        int markerCount = data.consumeInt(0, 4);
        for (int i = 0; i < markerCount; i++) {
            Marker marker = factory.getDetachedMarker(data.consumeBoolean() ? "marker-" + data.consumeInt(0, 5) : data.consumeString(12));
            if (data.consumeBoolean()) {
                marker.add(factory.getDetachedMarker("marker-" + data.consumeInt(0, 5)));
            }
            markers.add(marker);
        }

        String routed = router.route(markers);
        if (!routed.equals(defaultTopic) && !mappings.containsValue(routed)) {
            throw new IllegalStateException("routed to an unconfigured topic: " + routed);
        }
        if (!markers.isEmpty()) {
            String first = markers.get(0).getName();
            String direct = mappings.get(first);
            if (direct != null && !routed.equals(direct)) {
                throw new IllegalStateException(
                        "direct match not honored: marker " + first + " -> " + routed + ", expected " + direct);
            }
        }
    }

    /** Arbitrary names: accepted only when Kafka-valid, rejected only via IllegalArgumentException. */
    private static void arbitraryNames(FuzzedDataProvider data) {
        String defaultTopic = data.consumeString(300);
        String marker = data.consumeString(12);
        String mappedTopic = data.consumeRemainingAsString();
        Map<String, String> mappings = mappedTopic.isEmpty() ? Map.of() : Map.of(marker, mappedTopic);
        try {
            new TopicRouter(defaultTopic, mappings);
        } catch (IllegalArgumentException expected) {
            return;
        }
        for (String topic : accepted(defaultTopic, mappings)) {
            if (!KAFKA_TOPIC.matcher(topic).matches() || topic.equals(".") || topic.equals("..")) {
                throw new IllegalStateException("Kafka-invalid topic accepted: '" + topic + "'");
            }
        }
        if (!mappings.isEmpty() && marker.isBlank()) {
            throw new IllegalStateException("blank marker name accepted");
        }
    }

    private static List<String> accepted(String defaultTopic, Map<String, String> mappings) {
        List<String> topics = new ArrayList<>(mappings.values());
        topics.add(defaultTopic);
        return topics;
    }

    private static String validTopic(FuzzedDataProvider data) {
        int length = data.consumeInt(1, 249);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(TOPIC_CHARS.charAt(data.consumeInt(0, TOPIC_CHARS.length() - 1)));
        }
        String topic = sb.toString();
        return topic.equals(".") || topic.equals("..") ? topic + "a" : topic;
    }

    private TopicRouterFuzzer() {}
}
