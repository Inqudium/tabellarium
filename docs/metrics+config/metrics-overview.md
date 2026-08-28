## Vollständige Metrik-Übersicht (Stand nach allen Patches)

Alle Metriken tragen den `appender`-Tag, der den Logback-Appender-Namen widerspiegelt (`"unnamed"` wenn nicht gesetzt). Plus optional die Common-Tags, die der Operator beim `KafkaAppenderMetricsBinding`-Konstruktor mitgegeben hat.

### Counter

| Metrik                             | Tags                                | Wann wird inkrementiert                                      |
| ---------------------------------- | ----------------------------------- | ------------------------------------------------------------ |
| `kafka.appender.events.accepted`   | `appender`, `topic.class`           | Jedes Event, das `KafkaAppender.append()` betritt (nach Routing zur Topic-Klasse) |
| `kafka.appender.events.dispatched` | `appender`, `topic.class`           | Event wurde erfolgreich an `producer.send()` übergeben (Callback-Outcome noch unbekannt) |
| `kafka.appender.events.fallback`   | `appender`, `topic.class`, `reason` | Event ging zum Fallback-Appender statt zu Kafka              |
| `kafka.appender.fallback.dropped`  | `appender`                          | FallbackDispatcher musste verwerfen (Queue voll oder Shutdown-Timeout) |

### Timer

| Metrik                         | Tags                                 | Was wird gemessen                                    |
| ------------------------------ | ------------------------------------ | ---------------------------------------------------- |
| `kafka.appender.send.duration` | `appender`, `topic.class`, `outcome` | Wall-Clock von `producer.send()`-Aufruf bis Callback |

### Gauges

| Metrik                                   | Tags       | Was zeigt es                                                 |
| ---------------------------------------- | ---------- | ------------------------------------------------------------ |
| `kafka.appender.fallback.queue.size`     | `appender` | Aktuelle Tiefe der FallbackDispatcher-Queue (live pro Scrape) |
| `kafka.appender.fallback.queue.capacity` | `appender` | Maximale Tiefe der Queue (konstant)                          |

## Tag-Werte

### `appender` — pro Appender-Instanz ein Wert

| Wert                  | Quelle                                                       |
| --------------------- | ------------------------------------------------------------ |
| Logback-Appender-Name | Aus dem `<appender name="...">`-Attribut im XML              |
| `unnamed`             | Fallback wenn kein Name gesetzt (sollte in Produktion nicht vorkommen) |

### `topic.class` — 4 mögliche Werte

| Wert          | Wann gesetzt                                                 |
| ------------- | ------------------------------------------------------------ |
| `audit`       | Events für `TopicClass.AUDIT`                                |
| `functional`  | Events für `TopicClass.FUNCTIONAL`                           |
| `technical`   | Events für `TopicClass.TECHNICAL` (Default für unklassifizierte Events) |
| `performance` | Events für `TopicClass.PERFORMANCE`                          |

### `reason` — 4 mögliche Werte (nur bei `events.fallback`)

| Wert            | Bedeutung                                                    |
| --------------- | ------------------------------------------------------------ |
| `breaker.open`  | Circuit Breaker hat keine Permission gegeben (OPEN oder HALF_OPEN ausgeschöpft) |
| `throttle`      | Half-Open-Throttle: Probe-Gap noch nicht verstrichen         |
| `send.error`    | `producer.send()` warf synchron oder Callback meldete Exception |
| `encoder.error` | Hot-Path-Exception vor `send()` (Encoder, Routing, OOM)      |

### `outcome` — 2 mögliche Werte (nur bei `send.duration`)

| Wert      | Bedeutung                                                    |
| --------- | ------------------------------------------------------------ |
| `success` | Callback meldete `exception == null`                         |
| `error`   | Callback meldete Exception oder `producer.send()` warf synchron |

## Common Tags (vom Operator konfiguriert)

Tags, die der Operator via Konstruktor an `KafkaAppenderMetricsBinding` mitgibt, werden **jeder** Metrik beigefügt:

```kotlin
KafkaAppenderMetricsBinding(
    registry,
    Tags.of(
        "application", "payment-service",
        "region", "eu-central-1",
        "environment", "prod",
    )
)
```

Diese erscheinen dann zusätzlich zu den per-Metrik-Tags an allen oben genannten Counter, Timer und Gauges.

## Cardinality pro Appender-Instanz

| Metrik                    | Series-Anzahl                         |
| ------------------------- | ------------------------------------- |
| `events.accepted`         | 4 (eine pro `topic.class`)            |
| `events.dispatched`       | 4                                     |
| `events.fallback`         | 16 (4 × 4 = `topic.class` × `reason`) |
| `send.duration`           | 8 (4 × 2 = `topic.class` × `outcome`) |
| `fallback.dropped`        | 1                                     |
| `fallback.queue.size`     | 1                                     |
| `fallback.queue.capacity` | 1                                     |
| **Gesamt**                | **35**                                |

Multipliziert mit dem `appender`-Tag (im Default-Fall 1 Wert) und der Common-Tags-Cardinality (typisch 1, weil pro Service konstant).

## Resilience4j-Bridge (wenn `resilience4j-micrometer` im Classpath)

Wird automatisch via Reflection-basiertes Binding aktiviert, wenn `TaggedCircuitBreakerMetrics` verfügbar ist:

| Metrik                                       | Tags            | Typ                                                          |
| -------------------------------------------- | --------------- | ------------------------------------------------------------ |
| `resilience4j.circuitbreaker.state`          | `name`, `state` | Gauge: 1 wenn Breaker in diesem State, sonst 0               |
| `resilience4j.circuitbreaker.calls`          | `name`, `kind`  | Counter pro Call-Outcome (`successful`, `failed`, `ignored`, `not_permitted`) |
| `resilience4j.circuitbreaker.buffered.calls` | `name`, `kind`  | Gauge: aktuell im Sliding-Window                             |
| `resilience4j.circuitbreaker.failure.rate`   | `name`          | Gauge: aktuelle Failure-Rate in Prozent                      |
| `resilience4j.circuitbreaker.slow.call.rate` | `name`          | Gauge: aktueller Slow-Call-Anteil in Prozent                 |

**`name`-Werte** entsprechen den aktiven Topic-Klassen:

- `kafka-appender-audit`
- `kafka-appender-functional`
- `kafka-appender-technical`
- `kafka-appender-performance`

## Kafka-Producer-Bridge (wenn `KafkaClientMetrics` im Classpath)

Wird automatisch via Reflection aktiviert, wenn `io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics` verfügbar ist. Pro aktiver TopicClass ein eigener Binding:

| Metrik-Präfix                           | Beispiele                                 |
| --------------------------------------- | ----------------------------------------- |
| `kafka.producer.record.send.total`      | Records, die der Producer angenommen hat  |
| `kafka.producer.record.error.total`     | Records, die fehlgeschlagen sind          |
| `kafka.producer.record.size.avg`        | Mittlere Record-Größe                     |
| `kafka.producer.batch.size.avg`         | Mittlere Batch-Größe                      |
| `kafka.producer.request.latency.avg`    | Mittlere Request-Latency zu Brokern       |
| `kafka.producer.outgoing.byte.rate`     | Bytes/Sekunde zum Cluster                 |
| `kafka.producer.buffer.available.bytes` | Freie Buffer-Bytes                        |
| …                                       | (insgesamt ~40 Producer-interne Metriken) |

Alle mit Tag `topic.class` zur Disambiguierung zwischen den per-Klassen Producern.

## Wichtige Prometheus-Queries für ein Dashboard

```promql
# Throughput pro Topic-Klasse
rate(kafka_appender_events_accepted_total[1m])

# Loss-Rate aufgeschlüsselt nach Grund
rate(kafka_appender_events_fallback_total[1m])

# p99 Send-Latency
histogram_quantile(0.99,
    rate(kafka_appender_send_duration_seconds_bucket[5m]))

# Fallback-Queue-Sättigung in Prozent
kafka_appender_fallback_queue_size
  / kafka_appender_fallback_queue_capacity

# Echter Datenverlust (= dropped events)
rate(kafka_appender_fallback_dropped_total[5m])

# Circuit-Breaker-Zustand
resilience4j_circuitbreaker_state{name=~"kafka-appender-.*"}
```

## Wichtige Alarm-Schwellen

| Bedingung                                                    | Bedeutung                                                    | Severity                        |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------- |
| `kafka_appender_fallback_dropped_total > 0`                  | Echter Datenverlust, FallbackDispatcher-Queue überfüllt      | Kritisch                        |
| `kafka_appender_fallback_queue_size / capacity > 0.8` über 5 min | Fallback-Appender ist langsamer als Event-Rate               | Warnung                         |
| `resilience4j_circuitbreaker_state{state="open"} == 1`       | Cluster-Verlust für diese Topic-Klasse                       | Kritisch (für AUDIT/FUNCTIONAL) |
| `rate(kafka_appender_events_fallback{reason="send.error"}[1m]) > 0` über 10 min | Anhaltende Send-Fehler, die nicht durch Client-Errors gefiltert wurden | Warnung                         |
| `rate(kafka_appender_events_fallback{reason="encoder.error"}[5m]) > 0` | Hot-Path-Exceptions im Encoder/Routing                       | Warnung (Code-Bug)              |

Das ist die komplette Übersicht. Falls du noch ein konkretes Grafana-JSON-Dashboard auf Basis dieser Metriken möchtest, kann ich das aus den Queries ableiten — sag Bescheid.