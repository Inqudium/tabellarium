package eu.inqudium.tabellarium

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * Pins the assumption behind the self-logging guard
 * ([KafkaTransport.isOwnProducerThread]) to the Kafka client version
 * the build actually uses: the producer names its network thread
 * `<KafkaProducer.NETWORK_THREAD_PREFIX> | <client.id>`. The prefix is
 * public API and referenced directly; the separator is a literal inside
 * the `KafkaProducer` constructor with no constant to reference. This
 * test observes a real producer's thread - no broker is needed, the
 * constructor starts the thread without connecting - so a client
 * upgrade that changes the scheme fails here, on the dependency-update
 * PR, instead of silently letting the producer's own logging loop back
 * into the appender.
 */
class KafkaProducerThreadNamingContractTest {
    @Test
    fun `should name the producer network thread prefix plus separator plus client id`() {
        // What is to be tested? Whether a real KafkaProducer of the built
        //   client version names its network thread exactly as the guard's
        //   PRODUCER_NETWORK_THREAD_PREFIX plus the client.id expects.
        // How will the test case be deemed successful and why? Successful
        //   if exactly one live thread carries the client.id in its name and
        //   that name equals the prefix constant followed by the client.id -
        //   the string isOwnProducerThread would receive from such a
        //   thread's log events.
        // Why is it important to test this test case? The guard exists to
        //   stop a feedback loop that amplifies exactly during broker
        //   trouble. A silent naming change in the client would disable it
        //   without any other test noticing; this one turns that change
        //   into a red build on the client upgrade.

        // Given: a client.id unlikely to appear in any other thread name
        val clientId = "tabellarium-thread-naming-contract-${System.nanoTime()}"
        val producer =
            KafkaProducer(
                mapOf<String, Any>(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:1",
                    ProducerConfig.CLIENT_ID_CONFIG to clientId,
                ),
                ByteArraySerializer(),
                ByteArraySerializer(),
            )
        try {
            // When: the constructor has started the network thread
            val matching =
                Thread
                    .getAllStackTraces()
                    .keys
                    .map { it.name }
                    .filter { clientId in it }

            // Then
            assertThat(matching).containsExactly(KafkaTransport.PRODUCER_NETWORK_THREAD_PREFIX + clientId)
        } finally {
            producer.close(Duration.ZERO)
        }
    }
}
