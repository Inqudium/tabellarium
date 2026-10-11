package eu.inqudium.tabellarium

import org.apache.kafka.clients.producer.ProducerConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TopicClassTest {
    @Test
    fun `should keep every class's max block default at or below its cap`() {
        // What is to be tested? The invariant between the two max.block.ms
        //   facts a class carries: its default override and its cap.
        // How will the test case be deemed successful and why? Successful if
        //   every class has a numeric max.block.ms default that is at most
        //   its maxBlockMsCap. The builder fills the default first and then
        //   clamps to the cap, recording a MandatoryOverrideViolation for a
        //   value above it - so a default above the cap would log a spurious
        //   "overruled" warning on every start although the operator set
        //   nothing (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 11).
        // Why is it important to test this test case? The two numbers are
        //   edited in the same constructor call but nothing ties them
        //   together; the relation is a start-up property of every
        //   deployment, not of one configuration.

        // Given / When / Then
        TopicClass.entries.forEach { topicClass ->
            val default = topicClass.defaultOverrides[ProducerConfig.MAX_BLOCK_MS_CONFIG]?.toLongOrNull()
            assertThat(default).`as`("%s max.block.ms default", topicClass.name).isNotNull()
            assertThat(default!!).`as`("%s max.block.ms default vs. cap", topicClass.name).isLessThanOrEqualTo(topicClass.maxBlockMsCap)
        }
    }
}
