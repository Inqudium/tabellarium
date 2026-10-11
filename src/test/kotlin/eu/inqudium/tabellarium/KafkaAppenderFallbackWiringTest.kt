package eu.inqudium.tabellarium

import ch.qos.logback.core.Appender
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

/**
 * The appender-ref handling of [KafkaAppender]: how the single fallback
 * slot is filled, reported, detached and protected while the appender
 * is started.
 */
class KafkaAppenderFallbackWiringTest : KafkaAppenderTestBase() {
    @Nested
    inner class `Appender-ref handling` {
        @Test
        fun `should accept the first appender added via addAppender as the fallback`() {
            // What is to be tested? Whether the AppenderAttachable contract
            //   is wired such that Joran's <appender-ref> mechanism reaches
            //   the fallback slot.
            // How will the test case be deemed successful and why? Successful
            //   if calling addAppender(...) stores the appender as the
            //   fallback and a subsequent failed send routes the event
            //   through it. This pins down the path for the
            //   standard <appender-ref ref="..."/> Logback syntax.
            // Why is it important to test this test case? Without this,
            //   users could write the standard Logback XML and find that
            //   <appender-ref> silently has no effect - the worst kind of
            //   misconfiguration because nothing breaks loudly.

            // Given
            val fallback = RecordingAppender()
            val appender = newAppender(encoder = ThrowingEncoder())
            appender.addAppender(fallback)
            appender.start()

            // When: the hot path fails
            appender.doAppend(newTestLoggingEvent(message = "fallback me"))

            // Then: the fallback received the event
            pollUntil { fallback.events.size == 1 }
            assertThat(appender.fallbackAppender).isSameAs(fallback)
        }

        @Test
        fun `should warn and ignore subsequent appenders when more than one is attached`() {
            // What is to be tested? Whether the single-slot semantics is
            //   enforced when multiple <appender-ref> elements appear in
            //   the configuration.
            // How will the test case be deemed successful and why? Successful
            //   if the first addAppender call sets the fallback, the
            //   second emits a status warning identifying the ignored
            //   appender by name, and the fallback slot still holds the
            //   first appender. This pins down both the policy decision
            //   ("first wins") and its operator-visible signal.
            // Why is it important to test this test case? A silent acceptance
            //   of multiple appender-refs would leave operators unaware that
            //   their configuration intent is being overruled - exactly the
            //   class of silent compliance gap this whole refactor targets.

            // Given
            val first = RecordingAppender().apply { name = "FIRST" }
            val second = RecordingAppender().apply { name = "SECOND" }
            val appender = newAppender()

            // When
            appender.addAppender(first)
            appender.addAppender(second)

            // Then: first appender wins, warning identifies the ignored one
            assertThat(appender.fallbackAppender).isSameAs(first)
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("single fallback") && it.contains("SECOND") }
        }

        @Test
        fun `should report the attached appender via iteratorForAppenders and getAppender`() {
            // What is to be tested? Whether the AppenderAttachable read
            //   accessors - iteratorForAppenders, getAppender(name) and
            //   isAttached - reflect the single fallback slot consistently.
            // How will the test case be deemed successful and why? Successful
            //   if the iterator yields exactly the attached appender, lookup by
            //   its name returns the same instance, an unknown name yields null
            //   and isAttached is true. This pins the slot-as-collection view.
            // Why is it important to test this test case? Logback's
            //   configuration tooling and Joran walk appenders through these
            //   accessors; a slot that is invisible to them cannot be inspected
            //   or cleaned up by the framework.

            // Given
            val fallback = RecordingAppender().apply { name = "MY_FALLBACK" }
            val appender = newAppender()
            appender.addAppender(fallback)

            // When / Then
            assertThat(appender.iteratorForAppenders().asSequence().toList())
                .containsExactly(fallback)
            assertThat(appender.getAppender("MY_FALLBACK")).isSameAs(fallback)
            assertThat(appender.getAppender("UNKNOWN")).isNull()
            assertThat(appender.isAttached(fallback)).isTrue()
        }

        @Test
        fun `should detach the appender and free the slot when detachAppender is called`() {
            // What is to be tested? Whether detachAppender(instance) clears the
            //   fallback slot, reports success and makes the slot reusable for a
            //   subsequent addAppender.
            // How will the test case be deemed successful and why? Successful
            //   if detach returns true, fallbackAppender is null afterwards and
            //   a replacement can then be attached (rather than being ignored by
            //   the first-wins rule).
            // Why is it important to test this test case? Programmatic
            //   re-wiring of the fallback depends on detach actually freeing
            //   the slot; if it did not, every later addAppender would be
            //   silently ignored and the old, possibly stopped appender would
            //   keep receiving diverted events.

            // Given
            val fallback = RecordingAppender().apply { name = "FALLBACK" }
            val appender = newAppender()
            appender.addAppender(fallback)

            // When: detach by instance
            val detached = appender.detachAppender(fallback)

            // Then: slot is empty, detach reported success, slot is reusable
            assertThat(detached).isTrue()
            assertThat(appender.fallbackAppender).isNull()

            // And: a new appender can now be attached
            val replacement = RecordingAppender().apply { name = "REPLACEMENT" }
            appender.addAppender(replacement)
            assertThat(appender.fallbackAppender).isSameAs(replacement)
        }

        @Test
        fun `should stop and clear the fallback when detachAndStopAllAppenders is called before start`() {
            // What is to be tested? The not-started half of
            //   detachAndStopAllAppenders: the AppenderAttachable contract
            //   says "stop and detach", so before start() the slot's
            //   appender must be stopped and the slot cleared - and a second
            //   call on the empty slot must be a quiet no-op.
            // How will the test case be deemed successful and why? Successful
            //   if the fallback reports isStarted=false, the slot reads null,
            //   isAttached is false, and no refusal warning was emitted. Only
            //   the refused-while-started path was pinned before
            //   (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 14).
            // Why is it important to test this test case? Logback's
            //   configuration tooling calls this method to release
            //   appenders; a detach that left the fallback started would
            //   leak its file handles through every programmatic re-wiring.

            // Given
            val fallback = RecordingAppender().apply { name = "FALLBACK" }
            val appender = newAppender(fallback = fallback)
            assertThat(fallback.isStarted).isTrue()

            // When
            appender.detachAndStopAllAppenders()

            // Then: stopped and gone
            assertThat(fallback.isStarted).isFalse()
            assertThat(appender.fallbackAppender).isNull()
            assertThat(appender.isAttached(fallback)).isFalse()

            // And: a second call on the empty slot is a quiet no-op
            appender.detachAndStopAllAppenders()
            assertThat(appender.statusMessages()).noneMatch { it.contains("ignores detaching") }
        }

        @Test
        fun `should detach by name without stopping when detachAppender(name) is called before start`() {
            // What is to be tested? The by-name detach: only the attached
            //   appender's own name frees the slot (an unknown or null name
            //   is reported as "nothing detached"), and detaching does not
            //   stop the appender - that is detachAndStopAllAppenders' job.
            // How will the test case be deemed successful and why? Successful
            //   if an unknown and a null name return false and leave the slot
            //   intact, the matching name returns true and clears the slot,
            //   the appender is still started afterwards, and a repeated
            //   detach of the now-empty slot returns false. Only the
            //   instance variant was pinned before
            //   (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 14).
            // Why is it important to test this test case? Joran and
            //   programmatic re-wiring address appenders by name; a by-name
            //   detach that matched too widely or stopped the appender would
            //   differ from the instance variant without any signal.

            // Given
            val fallback = RecordingAppender().apply { name = "FALLBACK" }
            val appender = newAppender(fallback = fallback)

            // When / Then: a foreign or null name detaches nothing
            assertThat(appender.detachAppender("OTHER")).isFalse()
            assertThat(appender.detachAppender(null as String?)).isFalse()
            assertThat(appender.fallbackAppender).isSameAs(fallback)

            // And: the matching name frees the slot without stopping the appender
            assertThat(appender.detachAppender("FALLBACK")).isTrue()
            assertThat(appender.fallbackAppender).isNull()
            assertThat(fallback.isStarted).isTrue()
            assertThat(appender.detachAppender("FALLBACK")).isFalse()
            fallback.stop()
        }

        @Test
        fun `should refuse to detach the fallback while started and keep delivering to it`() {
            // What is to be tested? Whether the three detach paths
            //   (detachAppender by instance, by name, and
            //   detachAndStopAllAppenders) are refused on a started appender:
            //   the running transport read the slot once at start(), so a
            //   detach would only change the public slot - the accessors
            //   would report "no fallback" for a pipeline still delivering
            //   to one, and detachAndStopAllAppenders would stop an appender
            //   the dispatcher still targets.
            // How will the test case be deemed successful and why? Successful
            //   if every detach returns false or leaves the slot untouched,
            //   a status warning names the refusal, the fallback is still
            //   started, and an event diverted afterwards still reaches it.
            //   Before the fix the slot was nulled and the stopped fallback
            //   swallowed the diversions as delivered
            //   (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 7).
            // Why is it important to test this test case? Silent loss on the
            //   path whose purpose is visible loss, plus a lying accessor -
            //   bounded to programmatic re-wiring, which is exactly the code
            //   that reads the accessors.

            // Given: a started appender whose hot path diverts everything
            val fallback = RecordingAppender().apply { name = "FALLBACK" }
            val appender = newAppender(encoder = ThrowingEncoder(), fallback = fallback)
            appender.start()

            // When: every detach path is tried while started
            val byInstance = appender.detachAppender(fallback)
            val byName = appender.detachAppender("FALLBACK")
            appender.detachAndStopAllAppenders()

            // Then: refused, warned, slot intact, fallback alive and still fed
            assertThat(byInstance).isFalse()
            assertThat(byName).isFalse()
            assertThat(appender.fallbackAppender).isSameAs(fallback)
            assertThat(appender.isAttached(fallback)).isTrue()
            assertThat(fallback.isStarted).isTrue()
            assertThat(appender.statusMessages().filter { it.contains("ignores detaching its fallback appender while started") })
                .hasSize(3)
            appender.doAppend(newTestLoggingEvent(message = "diverted after refused detach"))
            appender.stop()
            assertThat(fallback.events.map { it.formattedMessage }).containsExactly("diverted after refused detach")
        }

        @Test
        fun `should refuse to attach a fallback while started and keep the drop policy`() {
            // What is to be tested? The mirror image of the refused detach:
            //   addAppender on a started appender that began without a
            //   fallback. The transport read the empty slot once at
            //   start(), so accepting the appender would only change the
            //   public slot - the accessors would report a fallback the
            //   pipeline never delivers to, and stop() would stop an
            //   appender that never received an event.
            // How will the test case be deemed successful and why? Successful
            //   if the slot stays empty, a status warning names the refused
            //   appender and the missing wiring, an event diverted
            //   afterwards is dropped (the documented policy of a start
            //   without fallback) rather than delivered, and stop() leaves
            //   the never-attached appender started. Before the fix the
            //   slot was filled while the transport kept dropping
            //   (docs/assessment/DEFECT_ANALYSIS-2026-09-15T22-05-50.md, L-1).
            // Why is it important to test this test case? A lying accessor
            //   on the path whose purpose is visible loss - and a
            //   programmatic re-wiring is exactly the code that reads it.

            // Given: a started appender without a fallback whose hot path diverts everything
            val appender = newAppender(encoder = ThrowingEncoder())
            appender.start()
            val lateFallback = RecordingAppender().apply { name = "LATE_FALLBACK" }
            lateFallback.start()

            // When: a fallback is attached while started
            appender.addAppender(lateFallback)

            // Then: refused, warned, slot empty, diversions still dropped
            assertThat(appender.fallbackAppender).isNull()
            assertThat(appender.isAttached(lateFallback)).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch {
                    it.contains("ignores attaching fallback appender 'LATE_FALLBACK' while started") &&
                        it.contains("without a fallback")
                }
            appender.doAppend(newTestLoggingEvent(message = "diverted after refused attach"))
            appender.stop()
            assertThat(lateFallback.events).isEmpty()
            assertThat(lateFallback.isStarted).isTrue()
            lateFallback.stop()
        }

        @Test
        fun `should name the wired fallback when refusing to attach a second one while started`() {
            // What is to be tested? The warning's second variant: a started
            //   appender that already delivers to a fallback names that
            //   fallback when a further attach is refused, so the operator
            //   reads which appender the pipeline actually targets.
            // How will the test case be deemed successful and why? Successful
            //   if the slot still holds the original fallback and the
            //   warning names both appenders.
            // Why is it important to test this test case? The lifecycle
            //   refusal replaces the "single fallback" warning on a started
            //   appender; without the target's name the message would say
            //   less than the one it replaces.

            // Given
            val wired = RecordingAppender().apply { name = "WIRED" }
            val appender = newAppender(fallback = wired)
            appender.start()

            // When
            appender.addAppender(RecordingAppender().apply { name = "SECOND" })

            // Then
            assertThat(appender.fallbackAppender).isSameAs(wired)
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("attaching fallback appender 'SECOND' while started") && it.contains("to 'WIRED'") }
        }
    }
}
