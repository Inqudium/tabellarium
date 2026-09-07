package eu.inqudium.tabellarium

/**
 * Runs independent close tasks concurrently and waits for all of them
 * within one overall budget - the shape both parallel teardowns of the
 * pipeline need ([ProducerRegistry.close] for the producers,
 * [KafkaAppender.stop] for the send dispatchers), written once. A
 * top-level function, like the module's other stateless helper
 * ([parseKafkaProducerProperties]).
 *
 * Rationale: closing N resources sequentially stacks their individual
 * timeouts (`N × timeout`, up to 40 s for four producers), which
 * overruns a Kubernetes `terminationGracePeriodSeconds: 30` and gets
 * the later closes killed mid-flight. One closer thread per resource
 * with a single shared deadline keeps the total at one timeout plus
 * margin regardless of N.
 *
 * Each task runs on its own daemon thread and must handle its own
 * exceptions (report, collect) - an exception escaping a task only
 * ends that thread. An interrupt of the calling thread ends the wait
 * early, leaves the daemon closers to finish on their own, and is
 * restored before returning. CAUTION: `Thread.join(0)` means "wait
 * forever" - the loop stops before the remaining budget reaches zero.
 *
 * @param budgetMs Overall wait for all tasks together.
 * @param tasks Thread name to task; names should be distinct for
 *              thread dumps, the order is the join order.
 */
internal fun closeInParallel(
    budgetMs: Long,
    tasks: List<Pair<String, () -> Unit>>,
) {
    if (tasks.isEmpty()) return
    val closers =
        tasks.map { (name, task) ->
            Thread(task, name).apply {
                isDaemon = true
                start()
            }
        }
    var interrupted = false
    val deadlineNanos = System.nanoTime() + budgetMs * 1_000_000
    for (closer in closers) {
        val remainingMs = (deadlineNanos - System.nanoTime()) / 1_000_000
        if (remainingMs <= 0) break
        try {
            closer.join(remainingMs)
        } catch (_: InterruptedException) {
            interrupted = true
            break
        }
    }
    if (interrupted) {
        Thread.currentThread().interrupt()
    }
}
