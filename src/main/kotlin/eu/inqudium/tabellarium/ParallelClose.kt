package eu.inqudium.tabellarium

/**
 * Runs independent close tasks concurrently and waits for all of them
 * within one overall budget - the shape both parallel teardowns of the
 * pipeline need, written once. A top-level function, like the module's
 * other stateless helper ([parseKafkaProducerProperties]).
 *
 * Rationale: closing N resources sequentially stacks their individual
 * timeouts (`N × timeout` - four producers at
 * [ProducerRegistry.DEFAULT_CLOSE_TIMEOUT] each), which
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
 * Safety: a task whose closer thread cannot be created (thread
 * exhaustion at shutdown throws `OutOfMemoryError: unable to create
 * native thread`) runs on the calling thread instead, after the
 * closers that did start. Every task is itself bounded, so the
 * degraded mode stacks budgets but still ends; the alternative - the
 * `Error` escaping the teardown - would skip every close that follows
 * this one and leak producers, workers and registry entries, the
 * symmetric case of the rollback contract `KafkaTransport.open` keeps
 * for the same `Error` (`DEFECT_ANALYSIS-2026-10-10T19-11-11`,
 * finding 8).
 *
 * @param budgetMs Overall wait for all tasks together.
 * @param tasks Thread name to task; names should be distinct for
 *              thread dumps, the order is the join order.
 * @param startCloser Creates and starts the daemon thread for one
 *                    task. Production keeps the default; it is the
 *                    seam that lets `ParallelCloseTest` model the
 *                    thread-creation failure.
 */
internal fun closeInParallel(
    budgetMs: Long,
    tasks: List<Pair<String, () -> Unit>>,
    startCloser: (name: String, task: () -> Unit) -> Thread = ::startDaemonCloser,
) {
    if (tasks.isEmpty()) return
    val closers = ArrayList<Thread>(tasks.size)
    val inline = ArrayList<() -> Unit>()
    for ((name, task) in tasks) {
        try {
            closers += startCloser(name, task)
        } catch (_: OutOfMemoryError) {
            inline += task
        }
    }
    // Rationale: the started closers are already running; the tasks
    // without a thread run here while they do, so the overall wait
    // below still covers the parallel ones.
    inline.forEach { task -> task() }
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

/** The default [closeInParallel] closer: a started daemon thread named [name] running [task]. */
private fun startDaemonCloser(
    name: String,
    task: () -> Unit,
): Thread =
    Thread(task, name).apply {
        isDaemon = true
        start()
    }
