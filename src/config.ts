export interface MemoryBudget {
	task_memory_bytes: number;
	/** What the harness itself keeps: event loop, buffers, the artifact index. */
	harness_reserve_bytes: number;
	/** Concurrent jobs. The worker reserves a slot at receive time, so busy children never exceed this. */
	slots: number;
	/** RLIMIT_DATA handed to each validator child for the duration of a job. */
	memory_limit_per_job_bytes: number;
	/** Resident runner processes, busy plus idle. */
	max_children: number;
	/** What a runner parent holds after importing its library: measured per version, estimated here. */
	idle_child_rss_bytes: number;
}

/**
 * Fargate has no per-process cgroups, so the aggregate is arithmetic the harness does once at start.
 * Every resident runner keeps its imported library in memory whether it is busy or idle, and every slot
 * can be running a job child at its limit on top of that; copy-on-write shares pages at fork but gives
 * no bound once the child allocates. So: the harness, plus every parent, plus every slot's job limit.
 */
export function check_memory_budget(budget: MemoryBudget): { worst_case_bytes: number; headroom_bytes: number } {
	if (budget.max_children < budget.slots) throw new Error("max_children must be at least slots, or a slot could have no child to run in");
	const worst_case_bytes =
		budget.harness_reserve_bytes + budget.max_children * budget.idle_child_rss_bytes + budget.slots * budget.memory_limit_per_job_bytes;
	const headroom_bytes = budget.task_memory_bytes - worst_case_bytes;
	if (headroom_bytes < 0) {
		throw new Error(`memory budget exceeds the task by ${-headroom_bytes} bytes: harness ${budget.harness_reserve_bytes} + ${budget.max_children} parents x ${budget.idle_child_rss_bytes} + ${budget.slots} slots x ${budget.memory_limit_per_job_bytes} > ${budget.task_memory_bytes}`);
	}
	return { worst_case_bytes, headroom_bytes };
}
