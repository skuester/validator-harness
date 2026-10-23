import crypto from "node:crypto";
import type { ArtifactStore } from "./artifact_store";
import type { Finalizer, Queue, QueueMessage, Registry } from "./ports";
import type { ProcessPool, RunnerChild } from "./process_pool";
import type { ArtifactSpec, EventListener, Execution, InstalledArtifact, JobMessage, Lane, RunOutcome } from "./types";

export interface WorkerConfig {
	lane: Lane;
	/** Concurrent jobs; reserved at receive time so the worker never holds a message it cannot start (rev 3.1). */
	slots: number;
	/** 2 s on the fast lane, 300 s on the slow lane. */
	budget_ms: number;
	memory_limit_per_job_bytes: number;
	receive_wait_ms: number;
	/**
	 * Jobs this task holds at once until the hot list is warm. A task that is still importing libraries
	 * keeps most of the backlog visible to tasks that can answer in 300 ms; a receive makes work invisible.
	 */
	warmup_max_jobs: number;
	prewarm: boolean;
	prewarm_concurrency: number;
}

export interface WorkerDeps {
	queue: Queue;
	registry: Registry;
	artifacts: ArtifactStore;
	pool: ProcessPool;
	finalizer: Finalizer;
	config: WorkerConfig;
	clock?: () => number;
	on_event?: EventListener;
}

function sha256_hex(text: string): string {
	return crypto.createHash("sha256").update(text, "utf8").digest("hex");
}

/**
 * The lane worker: receives from the queue, hands each job to a runner process, and finalizes the record.
 * Prewarming runs beside the receive loop, never ahead of it: a request for a version that is mid-prefetch
 * joins that download, and everything else is served cold or warm as the cache happens to stand.
 */
export class Worker {
	private reserved = 0;
	private stopping = false;
	private _warm = false;
	private slot_waiters: Array<() => void> = [];
	private readonly in_flight = new Set<Promise<void>>();
	private prewarm_run: Promise<void> | undefined;
	private readonly clock: () => number;

	constructor(private readonly deps: WorkerDeps) {
		this.clock = deps.clock ?? Date.now;
	}

	/** True once the prewarm pass is over: every hot version was tried, and the ones that installed are resident as room allowed. */
	get warm(): boolean {
		return this._warm;
	}

	/** The receive loop. Resolves after `stop()` once in-flight jobs have drained. */
	async run(): Promise<void> {
		const { queue, config } = this.deps;
		if (config.prewarm) this.prewarm_run = this.prewarm();
		else this._warm = true;
		while (!this.stopping) {
			const capacity = this._warm ? config.slots : Math.min(config.slots, config.warmup_max_jobs);
			const free = capacity - this.reserved;
			if (free <= 0) {
				await this.slot_released();
				continue;
			}
			const messages = await queue.receive(free, config.receive_wait_ms);
			for (const message of messages) {
				this.reserved += 1;
				const task: Promise<void> = this.handle(message)
					.catch((error) => this.emit({ type: "handler_error", job: message.body.job_id, error: String(error) }))
					.finally(() => {
						this.reserved -= 1;
						this.in_flight.delete(task);
						this.notify_slot();
					});
				this.in_flight.add(task);
			}
		}
		await Promise.allSettled([...this.in_flight]);
		if (this.prewarm_run) await this.prewarm_run.catch(() => undefined);
	}

	/** Stop receiving; `run()` returns once in-flight jobs finish. Slow jobs keep running under their lease. */
	stop(): void {
		this.stopping = true;
		this.notify_slot();
	}

	/**
	 * The hand-off, step by step. Steps 3 and 4 are where a cold version costs time: a download and an import
	 * on the first job for that version on this task, and nothing on the ones after it.
	 */
	private async handle(message: QueueMessage): Promise<void> {
		const { registry, artifacts, pool, finalizer, config } = this.deps;
		const job = message.body;
		const received_at = this.clock();
		this.emit({ type: "job_received", job: job.job_id, version: job.client_version, lane: job.lane });

		// 1. Checks that need no runner: deadline, the registry row, and the hash the API put on the record.
		const rejection = this.reject_before_running(job, registry.resolve(job.client_version));
		if (rejection) {
			await finalizer.finalize(job, { outcome: "failed", error: { code: rejection } });
			await this.deps.queue.ack(message);
			return;
		}
		const spec = registry.resolve(job.client_version) as ArtifactSpec;

		// 2. Artifact on disk. Cache hit, join the prefetch already downloading it, or fetch it now at request priority.
		let artifact: InstalledArtifact;
		try {
			artifact = await artifacts.ensure(spec, { priority: "request" });
		} catch (error) {
			// Infrastructure, not the job: leave the message unacked so the visibility timeout redelivers it elsewhere.
			this.emit({ type: "job_deferred", job: job.job_id, version: job.client_version, error: String(error) });
			return;
		}
		const artifact_ms = this.clock() - received_at;

		// 3. Hold the artifact against eviction until the runner process has it, then run.
		artifacts.hold(artifact.id);
		let child: RunnerChild;
		let outcome: RunOutcome | undefined;
		let execution: Execution;
		try {
			child = await pool.acquire(artifact);
			execution = { artifact_id: artifact.id, version: artifact.version, build_id: artifact.build_id, sha256: spec.sha256, runner_pid: child.handle?.pid, child: child.id };
			try {
				// 4. The runner forks a child under the job's memory and time limits and streams progress back.
				outcome = await child.handle!.run(
					{ job_id: job.job_id, simulation: job.simulation, budget_ms: config.budget_ms, memory_limit_bytes: config.memory_limit_per_job_bytes },
					(progress) => finalizer.publish_progress(job, progress),
				);
			} finally {
				// 5. Give the process back. A validator crash is reported by a healthy runner and the process stays warm;
				//    only a runner that died or hung is recycled.
				await pool.release(child, { crashed: child.handle?.exited || outcome?.outcome === "runner_unresponsive" });
			}
		} finally {
			artifacts.release_hold(artifact.id);
		}
		this.emit({ type: "job_run", job: job.job_id, version: job.client_version, build: artifact.build_id, outcome: outcome.outcome, artifact_ms, acquire_ms: this.clock() - received_at - artifact_ms, run_ms: outcome.elapsed_ms, child: child.id });

		// 6. Terminal write with what ran it, then ack. A fast-lane overrun is not a result; it is handed to the slow lane.
		if (outcome.outcome === "timeout" && job.lane === "fast") await finalizer.escalate(job, execution);
		else await finalizer.finalize(job, outcome, execution);
		await this.deps.queue.ack(message);
	}

	/** The failure code for a job that must not reach a runner, or undefined when it may. */
	private reject_before_running(job: JobMessage, spec: ArtifactSpec | undefined): string | undefined {
		if (Date.parse(job.deadline_at) <= Date.now()) return "expired";
		if (!spec) return "version_unknown";
		if (spec.status === "retired") return "version_retired";
		if (job.simulation_sha256 && sha256_hex(job.simulation) !== job.simulation_sha256) return "simulation_hash_mismatch";
		return undefined;
	}

	/** Install the hot list, a couple at a time at prefetch priority, and spawn an idle child for each while there is room. */
	private async prewarm(): Promise<void> {
		const { registry, artifacts, pool, config } = this.deps;
		const specs = registry.hot_versions();
		this.emit({ type: "prewarm_start", versions: specs.map((spec) => spec.version) });
		const started = this.clock();
		let next = 0;
		let failed = 0;
		const lanes = Array.from({ length: Math.max(1, config.prewarm_concurrency) }, async () => {
			while (next < specs.length && !this.stopping) {
				const spec = specs[next++];
				try {
					const artifact = await artifacts.ensure(spec, { priority: "prefetch" });
					const spawned = await pool.prewarm(artifact);
					this.emit({ type: "prewarm_version", version: spec.version, spawned });
				} catch (error) {
					failed += 1;
					this.emit({ type: "prewarm_failed", version: spec.version, error: String(error) });
				}
			}
		});
		await Promise.all(lanes);
		this._warm = true;
		this.emit({ type: "warm", elapsed_ms: this.clock() - started, children: pool.snapshot().length, failed });
	}

	private slot_released(): Promise<void> {
		return new Promise((resolve) => this.slot_waiters.push(resolve));
	}

	private notify_slot(): void {
		for (const resolve of this.slot_waiters.splice(0)) resolve();
	}

	private emit(event: { type: string; [key: string]: unknown }): void {
		this.deps.on_event?.({ component: "worker", ...event });
	}
}
