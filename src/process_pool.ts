import { LruIndex } from "./lru";
import type { RunnerHandle } from "./runner_endpoint";
import type { SpawnRunner } from "./runner_spawn";
import type { ArtifactRef, EventListener } from "./types";

export type ChildState = "starting" | "idle" | "busy" | "stopping";

/** One warm runner process. `starting` counts toward capacity so two acquires cannot both spawn into the last slot. */
export class RunnerChild {
	state: ChildState = "starting";
	jobs_done = 0;
	last_used_at: number;
	handle: RunnerHandle | undefined;

	constructor(readonly id: number, readonly artifact: ArtifactRef, readonly started_at: number) {
		this.last_used_at = started_at;
	}

	get version(): string {
		return this.artifact.version;
	}
}

export interface ProcessPoolOptions {
	/** Resident runner processes, busy plus idle. Set to slots plus spares; memory is budgeted from it (see config.ts). */
	max_children: number;
	spawn: SpawnRunner;
	/** Hot versions: prefetched on start, evicted only when no other idle child exists, respawned in the background after a recycle. */
	is_pinned?: (version: string) => boolean;
	/** Recycle a child after this many jobs. With fork-per-job the parent never runs validator code, so this is a bound, not a fix. */
	max_jobs_per_child?: number;
	stop_grace_ms?: number;
	clock?: () => number;
	on_event?: EventListener;
}

export interface ReleaseOptions {
	recycle?: boolean;
	crashed?: boolean;
}

/**
 * The warm-process table. Children are keyed by artifact (version plus build), ordered by last use.
 *
 * Eviction is LRU with three rules on top of plain recency: never a busy or starting child, non-pinned
 * before pinned, and a victim is removed from the table before its teardown is awaited, so the job that
 * needed the slot waits on the replacement's `ready`, not on the old process's exit.
 *
 * A child spawned for an acquire belongs to that acquire the moment it is ready: it is marked busy before
 * any waiter is woken. Without that, two acquires contending for the last slot evict each other's new
 * runner forever.
 */
export class ProcessPool {
	private readonly children = new LruIndex<number, RunnerChild>();
	private readonly spawning = new Map<string, Promise<RunnerChild>>();
	private readonly stopping = new Map<RunnerChild, Promise<void>>();
	private waiters: Array<() => void> = [];
	private next_id = 1;
	private closed = false;
	private readonly clock: () => number;
	private readonly max_children: number;
	private readonly is_pinned: (version: string) => boolean;

	constructor(private readonly opts: ProcessPoolOptions) {
		if (opts.max_children < 1) throw new Error("max_children must be at least 1");
		this.max_children = opts.max_children;
		this.clock = opts.clock ?? Date.now;
		this.is_pinned = opts.is_pinned ?? (() => false);
	}

	get size(): number {
		return this.children.size;
	}

	count(state: ChildState): number {
		return this.children.values().filter((child) => child.state === state).length;
	}

	/** Artifact ids with a resident child, including ones still shutting down, for the artifact cache's eviction rule. */
	live_ids(): Set<string> {
		const ids = new Set(this.children.values().map((child) => child.artifact.id));
		for (const child of this.stopping.keys()) ids.add(child.artifact.id);
		return ids;
	}

	snapshot(): Array<{ id: number; version: string; state: ChildState; jobs_done: number; pid: number | undefined }> {
		return this.children.values().map((child) => ({ id: child.id, version: child.version, state: child.state, jobs_done: child.jobs_done, pid: child.handle?.pid }));
	}

	/**
	 * Get a child for this artifact, marked busy. Warm if one is idle; otherwise spawn one that is ours on arrival,
	 * evicting the least recently used idle child if the table is full. Waits only when every child is busy or starting.
	 */
	async acquire(artifact: ArtifactRef): Promise<RunnerChild> {
		const started = this.clock();
		let cold = false;
		for (;;) {
			if (this.closed) throw new Error("pool is closed");
			const idle = this.find_idle(artifact.id);
			if (idle) {
				idle.state = "busy";
				idle.last_used_at = this.clock();
				this.children.touch(idle.id);
				this.emit({ type: "acquire", version: artifact.version, child: idle.id, cold, wait_ms: this.clock() - started });
				return idle;
			}
			cold = true;
			const in_flight = this.spawning.get(artifact.id);
			if (in_flight) {
				// Someone else's spawn of this artifact; when it lands it is theirs. Loop and take our own turn.
				await in_flight.catch(() => undefined);
				continue;
			}
			if (this.children.size < this.max_children) {
				const child = await this.spawn(artifact, "busy");
				this.emit({ type: "acquire", version: artifact.version, child: child.id, cold, wait_ms: this.clock() - started });
				return child;
			}
			const victim = this.pick_victim();
			if (victim) {
				void this.stop_child(victim, "evicted");
				continue;
			}
			this.emit({ type: "acquire_wait", version: artifact.version });
			await this.next_change();
		}
	}

	/** Give the child back. Recycles it when asked, when its process died, or when it has done enough jobs. */
	async release(child: RunnerChild, opts: ReleaseOptions = {}): Promise<void> {
		if (!this.children.has(child.id)) return;
		child.jobs_done += 1;
		child.last_used_at = this.clock();
		this.children.touch(child.id);
		const worn_out = this.opts.max_jobs_per_child !== undefined && child.jobs_done >= this.opts.max_jobs_per_child;
		const recycle = opts.recycle || opts.crashed || child.handle?.exited || worn_out;
		if (recycle) {
			const stopped = this.stop_child(child, opts.crashed ? "crashed" : "recycled");
			this.replace_if_pinned(child.artifact);
			await stopped;
		} else {
			child.state = "idle";
		}
		this.notify_change();
	}

	/** Spawn an idle child for an artifact if there is room. Never evicts: warming up must not push out working children. */
	async prewarm(artifact: ArtifactRef): Promise<boolean> {
		if (this.closed) return false;
		if (this.find_idle(artifact.id) || this.spawning.has(artifact.id)) return false;
		if (this.children.size >= this.max_children) return false;
		await this.spawn(artifact, "idle");
		return true;
	}

	/** Disk pressure: stop this artifact's idle children. True when nothing of it is left running. */
	async evict_idle_for(artifact_id: string): Promise<boolean> {
		const idle = this.children.values().filter((child) => child.artifact.id === artifact_id && child.state === "idle");
		await Promise.all(idle.map((child) => this.stop_child(child, "disk_pressure")));
		return !this.live_ids().has(artifact_id);
	}

	/** Close the pool: finish any spawn in flight so it can be stopped, then stop everything and wait for the exits. */
	async shutdown(): Promise<void> {
		this.closed = true;
		await Promise.allSettled([...this.spawning.values()]);
		const stops = this.children.values().map((child) => this.stop_child(child, "shutdown"));
		await Promise.allSettled([...stops, ...this.stopping.values()]);
		this.notify_change();
	}

	private find_idle(artifact_id: string): RunnerChild | undefined {
		return this.children.newest((child) => child.state === "idle" && child.artifact.id === artifact_id)?.[1];
	}

	private pick_victim(): RunnerChild | undefined {
		const unpinned = this.children.oldest((child) => child.state === "idle" && !this.is_pinned(child.version));
		if (unpinned) return unpinned[1];
		return this.children.oldest((child) => child.state === "idle")?.[1];
	}

	private spawn(artifact: ArtifactRef, on_ready: "idle" | "busy"): Promise<RunnerChild> {
		const child = new RunnerChild(this.next_id++, artifact, this.clock());
		this.children.set(child.id, child);
		this.emit({ type: "spawn", version: artifact.version, child: child.id });
		const pending = (async () => {
			let handle: RunnerHandle;
			try {
				handle = await this.opts.spawn(artifact);
			} catch (error) {
				this.children.delete(child.id);
				this.emit({ type: "spawn_failed", version: artifact.version, child: child.id, error: String(error) });
				throw error;
			} finally {
				this.spawning.delete(artifact.id);
			}
			if (this.closed || child.state === "stopping") {
				// Shutdown or an eviction raced the spawn: the process exists but has no place in the table.
				this.children.delete(child.id);
				await handle.stop(this.opts.stop_grace_ms ?? 2_000);
				this.notify_change();
				throw new Error(`pool closed while spawning ${artifact.version}`);
			}
			child.handle = handle;
			child.state = on_ready;
			child.last_used_at = this.clock();
			this.children.touch(child.id);
			handle.on_exit((code, signal) => this.on_child_exit(child, code, signal));
			this.emit({ type: "ready", version: artifact.version, child: child.id, spawn_ms: this.clock() - child.started_at, pid: handle.pid });
			this.notify_change();
			return child;
		})();
		pending.catch(() => undefined);
		this.spawning.set(artifact.id, pending);
		return pending;
	}

	private stop_child(child: RunnerChild, reason: string): Promise<void> {
		const was = child.state;
		child.state = "stopping";
		this.children.delete(child.id);
		this.emit({ type: "stop", version: child.version, child: child.id, reason, jobs_done: child.jobs_done, was });
		const stopped = (child.handle?.stop(this.opts.stop_grace_ms ?? 2_000) ?? Promise.resolve()).finally(() => {
			this.stopping.delete(child);
			this.notify_change();
		});
		this.stopping.set(child, stopped);
		this.notify_change();
		return stopped;
	}

	private on_child_exit(child: RunnerChild, code: number | null, signal: NodeJS.Signals | null): void {
		if (child.state === "stopping") return;
		this.children.delete(child.id);
		this.emit({ type: "exit", version: child.version, child: child.id, was: child.state, code, signal });
		this.notify_change();
		this.replace_if_pinned(child.artifact);
	}

	private replace_if_pinned(artifact: ArtifactRef): void {
		if (this.closed || !this.is_pinned(artifact.version)) return;
		const resident = this.children.values().some((child) => child.artifact.id === artifact.id);
		if (resident || this.spawning.has(artifact.id)) return;
		this.prewarm(artifact).catch((error) => this.emit({ type: "respawn_failed", version: artifact.version, error: String(error) }));
	}

	private next_change(): Promise<void> {
		return new Promise((resolve) => this.waiters.push(resolve));
	}

	private notify_change(): void {
		for (const resolve of this.waiters.splice(0)) resolve();
	}

	private emit(event: { type: string; [key: string]: unknown }): void {
		this.opts.on_event?.({ component: "pool", ...event });
	}
}
