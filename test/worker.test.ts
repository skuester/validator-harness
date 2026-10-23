import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ArtifactStore } from "../src/artifact_store";
import { build_demo_artifact } from "../src/demo_artifacts";
import { detect_host } from "../src/host";
import { MemoryQueue, MemoryRegistry, RecordingFinalizer } from "../src/memory_adapters";
import { ProcessPool } from "../src/process_pool";
import { make_runner_spawner } from "../src/runner_spawn";
import { LocalDirectorySource, type ArtifactSource } from "../src/sources";
import type { ArtifactSpec, HarnessEvent, JobMessage } from "../src/types";
import { Worker, type WorkerConfig } from "../src/worker";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function wait_until(condition: () => boolean, timeout_ms = 15_000): Promise<void> {
	const deadline = Date.now() + timeout_ms;
	while (!condition()) {
		if (Date.now() > deadline) throw new Error("condition not met in time");
		await sleep(20);
	}
}

let sequence = 0;
function job(version: string, simulation: Record<string, unknown>, lane: "fast" | "slow" = "fast", overrides: Partial<JobMessage> = {}): JobMessage {
	const now = new Date();
	const body = JSON.stringify(simulation);
	return {
		schema: "validator.job.v1",
		job_id: `job-${++sequence}`,
		tenant_id: "t_test",
		client_version: version,
		lane,
		created_at: now.toISOString(),
		deadline_at: new Date(now.getTime() + 60_000).toISOString(),
		simulation: body,
		simulation_sha256: crypto.createHash("sha256").update(body, "utf8").digest("hex"),
		...overrides,
	};
}

/** A source that can be slowed per key, to keep the hot version cold for a while. */
class SlowSource implements ArtifactSource {
	fetches = 0;
	delays = new Map<string, number>();
	constructor(private readonly inner: LocalDirectorySource) {}
	async fetch(key: string, destination: string): Promise<void> {
		this.fetches += 1;
		const delay = this.delays.get(key);
		if (delay) await sleep(delay);
		await this.inner.fetch(key, destination);
	}
}

interface Rig {
	root: string;
	specs: ArtifactSpec[];
	source: SlowSource;
	registry: MemoryRegistry;
	store: ArtifactStore;
	pool: ProcessPool;
	queue: MemoryQueue;
	finalizer: RecordingFinalizer;
	worker: Worker;
	events: HarnessEvent[];
	running: Promise<void>;
	stop: () => Promise<void>;
}

async function rig(opts: { max_children: number; config?: Partial<WorkerConfig>; delay_hot_ms?: number }): Promise<Rig> {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "vh-w-"));
	const artifacts_dir = path.join(root, "a");
	const specs = [
		await build_demo_artifact({ version: "2.7.3", build_id: "b1", out_dir: artifacts_dir, rank: 1 }),
		await build_demo_artifact({ version: "2.4.9", build_id: "b1", out_dir: artifacts_dir }),
	];
	const events: HarnessEvent[] = [];
	const on_event = (event: HarnessEvent) => {
		events.push(event);
	};
	const source = new SlowSource(new LocalDirectorySource(artifacts_dir));
	if (opts.delay_hot_ms) source.delays.set(specs[0].key, opts.delay_hot_ms);
	const registry = new MemoryRegistry(specs);
	let pool: ProcessPool;
	const store = new ArtifactStore({ root: path.join(root, "c"), max_bytes: 64 * 1024 * 1024, source, host: detect_host(), is_live: (id) => pool.live_ids().has(id), make_evictable: (id) => pool.evict_idle_for(id), on_event });
	await store.open();
	pool = new ProcessPool({
		max_children: opts.max_children,
		spawn: make_runner_spawner({ run_dir: path.join(root, "r"), stderr: "inherit" }),
		is_pinned: (version) => registry.resolve(version)?.rank !== undefined,
		on_event,
	});
	const queue = new MemoryQueue();
	const finalizer = new RecordingFinalizer(on_event);
	const worker = new Worker({
		queue,
		registry,
		artifacts: store,
		pool,
		finalizer,
		on_event,
		config: {
			lane: "fast",
			slots: 2,
			budget_ms: 1_000,
			memory_limit_per_job_bytes: 1024 * 1024 * 1024,
			receive_wait_ms: 50,
			warmup_max_jobs: 1,
			prewarm: true,
			prewarm_concurrency: 2,
			...opts.config,
		},
	});
	const running = worker.run();
	const stop = async () => {
		worker.stop();
		queue.close();
		await running;
		await pool.shutdown();
		await store.close();
		fs.rmSync(root, { recursive: true, force: true });
	};
	return { root, specs, source, registry, store, pool, queue, finalizer, worker, events, running, stop };
}

describe("Worker with real runner processes", () => {
	test("prewarms the hot version in the background and leaves legacy versions alone", async () => {
		const r = await rig({ max_children: 2 });
		try {
			await wait_until(() => r.worker.warm && r.pool.count("idle") === 1);
			expect(r.source.fetches).toBe(1);
			expect(r.pool.snapshot()[0]).toMatchObject({ version: "2.7.3", state: "idle" });
			expect(r.events.some((event) => event.type === "install_start" && event.priority === "prefetch")).toBe(true);
		} finally {
			await r.stop();
		}
	});

	test("hands jobs to the right version, warm or cold, and the result names the version that ran", async () => {
		const r = await rig({ max_children: 2 });
		try {
			r.queue.push(job("2.7.3", { valid: true }));
			r.queue.push(job("2.4.9", { valid: false }));
			r.queue.push(job("2.7.3", { valid: true }));
			await r.finalizer.wait_for(3);
			for (const { job: j, outcome, execution } of r.finalizer.finalized) {
				expect(outcome.outcome).toBe("succeeded");
				expect((outcome.result as { validated_by: string }).validated_by).toBe(j.client_version);
				expect((outcome.result as { valid: boolean }).valid).toBe(j.client_version === "2.7.3");
				expect(JSON.parse(outcome.result_json as string)).toEqual(outcome.result); // the record stores the bytes
				expect(execution).toMatchObject({ version: j.client_version, build_id: "b1", artifact_id: `${j.client_version}@b1` });
				expect(execution?.runner_pid).toBeGreaterThan(0);
			}
			expect(r.source.fetches).toBe(2);
			expect(r.finalizer.progress.length).toBeGreaterThanOrEqual(6);
			await wait_until(() => r.queue.acked.length === 3);
			const cold = r.events.filter((event) => event.component === "pool" && event.type === "acquire" && event.version === "2.4.9");
			expect(cold[0]?.cold).toBe(true);
		} finally {
			await r.stop();
		}
	});

	test("a job that arrives before prewarm finishes joins the prefetch instead of downloading twice", async () => {
		const r = await rig({ max_children: 2 });
		try {
			r.queue.push(job("2.7.3", { valid: true }));
			await r.finalizer.wait_for(1);
			await wait_until(() => r.worker.warm);
			expect(r.source.fetches).toBe(1);
			expect(r.events.some((event) => event.type === "join_in_flight" || event.type === "cache_hit")).toBe(true);
		} finally {
			await r.stop();
		}
	});

	test("a fast-lane overrun is killed at the budget and escalated, not finalized", async () => {
		const r = await rig({ max_children: 2 });
		try {
			const started = Date.now();
			r.queue.push(job("2.7.3", { sleep_ms: 3_000 }));
			await r.finalizer.wait_for(1);
			expect(r.finalizer.escalated).toHaveLength(1);
			expect(r.finalizer.finalized).toHaveLength(0);
			expect(Date.now() - started).toBeLessThan(2_500);
			r.queue.push(job("2.7.3", { valid: true }));
			await r.finalizer.wait_for(2);
			expect(r.finalizer.finalized[0].outcome.outcome).toBe("succeeded");
			expect(r.finalizer.escalated[0].execution.build_id).toBe("b1");
		} finally {
			await r.stop();
		}
	});

	test("jobs that must not reach a runner are finalized without one: expired, unknown, retired, tampered", async () => {
		const r = await rig({ max_children: 2 });
		try {
			await wait_until(() => r.worker.warm);
			const spawns = () => r.events.filter((event) => event.type === "spawn").length;
			const before = spawns();
			r.queue.push(job("2.7.3", { valid: true }, "fast", { deadline_at: new Date(Date.now() - 1_000).toISOString() }));
			r.queue.push(job("9.9.9", { valid: true }));
			r.registry.replace({ ...r.specs[1], status: "retired" });
			r.queue.push(job("2.4.9", { valid: true }));
			r.queue.push(job("2.7.3", { valid: true }, "fast", { simulation_sha256: "0".repeat(64) }));
			await r.finalizer.wait_for(4);
			const codes = r.finalizer.finalized.map(({ outcome }) => outcome.error?.code).sort();
			expect(codes).toEqual(["expired", "simulation_hash_mismatch", "version_retired", "version_unknown"]);
			expect(r.finalizer.finalized.every(({ execution }) => execution === undefined)).toBe(true);
			expect(spawns()).toBe(before);
			await wait_until(() => r.queue.acked.length === 4);
		} finally {
			await r.stop();
		}
	});

	test("one runner serves job after job with no busy refusals: a result means the runner is ready again", async () => {
		const r = await rig({ max_children: 1, config: { slots: 1, prewarm: false } });
		try {
			for (let i = 0; i < 6; i += 1) r.queue.push(job("2.7.3", { valid: true }));
			await r.finalizer.wait_for(6);
			expect(r.finalizer.finalized.map(({ outcome }) => outcome.outcome)).toEqual(Array(6).fill("succeeded"));
			expect(r.events.filter((event) => event.type === "spawn")).toHaveLength(1);
		} finally {
			await r.stop();
		}
	});

	test("while not yet warm, the task holds at most warmup_max_jobs jobs however many slots it has", async () => {
		const r = await rig({ max_children: 2, delay_hot_ms: 400, config: { slots: 2, warmup_max_jobs: 1 } });
		try {
			for (let i = 0; i < 4; i += 1) r.queue.push(job("2.4.9", { valid: true }));
			let in_flight = 0;
			let peak_before_warm = 0;
			for (const event of r.events) {
				if (event.type === "warm") break;
				if (event.type === "job_received") peak_before_warm = Math.max(peak_before_warm, ++in_flight);
				if (event.type === "finalize") in_flight -= 1;
			}
			await r.finalizer.wait_for(4);
			await wait_until(() => r.worker.warm);
			in_flight = 0;
			peak_before_warm = 0;
			for (const event of r.events) {
				if (event.type === "warm") break;
				if (event.type === "job_received") peak_before_warm = Math.max(peak_before_warm, ++in_flight);
				if (event.type === "finalize") in_flight -= 1;
			}
			expect(peak_before_warm).toBeLessThanOrEqual(1);
		} finally {
			await r.stop();
		}
	});

	test("a validator crash is reported by a runner that stays warm for the next job", async () => {
		const r = await rig({ max_children: 2 });
		try {
			r.queue.push(job("2.7.3", { crash: true }));
			await r.finalizer.wait_for(1);
			expect(r.finalizer.finalized[0].outcome).toMatchObject({ outcome: "crashed", error: { code: "validator_crashed", exit_code: 3 } });
			const spawns_before = r.events.filter((event) => event.type === "spawn").length;
			r.queue.push(job("2.7.3", { valid: true }));
			await r.finalizer.wait_for(2);
			expect(r.finalizer.finalized[1].outcome.outcome).toBe("succeeded");
			expect(r.events.filter((event) => event.type === "spawn").length).toBe(spawns_before);
		} finally {
			await r.stop();
		}
	});

	test("with one resident child, versions take turns: each cold job evicts the idle child of the other", async () => {
		const r = await rig({ max_children: 1 });
		try {
			await wait_until(() => r.worker.warm && r.pool.count("idle") === 1);
			r.queue.push(job("2.4.9", { valid: true }));
			await r.finalizer.wait_for(1);
			expect(r.events.some((event) => event.type === "stop" && event.reason === "evicted" && event.version === "2.7.3")).toBe(true);
			expect(r.pool.snapshot()).toEqual([expect.objectContaining({ version: "2.4.9", state: "idle" })]);
			r.queue.push(job("2.7.3", { valid: true }));
			await r.finalizer.wait_for(2);
			expect(r.events.some((event) => event.type === "stop" && event.reason === "evicted" && event.version === "2.4.9")).toBe(true);
			expect(r.pool.snapshot()).toEqual([expect.objectContaining({ version: "2.7.3", state: "idle" })]);
			expect(r.source.fetches).toBe(2);
		} finally {
			await r.stop();
		}
	});

	test("a rebuilt version runs on its new build without disturbing the old one", async () => {
		const r = await rig({ max_children: 2 });
		try {
			r.queue.push(job("2.7.3", { valid: true }));
			await r.finalizer.wait_for(1);
			const rebuilt = await build_demo_artifact({ version: "2.7.3", build_id: "b2", out_dir: path.join(r.root, "a"), rank: 1 });
			r.registry.replace(rebuilt);
			r.queue.push(job("2.7.3", { valid: true }));
			await r.finalizer.wait_for(2);
			expect(r.pool.live_ids()).toEqual(new Set(["2.7.3@b1", "2.7.3@b2"]));
			expect(r.finalizer.finalized.map(({ execution }) => execution?.build_id)).toEqual(["b1", "b2"]);
			expect(r.store.installed().sort()).toEqual(["2.4.9@b1", "2.7.3@b1", "2.7.3@b2"].filter((id) => r.store.installed().includes(id)));
		} finally {
			await r.stop();
		}
	});

	test("the memory limit is enforced on the validator child", async () => {
		const r = await rig({ max_children: 2, config: { memory_limit_per_job_bytes: 256 * 1024 * 1024 } });
		try {
			r.queue.push(job("2.7.3", { allocate_mb: 1024 }));
			await r.finalizer.wait_for(1);
			expect(r.finalizer.finalized[0].outcome).toMatchObject({ outcome: "crashed", error: { code: "validator_oom" } });
			r.queue.push(job("2.7.3", { valid: true }));
			await r.finalizer.wait_for(2);
			expect(r.finalizer.finalized[1].outcome.outcome).toBe("succeeded");
		} finally {
			await r.stop();
		}
	});

	test("stopping drains in-flight work and leaves no runner processes behind", async () => {
		const r = await rig({ max_children: 2 });
		r.queue.push(job("2.7.3", { sleep_ms: 300 }));
		await wait_until(() => r.pool.count("busy") === 1);
		const pids = r.pool.snapshot().map((child) => child.pid as number);
		expect(pids.length).toBeGreaterThan(0);
		await r.stop();
		expect(r.finalizer.finalized).toHaveLength(1);
		expect(r.pool.size).toBe(0);
		for (const pid of pids) expect(() => process.kill(pid, 0)).toThrow(); // the OS agrees
	});
});
