/**
 * End to end on one machine: three artifacts in a local "S3", a three-child pool, a fast-lane worker.
 * Watch the event log: prewarm of the hot versions runs beside the receive loop, the first job for the
 * legacy version pays a fetch and an import, a second concurrent job for the hot version evicts the unpinned
 * legacy runner rather than a pinned one, the overrun is escalated, the crash is reported by a runner that stays warm.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ArtifactStore } from "./artifact_store";
import { check_memory_budget } from "./config";
import { build_demo_artifact } from "./demo_artifacts";
import { detect_host } from "./host";
import { MemoryQueue, MemoryRegistry, RecordingFinalizer } from "./memory_adapters";
import { ProcessPool } from "./process_pool";
import { make_runner_spawner } from "./runner_spawn";
import { LocalDirectorySource } from "./sources";
import type { HarnessEvent, JobMessage } from "./types";
import { Worker } from "./worker";

const GiB = 1024 ** 3;
const MiB = 1024 ** 2;

function job(version: string, simulation: Record<string, unknown>, lane: "fast" | "slow" = "fast"): JobMessage {
	const now = new Date();
	return {
		schema: "validator.job.v1",
		job_id: `01J${Math.random().toString(36).slice(2, 10).toUpperCase()}`,
		tenant_id: "t_demo",
		client_version: version,
		lane,
		created_at: now.toISOString(),
		deadline_at: new Date(now.getTime() + 20 * 60_000).toISOString(),
		simulation: JSON.stringify(simulation),
	};
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function main(): Promise<void> {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "vh-"));
	// In the container these point at the mounted artifact share, the cache volume and the socket directory.
	const artifacts_dir = process.env.VH_ARTIFACTS ?? path.join(root, "artifacts");
	const cache_root = process.env.VH_CACHE ?? path.join(root, "cache");
	const run_dir = process.env.VH_RUN_DIR ?? path.join(root, "run");
	const cache_max_bytes = Number(process.env.VH_CACHE_MAX_MB ?? 64) * MiB;
	const host = detect_host();
	console.log(`host ${host.arch} glibc ${host.glibc}; artifacts ${artifacts_dir}; cache ${cache_root} (${cache_max_bytes / MiB} MiB)`);
	const started = Date.now();
	const log = (event: HarnessEvent) => {
		const { component, type, ...rest } = event;
		const fields = Object.entries(rest).map(([key, value]) => `${key}=${typeof value === "string" ? value : JSON.stringify(value)}`).join(" ");
		console.log(`${String(Date.now() - started).padStart(6)} ms  ${component.padEnd(9)} ${type.padEnd(16)} ${fields}`);
	};

	// A 4 GiB fast task: two slots at 1 GiB each, three resident runners at ~400 MiB, the harness at 256 MiB.
	console.log(check_memory_budget({ task_memory_bytes: 4 * GiB, harness_reserve_bytes: 256 * MiB, slots: 2, memory_limit_per_job_bytes: 1 * GiB, max_children: 3, idle_child_rss_bytes: 400 * MiB }));

	const specs = [
		await build_demo_artifact({ version: "2.7.3", out_dir: artifacts_dir, rank: 1 }),
		await build_demo_artifact({ version: "2.6.0", out_dir: artifacts_dir, rank: 2 }),
		await build_demo_artifact({ version: "2.4.9", out_dir: artifacts_dir }),
	];
	const registry = new MemoryRegistry(specs);
	let pool: ProcessPool;
	const store = new ArtifactStore({
		root: cache_root,
		max_bytes: cache_max_bytes,
		source: new LocalDirectorySource(artifacts_dir),
		host,
		is_live: (id) => pool.live_ids().has(id),
		make_evictable: (id) => pool.evict_idle_for(id),
		on_event: log,
	});
	await store.open();
	pool = new ProcessPool({
		max_children: 3,
		spawn: make_runner_spawner({ run_dir, stderr: "inherit" }),
		is_pinned: (version) => registry.resolve(version)?.rank !== undefined,
		max_jobs_per_child: 1000,
		on_event: log,
	});
	const queue = new MemoryQueue();
	const finalizer = new RecordingFinalizer(log);
	const worker = new Worker({
		queue,
		registry,
		artifacts: store,
		pool,
		finalizer,
		config: { lane: "fast", slots: 2, budget_ms: 1_000, memory_limit_per_job_bytes: 1 * GiB, receive_wait_ms: 250, warmup_max_jobs: 1, prewarm: true, prewarm_concurrency: 2 },
		on_event: log,
	});

	const running = worker.run();
	queue.push(job("2.7.3", { valid: true }));             // arrives while 2.7.3 is still being prewarmed: joins that install
	await sleep(600);
	queue.push(job("2.7.3", { valid: false }));            // warm
	queue.push(job("2.6.0", { valid: true }));             // warm, prewarmed
	queue.push(job("2.4.9", { valid: true }));             // cold: fetch and import; takes the third seat in the pool
	await sleep(1_500);
	queue.push(job("2.7.3", { sleep_ms: 1_500 }));         // overruns the 1 s fast budget: killed and escalated
	queue.push(job("2.7.3", { crash: true }));             // needs a second 2.7.3 runner while the first is busy: the unpinned 2.4.9 runner is evicted, the pinned 2.6.0 stays; then the validator child dies and the runner reports it and stays warm
	queue.push(job("2.7.3", { valid: true }));             // served by the same warm runner

	await finalizer.wait_for(7);
	worker.stop();
	queue.close();
	await running;
	await pool.shutdown();
	await store.close();
	fs.rmSync(root, { recursive: true, force: true }); // only the temp root; mounted directories persist for the next run

	console.log("\nfinalized:");
	for (const { job: j, outcome, execution } of finalizer.finalized) {
		console.log(`  ${j.client_version.padEnd(6)} ${(execution?.build_id ?? "-").padEnd(10)} ${outcome.outcome.padEnd(10)} ${outcome.error?.code ?? outcome.result_json}`);
	}
	console.log(`escalated: ${finalizer.escalated.map(({ job: j }) => j.client_version).join(", ")}`);
	console.log(`progress events: ${finalizer.progress.length}`);
}

main().catch((error) => {
	console.error(error);
	process.exit(1);
});
