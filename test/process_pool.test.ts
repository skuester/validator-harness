import { ProcessPool } from "../src/process_pool";
import type { ExitListener, RunnerHandle } from "../src/runner_endpoint";
import type { ArtifactRef, RunOutcome, RunRequest } from "../src/types";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const tick = () => sleep(5);

class FakeHandle implements RunnerHandle {
	exited = false;
	readonly pid = 1;
	private readonly listeners: ExitListener[] = [];

	constructor(readonly version: string) {}

	async run(_request: RunRequest): Promise<RunOutcome> {
		await sleep(2);
		return { outcome: "succeeded", result: { by: this.version } };
	}

	cancel(): void {}

	async stop(): Promise<void> {
		if (this.exited) return;
		this.exited = true;
		for (const listener of this.listeners.splice(0)) listener(0, null);
	}

	on_exit(listener: ExitListener): void {
		this.listeners.push(listener);
	}

	/** The process died on its own. */
	crash(): void {
		this.exited = true;
		for (const listener of this.listeners.splice(0)) listener(null, "SIGKILL");
	}
}

const ref = (version: string, build_id = "b1"): ArtifactRef => ({ id: `${version}@${build_id}`, version, build_id, path: `/fake/${version}` });

interface PoolOptions {
	max_children: number;
	pinned?: string[];
	max_jobs_per_child?: number;
	/** When true, spawns complete only when the test calls `finish_spawn()`. */
	manual?: boolean;
}

function make_pool(opts: PoolOptions) {
	const spawned: FakeHandle[] = [];
	const pending: Array<() => void> = [];
	const pool = new ProcessPool({
		max_children: opts.max_children,
		max_jobs_per_child: opts.max_jobs_per_child,
		is_pinned: (version) => opts.pinned?.includes(version) ?? false,
		spawn: async (artifact) => {
			if (opts.manual) await new Promise<void>((resolve) => pending.push(resolve));
			else await sleep(2);
			const handle = new FakeHandle(artifact.version);
			spawned.push(handle);
			return handle;
		},
	});
	const finish_spawn = async () => {
		pending.shift()?.();
		await tick();
	};
	return { pool, spawned, pending, finish_spawn };
}

describe("ProcessPool", () => {
	test("reuses a warm child for the same artifact", async () => {
		const { pool, spawned } = make_pool({ max_children: 2 });
		const first = await pool.acquire(ref("A"));
		await pool.release(first);
		const second = await pool.acquire(ref("A"));
		expect(second).toBe(first);
		expect(spawned).toHaveLength(1);
		await pool.shutdown();
	});

	test("evicts the least recently used idle child when full, never a busy one", async () => {
		const { pool, spawned } = make_pool({ max_children: 2 });
		const a = await pool.acquire(ref("A"));
		await pool.release(a);
		const b = await pool.acquire(ref("B"));
		await pool.release(b);
		const b_again = await pool.acquire(ref("B"));
		const c = await pool.acquire(ref("C"));
		expect(spawned[0].exited).toBe(true);
		expect(spawned[1].exited).toBe(false);
		expect(pool.snapshot().map((child) => child.version).sort()).toEqual(["B", "C"]);
		await pool.release(b_again);
		await pool.release(c);
		await pool.shutdown();
	});

	test("pinned versions are evicted only after every unpinned idle child", async () => {
		const { pool, spawned } = make_pool({ max_children: 2, pinned: ["A"] });
		await pool.release(await pool.acquire(ref("A")));
		await pool.release(await pool.acquire(ref("B")));
		const c = await pool.acquire(ref("C"));
		expect(spawned[0].exited).toBe(false);
		expect(spawned[1].exited).toBe(true);
		await pool.release(c);
		await pool.shutdown();
	});

	test("prewarm fills idle capacity and never evicts", async () => {
		const { pool } = make_pool({ max_children: 2 });
		expect(await pool.prewarm(ref("A"))).toBe(true);
		expect(await pool.prewarm(ref("B"))).toBe(true);
		expect(await pool.prewarm(ref("C"))).toBe(false);
		expect(await pool.prewarm(ref("A"))).toBe(false);
		expect(pool.count("idle")).toBe(2);
		await pool.shutdown();
	});

	test("recycles a worn-out child and respawns pinned versions in the background", async () => {
		const { pool, spawned } = make_pool({ max_children: 2, pinned: ["A"], max_jobs_per_child: 2 });
		await pool.release(await pool.acquire(ref("A")));
		await pool.release(await pool.acquire(ref("A")));
		expect(spawned[0].exited).toBe(true);
		await sleep(20);
		expect(spawned).toHaveLength(2);
		expect(pool.snapshot()).toEqual([expect.objectContaining({ version: "A", state: "idle", jobs_done: 0 })]);
		await pool.shutdown();
	});

	test("a second acquire for the same cold artifact waits for the first child instead of spawning when there is no room", async () => {
		const { pool, spawned } = make_pool({ max_children: 1 });
		const first = pool.acquire(ref("A"));
		const second = pool.acquire(ref("A"));
		const child = await first;
		await sleep(5);
		await pool.release(child);
		expect(await second).toBe(child);
		expect(spawned).toHaveLength(1);
		await pool.shutdown();
	});

	test("a spawned child belongs to the acquire that asked for it: two cold acquires on one slot do not evict each other forever", async () => {
		const { pool, spawned, finish_spawn } = make_pool({ max_children: 1, manual: true });
		const want_a = pool.acquire(ref("A"));
		const want_b = pool.acquire(ref("B"));
		await tick();
		expect(pool.snapshot()).toEqual([expect.objectContaining({ version: "A", state: "starting" })]);
		await finish_spawn();
		const a = await want_a;
		expect(a.state).toBe("busy");
		await tick();
		expect(spawned).toHaveLength(1); // B is waiting, not evicting A
		await pool.release(a);
		await tick();
		expect(spawned[0].exited).toBe(true); // now B evicts the idle A and spawns
		await finish_spawn();
		const b = await want_b;
		expect(b.version).toBe("B");
		expect(b.state).toBe("busy");
		expect(spawned).toHaveLength(2);
		await pool.release(b);
		await pool.shutdown();
	});

	test("shutdown during a spawn stops the process that arrives afterwards and fails the acquire", async () => {
		const { pool, spawned, finish_spawn } = make_pool({ max_children: 1, manual: true });
		const want = pool.acquire(ref("A"));
		const rejected = expect(want).rejects.toThrow(/closed/);
		await tick();
		const closing = pool.shutdown();
		await tick();
		await finish_spawn();
		await closing;
		await rejected;
		expect(spawned).toHaveLength(1);
		expect(spawned[0].exited).toBe(true);
		expect(pool.size).toBe(0);
	});

	test("disk pressure stops idle children of an artifact but not busy ones", async () => {
		const { pool } = make_pool({ max_children: 2 });
		const a = await pool.acquire(ref("A"));
		await pool.release(a);
		expect(await pool.evict_idle_for("A@b1")).toBe(true);
		expect(pool.live_ids().has("A@b1")).toBe(false);
		const b = await pool.acquire(ref("B"));
		expect(await pool.evict_idle_for("B@b1")).toBe(false);
		expect(b.state).toBe("busy");
		await pool.release(b);
		await pool.shutdown();
	});

	test("a child that dies on its own leaves the table and the next acquire spawns fresh", async () => {
		const { pool, spawned } = make_pool({ max_children: 2 });
		const a = await pool.acquire(ref("A"));
		await pool.release(a);
		spawned[0].crash();
		expect(pool.size).toBe(0);
		await pool.acquire(ref("A"));
		expect(spawned).toHaveLength(2);
		await pool.shutdown();
	});

	test("two builds of one version are different artifacts", async () => {
		const { pool, spawned } = make_pool({ max_children: 2 });
		await pool.release(await pool.acquire(ref("A", "b1")));
		await pool.release(await pool.acquire(ref("A", "b2")));
		expect(spawned).toHaveLength(2);
		expect(pool.live_ids()).toEqual(new Set(["A@b1", "A@b2"]));
		await pool.shutdown();
	});
});
