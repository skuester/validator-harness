import crypto from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ArtifactStore, DiskBudgetError, IntegrityError, validate_spec, verify_tree } from "../src/artifact_store";
import { IncompatibleArtifactError, type HostInfo } from "../src/host";
import { LocalDirectorySource, type ArtifactSource } from "../src/sources";
import type { ArtifactSpec } from "../src/types";

const host: HostInfo = { arch: "x86_64", glibc: "2.36", protocols: ["runner/1"] };
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function fake_artifact(dir: string, version: string, build_id: string, payload_bytes: number, overrides: Partial<ArtifactSpec> = {}, manifest_overrides: Record<string, unknown> = {}): ArtifactSpec {
	const stage = fs.mkdtempSync(path.join(dir, "stage-"));
	fs.writeFileSync(path.join(stage, "manifest.json"), JSON.stringify({ version, build_id, python: "python3", runner: "runner.py", adapter_dir: ".", adapter_module: "adapter", runner_protocol: "runner/1", ...manifest_overrides }));
	fs.writeFileSync(path.join(stage, "payload.bin"), crypto.randomBytes(payload_bytes));
	const key = `${version}-${build_id}.tar.gz`;
	execFileSync("tar", ["-czf", path.join(dir, key), "-C", stage, "."]);
	fs.rmSync(stage, { recursive: true, force: true });
	const sha256 = crypto.createHash("sha256").update(fs.readFileSync(path.join(dir, key))).digest("hex");
	return { version, build_id, key, sha256, size_bytes: payload_bytes, arch: "x86_64", runner_protocol: "runner/1", ...overrides };
}

/** A source whose fetches can be made slow, to hold an install in flight. */
class SlowSource implements ArtifactSource {
	fetches = 0;
	delay_ms = 0;
	constructor(private readonly inner: LocalDirectorySource) {}
	async fetch(key: string, destination: string): Promise<void> {
		this.fetches += 1;
		if (this.delay_ms) await sleep(this.delay_ms);
		await this.inner.fetch(key, destination);
	}
}

describe("ArtifactStore", () => {
	let root: string;
	let archives: string;
	let source: SlowSource;

	beforeEach(() => {
		root = fs.mkdtempSync(path.join(os.tmpdir(), "vh-store-"));
		archives = path.join(root, "archives");
		fs.mkdirSync(archives);
		source = new SlowSource(new LocalDirectorySource(archives));
	});

	afterEach(() => fs.rmSync(root, { recursive: true, force: true }));

	async function open_store(max_bytes: number, extra: Partial<ConstructorParameters<typeof ArtifactStore>[0]> = {}): Promise<ArtifactStore> {
		const store = new ArtifactStore({ root: path.join(root, "cache"), max_bytes, source, host, ...extra });
		await store.open();
		return store;
	}

	test("installs once, verifies the digest, and serves later requests from disk", async () => {
		const store = await open_store(10_000);
		const spec = fake_artifact(archives, "2.7.3", "b1", 1_000);
		const first = await store.ensure(spec);
		expect(first.path).toBe(store.install_path("2.7.3@b1"));
		expect(first.manifest.version).toBe("2.7.3");
		expect(fs.existsSync(path.join(first.path, "payload.bin"))).toBe(true);
		expect(fs.statSync(path.join(first.path, "payload.bin")).mode & 0o222).toBe(0); // files read-only
		const second = await store.ensure(spec);
		expect(second.path).toBe(first.path);
		expect(source.fetches).toBe(1);
		expect(store.used_bytes()).toBeGreaterThanOrEqual(1_000); // measured, not declared
		await store.close();
	});

	test("rejects an archive whose digest does not match the registry and installs nothing", async () => {
		const store = await open_store(10_000);
		const spec = fake_artifact(archives, "2.7.3", "b1", 1_000, { sha256: "0".repeat(64) });
		await expect(store.ensure(spec)).rejects.toBeInstanceOf(IntegrityError);
		expect(store.installed()).toEqual([]);
		expect(store.committed_bytes()).toBe(0);
		expect(fs.readdirSync(path.join(root, "cache", "tmp"))).toEqual([]);
	});

	test("rejects a manifest that disagrees with the registry row", async () => {
		const store = await open_store(10_000);
		const spec = fake_artifact(archives, "2.7.3", "b1", 100, {}, { build_id: "b9" });
		await expect(store.ensure(spec)).rejects.toThrow(/manifest says 2.7.3@b9/);
		expect(store.installed()).toEqual([]);
	});

	test("refuses unsafe identifiers before touching the disk or the network", async () => {
		const store = await open_store(10_000);
		const spec = fake_artifact(archives, "2.7.3", "b1", 100);
		await expect(store.ensure({ ...spec, version: "../../escape" })).rejects.toBeInstanceOf(IntegrityError);
		await expect(store.ensure({ ...spec, build_id: "b1/x" })).rejects.toBeInstanceOf(IntegrityError);
		expect(() => validate_spec({ ...spec, sha256: "nope" })).toThrow(IntegrityError);
		expect(source.fetches).toBe(0);
	});

	test("refuses an artifact built for another architecture before fetching it", async () => {
		const store = await open_store(10_000);
		const spec = fake_artifact(archives, "2.7.3", "b1", 1_000, { arch: "aarch64" });
		await expect(store.ensure(spec)).rejects.toBeInstanceOf(IncompatibleArtifactError);
		expect(source.fetches).toBe(0);
	});

	test("concurrent requests for one artifact share a single download", async () => {
		const store = await open_store(10_000);
		const spec = fake_artifact(archives, "2.7.3", "b1", 1_000);
		const results = await Promise.all([store.ensure(spec, { priority: "prefetch" }), store.ensure(spec), store.ensure(spec)]);
		expect(new Set(results.map((artifact) => artifact.path)).size).toBe(1);
		expect(source.fetches).toBe(1);
	});

	test("a join with a different digest is refused", async () => {
		const store = await open_store(10_000);
		const spec = fake_artifact(archives, "2.7.3", "b1", 1_000);
		source.delay_ms = 100;
		const first = store.ensure(spec);
		await expect(store.ensure({ ...spec, sha256: "f".repeat(64) })).rejects.toBeInstanceOf(IntegrityError);
		await first;
	});

	test("two installs in flight cannot both pass an empty-looking budget check; the second waits for the first", async () => {
		// Each install reserves twice its size while in flight; 3,500 holds one reservation plus one installed tree,
		// so the second install must wait for the first to land rather than overshoot.
		const store = await open_store(3_500);
		const a = fake_artifact(archives, "A", "b1", 1_000);
		const b = fake_artifact(archives, "B", "b1", 1_000);
		source.delay_ms = 50;
		await Promise.all([store.ensure(a), store.ensure(b)]);
		expect(store.installed().sort()).toEqual(["A@b1", "B@b1"]);
		expect(store.committed_bytes()).toBeLessThanOrEqual(3_500);
	});

	test("evicts the least recently used artifact to stay under the byte budget, but never a live or held one", async () => {
		const live = new Set<string>();
		const store = await open_store(3_500, { is_live: (id) => live.has(id) });
		const a = fake_artifact(archives, "A", "b1", 1_000);
		const b = fake_artifact(archives, "B", "b1", 1_000);
		const c = fake_artifact(archives, "C", "b1", 1_000);
		await store.ensure(a);
		await store.ensure(b);
		await store.ensure(a);
		await store.ensure(c);
		expect(store.installed().sort()).toEqual(["A@b1", "C@b1"]);
		expect(fs.existsSync(store.install_path("B@b1"))).toBe(false);

		live.add("A@b1");
		live.add("C@b1");
		await expect(store.ensure(b)).rejects.toBeInstanceOf(DiskBudgetError);
		live.delete("A@b1");
		store.hold("A@b1");
		await expect(store.ensure(b)).rejects.toBeInstanceOf(DiskBudgetError);
		store.release_hold("A@b1");
		await store.ensure(b);
		expect(store.installed().sort()).toEqual(["B@b1", "C@b1"]);
	});

	test("under pressure, asks the pool to stop idle runners so a cache full of resident versions still makes room", async () => {
		const live = new Set<string>(["A@b1", "B@b1"]);
		const stopped: string[] = [];
		const store = await open_store(3_500, {
			is_live: (id) => live.has(id),
			make_evictable: async (id) => {
				stopped.push(id);
				live.delete(id);
				return true;
			},
		});
		await store.ensure(fake_artifact(archives, "A", "b1", 1_000));
		await store.ensure(fake_artifact(archives, "B", "b1", 1_000));
		await store.ensure(fake_artifact(archives, "C", "b1", 1_000));
		expect(stopped).toEqual(["A@b1"]);
		expect(store.installed().sort()).toEqual(["B@b1", "C@b1"]);
	});

	test("reopening keeps what the index knows, forgets what is missing, and removes what the index never recorded", async () => {
		const store = await open_store(10_000);
		await store.ensure(fake_artifact(archives, "A", "b1", 500));
		await store.ensure(fake_artifact(archives, "B", "b1", 500));
		await store.close();
		fs.rmSync(store.install_path("B@b1"), { recursive: true, force: true });
		fs.mkdirSync(store.install_path("Z@b1"));
		fs.writeFileSync(path.join(store.install_path("Z@b1"), "manifest.json"), "{}");
		const reopened = await open_store(10_000);
		expect(reopened.installed()).toEqual(["A@b1"]);
		expect(fs.existsSync(store.install_path("Z@b1"))).toBe(false);
		expect(reopened.used_bytes()).toBeGreaterThanOrEqual(500);
	});

	test("an archive with a symlink pointing outside itself is refused", () => {
		const stage = fs.mkdtempSync(path.join(root, "tree-"));
		fs.symlinkSync("/etc/passwd", path.join(stage, "escape"));
		expect(() => verify_tree(stage)).toThrow(/escapes the artifact/);
		fs.rmSync(path.join(stage, "escape"));
		fs.writeFileSync(path.join(stage, "real"), "x");
		fs.symlinkSync("real", path.join(stage, "inside"));
		expect(() => verify_tree(stage)).not.toThrow();
	});
});
