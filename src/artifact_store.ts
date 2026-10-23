import crypto from "node:crypto";
import { execFile } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { promisify } from "node:util";
import { check_compatible, type HostInfo } from "./host";
import { LruIndex } from "./lru";
import { read_manifest } from "./runner_spawn";
import type { ArtifactSource } from "./sources";
import type { ArtifactSpec, EventListener, InstalledArtifact } from "./types";

const exec_file = promisify(execFile);

export type InstallPriority = "request" | "prefetch";
export type Extractor = (archive_path: string, destination_dir: string) => Promise<void>;

export class IntegrityError extends Error {}
export class DiskBudgetError extends Error {}

/** Registry identifiers become directory names, so they are validated before they touch a path. */
const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const DIGEST_PATTERN = /^[0-9a-f]{64}$/;

export interface ArtifactStoreOptions {
	/** Artifacts install under `${root}/versions/<version>+<build_id>`. Build them at that path so nothing needs relocating. */
	root: string;
	max_bytes: number;
	source: ArtifactSource;
	host: HostInfo;
	/** Artifacts with a resident runner process are never evicted outright. */
	is_live?: (artifact_id: string) => boolean;
	/** Under disk pressure, ask the pool to stop a live artifact's idle runners; true when nothing of it is left running. */
	make_evictable?: (artifact_id: string) => Promise<boolean>;
	/** Prefetches share this many download slots; a request for a queued prefetch promotes it past the queue. */
	max_concurrent_prefetch?: number;
	extract?: Extractor;
	clock?: () => number;
	on_event?: EventListener;
}

interface IndexEntry {
	version: string;
	build_id: string;
	sha256: string;
	bytes: number;
	installed_at: number;
	last_used_at: number;
}

interface InFlight {
	spec: ArtifactSpec;
	promise: Promise<InstalledArtifact>;
	promote: () => void;
}

/** Extraction runs at the lowest CPU priority so a prefetch does not slow a validator that is mid-job. */
export const tar_extractor: Extractor = async (archive_path, destination_dir) => {
	await exec_file("nice", ["-n", "19", "tar", "-xzf", archive_path, "-C", destination_dir, "--no-same-owner", "--no-same-permissions"]);
};

export function validate_spec(spec: ArtifactSpec): void {
	if (!ID_PATTERN.test(spec.version) || !ID_PATTERN.test(spec.build_id)) {
		throw new IntegrityError(`refusing artifact with unsafe identifiers: version ${JSON.stringify(spec.version)}, build ${JSON.stringify(spec.build_id)}`);
	}
	if (!DIGEST_PATTERN.test(spec.sha256)) throw new IntegrityError(`${spec.version}@${spec.build_id}: digest is not a hex SHA-256`);
	if (!(spec.size_bytes > 0)) throw new IntegrityError(`${spec.version}@${spec.build_id}: size_bytes must be positive`);
}

export function directory_bytes(directory: string): number {
	let total = 0;
	for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
		const entry_path = path.join(directory, entry.name);
		if (entry.isSymbolicLink()) continue;
		total += entry.isDirectory() ? directory_bytes(entry_path) : fs.statSync(entry_path).size;
	}
	return total;
}

/** Every symlink in the tree must resolve inside it. tar already refuses absolute and `..` members; this covers link targets. */
export function verify_tree(root: string): void {
	const real_root = fs.realpathSync(root) + path.sep;
	const walk = (directory: string): void => {
		for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
			const entry_path = path.join(directory, entry.name);
			if (entry.isSymbolicLink()) {
				let target: string;
				try {
					target = fs.realpathSync(entry_path);
				} catch {
					throw new IntegrityError(`dangling symlink in artifact: ${path.relative(root, entry_path)}`);
				}
				if (!target.startsWith(real_root)) throw new IntegrityError(`symlink escapes the artifact: ${path.relative(root, entry_path)} -> ${target}`);
			} else if (entry.isDirectory()) {
				walk(entry_path);
			} else if (!entry.isFile()) {
				throw new IntegrityError(`unsupported entry in artifact: ${path.relative(root, entry_path)}`);
			}
		}
	};
	walk(root);
}

/** Files read-only: a defence against accidents, not a boundary, since a same-UID process can undo it. Directories stay writable so eviction can unlink. */
function make_files_read_only(directory: string): void {
	for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
		const entry_path = path.join(directory, entry.name);
		if (entry.isDirectory()) make_files_read_only(entry_path);
		else if (entry.isFile()) fs.chmodSync(entry_path, fs.statSync(entry_path).mode & 0o555);
	}
}

async function sha256_file(file_path: string): Promise<string> {
	const hash = crypto.createHash("sha256");
	for await (const chunk of fs.createReadStream(file_path)) hash.update(chunk as Buffer);
	return hash.digest("hex");
}

/** A counting gate whose queued tickets can be promoted to the front when a request arrives for them. */
class PrefetchGate {
	private active = 0;
	private readonly queue: Array<() => void> = [];

	constructor(private readonly limit: number) {}

	ticket(): { acquired: Promise<() => void>; promote: () => void } {
		let granted = false;
		let grant: () => void = () => undefined;
		const acquired = new Promise<() => void>((resolve) => {
			grant = () => {
				if (granted) return;
				granted = true;
				this.active += 1;
				let released = false;
				resolve(() => {
					if (released) return;
					released = true;
					this.active -= 1;
					this.dispatch();
				});
			};
		});
		if (this.active < this.limit && this.queue.length === 0) grant();
		else this.queue.push(grant);
		return {
			acquired,
			promote: () => {
				const index = this.queue.indexOf(grant);
				if (index >= 0) this.queue.splice(index, 1);
				grant();
			},
		};
	}

	private dispatch(): void {
		while (this.active < this.limit && this.queue.length > 0) this.queue.shift()?.();
	}
}

/**
 * The on-disk artifact cache: a byte-bounded LRU of installed versions, with in-flight deduplication so a job
 * that arrives while its version is being prefetched waits on that download instead of starting a second one.
 *
 * The budget counts installed trees plus space reserved for installs in progress (the archive and the
 * extraction scratch), so two installs cannot both pass an empty-looking check. A live artifact (resident
 * runner, or held by a job between install and spawn) is never removed; under pressure the pool is asked to
 * stop idle runners first, so a full cache of idle versions can still make room for a new one.
 */
export class ArtifactStore {
	private readonly index = new LruIndex<string, IndexEntry>();
	private readonly in_flight = new Map<string, InFlight>();
	private readonly reserved = new Map<string, number>();
	private readonly holds = new Map<string, number>();
	private readonly prefetch_gate: PrefetchGate;
	private readonly extract: Extractor;
	private readonly clock: () => number;
	private persist_chain: Promise<void> = Promise.resolve();
	private persist_timer: NodeJS.Timeout | undefined;

	constructor(private readonly opts: ArtifactStoreOptions) {
		this.prefetch_gate = new PrefetchGate(opts.max_concurrent_prefetch ?? 2);
		this.extract = opts.extract ?? tar_extractor;
		this.clock = opts.clock ?? Date.now;
	}

	static artifact_id(spec: { version: string; build_id: string }): string {
		return `${spec.version}@${spec.build_id}`;
	}

	install_path(artifact_id: string): string {
		const versions = path.join(this.opts.root, "versions");
		const target = path.join(versions, artifact_id.replace("@", "+"));
		if (!target.startsWith(versions + path.sep)) throw new IntegrityError(`artifact id escapes the cache: ${artifact_id}`);
		return target;
	}

	/** Load the index, drop entries whose directory is gone, remove directories the index does not know, clear scratch. */
	async open(): Promise<void> {
		const versions = path.join(this.opts.root, "versions");
		fs.mkdirSync(versions, { recursive: true });
		fs.rmSync(path.join(this.opts.root, "tmp"), { recursive: true, force: true });
		fs.mkdirSync(path.join(this.opts.root, "tmp"), { recursive: true });
		const index_path = path.join(this.opts.root, "index.json");
		if (fs.existsSync(index_path)) {
			const saved = JSON.parse(fs.readFileSync(index_path, "utf8")) as { entries: Array<[string, IndexEntry]> };
			for (const [id, entry] of saved.entries) {
				if (fs.existsSync(path.join(this.install_path(id), "manifest.json"))) this.index.set(id, entry);
				else this.emit({ type: "reconcile_dropped", artifact: id });
			}
		}
		for (const name of fs.readdirSync(versions)) {
			const id = name.replace("+", "@");
			if (this.index.has(id)) continue;
			// Installed but never indexed (a crash between rename and persist): its digest was never recorded, so it goes.
			fs.rmSync(path.join(versions, name), { recursive: true, force: true });
			this.emit({ type: "reconcile_removed", artifact: id });
		}
		await this.persist();
	}

	/** Flush the index and stop timers. */
	async close(): Promise<void> {
		if (this.persist_timer) clearTimeout(this.persist_timer);
		this.persist_timer = undefined;
		await this.persist();
	}

	used_bytes(): number {
		return this.index.values().reduce((sum, entry) => sum + entry.bytes, 0);
	}

	/** Installed bytes plus reservations for installs in progress: what the budget is checked against. */
	committed_bytes(): number {
		let reserved = 0;
		for (const bytes of this.reserved.values()) reserved += bytes;
		return this.used_bytes() + reserved;
	}

	installed(): string[] {
		return [...this.index.entries_oldest_first()].map(([id]) => id);
	}

	/** Keep an artifact from being evicted while a job holds it between install and spawn. */
	hold(artifact_id: string): void {
		this.holds.set(artifact_id, (this.holds.get(artifact_id) ?? 0) + 1);
	}

	release_hold(artifact_id: string): void {
		const count = (this.holds.get(artifact_id) ?? 0) - 1;
		if (count > 0) this.holds.set(artifact_id, count);
		else this.holds.delete(artifact_id);
	}

	/** Make the artifact present on disk and return where it is. Cache hit, join an in-flight install, or install. */
	async ensure(spec: ArtifactSpec, opts: { priority: InstallPriority } = { priority: "request" }): Promise<InstalledArtifact> {
		validate_spec(spec);
		check_compatible(spec, this.opts.host);
		const id = ArtifactStore.artifact_id(spec);
		const entry = this.index.get(id);
		if (entry) {
			if (entry.sha256 !== spec.sha256) {
				throw new IntegrityError(`${id} is installed with digest ${entry.sha256} but the registry now says ${spec.sha256}; a rebuild needs a new build_id`);
			}
			entry.last_used_at = this.clock();
			this.index.touch(id);
			this.schedule_persist();
			this.emit({ type: "cache_hit", artifact: id });
			return this.installed_artifact(id, entry);
		}
		const in_flight = this.in_flight.get(id);
		if (in_flight) {
			if (in_flight.spec.sha256 !== spec.sha256) {
				throw new IntegrityError(`${id} is being installed with digest ${in_flight.spec.sha256} but this request says ${spec.sha256}`);
			}
			if (opts.priority === "request") in_flight.promote();
			this.emit({ type: "join_in_flight", artifact: id, priority: opts.priority });
			return in_flight.promise;
		}
		const ticket = this.prefetch_gate.ticket();
		if (opts.priority === "request") ticket.promote();
		const promise = this.install(spec, opts.priority, ticket.acquired).finally(() => this.in_flight.delete(id));
		this.in_flight.set(id, { spec, promise, promote: ticket.promote });
		return promise;
	}

	private async install(spec: ArtifactSpec, priority: InstallPriority, gate: Promise<() => void>): Promise<InstalledArtifact> {
		const id = ArtifactStore.artifact_id(spec);
		const release = await gate;
		const scratch = path.join(this.opts.root, "tmp", `${id.replace(/[^A-Za-z0-9.]+/g, "_")}-${crypto.randomBytes(4).toString("hex")}`);
		const archive = `${scratch}.tar.gz`;
		this.emit({ type: "install_start", artifact: id, priority, bytes: spec.size_bytes });
		try {
			// Reserve the tree plus the archive and extraction scratch before any byte lands.
			await this.evict_to_fit(spec.size_bytes * 2);
			this.reserved.set(id, spec.size_bytes * 2);
			const fetch_started = this.clock();
			await this.opts.source.fetch(spec.key, archive);
			const fetch_ms = this.clock() - fetch_started;
			const digest = await sha256_file(archive);
			if (digest !== spec.sha256) throw new IntegrityError(`${id}: archive digest ${digest} does not match registry digest ${spec.sha256}`);
			const extract_started = this.clock();
			fs.mkdirSync(scratch, { recursive: true });
			await this.extract(archive, scratch);
			const extract_ms = this.clock() - extract_started;
			verify_tree(scratch);
			const manifest = read_manifest(scratch);
			if (manifest.version !== spec.version || manifest.build_id !== spec.build_id || manifest.runner_protocol !== spec.runner_protocol) {
				throw new IntegrityError(`${id}: manifest says ${manifest.version}@${manifest.build_id} / ${manifest.runner_protocol}, registry says ${spec.version}@${spec.build_id} / ${spec.runner_protocol}`);
			}
			const measured = directory_bytes(scratch);
			make_files_read_only(scratch);
			const destination = this.install_path(id);
			fs.rmSync(destination, { recursive: true, force: true });
			fs.renameSync(scratch, destination);
			const entry: IndexEntry = { version: spec.version, build_id: spec.build_id, sha256: spec.sha256, bytes: measured, installed_at: this.clock(), last_used_at: this.clock() };
			this.index.set(id, entry);
			this.reserved.delete(id);
			await this.persist();
			this.emit({ type: "install_done", artifact: id, priority, fetch_ms, extract_ms, bytes: measured, declared_bytes: spec.size_bytes, used_bytes: this.used_bytes() });
			return this.installed_artifact(id, entry);
		} catch (error) {
			this.emit({ type: "install_failed", artifact: id, priority, error: String(error) });
			throw error;
		} finally {
			this.reserved.delete(id);
			fs.rmSync(archive, { force: true });
			fs.rmSync(scratch, { recursive: true, force: true });
			release();
		}
	}

	/**
	 * Make room. Evict least recently used first; when nothing can go right now, wait for an install that holds
	 * a reservation to finish (its reservation shrinks to its installed size) and look again. Waiters hold no
	 * reservation, so they only ever wait on installs that are making progress.
	 */
	private async evict_to_fit(bytes_needed: number): Promise<void> {
		while (this.committed_bytes() + bytes_needed > this.opts.max_bytes) {
			if (await this.evict_one()) continue;
			const in_progress = [...this.in_flight.entries()].filter(([id]) => this.reserved.has(id)).map(([, flight]) => flight.promise.catch(() => undefined));
			if (in_progress.length === 0) {
				throw new DiskBudgetError(`need ${bytes_needed} bytes, ${this.committed_bytes()} of ${this.opts.max_bytes} committed and every installed artifact is busy or held`);
			}
			await Promise.race(in_progress);
		}
	}

	/** Remove the least recently used artifact that is neither in flight nor held; a live one only once the pool has stopped its idle runners. */
	private async evict_one(): Promise<boolean> {
		for (const [id, entry] of [...this.index.entries_oldest_first()]) {
			if (this.in_flight.has(id) || this.holds.has(id)) continue;
			if (this.opts.is_live?.(id)) {
				if (!this.opts.make_evictable || !(await this.opts.make_evictable(id))) continue;
				if (!this.index.has(id)) return true;
			}
			this.index.delete(id);
			fs.rmSync(this.install_path(id), { recursive: true, force: true });
			await this.persist();
			this.emit({ type: "evict", artifact: id, bytes: entry.bytes, idle_ms: this.clock() - entry.last_used_at });
			return true;
		}
		return false;
	}

	private installed_artifact(id: string, entry: IndexEntry): InstalledArtifact {
		const install_path = this.install_path(id);
		return { id, version: entry.version, build_id: entry.build_id, path: install_path, manifest: read_manifest(install_path) };
	}

	/** Recency changes on cache hits are persisted a few seconds later, not on every job. */
	private schedule_persist(): void {
		if (this.persist_timer) return;
		this.persist_timer = setTimeout(() => {
			this.persist_timer = undefined;
			void this.persist();
		}, 5_000);
		this.persist_timer.unref?.();
	}

	/** Index writes are serialized, and a failed write is reported without poisoning later ones. */
	private persist(): Promise<void> {
		const index_path = path.join(this.opts.root, "index.json");
		const body = JSON.stringify({ entries: [...this.index.entries_oldest_first()] }, null, "\t");
		this.persist_chain = this.persist_chain
			.then(async () => {
				await fs.promises.writeFile(`${index_path}.tmp`, body);
				await fs.promises.rename(`${index_path}.tmp`, index_path);
			})
			.catch((error) => this.emit({ type: "persist_failed", error: String(error) }));
		return this.persist_chain;
	}

	private emit(event: { type: string; [key: string]: unknown }): void {
		this.opts.on_event?.({ component: "artifacts", ...event });
	}
}
