export type Lane = "fast" | "slow";

/** The SQS job envelope (design section 4). The simulation travels as the exact string the API received. */
export interface JobMessage {
	schema: "validator.job.v1";
	job_id: string;
	tenant_id: string;
	client_version: string;
	lane: Lane;
	created_at: string;
	deadline_at: string;
	simulation: string;
	simulation_sha256?: string;
	escalated_from?: Lane | null;
}

/** What the harness hands a runner process for one job. */
export interface RunRequest {
	job_id: string;
	simulation: string;
	budget_ms: number;
	memory_limit_bytes: number;
}

export type Outcome = "succeeded" | "failed" | "timeout" | "canceled" | "crashed" | "runner_unresponsive";

export interface RunOutcome {
	outcome: Outcome;
	/** The validator's result exactly as the runner serialized it. This is what the record stores; `result` is a parsed convenience. */
	result_json?: string;
	result?: unknown;
	error?: { code: string; message?: string; signal?: number | string | null; exit_code?: number | null };
	elapsed_ms?: number;
}

export interface Progress {
	stage: string;
	pct: number;
}

export type ProgressListener = (progress: Progress) => void;

/** One registry row: where a version's artifact lives and what host it needs. */
export interface ArtifactSpec {
	version: string;
	/** Changes when a version is rebuilt (a fix). Two builds of one version can be resident at once, so a fix rolls without touching running jobs. */
	build_id: string;
	/** Object key in the artifact source (an S3 key, or a file name for the local source). */
	key: string;
	sha256: string;
	/** Installed size on disk; the input to the cache's byte budget. */
	size_bytes: number;
	arch: "aarch64" | "x86_64" | string;
	min_glibc?: string;
	runner_protocol: string;
	/** Position in the hot list. Hot versions are prefetched on start and evicted last. Undefined means legacy. */
	rank?: number;
	/** Lifecycle from the registry. Missing means active. A retired version is refused by the worker. */
	status?: "active" | "canary" | "deprecated" | "retired";
}

/** What actually ran a job: written to the record with the result so a verdict stays traceable after the registry moves on. */
export interface Execution {
	artifact_id: string;
	version: string;
	build_id: string;
	sha256: string;
	runner_pid: number | undefined;
	child: number;
}

/** manifest.json inside an artifact: how to start its runner. */
export interface ArtifactManifest {
	version: string;
	build_id: string;
	/** Interpreter, relative to the install path (bin/python3) or a bare command on PATH. */
	python: string;
	runner: string;
	adapter_dir: string;
	adapter_module: string;
	runner_protocol: string;
	arch?: string;
	min_glibc?: string;
}

/** An artifact that is on local disk and can be spawned. */
export interface ArtifactRef {
	id: string;
	version: string;
	build_id: string;
	path: string;
}

export interface InstalledArtifact extends ArtifactRef {
	manifest: ArtifactManifest;
}

export interface HarnessEvent {
	component: "pool" | "artifacts" | "worker";
	type: string;
	[key: string]: unknown;
}

export type EventListener = (event: HarnessEvent) => void;
