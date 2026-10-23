import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { RunnerEndpoint, type RunnerHandle } from "./runner_endpoint";
import type { ArtifactManifest, ArtifactRef } from "./types";

export type SpawnRunner = (artifact: ArtifactRef) => Promise<RunnerHandle>;

export interface SpawnerOptions {
	/** Where the Unix sockets go. Keep it short: socket paths have a ~100 byte limit. */
	run_dir: string;
	ready_timeout_ms?: number;
	expected_protocol?: string;
	stderr?: "inherit" | "ignore";
}

export function read_manifest(install_path: string): ArtifactManifest {
	return JSON.parse(fs.readFileSync(path.join(install_path, "manifest.json"), "utf8")) as ArtifactManifest;
}

/**
 * Starts a runner from an installed artifact. The interpreter, the runner entry point and the adapter all come
 * from the artifact's manifest, so the harness image contains no Python and knows nothing about any version.
 */
export function make_runner_spawner(opts: SpawnerOptions): SpawnRunner {
	fs.mkdirSync(opts.run_dir, { recursive: true });
	let sequence = 0;
	return async (artifact) => {
		const manifest = read_manifest(artifact.path);
		const python = manifest.python.includes("/") ? path.join(artifact.path, manifest.python) : manifest.python;
		const socket_path = path.join(opts.run_dir, `${artifact.id.replace(/[^A-Za-z0-9.]+/g, "_")}-${++sequence}.sock`);
		return RunnerEndpoint.create({
			socket_path,
			version: artifact.version,
			expected_protocol: opts.expected_protocol ?? "runner/1",
			ready_timeout_ms: opts.ready_timeout_ms ?? 60_000,
			spawn_child: () =>
				spawn(
					python,
					[
						path.join(artifact.path, manifest.runner),
						"--connect", socket_path,
						"--adapter-dir", path.join(artifact.path, manifest.adapter_dir),
						"--adapter", manifest.adapter_module,
						"--version", artifact.version,
					],
					{
						cwd: artifact.path,
						env: {
							PATH: process.env.PATH ?? "/usr/bin:/bin",
							HOME: process.env.HOME ?? "/tmp",
							OMP_NUM_THREADS: "1",
							OPENBLAS_NUM_THREADS: "1",
							MKL_NUM_THREADS: "1",
							PYTHONUNBUFFERED: "1",
							PYTHONDONTWRITEBYTECODE: "1",
							VH_HARNESS_PID: String(process.pid),
						},
						stdio: ["ignore", "ignore", opts.stderr ?? "inherit"],
						detached: true,
					},
				),
		});
	};
}
