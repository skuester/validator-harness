import crypto from "node:crypto";
import { execFile } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { promisify } from "node:util";
import { directory_bytes } from "./artifact_store";
import { detect_host } from "./host";
import type { ArtifactManifest, ArtifactSpec } from "./types";

const exec_file = promisify(execFile);
/** The frozen runner's source, whether this file runs from src/ or from dist/src/. */
const RUNNER_DIR = [path.resolve(__dirname, "..", "runner"), path.resolve(__dirname, "..", "..", "runner")].find((dir) =>
	fs.existsSync(path.join(dir, "runner.py")),
) as string;

export interface DemoArtifactOptions {
	version: string;
	/** Defaults to a digest of the runner and adapter sources, so a changed runner is a new build and never a changed artifact. */
	build_id?: string;
	/** Where the archive is written; a LocalDirectorySource over this directory stands in for S3. */
	out_dir: string;
	/** Interpreter the artifact declares. A real artifact ships its own at bin/python3; the demo borrows the system one. */
	python?: string;
	rank?: number;
	arch?: string;
}

/**
 * Builds an artifact the way CI would, minus the venv: manifest, the frozen runner, and the version's adapter.
 * A real build adds `uv venv` plus `uv pip install --require-hashes` at the install path, then tars the result.
 */
export function source_build_id(): string {
	const hash = crypto.createHash("sha256");
	for (const name of ["runner.py", "adapter_example.py"]) hash.update(fs.readFileSync(path.join(RUNNER_DIR, name)));
	return `b${hash.digest("hex").slice(0, 8)}`;
}

export async function build_demo_artifact(opts: DemoArtifactOptions): Promise<ArtifactSpec> {
	const build_id = opts.build_id ?? source_build_id();
	fs.mkdirSync(opts.out_dir, { recursive: true });
	// Already in the share from an earlier run: reuse it, so its digest matches what a persisted cache index holds.
	const existing = path.join(opts.out_dir, `${opts.version}-${build_id}.tar.gz.spec.json`);
	if (fs.existsSync(existing)) {
		return { ...(JSON.parse(fs.readFileSync(existing, "utf8")) as ArtifactSpec), rank: opts.rank };
	}
	const stage = fs.mkdtempSync(path.join(opts.out_dir, "stage-"));
	fs.mkdirSync(path.join(stage, "runner"));
	fs.copyFileSync(path.join(RUNNER_DIR, "runner.py"), path.join(stage, "runner", "runner.py"));
	fs.mkdirSync(path.join(stage, "adapter"));
	const adapter = fs.readFileSync(path.join(RUNNER_DIR, "adapter_example.py"), "utf8").replace("__VERSION__", opts.version);
	fs.writeFileSync(path.join(stage, "adapter", "adapter.py"), adapter);
	const arch = opts.arch ?? detect_host().arch;
	const manifest: ArtifactManifest = {
		version: opts.version,
		build_id,
		python: opts.python ?? "python3",
		runner: "runner/runner.py",
		adapter_dir: "adapter",
		adapter_module: "adapter",
		runner_protocol: "runner/1",
		arch,
	};
	fs.writeFileSync(path.join(stage, "manifest.json"), JSON.stringify(manifest, null, "\t"));
	const key = `${opts.version}-${build_id}.tar.gz`;
	const archive = path.join(opts.out_dir, key);
	await exec_file("tar", ["-czf", archive, "-C", stage, "."]);
	const sha256 = crypto.createHash("sha256").update(fs.readFileSync(archive)).digest("hex");
	const size_bytes = directory_bytes(stage);
	fs.rmSync(stage, { recursive: true, force: true });
	const spec: ArtifactSpec = { version: opts.version, build_id, key, sha256, size_bytes, arch, runner_protocol: "runner/1", rank: opts.rank };
	fs.writeFileSync(`${archive}.spec.json`, JSON.stringify(spec, null, "\t"));
	return spec;
}
