import { execFileSync } from "node:child_process";
import type { ArtifactSpec } from "./types";

export interface HostInfo {
	arch: string;
	glibc: string;
	protocols: string[];
}

export class IncompatibleArtifactError extends Error {}

/** The host is the task's Linux image; glibc comes from it, and the artifact contract is checked against it. */
export function detect_host(protocols: string[] = ["runner/1"]): HostInfo {
	const arch = ({ arm64: "aarch64", x64: "x86_64" } as Record<string, string>)[process.arch] ?? process.arch;
	const glibc = execFileSync("getconf", ["GNU_LIBC_VERSION"], { encoding: "utf8" }).trim().split(/\s+/).pop() as string;
	return { arch, glibc, protocols };
}

/** Dotted numeric compare: negative when a < b. */
export function compare_versions(a: string, b: string): number {
	const left = a.split(".").map(Number);
	const right = b.split(".").map(Number);
	for (let i = 0; i < Math.max(left.length, right.length); i += 1) {
		const diff = (left[i] ?? 0) - (right[i] ?? 0);
		if (diff !== 0) return diff;
	}
	return 0;
}

/**
 * The artifact contract, checked before a byte is downloaded. Native wheels built against an older glibc run on a
 * newer one, so the host only ever needs to move forward; an artifact that needs more than the host has is refused
 * and belongs on its own image and stack instead.
 */
export function check_compatible(spec: ArtifactSpec, host: HostInfo): void {
	if (spec.arch !== host.arch) {
		throw new IncompatibleArtifactError(`${spec.version}@${spec.build_id} is built for ${spec.arch}; this host is ${host.arch}`);
	}
	if (!host.protocols.includes(spec.runner_protocol)) {
		throw new IncompatibleArtifactError(`${spec.version}@${spec.build_id} speaks ${spec.runner_protocol}; this harness speaks ${host.protocols.join(", ")}`);
	}
	if (spec.min_glibc && compare_versions(spec.min_glibc, host.glibc) > 0) {
		throw new IncompatibleArtifactError(`${spec.version}@${spec.build_id} needs glibc ${spec.min_glibc}; this host has ${host.glibc}`);
	}
}
