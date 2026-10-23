import { check_compatible, compare_versions, detect_host, IncompatibleArtifactError, type HostInfo } from "../src/host";
import type { ArtifactSpec } from "../src/types";

const spec = (overrides: Partial<ArtifactSpec> = {}): ArtifactSpec => ({
	version: "2.7.3",
	build_id: "b1",
	key: "2.7.3-b1.tar.gz",
	sha256: "0".repeat(64),
	size_bytes: 1,
	arch: "aarch64",
	runner_protocol: "runner/1",
	...overrides,
});

describe("compare_versions", () => {
	test("orders numerically, component by component", () => {
		expect(compare_versions("2.28", "2.35")).toBeLessThan(0);
		expect(compare_versions("2.35", "2.28")).toBeGreaterThan(0);
		expect(compare_versions("2.10", "2.9")).toBeGreaterThan(0);
		expect(compare_versions("2.31", "2.31")).toBe(0);
	});

	test("treats a missing component as zero", () => {
		expect(compare_versions("2", "2.0")).toBe(0);
		expect(compare_versions("2.0.1", "2.0")).toBeGreaterThan(0);
	});
});

describe("check_compatible", () => {
	const host: HostInfo = { arch: "aarch64", glibc: "2.31", protocols: ["runner/1"] };

	test("accepts an artifact that matches the host", () => {
		expect(() => check_compatible(spec({ min_glibc: "2.28" }), host)).not.toThrow();
		expect(() => check_compatible(spec({ min_glibc: "2.31" }), host)).not.toThrow();
		expect(() => check_compatible(spec(), host)).not.toThrow();
	});

	test("refuses the wrong architecture", () => {
		expect(() => check_compatible(spec({ arch: "x86_64" }), host)).toThrow(IncompatibleArtifactError);
	});

	test("refuses a runner protocol this harness does not speak", () => {
		expect(() => check_compatible(spec({ runner_protocol: "runner/2" }), host)).toThrow(IncompatibleArtifactError);
	});

	test("refuses an artifact that needs a newer glibc than the host has", () => {
		expect(() => check_compatible(spec({ min_glibc: "2.34" }), host)).toThrow(IncompatibleArtifactError);
	});

});

describe("detect_host", () => {
	test("reports the architecture in the artifact contract's terms and the host's glibc", () => {
		const host = detect_host();
		expect(["aarch64", "x86_64"]).toContain(host.arch);
		expect(host.protocols).toEqual(["runner/1"]);
		expect(host.glibc).toMatch(/^\d+\.\d+/);
	});
});
