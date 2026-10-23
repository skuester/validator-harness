import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { RunnerEndpoint } from "../src/runner_endpoint";

let run_dir: string;
beforeAll(() => {
	run_dir = fs.mkdtempSync(path.join(os.tmpdir(), "vh-ep-"));
});
afterAll(() => fs.rmSync(run_dir, { recursive: true, force: true }));

type Scenario = "deaf" | "wrong_protocol" | "die_before_ready" | "hang_on_run" | "garbage_on_run" | "huge_on_run";

/** A fake runner in Python: connects, says ready, and then misbehaves as the scenario says. */
function fake_runner(scenario: Scenario): (socket_path: string) => ReturnType<typeof spawn> {
	const script = `
import json, signal, socket, sys, time
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect(sys.argv[1])
scenario = sys.argv[2]
if scenario == "die_before_ready":
    sys.exit(7)
if scenario == "deaf":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
protocol = "runner/9" if scenario == "wrong_protocol" else "runner/1"
sock.sendall((json.dumps({"type": "ready", "protocol": protocol, "version": "x", "pid": 0}) + "\\n").encode())
while True:
    data = sock.recv(65536)
    if not data:
        sys.exit(0)
    if scenario == "garbage_on_run":
        sock.sendall(b"null\\n")
    elif scenario == "huge_on_run":
        sock.sendall(b'{"type":"progress","job_id":"j1","stage":"' + b"x" * (5 * 1024 * 1024) + b'","pct":0}\\n')
    time.sleep(0.05)
`;
	return (socket_path) => spawn("python3", ["-c", script, socket_path, scenario], { stdio: ["ignore", "ignore", "inherit"], detached: true });
}

function create(name: string, scenario: Scenario, grace_ms = 100): Promise<RunnerEndpoint> {
	const socket_path = path.join(run_dir, `${name}.sock`);
	return RunnerEndpoint.create({ socket_path, version: "x", expected_protocol: "runner/1", ready_timeout_ms: 5_000, grace_ms, spawn_child: () => fake_runner(scenario)(socket_path) });
}

const request = (job_id = "j1") => ({ job_id, simulation: "{}", budget_ms: 100, memory_limit_bytes: 0 });

describe("RunnerEndpoint", () => {
	test("refuses a runner that speaks another protocol and cleans up its process", async () => {
		await expect(create("wrong", "wrong_protocol")).rejects.toThrow(/speaks runner\/9/);
		expect(fs.existsSync(path.join(run_dir, "wrong.sock"))).toBe(false);
	});

	test("reports a runner that dies before it is ready", async () => {
		await expect(create("dead", "die_before_ready")).rejects.toThrow(/exited before ready/);
	});

	test("escalates from shutdown to SIGTERM to SIGKILL for a runner that ignores both", async () => {
		const endpoint = await create("deaf", "deaf");
		const pid = endpoint.pid as number;
		expect(pid).toBeGreaterThan(0);
		const started = Date.now();
		await endpoint.stop(100);
		expect(endpoint.exited).toBe(true);
		expect(Date.now() - started).toBeLessThan(3_000);
		expect(() => process.kill(pid, 0)).toThrow(); // really gone, not just marked
		let listener_called = false;
		endpoint.on_exit(() => {
			listener_called = true;
		});
		expect(listener_called).toBe(true);
	});

	test("a runner that never answers a run is cancelled, then killed, and the job is reported unresponsive", async () => {
		const endpoint = await create("hang", "hang_on_run");
		const outcome = await endpoint.run(request());
		expect(outcome.outcome).toBe("runner_unresponsive");
		expect(endpoint.exited).toBe(true);
		await expect(endpoint.run(request("j2"))).resolves.toMatchObject({ outcome: "crashed", error: { code: "runner_exited" } });
		await endpoint.stop(0);
	});

	test("a malformed frame fails the job and ends the runner instead of crashing the harness", async () => {
		const endpoint = await create("garbage", "garbage_on_run");
		const outcome = await endpoint.run(request());
		expect(outcome).toMatchObject({ outcome: "crashed", error: { code: "malformed_frame" } });
		await endpoint.stop(0);
		expect(endpoint.exited).toBe(true);
	});

	test("a frame over the limit fails the job and ends the runner", async () => {
		const endpoint = await create("huge", "huge_on_run", 5_000);
		const outcome = await endpoint.run({ ...request(), budget_ms: 5_000 });
		expect(outcome).toMatchObject({ outcome: "crashed", error: { code: "frame_too_large" } });
		await endpoint.stop(0);
		expect(endpoint.exited).toBe(true);
	});

	test("a second run while one is pending is a programming error", async () => {
		const endpoint = await create("busy", "hang_on_run");
		const first = endpoint.run(request());
		expect(() => endpoint.run(request("j2"))).toThrow(/busy/);
		await first;
		await endpoint.stop(0);
	});
});
