import fs from "node:fs";
import net from "node:net";
import { StringDecoder } from "node:string_decoder";
import type { ChildProcess } from "node:child_process";
import type { ProgressListener, RunOutcome, RunRequest } from "./types";

export type ExitListener = (code: number | null, signal: NodeJS.Signals | null) => void;

/** Longest line accepted from a runner. Results are assumed under 50 KiB; this is the ceiling before the runner is treated as broken. */
export const FRAME_LIMIT = 4 * 1024 * 1024;

/** What the pool needs from a running runner process. RunnerEndpoint is the real one; pool tests use fakes. */
export interface RunnerHandle {
	readonly version: string;
	readonly pid: number | undefined;
	readonly exited: boolean;
	run(request: RunRequest, on_progress?: ProgressListener): Promise<RunOutcome>;
	cancel(job_id: string): void;
	stop(grace_ms: number): Promise<void>;
	on_exit(listener: ExitListener): void;
}

export interface EndpointOptions {
	socket_path: string;
	version: string;
	expected_protocol: string;
	ready_timeout_ms: number;
	/** Time past a job's budget before the harness decides the runner itself is hung. The runner enforces the budget; this is the backstop. */
	grace_ms?: number;
	spawn_child: () => ChildProcess;
}

interface ReadyInfo {
	protocol: string;
	version: string;
	pid: number;
	import_ms?: number;
}

interface Pending {
	job_id: string;
	settle: (outcome: RunOutcome) => void;
	on_progress?: ProgressListener;
	timer?: NodeJS.Timeout;
	/** Set once the harness has given up on the runner and killed it, so the exit is reported as unresponsive, not crashed. */
	killed_by_harness?: boolean;
	/** Set when the backstop fired: a `canceled` that comes back then is a budget overrun, not a client cancel. */
	budget_exceeded?: boolean;
}

/**
 * The harness side of `runner/1`: one Unix socket per runner process, newline-delimited JSON both ways.
 * The harness listens and the runner connects, so the moment the runner is up there is a connection,
 * and the first thing on it is `ready` (sent after the validator library has been imported).
 */
export class RunnerEndpoint implements RunnerHandle {
	readonly version: string;
	readonly ready: Promise<ReadyInfo>;
	private socket: net.Socket | undefined;
	private pending: Pending | undefined;
	private _exited = false;
	private readonly exit_listeners: ExitListener[] = [];
	private ready_resolve: ((info: ReadyInfo) => void) | undefined;
	private ready_reject: ((error: Error) => void) | undefined;
	private readonly grace_ms: number;

	private constructor(
		private readonly server: net.Server,
		private readonly child: ChildProcess,
		private readonly opts: EndpointOptions,
	) {
		this.version = opts.version;
		this.grace_ms = opts.grace_ms ?? 1_500;
		this.ready = new Promise<ReadyInfo>((resolve, reject) => {
			this.ready_resolve = resolve;
			this.ready_reject = reject;
		});
		this.ready.catch(() => undefined);
		server.on("connection", (socket) => this.on_connection(socket));
		child.on("exit", (code, signal) => this.on_exit_event(code, signal));
		child.on("error", (error) => this.ready_reject?.(error));
	}

	/** Listen first, then spawn. Resolves once the runner reports `ready` with the expected protocol. */
	static async create(opts: EndpointOptions): Promise<RunnerEndpoint> {
		fs.rmSync(opts.socket_path, { force: true });
		const server = net.createServer();
		await new Promise<void>((resolve, reject) => {
			server.once("error", reject);
			server.listen(opts.socket_path, () => {
				server.off("error", reject);
				resolve();
			});
		});
		const endpoint = new RunnerEndpoint(server, opts.spawn_child(), opts);
		const timer = setTimeout(
			() => endpoint.ready_reject?.(new Error(`runner for ${opts.version} not ready after ${opts.ready_timeout_ms} ms`)),
			opts.ready_timeout_ms,
		);
		try {
			const info = await endpoint.ready;
			if (info.protocol !== opts.expected_protocol) {
				throw new Error(`runner for ${opts.version} speaks ${info.protocol}, harness expects ${opts.expected_protocol}`);
			}
			return endpoint;
		} catch (error) {
			await endpoint.stop(0);
			throw error;
		} finally {
			clearTimeout(timer);
		}
	}

	get pid(): number | undefined {
		return this.child.pid;
	}

	get exited(): boolean {
		return this._exited;
	}

	/** Hand one job to the runner. One job at a time per process; the pool guarantees that. */
	run(request: RunRequest, on_progress?: ProgressListener): Promise<RunOutcome> {
		if (this.pending) throw new Error(`runner ${this.version} is busy with ${this.pending.job_id}`);
		if (this._exited || !this.socket) {
			return Promise.resolve({ outcome: "crashed", error: { code: "runner_exited" } });
		}
		return new Promise<RunOutcome>((resolve) => {
			const pending: Pending = {
				job_id: request.job_id,
				on_progress,
				settle: (outcome) => {
					if (pending.timer) clearTimeout(pending.timer);
					this.pending = undefined;
					resolve(outcome);
				},
			};
			this.pending = pending;
			pending.timer = setTimeout(() => {
				pending.budget_exceeded = true;
				this.cancel(request.job_id);
				pending.timer = setTimeout(() => {
					pending.killed_by_harness = true;
					this.kill("SIGKILL");
					void this.wait_exit(1_000).then(() => pending.settle({ outcome: "runner_unresponsive" }));
				}, 500);
			}, request.budget_ms + this.grace_ms);
			this.write({ type: "run", ...request });
		});
	}

	cancel(job_id: string): void {
		this.write({ type: "cancel", job_id });
	}

	/** Polite shutdown first, then SIGTERM, then SIGKILL to the process group so a forked validator child cannot be orphaned. */
	async stop(grace_ms: number): Promise<void> {
		if (!this._exited) {
			this.write({ type: "shutdown" });
			if (!(await this.wait_exit(grace_ms))) {
				this.kill("SIGTERM");
				if (!(await this.wait_exit(1_000))) {
					this.kill("SIGKILL");
					await this.wait_exit(2_000);
				}
			}
		}
		this.close_server();
	}

	on_exit(listener: ExitListener): void {
		if (this._exited) listener(this.child.exitCode, this.child.signalCode);
		else this.exit_listeners.push(listener);
	}

	private on_connection(socket: net.Socket): void {
		if (this.socket) {
			socket.destroy();
			return;
		}
		this.socket = socket;
		const decoder = new StringDecoder("utf8");
		let buffer = "";
		socket.on("data", (chunk) => {
			buffer += decoder.write(chunk);
			if (buffer.length > FRAME_LIMIT) {
				this.protocol_error("frame_too_large");
				return;
			}
			let newline = buffer.indexOf("\n");
			while (newline >= 0) {
				const line = buffer.slice(0, newline);
				buffer = buffer.slice(newline + 1);
				if (line) this.on_line(line);
				newline = buffer.indexOf("\n");
			}
		});
		socket.on("error", () => undefined);
		socket.on("close", () => {
			this.socket = undefined;
			// After a harness-initiated kill the process exit settles the job, so `exited` is true by then.
			if (!this.pending?.killed_by_harness) this.fail_pending("runner_disconnected");
		});
	}

	private on_line(line: string): void {
		const message = this.parse_frame(line);
		if (!message) return;
		switch (message.type) {
			case "ready":
				this.ready_resolve?.(message as unknown as ReadyInfo);
				return;
			case "progress":
				this.on_progress(message);
				return;
			case "result":
				this.on_result(message);
				return;
		}
	}

	/** A frame must be a JSON object; anything else means the runner is not speaking the protocol. */
	private parse_frame(line: string): Record<string, unknown> | undefined {
		let message: unknown;
		try {
			message = JSON.parse(line);
		} catch {
			message = null;
		}
		if (message === null || typeof message !== "object") {
			this.protocol_error("malformed_frame");
			return undefined;
		}
		return message as Record<string, unknown>;
	}

	private on_progress(message: Record<string, unknown>): void {
		if (!this.pending || message.job_id !== this.pending.job_id) return;
		this.pending.on_progress?.({ stage: String(message.stage), pct: Number(message.pct) });
	}

	/** The result keeps the validator's bytes in `result_json`; `result` is parsed for convenience only. */
	private on_result(message: Record<string, unknown>): void {
		if (!this.pending || message.job_id !== this.pending.job_id) return;
		const { type: _type, job_id: _job_id, ...rest } = message;
		const outcome = rest as unknown as RunOutcome;
		if (typeof outcome.result_json === "string") {
			try {
				outcome.result = JSON.parse(outcome.result_json);
			} catch {
				outcome.result = undefined;
			}
		}
		if (outcome.outcome === "canceled" && this.pending.budget_exceeded) outcome.outcome = "timeout";
		this.pending.settle(outcome);
	}

	/** The runner broke the framing contract: it is killed, and the pending job is reported as a runner failure. */
	private protocol_error(code: string): void {
		if (this.pending) this.pending.killed_by_harness = false;
		this.fail_pending(code);
		this.socket?.destroy();
		this.kill("SIGKILL");
	}

	private on_exit_event(code: number | null, signal: NodeJS.Signals | null): void {
		this._exited = true;
		this.ready_reject?.(new Error(`runner for ${this.version} exited before ready (code ${code}, signal ${signal})`));
		this.fail_pending("runner_exited", code, signal);
		this.close_server();
		for (const listener of this.exit_listeners.splice(0)) listener(code, signal);
	}

	private fail_pending(code: string, exit_code: number | null = null, signal: NodeJS.Signals | null = null): void {
		if (!this.pending) return;
		const outcome: RunOutcome["outcome"] = this.pending.killed_by_harness ? "runner_unresponsive" : "crashed";
		this.pending.settle({ outcome, error: { code, exit_code, signal } });
	}

	private wait_exit(ms: number): Promise<boolean> {
		if (this._exited) return Promise.resolve(true);
		return new Promise((resolve) => {
			const timer = setTimeout(() => resolve(false), ms);
			this.exit_listeners.push(() => {
				clearTimeout(timer);
				resolve(true);
			});
		});
	}

	/** Runners are spawned detached, so their pid is a process group: the signal reaches the runner and anything under it. */
	private kill(signal: NodeJS.Signals): void {
		if (this._exited || !this.child.pid) return;
		try {
			process.kill(-this.child.pid, signal);
		} catch {
			// already gone
		}
	}

	private write(message: Record<string, unknown>): void {
		if (!this.socket || this.socket.destroyed) return;
		this.socket.write(JSON.stringify(message) + "\n");
	}

	private close_server(): void {
		this.socket?.destroy();
		this.socket = undefined;
		this.server.close();
		fs.rmSync(this.opts.socket_path, { force: true });
	}
}
