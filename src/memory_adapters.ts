import type { Finalizer, Queue, QueueMessage, Registry } from "./ports";
import type { ArtifactSpec, EventListener, Execution, JobMessage, Progress, RunOutcome } from "./types";

/** An in-memory queue with SQS's shape: receive up to N, hold them in flight, delete on ack. Unacked messages stay in flight. */
export class MemoryQueue implements Queue {
	private readonly ready: QueueMessage[] = [];
	private readonly in_flight = new Map<string, QueueMessage>();
	private waiters: Array<() => void> = [];
	private closed = false;
	private sequence = 0;
	readonly acked: string[] = [];

	push(body: JobMessage): void {
		this.sequence += 1;
		this.ready.push({ id: `m-${this.sequence}`, receipt: `r-${this.sequence}`, receive_count: 1, body });
		this.wake();
	}

	async receive(max: number, wait_ms: number): Promise<QueueMessage[]> {
		if (this.ready.length === 0 && !this.closed) {
			await new Promise<void>((resolve) => {
				const timer = setTimeout(() => {
					this.waiters = this.waiters.filter((waiter) => waiter !== wake);
					resolve();
				}, wait_ms);
				const wake = () => {
					clearTimeout(timer);
					resolve();
				};
				this.waiters.push(wake);
			});
		}
		const taken = this.ready.splice(0, max);
		for (const message of taken) this.in_flight.set(message.receipt, message);
		return taken;
	}

	async ack(message: QueueMessage): Promise<void> {
		if (this.in_flight.delete(message.receipt)) this.acked.push(message.body.job_id);
	}

	/** Make any blocked receive return at once; the worker loop then sees `stopping`. */
	close(): void {
		this.closed = true;
		this.wake();
	}

	get in_flight_count(): number {
		return this.in_flight.size;
	}

	private wake(): void {
		for (const waiter of this.waiters.splice(0)) waiter();
	}
}

export class MemoryRegistry implements Registry {
	private readonly by_version = new Map<string, ArtifactSpec>();

	constructor(specs: ArtifactSpec[]) {
		for (const spec of specs) this.by_version.set(spec.version, spec);
	}

	resolve(version: string): ArtifactSpec | undefined {
		return this.by_version.get(version);
	}

	hot_versions(): ArtifactSpec[] {
		return [...this.by_version.values()]
			.filter((spec) => spec.rank !== undefined)
			.sort((a, b) => (a.rank as number) - (b.rank as number));
	}

	/** A fix shipped: same version, new build and digest. Running jobs finish on the old build; new ones get the new one. */
	replace(spec: ArtifactSpec): void {
		this.by_version.set(spec.version, spec);
	}
}

/** Records what the worker would have written, so tests and the demo can assert on it. */
export class RecordingFinalizer implements Finalizer {
	readonly finalized: Array<{ job: JobMessage; outcome: RunOutcome; execution?: Execution }> = [];
	readonly escalated: Array<{ job: JobMessage; execution: Execution }> = [];
	readonly progress: Array<{ job_id: string; progress: Progress }> = [];
	private waiters: Array<() => void> = [];

	constructor(private readonly on_event?: EventListener) {}

	async finalize(job: JobMessage, outcome: RunOutcome, execution?: Execution): Promise<void> {
		this.finalized.push({ job, outcome, execution });
		this.on_event?.({ component: "worker", type: "finalize", job: job.job_id, version: job.client_version, build: execution?.build_id, outcome: outcome.outcome, elapsed_ms: outcome.elapsed_ms, error: outcome.error?.code });
		this.wake();
	}

	async escalate(job: JobMessage, execution: Execution): Promise<void> {
		this.escalated.push({ job, execution });
		this.on_event?.({ component: "worker", type: "escalate", job: job.job_id, version: job.client_version, build: execution.build_id });
		this.wake();
	}

	publish_progress(job: JobMessage, progress: Progress): void {
		this.progress.push({ job_id: job.job_id, progress });
	}

	get settled(): number {
		return this.finalized.length + this.escalated.length;
	}

	/** Resolve once this many jobs have been finalized or escalated. */
	wait_for(count: number, timeout_ms = 20_000): Promise<void> {
		if (this.settled >= count) return Promise.resolve();
		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => reject(new Error(`only ${this.settled} of ${count} jobs settled after ${timeout_ms} ms`)), timeout_ms);
			const check = () => {
				if (this.settled >= count) {
					clearTimeout(timer);
					resolve();
				} else {
					this.waiters.push(check);
				}
			};
			this.waiters.push(check);
		});
	}

	private wake(): void {
		for (const waiter of this.waiters.splice(0)) waiter();
	}
}
