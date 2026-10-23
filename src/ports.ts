import type { ArtifactSpec, Execution, JobMessage, Progress, RunOutcome } from "./types";

export interface QueueMessage {
	id: string;
	receipt: string;
	receive_count: number;
	body: JobMessage;
}

/** The lane's SQS queue. In production: long-poll receive, delete on ack, visibility heartbeat on the slow lane. */
export interface Queue {
	receive(max: number, wait_ms: number): Promise<QueueMessage[]>;
	ack(message: QueueMessage): Promise<void>;
}

/** The version registry: one row per supported version, with its artifact and its place in the hot list. */
export interface Registry {
	resolve(version: string): ArtifactSpec | undefined;
	/** Versions to prefetch on start, most popular first. */
	hot_versions(): ArtifactSpec[];
}

/**
 * Everything that happens to the job record once the validator has answered. In production these are the
 * conditional DynamoDB writes and the Redis publish from the design's finalization contract; the worker acks
 * the message only after `finalize` returns, so the record is always the durable copy.
 */
export interface Finalizer {
	/** `execution` is absent when the job never reached a runner (expired, unknown version, hash mismatch). */
	finalize(job: JobMessage, outcome: RunOutcome, execution?: Execution): Promise<void>;
	/** Fast-lane overrun: flip the record to the slow lane, send the slow message, mark it dispatched. */
	escalate(job: JobMessage, execution: Execution): Promise<void>;
	publish_progress(job: JobMessage, progress: Progress): void;
}
