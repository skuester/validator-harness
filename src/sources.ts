import fs from "node:fs";
import { pipeline } from "node:stream/promises";

/** Where artifact archives come from. The store only needs "put the bytes for this key in this file". */
export interface ArtifactSource {
	fetch(key: string, destination_path: string): Promise<void>;
}

/** Archives in a local directory. Used by the demo and the tests. */
export class LocalDirectorySource implements ArtifactSource {
	fetches = 0;

	constructor(private readonly directory: string) {}

	async fetch(key: string, destination_path: string): Promise<void> {
		this.fetches += 1;
		await fs.promises.copyFile(`${this.directory}/${key}`, destination_path);
	}
}

/**
 * Archives in S3. Takes a function rather than an SDK client so this package has no AWS dependency; wire it as
 *
 *   new StreamSource(async (key) => (await s3.send(new GetObjectCommand({ Bucket, Key: key }))).Body as Readable)
 *
 * Ranged parallel GETs would speed up large artifacts; a single stream is enough to show the shape.
 */
export class StreamSource implements ArtifactSource {
	constructor(private readonly open: (key: string) => Promise<NodeJS.ReadableStream>) {}

	async fetch(key: string, destination_path: string): Promise<void> {
		const body = await this.open(key);
		await pipeline(body, fs.createWriteStream(destination_path));
	}
}
