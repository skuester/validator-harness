/**
 * An insertion-ordered map used as a recency index: the first entry is the least recently used.
 * Both the process table and the artifact cache sit on this; eviction policy lives in their owners,
 * because what counts as evictable (idle, not pinned, no live child) is their business, not the index's.
 */
export class LruIndex<K, V> {
	private readonly map = new Map<K, V>();

	get size(): number {
		return this.map.size;
	}

	has(key: K): boolean {
		return this.map.has(key);
	}

	get(key: K): V | undefined {
		return this.map.get(key);
	}

	/** Insert or replace, and mark as most recently used. */
	set(key: K, value: V): void {
		this.map.delete(key);
		this.map.set(key, value);
	}

	/** Mark as most recently used without changing the value. */
	touch(key: K): void {
		const value = this.map.get(key);
		if (value === undefined) return;
		this.map.delete(key);
		this.map.set(key, value);
	}

	delete(key: K): boolean {
		return this.map.delete(key);
	}

	/** The least recently used entry that satisfies the predicate. */
	oldest(predicate: (value: V, key: K) => boolean = () => true): [K, V] | undefined {
		for (const [key, value] of this.map) {
			if (predicate(value, key)) return [key, value];
		}
		return undefined;
	}

	/** The most recently used entry that satisfies the predicate. */
	newest(predicate: (value: V, key: K) => boolean = () => true): [K, V] | undefined {
		let found: [K, V] | undefined;
		for (const [key, value] of this.map) {
			if (predicate(value, key)) found = [key, value];
		}
		return found;
	}

	*entries_oldest_first(): IterableIterator<[K, V]> {
		yield* this.map.entries();
	}

	values(): V[] {
		return [...this.map.values()];
	}
}
