import { LruIndex } from "../src/lru";

describe("LruIndex", () => {
	test("oldest and newest follow use, not insertion", () => {
		const index = new LruIndex<string, number>();
		index.set("a", 1);
		index.set("b", 2);
		index.set("c", 3);
		index.touch("a");
		expect(index.oldest()?.[0]).toBe("b");
		expect(index.newest()?.[0]).toBe("a");
	});

	test("predicates skip entries that do not qualify", () => {
		const index = new LruIndex<string, { busy: boolean }>();
		index.set("a", { busy: true });
		index.set("b", { busy: false });
		expect(index.oldest((value) => !value.busy)?.[0]).toBe("b");
		expect(index.oldest((value) => value.busy && false)).toBeUndefined();
	});
});
