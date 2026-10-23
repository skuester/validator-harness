import { check_memory_budget } from "../src/config";

const GiB = 1024 ** 3;
const MiB = 1024 ** 2;

describe("check_memory_budget", () => {
	test("charges every resident runner parent plus every slot's job limit", () => {
		const result = check_memory_budget({
			task_memory_bytes: 4 * GiB,
			harness_reserve_bytes: 256 * MiB,
			slots: 2,
			memory_limit_per_job_bytes: 1 * GiB,
			max_children: 3,
			idle_child_rss_bytes: 400 * MiB,
		});
		expect(result.worst_case_bytes).toBe(256 * MiB + 3 * 400 * MiB + 2 * GiB);
		expect(result.headroom_bytes).toBe(4 * GiB - result.worst_case_bytes);
	});

	test("refuses the design's old shape: two 1.5 GiB slots plus parents do not fit 4 GiB", () => {
		expect(() =>
			check_memory_budget({ task_memory_bytes: 4 * GiB, harness_reserve_bytes: 256 * MiB, slots: 2, memory_limit_per_job_bytes: 1.5 * GiB, max_children: 3, idle_child_rss_bytes: 400 * MiB }),
		).toThrow(/exceeds the task by/);
	});

	test("refuses fewer resident children than slots", () => {
		expect(() =>
			check_memory_budget({ task_memory_bytes: 4 * GiB, harness_reserve_bytes: 0, slots: 2, memory_limit_per_job_bytes: 1 * GiB, max_children: 1, idle_child_rss_bytes: 0 }),
		).toThrow(/at least slots/);
	});
});
