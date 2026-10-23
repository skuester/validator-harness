"""Unit tests for the frozen runner, driven through a socketpair exactly as the harness would drive it."""
import json
import os
import resource
import signal
import socket
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import runner  # noqa: E402
import adapter_example  # noqa: E402


class StubAdapter:
    """What a per-version adapter looks like to the runner; steered by fields in the simulation JSON."""

    VERSION = "stub"

    @staticmethod
    def validate(simulation, *, deadline_s, progress):
        sim = json.loads(simulation)
        progress("parse", 0.5)
        if sim.get("sleep_ms"):
            time.sleep(sim["sleep_ms"] / 1000.0)
        if sim.get("allocate_mb"):
            _hold = bytearray(int(sim["allocate_mb"]) * 1024 * 1024)
        if sim.get("spam_progress_ms"):
            until = time.monotonic() + sim["spam_progress_ms"] / 1000.0
            while time.monotonic() < until:
                progress("spam", 0.1)
                time.sleep(0.005)
        if sim.get("close_pipe_and_linger"):
            os.closerange(3, 1024)
            time.sleep(5)
        if sim.get("huge_result_mb"):
            return {"blob": "x" * (int(sim["huge_result_mb"]) * 1024 * 1024)}
        if sim.get("raise"):
            raise ValueError("monitor outside domain")
        if sim.get("oom"):
            raise MemoryError()
        if sim.get("crash"):
            os._exit(3)
        return {"valid": True, "echo": sim}


def request(job_id="j1", budget_ms=2000, memory_limit_bytes=None, **sim):
    return {"type": "run", "job_id": job_id, "simulation": json.dumps(sim), "budget_ms": budget_ms, "memory_limit_bytes": memory_limit_bytes}


class HarnessEnd:
    """The harness side of the socketpair: writes messages, reads one parsed line at a time with a timeout."""

    def __init__(self, sock):
        self.sock = sock
        self.sock.settimeout(5.0)
        self.lines = runner.LineBuffer()
        self.pending = []

    def send(self, obj):
        self.sock.sendall(runner.encode(obj))

    def read(self):
        while not self.pending:
            data = self.sock.recv(65536)
            if not data:
                return None
            self.pending.extend(self.lines.feed(data))
        return json.loads(self.pending.pop(0))


class RunnerTest(unittest.TestCase):
    def setUp(self):
        harness_sock, runner_sock = socket.socketpair()
        self.harness = HarnessEnd(harness_sock)
        self.runner = runner.Runner(runner_sock, StubAdapter, "stub", 0)

    def tearDown(self):
        self.runner.kill_active()
        self.harness.sock.close()
        self.runner.sock.close()

    def drive_until_idle(self, limit_s=5.0):
        deadline = time.monotonic() + limit_s
        while self.runner.active is not None and time.monotonic() < deadline:
            self.runner.serve_once()
        self.assertIsNone(self.runner.active, "job did not finish in time")

    def read_until(self, kind):
        collected = []
        while True:
            message = self.harness.read()
            self.assertIsNotNone(message)
            collected.append(message)
            if message["type"] == kind:
                return collected

    def test_line_buffer_splits_lines_and_keeps_the_partial_tail(self):
        buffer = runner.LineBuffer()
        self.assertEqual(buffer.feed(b'{"a":1}\n{"b":'), [b'{"a":1}'])
        self.assertEqual(buffer.feed(b"2}\n\n"), [b'{"b":2}'])
        self.assertEqual(buffer.buffer, b"")

    def test_line_buffer_refuses_a_frame_over_the_limit(self):
        buffer = runner.LineBuffer(limit=16)
        with self.assertRaises(runner.FrameTooLarge):
            buffer.feed(b"x" * 17)
        buffer = runner.LineBuffer(limit=16)
        with self.assertRaises(runner.FrameTooLarge):
            buffer.feed(b"x" * 17 + b"\n")

    def test_serve_reports_ready_and_stops_on_shutdown(self):
        thread = threading.Thread(target=self.runner.serve, daemon=True)
        thread.start()
        ready = self.harness.read()
        self.assertEqual(ready["type"], "ready")
        self.assertEqual(ready["protocol"], runner.PROTOCOL)
        self.assertEqual(ready["version"], "stub")
        self.harness.send({"type": "shutdown"})
        thread.join(5.0)
        self.assertFalse(thread.is_alive())

    def test_a_job_runs_in_a_child_and_streams_progress_before_its_result(self):
        self.harness.send(request(valid=True))
        self.runner.serve_once()
        self.assertIsNotNone(self.runner.active)
        self.drive_until_idle()
        messages = self.read_until("result")
        self.assertEqual([m["type"] for m in messages], ["progress", "result"])
        self.assertEqual(messages[0]["stage"], "parse")
        result = messages[1]
        self.assertEqual(result["job_id"], "j1")
        self.assertEqual(result["outcome"], "succeeded")
        self.assertEqual(json.loads(result["result_json"])["echo"], {"valid": True})
        self.assertGreaterEqual(result["elapsed_ms"], 0)

    def test_a_result_is_sent_only_once_the_child_is_reaped_so_the_next_job_can_start_at_once(self):
        self.harness.send(request(job_id="first"))
        self.runner.serve_once()
        pid = self.runner.active["pid"]
        self.drive_until_idle()
        self.assertEqual(self.read_until("result")[-1]["job_id"], "first")
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)  # already reaped when the result went out
        self.harness.send(request(job_id="second"))
        self.runner.serve_once()
        self.assertIsNotNone(self.runner.active)
        self.drive_until_idle()
        self.assertEqual(self.read_until("result")[-1]["outcome"], "succeeded")

    def test_the_deadline_holds_even_when_the_child_never_stops_talking(self):
        self.harness.send(request(budget_ms=150, spam_progress_ms=5000))
        self.runner.serve_once()
        started = time.monotonic()
        self.drive_until_idle()
        self.assertLess(time.monotonic() - started, 1.5)
        self.assertEqual(self.read_until("result")[-1]["outcome"], "timeout")

    def test_a_child_that_closes_its_pipe_but_keeps_running_is_killed_and_reported(self):
        self.harness.send(request(close_pipe_and_linger=True))
        self.runner.serve_once()
        pid = self.runner.active["pid"]
        started = time.monotonic()
        self.drive_until_idle()
        self.assertLess(time.monotonic() - started, 1.5)
        result = self.read_until("result")[-1]
        self.assertEqual(result["outcome"], "crashed")
        self.assertEqual(result["error"]["code"], "validator_crashed")
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, 0)

    def test_a_result_over_the_frame_limit_is_a_crash_not_a_parent_allocation(self):
        self.harness.send(request(huge_result_mb=5))
        self.runner.serve_once()
        self.drive_until_idle()
        result = self.read_until("result")[-1]
        self.assertEqual((result["outcome"], result["error"]["code"]), ("crashed", "result_too_large"))
        self.assertLess(len(self.runner.sock_lines.buffer), 1024)

    def test_an_overrun_is_killed_at_the_budget_and_reported_as_timeout(self):
        self.harness.send(request(budget_ms=150, sleep_ms=5000))
        self.runner.serve_once()
        pid = self.runner.active["pid"]
        started = time.monotonic()
        self.drive_until_idle()
        self.assertLess(time.monotonic() - started, 2.0)
        result = self.read_until("result")[-1]
        self.assertEqual(result["outcome"], "timeout")
        self.assertGreaterEqual(result["elapsed_ms"], 150)
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, 0)  # already reaped: no zombie left behind

    def test_cancel_kills_the_active_job(self):
        self.harness.send(request(sleep_ms=5000))
        self.runner.serve_once()
        started = time.monotonic()
        self.harness.send({"type": "cancel", "job_id": "j1"})
        self.runner.serve_once()
        self.assertIsNone(self.runner.active)
        self.assertLess(time.monotonic() - started, 1.0, "cancel must kill the child, not wait for it")
        self.assertEqual(self.read_until("result")[-1]["outcome"], "canceled")

    def test_cancel_for_another_job_is_ignored(self):
        self.harness.send(request(sleep_ms=100))
        self.runner.serve_once()
        self.harness.send({"type": "cancel", "job_id": "someone-else"})
        self.runner.serve_once()
        self.assertIsNotNone(self.runner.active)
        self.drive_until_idle()
        self.assertEqual(self.read_until("result")[-1]["outcome"], "succeeded")

    def test_a_second_run_while_busy_is_refused_without_touching_the_first(self):
        self.harness.send(request(job_id="first", sleep_ms=100))
        self.runner.serve_once()
        self.harness.send(request(job_id="second"))
        self.runner.serve_once()
        refused = self.harness.read()
        self.assertEqual((refused["job_id"], refused["outcome"], refused["error"]["code"]), ("second", "failed", "runner_busy"))
        self.drive_until_idle()
        self.assertEqual(self.read_until("result")[-1]["job_id"], "first")

    def test_a_validator_exception_is_a_failed_verdict(self):
        self.harness.send(request(**{"raise": True}))
        self.runner.serve_once()
        self.drive_until_idle()
        result = self.read_until("result")[-1]
        self.assertEqual(result["outcome"], "failed")
        self.assertEqual(result["error"]["code"], "validator_error")
        self.assertIn("monitor outside domain", result["error"]["message"])

    def test_a_child_that_exits_without_a_result_is_reported_crashed_with_its_exit_code(self):
        self.harness.send(request(crash=True))
        self.runner.serve_once()
        self.drive_until_idle()
        result = self.read_until("result")[-1]
        self.assertEqual(result["outcome"], "crashed")
        self.assertEqual(result["error"], {"code": "validator_crashed", "exit_code": 3})

    def test_a_child_killed_by_a_signal_is_reported_crashed_with_the_signal(self):
        self.harness.send(request(sleep_ms=5000))
        self.runner.serve_once()
        os.kill(self.runner.active["pid"], signal.SIGKILL)
        self.drive_until_idle()
        result = self.read_until("result")[-1]
        self.assertEqual(result["outcome"], "crashed")
        self.assertEqual(result["error"], {"code": "validator_crashed", "signal": int(signal.SIGKILL)})

    def test_losing_the_harness_kills_the_active_job_and_exits(self):
        self.harness.send(request(sleep_ms=5000))
        self.runner.serve_once()
        pid = self.runner.active["pid"]
        self.harness.sock.close()
        with self.assertRaises(SystemExit):
            self.runner.serve_once()
        self.assertIsNone(self.runner.active)
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, 0)

    def test_shutdown_while_busy_kills_the_job_and_exits(self):
        self.harness.send(request(sleep_ms=5000))
        self.runner.serve_once()
        self.harness.send({"type": "shutdown"})
        with self.assertRaises(SystemExit):
            self.runner.serve_once()
        self.assertIsNone(self.runner.active)

    def test_send_to_a_vanished_harness_exits_instead_of_raising(self):
        self.harness.sock.close()
        with self.assertRaises(SystemExit):
            for _ in range(64):  # enough to overflow the socket buffer and surface the error
                self.runner.send({"type": "progress", "job_id": "x", "stage": "s" * 4096, "pct": 0})

    def test_kill_active_tolerates_a_child_that_is_already_gone(self):
        self.harness.send(request())
        self.runner.serve_once()
        pid = self.runner.active["pid"]
        os.waitpid(pid, 0)  # reap it out from under the runner
        self.runner.kill_active()
        self.assertIsNone(self.runner.active)

    def test_run_child_reports_success_oom_and_errors_in_process(self):
        def run(**sim):
            read_end, write_end = os.pipe()
            self.runner.run_child(write_end, request(**sim))
            data = b""
            while True:
                chunk = os.read(read_end, 65536)
                if not chunk:
                    break
                data += chunk
            os.close(read_end)
            return [json.loads(line) for line in data.split(b"\n") if line]

        ok = run(valid=True)
        self.assertEqual([m["type"] for m in ok], ["progress", "result"])
        self.assertEqual(ok[-1]["outcome"], "succeeded")
        self.assertEqual(json.loads(ok[-1]["result_json"])["valid"], True)
        self.assertEqual(run(oom=True)[-1]["error"], {"code": "validator_oom"})
        self.assertEqual(run(**{"raise": True})[-1]["outcome"], "failed")

    def test_the_memory_limit_is_enforced_on_the_child(self):
        self.harness.send(request(memory_limit_bytes=256 << 20, allocate_mb=1024))
        self.runner.serve_once()
        self.drive_until_idle()
        result = self.read_until("result")[-1]
        self.assertEqual(result["outcome"], "crashed")
        self.assertEqual(result["error"], {"code": "validator_oom"})

    def test_a_memory_limit_above_the_hard_limit_is_reported_not_ignored(self):
        # Lowering a hard limit is irreversible for a process, so the scenario runs in a throwaway fork:
        # cap RLIMIT_DATA there, ask the job for twice the cap, and the child must report it rather than run unlimited.
        pid = os.fork()
        if pid == 0:
            code = 2
            try:
                _soft, hard = resource.getrlimit(resource.RLIMIT_DATA)
                cap = 1 << 30 if hard == resource.RLIM_INFINITY else min(hard, 1 << 30)
                resource.setrlimit(resource.RLIMIT_DATA, (cap, cap))
                self.harness.send(request(memory_limit_bytes=cap * 2))
                self.runner.serve_once()
                self.drive_until_idle()
                result = self.read_until("result")[-1]
                code = 0 if result.get("error", {}).get("code") == "memory_limit_failed" else 1
                if code:
                    sys.stderr.write(f"unexpected result in forked scenario: {result}\n")
            except BaseException as error:
                sys.stderr.write(f"forked scenario failed: {error!r}\n")
            finally:
                sys.stderr.flush()
                os._exit(code)
        _, status = os.waitpid(pid, 0)
        self.assertEqual(os.WEXITSTATUS(status), 0, "the forked scenario failed; its stderr says why")

    def test_die_with_parent_accepts_the_real_parent_and_registers_the_signal(self):
        runner.die_with_parent(os.getppid())


class AdapterExampleTest(unittest.TestCase):
    def test_validates_and_names_its_version(self):
        stages = []
        result = adapter_example.validate(b'{"valid": true}', deadline_s=1.0, progress=lambda stage, pct: stages.append(stage))
        self.assertTrue(result["valid"])
        self.assertEqual(result["validated_by"], adapter_example.VERSION)
        self.assertEqual(stages, ["parse", "rules"])

    def test_reports_an_invalid_simulation_as_an_error_not_an_exception(self):
        result = adapter_example.validate(b'{"valid": false}', deadline_s=1.0, progress=lambda *_: None)
        self.assertFalse(result["valid"])
        self.assertEqual(result["errors"][0]["code"], "monitor_outside_domain")

    def test_sleep_and_allocate_are_honoured(self):
        started = time.monotonic()
        adapter_example.validate(b'{"sleep_ms": 50, "allocate_mb": 1}', deadline_s=1.0, progress=lambda *_: None)
        self.assertGreaterEqual(time.monotonic() - started, 0.05)


if __name__ == "__main__":
    unittest.main()
