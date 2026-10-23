#!/usr/bin/env python3
"""runner/1: the frozen, per-version half of the harness contract.

Standard library only, so it runs on whatever interpreter the version's artifact ships. Linux only, on
purpose: it relies on RLIMIT_DATA, PR_SET_PDEATHSIG and process groups, which is what the task gives it.
The harness listens on a Unix socket. This process connects, imports the adapter (the expensive
step, because that is where the validator library loads), reports `ready`, and then runs one job
at a time. Each job executes in a forked child, in its own process group, under its own memory
limit and wall-clock budget, so a leak, a crash or an overrun ends the child and never the warm parent.

A result is reported only after the child has been reaped: when the harness sees `result`, the runner
can take the next job. Frames on both channels are capped, so a runaway result cannot grow the parent.

Messages are newline-delimited JSON.

  harness -> runner   {"type": "run", "job_id", "simulation", "budget_ms", "memory_limit_bytes"}
                      {"type": "cancel", "job_id"}
                      {"type": "shutdown"}
  runner -> harness   {"type": "ready", "protocol", "version", "pid", "import_ms"}
                      {"type": "progress", "job_id", "stage", "pct"}
                      {"type": "result", "job_id", "outcome", "result_json" | "error", "elapsed_ms"}
"""
import argparse
import ctypes
import importlib
import json
import os
import resource
import selectors
import signal
import socket
import sys
import time

PROTOCOL = "runner/1"
FRAME_LIMIT = 4 * 1024 * 1024
REAP_GRACE_S = 0.1
PR_SET_PDEATHSIG = 1


class FrameTooLarge(Exception):
    pass


def encode(obj):
    return (json.dumps(obj, separators=(",", ":")) + "\n").encode("utf-8")


def write_all(fd, data):
    while data:
        written = os.write(fd, data)
        data = data[written:]


class LineBuffer:
    def __init__(self, limit=FRAME_LIMIT):
        self.buffer = b""
        self.limit = limit

    def feed(self, data):
        self.buffer += data
        lines = self.buffer.split(b"\n")
        self.buffer = lines.pop()
        if len(self.buffer) > self.limit or any(len(line) > self.limit for line in lines):
            raise FrameTooLarge()
        return [line for line in lines if line]


def apply_memory_limit(limit_bytes):
    """RLIMIT_DATA on the child, as the design specifies: heap plus anonymous mappings, which is what a
    runaway NumPy allocation is, without the false kills an address-space limit causes."""
    if limit_bytes:
        resource.setrlimit(resource.RLIMIT_DATA, (int(limit_bytes), int(limit_bytes)))


def close_fds_except(keep):
    """The validator child keeps its result pipe and nothing else: not the harness socket, not the selector."""
    os.closerange(3, keep)
    os.closerange(keep + 1, os.sysconf("SC_OPEN_MAX"))


def die_with_parent(expected_parent):
    """Ask the kernel to kill this process when its parent dies, then check it is still the parent we expect:
    the death signal is not retroactive, so a parent that died before registration has to be noticed by hand."""
    ctypes.CDLL(None, use_errno=True).prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0)
    if os.getppid() != expected_parent:
        os._exit(0)


def kill_group(pid):
    """SIGKILL the job child's process group, so anything the validator spawned goes with it."""
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


class Runner:
    def __init__(self, sock, adapter, version, import_ms):
        self.sock = sock
        self.adapter = adapter
        self.version = version
        self.import_ms = import_ms
        self.selector = selectors.DefaultSelector()
        self.selector.register(sock, selectors.EVENT_READ, "sock")
        self.sock_lines = LineBuffer()
        self.active = None

    def send(self, obj):
        try:
            self.sock.sendall(encode(obj))
        except OSError:
            raise SystemExit(0)

    def serve(self):
        self.send({"type": "ready", "protocol": PROTOCOL, "version": self.version, "pid": os.getpid(), "import_ms": self.import_ms})
        while True:
            self.serve_once()

    def serve_once(self):
        """One pass: wait for the socket or the active job's pipe, then enforce the job's deadline regardless
        of how busy the pipe was."""
        timeout = None
        if self.active:
            timeout = max(0.0, self.active["deadline"] - time.monotonic())
        events = self.selector.select(timeout)
        for key, _ in events:
            if key.data == "sock":
                self.on_socket()
            elif key.data == "pipe" and self.active is not None and key.fd == self.active["pipe"]:
                self.on_pipe()
        if self.active and time.monotonic() >= self.active["deadline"]:
            self.finish_active("timeout")

    def on_socket(self):
        data = self.sock.recv(65536)
        if not data:
            self.kill_active()
            raise SystemExit(0)
        try:
            lines = self.sock_lines.feed(data)
        except FrameTooLarge:
            self.kill_active()
            raise SystemExit(0)
        for line in lines:
            message = json.loads(line)
            kind = message.get("type") if isinstance(message, dict) else None
            if kind == "run":
                self.start(message)
            elif kind == "cancel":
                if self.active and self.active["job_id"] == message.get("job_id"):
                    self.finish_active("canceled")
            elif kind == "shutdown":
                self.kill_active()
                raise SystemExit(0)

    def start(self, request):
        if self.active:
            self.send({"type": "result", "job_id": request["job_id"], "outcome": "failed", "error": {"code": "runner_busy"}})
            return
        read_end, write_end = os.pipe()
        os.set_blocking(read_end, False)
        parent = os.getpid()
        pid = os.fork()
        if pid == 0:
            try:
                os.setpgid(0, 0)
                self.sock.close()
                self.selector.close()
                close_fds_except(write_end)
                die_with_parent(parent)
                self.run_child(write_end, request)
            finally:
                os._exit(0)
        os.close(write_end)
        try:
            os.setpgid(pid, pid)  # set from both sides, so a cancel can never race the child's own setpgid
        except (ProcessLookupError, PermissionError):
            pass
        self.active = {
            "job_id": request["job_id"],
            "pid": pid,
            "pipe": read_end,
            "lines": LineBuffer(),
            "started": time.monotonic(),
            "deadline": time.monotonic() + request["budget_ms"] / 1000.0,
            "result": None,
            "overflow": False,
        }
        self.selector.register(read_end, selectors.EVENT_READ, "pipe")

    def run_child(self, pipe, request):
        def emit(obj):
            write_all(pipe, encode(obj))

        def progress(stage, pct):
            emit({"type": "progress", "stage": str(stage), "pct": float(pct)})

        started = time.monotonic()
        try:
            apply_memory_limit(request.get("memory_limit_bytes"))
        except (ValueError, OSError) as error:
            emit({"type": "result", "outcome": "crashed", "error": {"code": "memory_limit_failed", "message": str(error)}})
            os.close(pipe)
            return
        try:
            result = self.adapter.validate(
                request["simulation"].encode("utf-8"),
                deadline_s=request["budget_ms"] / 1000.0,
                progress=progress,
            )
            # Serialized once, here, and carried as an opaque string from now on: what the record stores is these bytes.
            emit({"type": "result", "outcome": "succeeded", "result_json": json.dumps(result, separators=(",", ":")),
                  "elapsed_ms": int((time.monotonic() - started) * 1000)})
        except MemoryError:
            emit({"type": "result", "outcome": "crashed", "error": {"code": "validator_oom"},
                  "elapsed_ms": int((time.monotonic() - started) * 1000)})
        except Exception as error:  # the validator raised: a verdict about the input, not a crash
            emit({"type": "result", "outcome": "failed",
                  "error": {"code": "validator_error", "message": f"{type(error).__name__}: {error}"[:2000]},
                  "elapsed_ms": int((time.monotonic() - started) * 1000)})
        finally:
            os.close(pipe)

    def on_pipe(self):
        active = self.active
        try:
            data = os.read(active["pipe"], 65536)
        except BlockingIOError:
            return
        if data:
            try:
                lines = active["lines"].feed(data)
            except FrameTooLarge:
                active["overflow"] = True
                kill_group(active["pid"])
                return
            for line in lines:
                message = json.loads(line)
                if message["type"] == "progress":
                    self.send({"type": "progress", "job_id": active["job_id"], "stage": message["stage"], "pct": message["pct"]})
                elif message["type"] == "result":
                    active["result"] = message  # held until the child is reaped, so `result` means "ready for the next job"
            return
        # EOF: the child closed its pipe. Give it a moment to exit on its own, then insist.
        status = self.reap_active()
        self.close_active()
        self.send(self.terminal_message(active, status))

    def reap_active(self):
        pid = self.active["pid"]
        deadline = time.monotonic() + REAP_GRACE_S
        while True:
            done, status = os.waitpid(pid, os.WNOHANG)
            if done:
                return status
            if time.monotonic() >= deadline:
                kill_group(pid)
                return os.waitpid(pid, 0)[1]
            time.sleep(0.001)

    def terminal_message(self, active, status):
        elapsed = int((time.monotonic() - active["started"]) * 1000)
        if active["overflow"]:
            return {"type": "result", "job_id": active["job_id"], "outcome": "crashed", "error": {"code": "result_too_large"}, "elapsed_ms": elapsed}
        if active["result"] is not None:
            message = active["result"]
            message["job_id"] = active["job_id"]
            return message
        error = {"code": "validator_crashed"}
        if os.WIFSIGNALED(status):
            error["signal"] = os.WTERMSIG(status)
        else:
            error["exit_code"] = os.WEXITSTATUS(status)
        return {"type": "result", "job_id": active["job_id"], "outcome": "crashed", "error": error, "elapsed_ms": elapsed}

    def finish_active(self, outcome):
        """Timeout or cancel: kill the child's group and report, unless a result already arrived, which wins."""
        active = self.active
        self.kill_active()
        if active["result"] is not None:
            self.send(self.terminal_message(active, 0))
            return
        elapsed = int((time.monotonic() - active["started"]) * 1000)
        self.send({"type": "result", "job_id": active["job_id"], "outcome": outcome, "elapsed_ms": elapsed})

    def kill_active(self):
        if not self.active:
            return
        kill_group(self.active["pid"])
        try:
            os.waitpid(self.active["pid"], 0)
        except ChildProcessError:
            pass
        self.close_active()

    def close_active(self):
        if not self.active:
            return
        try:
            self.selector.unregister(self.active["pipe"])
        except (KeyError, ValueError):
            pass
        os.close(self.active["pipe"])
        self.active = None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--connect", required=True, help="Unix socket the harness is listening on")
    parser.add_argument("--adapter-dir", required=True)
    parser.add_argument("--adapter", default="adapter")
    parser.add_argument("--version", default="")
    args = parser.parse_args()

    # The harness passes its pid so a runner that outlives it (stuck in an import, say) still dies with it.
    die_with_parent(int(os.environ.get("VH_HARNESS_PID", os.getppid())))

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(args.connect)

    started = time.monotonic()
    sys.path.insert(0, args.adapter_dir)
    adapter = importlib.import_module(args.adapter)
    import_ms = int((time.monotonic() - started) * 1000)
    version = args.version or getattr(adapter, "VERSION", "")

    runner = Runner(sock, adapter, version, import_ms)

    def on_term(*_):
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, on_term)
    try:
        runner.serve()
    finally:
        runner.kill_active()


if __name__ == "__main__":
    main()
