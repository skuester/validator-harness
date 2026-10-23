"""A stand-in for the per-version adapter. The real one imports tidy3d and calls its validator.

This one reads a few fields from the simulation JSON so the demo and tests can steer it:
  sleep_ms      run this long (overruns a fast-lane budget to show escalation)
  allocate_mb   allocate this much (hits the memory limit on Linux)
  crash         exit abruptly without a result
  valid         false to return a validation error
"""
import json
import os
import time

VERSION = "__VERSION__"


def validate(simulation: bytes, *, deadline_s: float, progress):
    sim = json.loads(simulation)
    progress("parse", 0.2)
    if sim.get("sleep_ms"):
        time.sleep(sim["sleep_ms"] / 1000.0)
    if sim.get("allocate_mb"):
        _hold = bytearray(int(sim["allocate_mb"]) * 1024 * 1024)
    if sim.get("crash"):
        os._exit(3)
    progress("rules", 0.8)
    errors = [] if sim.get("valid", True) else [{"code": "monitor_outside_domain", "path": "monitors[0]", "message": "outside the simulation domain"}]
    return {
        "valid": not errors,
        "errors": errors,
        "warnings": [],
        "estimate": {"flex_credits": round(len(simulation) / 10000, 3)},
        "validated_by": VERSION,
    }
