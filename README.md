# Validator harness

The worker-side half of the multi-version validator pool: one generic harness image that can run any supported client version, because versions arrive as **artifacts** it fetches and caches rather than images it is built from.

This repository is a working model of that harness, with tests that drive real runner processes. It is not production code. The AWS edges (SQS, DynamoDB, Redis, S3) are interfaces with in-memory stand-ins, so the mechanisms can be read and run on a laptop.

```
make test     # Jest (the worker tests spawn real Python runner processes), then the runner's Python unit tests
make crap     # the CRAP gate for both languages: every function must score below 12
make demo     # one worker, three versions, seven jobs, with the event trace printed; run it twice
make shell    # poke around: ls /opt/validator/artifacts /opt/validator/cache
make clean    # remove the image and both volumes
```

Needs Docker and `make`. Nothing else: the image carries Node, Python and the tools, and the harness is Linux-only on purpose. It relies on `RLIMIT_DATA`, `PR_SET_PDEATHSIG`, process groups and a readable glibc version, which is what a Fargate task gives it and what no other host would reproduce faithfully.

## The three mechanisms

**1. The artifact cache** (`src/artifact_store.ts`). A version is a tarball: an interpreter, a venv with the pinned validator library, a few-line adapter, and the frozen `runner.py`. The registry row for a version says where the tarball is, its digest, its installed size, and what host it needs. `ensure(spec)` makes it present on local disk: cache hit, join a download already in flight, or fetch, verify the digest, extract, and rename into place. The cache is a byte-bounded LRU over installed versions. It never evicts a version with a resident runner process.

**2. The process pool** (`src/process_pool.ts`). A table of warm runner processes, one per resident version, ordered by last use. `acquire(artifact)` returns an idle runner for that version or spawns one, evicting the least recently used idle runner if the table is full. Three rules sit on top of plain LRU: never a busy or starting child; non-pinned (legacy) before pinned (hot); and a victim leaves the table before its teardown is awaited, so the job that needed the slot waits on the new runner's `ready`, not on the old process's exit.

**3. The hand-off** (`src/worker.ts`, `handle()`). Receive a message, hold a slot, and then:

```
1. reject without a runner   expired, unknown or retired version, or the simulation's hash not matching the record
2. artifacts.ensure()        cache hit | join prefetch | fetch+verify+extract                 <- cold cost #1
3. artifacts.hold()          the artifact cannot be evicted while this job is between install and spawn
4. pool.acquire()            warm runner | spawn one that is ours on arrival (import the library) <- cold cost #2
5. runner.run()              fork a child in its own process group under RLIMIT_DATA and the budget; progress streams back
6. pool.release()            runner stays warm; recycled only if the runner itself died or hung
7. finalize or escalate      with what ran it (artifact, build, digest, pid), then ack: the record is the durable copy
```

The runner contract (`runner/runner.py`) is the `runner/1` socket protocol from the design: newline-delimited JSON over a Unix socket, `ready` after the import, `run` / `progress` / `result`, `cancel`, `shutdown`. The harness listens and the runner connects, so a process is reachable the instant it is up. The runner forks one child per job; the warm parent never executes validator code, so a leak, a crash or an overrun ends a child, never the parent. Two rules make the contract safe to drive at speed: a `result` is sent only after the child has been reaped, so receiving one means the runner can take the next job at once, and the deadline is enforced on every pass of the loop, so a child that never stops emitting progress cannot talk its way past its budget. Frames are capped at 4 MiB on both channels; a result over that is reported as a crash rather than parsed in the unlimited parent. The validator's result crosses the boundary as the JSON bytes the runner serialized (`result_json`); the record stores those bytes, and the parsed `result` is a convenience.

From the demo, the cold path for a legacy version and the warm path right after it:

```
   651 ms  worker    job_received     version=2.4.9
   651 ms  artifacts install_start    artifact=2.4.9@b1 priority=request
   671 ms  artifacts install_done     fetch_ms=0 extract_ms=19
   672 ms  pool      stop             version=2.7.3 child=2 reason=evicted was=idle
   672 ms  pool      spawn            version=2.4.9 child=3
   730 ms  pool      ready            version=2.4.9 child=3 spawn_ms=58
   732 ms  worker    job_run          version=2.4.9 outcome=succeeded artifact_ms=21 acquire_ms=58
  2325 ms  worker    finalize         version=2.7.3 outcome=crashed error=validator_crashed
  2326 ms  pool      acquire          version=2.7.3 child=5 cold=false wait_ms=0
```

## What this does to the developer experience

The point of the artifact pool is that a version stops being infrastructure. Nothing about the fleet changes when a version is added, fixed or retired.

| You want to | You do | What deploys | In this model |
|---|---|---|---|
| Add a version | CI builds the artifact from `versions/<v>/` (lock file, python version, adapter), uploads it, writes the registry row with status `canary` | Nothing. Workers fetch it on first use | Fetch, verify and spawn on first use are built. The CI build and the `canary` routing are not; `status` exists on the row and `retired` is enforced |
| Fix a version | Same build with a new `build_id`; flip the registry row's `build_id` and digest | Nothing. Jobs in flight finish on the old build; new jobs get the new one; the old build ages out of the cache | Built: two builds of one version are different artifacts, and the record names which one ran |
| Roll back a fix | Flip the row back | Nothing | Built, with the same caveat as any registry change: consistency across tasks during a flip is the registry's problem, not the harness's |
| Canary a fix | Two rows for one version with a weight; the harness picks by weight | Nothing | Not built. It is a registry read and a weighted choice, plus a rule that a retry keeps the build its first attempt chose |
| Retire a version | Status `retired` in the registry; drain, then delete the tarball | Nothing | The worker refuses retired versions. Draining before deleting the tarball is the operator's job: queued jobs and 30 s registry caches still reference it |
| Fix the harness | Build the harness image, roll the fleet | The harness only; artifacts are untouched | Built. Protocol conformance alone does not prove a host change kept native wheels, limits and fork behaviour working; that needs the golden corpus against the new host |
| Reproduce a production verdict locally | Run the harness image with one artifact | Identical to production: same tarball, same path, same runner | The demo borrows the system `python3`; a real artifact ships its own interpreter, which is what makes the reproduction exact |

Compare the per-version design, where a version is an image plus a stack and a harness fix is one stack update per version, and the single fat image holding every version, where any of these is a full rebuild, a full golden-corpus matrix and a fleet rollout.

Two details make the "nothing deploys" column true.

**Builds are identified, not just versions.** The pool keys children by `version@build_id` and the cache installs to `versions/<version>+<build_id>`, so a rebuilt version is a second artifact beside the first rather than a change underneath a running process. The test "a rebuilt version runs on its new build without disturbing the old one" shows both builds resident at once.

**Nothing in the artifact gets relocated.** Build the artifact at the same absolute path the harness installs it to: `<cache root>/versions/<version>+<build_id>`, which in the container is `/opt/validator/cache/versions/2.7.3+b42`. Venvs carry absolute paths in `pyvenv.cfg` and script shebangs; installing at the build path makes that a non-issue. A venv alone does not bundle its interpreter, so the artifact carries one: a standalone build installed inside the artifact, with the venv created from it. The demo artifacts borrow the system `python3` so they run anywhere; a real one ships its own interpreter.

### The artifact contract

`manifest.json`:

```json
{
	"version": "2.7.3",
	"build_id": "b42",
	"python": "bin/python3",
	"runner": "runner/runner.py",
	"adapter_dir": "adapter",
	"adapter_module": "adapter",
	"runner_protocol": "runner/1",
	"arch": "aarch64",
	"min_glibc": "2.28"
}
```

The registry row repeats `arch`, `min_glibc` and `runner_protocol` so the host can refuse an artifact before downloading it (`src/host.ts`). The rule is deliberately one-directional: native wheels built against an older glibc run on a newer one, so the host only ever moves forward. Architecture plus glibc is necessary, not sufficient: manylinux also assumes a handful of system libraries, so an artifact is proven on the host image it will run on, by the golden corpus, before its row goes active. A version that cannot meet the contract is the exception that still earns its own image and stack.

Identifiers are validated before they touch a path (`[A-Za-z0-9][A-Za-z0-9._-]{0,63}`), the install path is confined to the cache, every symlink in an extracted tree must resolve inside it, and the manifest must agree with the registry row on version, build and protocol. Files are made read-only after extraction as a defence against accidents; it is not a boundary, because a validator child runs as the same user as the harness.

A CI build, on the host image the artifact will run on, is roughly:

```
ROOT=/opt/validator/cache/versions/2.7.3+b42
uv python install 3.9 --install-dir $ROOT/python          # a standalone interpreter inside the artifact
uv venv $ROOT/venv --python $ROOT/python/*/bin/python3
uv pip install --python $ROOT/venv/bin/python --require-hashes -r versions/2.7.3/requirements.lock
cp runner/runner.py $ROOT/runner/ ; cp versions/2.7.3/adapter.py $ROOT/adapter/ ; write $ROOT/manifest.json
tar --sort=name --mtime=@0 --owner=0 --group=0 -czf 2.7.3-b42.tar.gz -C $ROOT .   # reproducible bytes
aws s3 cp ... ; register { version, build_id, key, sha256, size_bytes, arch, min_glibc, runner_protocol, rank, status }
```

## Warming up without getting in the way

On start, the worker begins receiving immediately and prewarms in the background (`Worker.prewarm`). The hot list is fetched two at a time at prefetch priority, extraction runs under `nice -n 19`, and an idle runner is spawned for each hot version while the table has room. Prewarm never evicts.

Three things keep it from competing with requests:

- **Requests never queue behind prefetches.** A request-priority install starts at once, and a request for a version whose prefetch is still queued promotes that prefetch to the front.
- **A request for a version mid-download joins it** rather than starting a second one (`join_in_flight` in the trace).
- **A task that is not yet warm holds at most `warmup_max_jobs` jobs at once**, however many slots it has. A receive makes work invisible to every other consumer for the visibility timeout, so a cold task that pulled a full batch would be hiding work from tasks that could answer in 300 ms. The cap is on jobs held, not on the size of one receive.

Prewarming on start is the right default under the design's scheduled warm floor: a task joining during a scale-out finds a backlog already being served by warm tasks, so serving one job cold is better than idling for twenty seconds. The honest limits: `nice` lowers CPU priority only, it does not reserve disk bandwidth or memory for a validator mid-job; a prewarm that fails (a bad hot-list row) is reported and the task still becomes warm; and "warm" means the pass finished, not that every hot version got a resident runner, because the table may have been full. Gating readiness until the hot list is resident is the better choice only when a task is the sole capacity for its versions, which is what the floor exists to prevent.

## Memory and process safety

- **Budget once, at start.** `check_memory_budget()` (`src/config.ts`) adds the harness reserve, every resident runner parent at its resting size (busy or idle: a busy runner's parent is still holding its imported library), and every slot at its job limit, and refuses to start if that exceeds the task. Fargate has no per-process cgroups; this arithmetic plus the container's memory limit (`mem_limit` in compose) is the whole story. The design's old fast shape, two 1.5 GiB jobs in a 4 GiB task, does not pass once parents are counted; the demo runs two 1 GiB jobs with three resident runners.
- **Limits in the child, not the parent.** The forked job child sets `RLIMIT_DATA` on itself, as the design specifies: heap and anonymous mappings, which is what a runaway NumPy allocation is, without the false kills an address-space limit causes. A limit the kernel refuses is reported as `memory_limit_failed` rather than silently skipped. The runner parent kills the child at the lane's budget on every pass of its loop. The harness keeps a backstop timer past the budget for a runner that is itself hung, and a cancel it sends for that reason comes back as `timeout`, so the fast lane still escalates instead of recording a client cancel.
- **No orphans, bounded leaks.** The job child is its own process group, so the runner's SIGKILL takes anything the validator spawned. Both the runner and the job child ask the kernel to kill them when their parent dies (`PR_SET_PDEATHSIG`), and both check the parent is still the expected one afterwards, because the signal is not retroactive. Runners are spawned in their own process group so the harness's last-resort SIGKILL takes the group. The job child closes every inherited descriptor except its result pipe, so a validator can never hold the harness socket open or read another job's data. What is not contained: a validator child runs as the same user as the harness and can write to the cache; separating that needs a second uid or a user namespace, which this model does not do.
- **Recycle bound.** `max_jobs_per_child` recycles a runner after N jobs. With fork-per-job it is a bound rather than a fix, and pinned versions are respawned in the background after a recycle.

## The container

`Dockerfile` builds the task's host from a digest-pinned `node:20-bookworm-slim` plus Debian's `python3`, builds the TypeScript, and runs as a non-root user with three directories mounted the way production would mount them:

| Mount | Models | Compose volume |
|---|---|---|
| `/opt/validator/artifacts` | the artifact share: S3 or EFS, holding each version's tarball and its registry row | `artifacts` (named) |
| `/opt/validator/cache` | the task's ephemeral disk: the byte-bounded LRU of installed versions | `cache` (named) |
| `/opt/validator/run` | where the Unix sockets live | inside the container |

Compose caps the container at 4 GiB (`mem_limit`), which is the number the memory budget is checked against. The demo reads the mounts from `VH_ARTIFACTS`, `VH_CACHE`, `VH_RUN_DIR` and `VH_CACHE_MAX_MB`, and leaves them in place when it finishes. Both named volumes outlive the container, so running `make demo` twice shows the second run finding every version already installed (`cache_hit` instead of `install_start`) from the index the first run persisted, while the runner processes are spawned fresh because processes never outlive a task. The demo's build id is a digest of the runner and adapter sources, so rebuilding the image with a changed runner produces new builds beside the old ones in the share, exactly as a fix to a version would, and the old builds age out of the cache. To browse the files from the host, swap the `artifacts` volume for the bind mount commented in `docker-compose.yml`.

## Quality gate: CRAP below 12

CRAP (Change Risk Anti-Patterns) scores each function as `cc^2 * (1 - coverage)^3 + cc`, where `cc` is cyclomatic complexity and `coverage` is how much of that function the tests execute. The gate here fails any function at or above 12, a tight setting against the conventional 30: roughly, any function with `cc` above 11 is refused outright, and a function with `cc` 7 needs about 60% of its statements covered. `make crap` runs both scorers:

- **TypeScript**: [crap4ts](https://www.npmjs.com/package/crap4ts) over Jest's coverage JSON (`npm run crap:ts`). `src/demo.ts` and `src/index.ts` are excluded; everything else in `src/` is scored, including files no test imports.
- **Python**: `tools/crap_py.py` (`npm run crap:py`), standard library only: complexity from `ast`, coverage from running `runner/test_runner.py` under `trace`. It exists because the frozen runner has to be Python (it runs on each version's own interpreter) and crap4ts does not read Python. Code that runs only inside the forked validator child is invisible to it and is exercised instead by the TypeScript tests, which drive real runner processes.

## Layout

```
src/types.ts             job envelope, run request/outcome, registry row, manifest
src/lru.ts               recency index shared by the pool and the cache
src/runner_endpoint.ts   harness side of runner/1: socket, framing, run/cancel/stop, backstop timer
src/runner_spawn.ts      starts a runner from an installed artifact's manifest
src/process_pool.ts      warm process table, LRU eviction, prewarm, recycle, respawn
src/artifact_store.ts    byte-bounded artifact cache: fetch, verify, extract, evict, in-flight join
src/host.ts              arch / glibc / protocol compatibility check
src/sources.ts           artifact sources: local directory (tests), stream (S3 via the AWS SDK)
src/config.ts            memory budget check
src/ports.ts             Queue, Registry, Finalizer interfaces
src/memory_adapters.ts   in-memory queue, registry, recording finalizer
src/worker.ts            receive loop, prewarm, the hand-off
src/demo_artifacts.ts    builds demo artifacts the way CI would, minus the venv
src/demo.ts              end to end on one machine
runner/runner.py         the frozen runner: stdlib only, fork per job, limits, progress
runner/adapter_example.py  stand-in for the per-version adapter; steerable via simulation fields
runner/test_runner.py    the runner driven through a socketpair exactly as the harness drives it
runner/test_crap_py.py   the Python scorer checked against a fixture
test/                    pool, cache, host and endpoint logic with fakes; worker through real runner processes
tools/crap_py.py         CRAP scorer for the Python side
Dockerfile, docker-compose.yml, docker/entrypoint.sh, Makefile
                         the host, its mounts, and the make targets that drive everything
```

## What production replaces

| Interface | Here | In production |
|---|---|---|
| `Queue` | `MemoryQueue` | SQS long poll, delete on ack, visibility heartbeat on the slow lane |
| `Registry` | `MemoryRegistry` | the versions table, cached in the harness, refreshed every 30 s |
| `Finalizer` | `RecordingFinalizer` | the conditional DynamoDB terminal write, the escalation flip and slow send, the Redis publish |
| `ArtifactSource` | `LocalDirectorySource` | `StreamSource` over the AWS SDK's `GetObject`; ranged parallel GETs for large artifacts |
| events | `on_event` callback | EMF metrics with `{version, lane}` dimensions and structured logs |

Not modelled, and the production adapters will need worker changes for some of them rather than a swap:

- the slow-lane lease, its heartbeat and fencing, and the cancellation watcher that reads the record while a slow job runs;
- the three-step escalation hand-off (flip the record, send to the slow queue, mark dispatched) and the redelivery rule that repairs a crash between the steps;
- redelivery itself: the in-memory queue never redelivers, so the paths that depend on it (a deferred job after an install failure, a crash before ack) are asserted by leaving the message unacked, not by seeing it come back;
- per-version admission caps inside the shared pool, and rank-weighted eviction (pinned is a boolean; weighting by hot-list rank is a one-line change to `pick_victim`);
- the canary split between two builds of one version;
- fork safety of the validator library after import: `OMP_NUM_THREADS=1` is a precaution, not proof, and the versions that are safe to fork after import have to be established per library.

What the finalizer does receive is what it needs to write the record the design describes: the outcome, the result bytes, and an `Execution` naming the artifact, build, digest and runner that produced them, so a verdict stays traceable after the registry has moved on.
