# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Distributed compute cluster running on 6× Raspberry Pi 5 (`192.168.1.20`–`192.168.1.25`, worker-01..06). The laptop is the orchestrator/dev machine; the Pis are the cluster. Two workloads share the same pipeline:

- **geometry mode** — continuously stream 3D points for a live visualizer (torus, lorenz, mandelbulb, klein).
- **render mode** — SDF ray-marching render farm. One frame is split into tiles; tiles are distributed as tasks; results are composited back into PNGs and optionally muxed into an MP4.

UI/comments/log messages are predominantly **pt-BR** — preserve the language when editing.

## Topology

Each Pi runs:

- **Redis** — 3 instances per node (`redis-a` :6379, `redis-b` :6380, `redis-c` :6381) forming a 6-master × 2-replica cluster via `docker-compose.redis.yml`. Replicas are placed on neighbor nodes with +1/+2 offsets, so any 2 hosts down still leaves every master with a replica.
- **Raft daemon** (`cluster-daemon.service` → `app/daemon.py`) — pysyncobj, port 4321. One leader is elected; only the leader enqueues tasks.
- **API** (`cluster-api.service` → `app/api.py`) — FastAPI on :8000. Every node exposes the same endpoints (`/health`, `/queue/status`, `/geometry`, `/mode`, `/render/*`, `/results/*`).
- **Workers** (`cluster-worker@{0,1,2,3}.service` → `app/worker.py`) — 4 per node (24 total). Each worker spawns 6 fetch threads, one per shard.

Laptop side: `laptop/compositor.py` (live tile compositor) and `laptop/render_movie.py` (multi-frame driver that runs one persistent compositor session).

## Redis key layout (critical)

Everything goes through Redis Cluster, so **hash tags matter**. Adding a key without a tag will land on a random slot and break pipelines with `CROSSSLOT`.

- `{s0}:tasks` .. `{s5}:tasks` — 6 task streams, one per master. Daemon round-robins `XADD` across them; workers read all 6 via consumer group `cluster-workers`.
- `{s0}:results` .. `{s5}:results` — mirror shards for results. Workers write the result into the same shard index they consumed from.
- `{ctrl}:mode` / `{ctrl}:geometry` / `{ctrl}:cluster:leader` / `{ctrl}:render:job` / `{ctrl}:render:current` — control plane, all share the `{ctrl}` tag so pipelines touching them stay in one slot.
- `{ctrl}:render:done:<frame_id>` — tile-completion counter (INCRBY per result).
- `{ctrl}:stats:proc:<node_name>` — per-node processed-task counter.
- `last_id` for `/results/stream` is **composite**: `"id0,id1,id2,id3,id4,id5"` (one cursor per shard). The single-value `"0-0"` is accepted for compatibility.

See `app/config.py` for `SHARDS`, `TASK_KEYS`, `RESULT_KEYS`, `GROUP_NAME`, and helpers `ctrl_done_key()` / `ctrl_stats_key()`.

## Control flow

- **Leader election** — every daemon runs `ClusterState(SyncObj)`; the leader writes its IP to `{ctrl}:cluster:leader` (TTL 5s).
- **Task generation** — leader only. In `geometry` mode: `generate_batch(geom, batch_id, 2000)` → XADD round-robin, throttled to `MAX_PENDENTES=30_000` summed across shards. In `render` mode: reads `{ctrl}:render:job`, calls `generate_render_tasks(...)`, XADDs tiles.
- **Consumption** — each worker thread does `XREADGROUP` on its assigned shard, processes (`process_batch` for geometry, `process_render_task` for render), and a single `write_loop` pipelines `XADD` result + `INCRBY` stats + `XACK` back on that shard.
- **Fault recovery** — `app/recovery.py` runs on **every** node (not just leader). XAUTOCLAIM is per-key and atomic, so concurrent recoveries are safe and survive a leader death. Messages idle > `PENDING_TIMEOUT_MS=3000` get requeued.
- **Crash behaviour** — daemon counts Redis errors; after 300 consecutive failures (~15 s) it `sys.exit(2)` so systemd restarts it and Raft can re-elect a healthy leader.

## Dev commands

**All orchestration goes through `cluster.ps1`.** Dot-source it once (or add to `$PROFILE`):

```powershell
. .\cluster.ps1
```

Then use the `Cluster` command. Common ones:

```powershell
Cluster status                              # health + leader + queue summary
Cluster watch                               # status in a 3s loop
Cluster deploy [-Nodes 20,21]               # scp code + restart services (wraps deploy\push.ps1)
Cluster restart [-Nodes ...]                # systemctl restart all cluster-* units
Cluster mode [geometry|render]              # read/set {ctrl}:mode
Cluster geometry [torus|lorenz|mandelbulb|klein]
Cluster render-frame -Scene mandelbulb -FrameId t1 -Width 640 -Height 360
Cluster render-movie -Scene menger -Frames 60 -Fps 24
Cluster render-progress
Cluster logs <N> -Service cluster-worker@0 -Follow
Cluster exec -Command "df -h /data/redis-a"
Cluster redis-fix                           # wraps deploy\recover-redis.ps1
Cluster clear-streams                       # drop {sN}:tasks and {sN}:results
Cluster reboot <N> [-Force] | shutdown [-Force]
```

`Cluster help` prints the full list. Raw equivalents without the wrapper:

- `.\deploy\push.ps1` — code distribution + service restart.
- `.\deploy\setup-tmpfs.ps1` — create `/data/redis-{a,b,c}` tmpfs mounts (required before first Redis boot on a node).
- `.\deploy\recover-redis.ps1` — rebuild the Redis Cluster from scratch (all 6 nodes required).
- `.\deploy\api.ps1 -Action <health|mode-get|mode-set|...>` — direct API calls.
- `python watchdog.py` — terminal dashboard (polls `/health` + `/queue/status` every 2 s).

On the Pis, services live under `/home/pi/cluster/` and run from the venv at `/home/pi/cluster/app/venv/`. Python deps pinned in `app/requirements.txt` (workers) and `laptop/requirements.txt` (laptop tooling). `ffmpeg` must be on `PATH` for `render_movie.py` to mux frames into `output/render.mp4`.

## Testing

There is no automated test suite. Smoke-test changes by deploying and watching:

```powershell
Cluster deploy -Nodes 20            # push to one node first to sanity-check
Cluster status
Cluster logs 20 -Service cluster-worker@0 -Follow
```

`deploy/test_imports.py` exists only to verify that imports on the Pi venv succeed.

## Things that bite

- **Hash tags.** Any new Redis key in the control plane must use `{ctrl}` (or a shard tag). Pipelines that mix tags will fail with CROSSSLOT.
- **Leader-only work.** Don't duplicate task generation outside the leader check in `daemon.py`. Recovery, however, must stay node-local.
- **Consumer names.** Workers identify themselves as `<MY_NAME>-w<WORKER_ID>` (e.g. `worker-03-w2`). Changing this format invalidates pending-message ownership across a deploy.
- **Render stream cursor.** The compositor reads results starting at `$` (now). Jobs submitted before the compositor is ready won't be rendered — re-submit with a fresh `frame_id`.
- **Mode switches clear streams.** `POST /geometry` wipes all 6 task + result streams so geometries don't intermix. Don't rely on pre-switch data surviving.
- **`MY_IP` detection.** `config.py` picks the IP used to reach 8.8.8.8. Override with `NODE_IP=...` in the environment if that routing ever misbehaves on a node.
