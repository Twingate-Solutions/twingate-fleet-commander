# Architecture

## Control-plane model

FC is a single-process container that runs on the same Docker host as the Connectors it manages. There is no external scheduler, no separate worker, and no multi-host coordination in v1. A single asyncio event loop drives both the autoscaling control loop and the uvicorn/FastAPI server via `asyncio.gather`.

```
                ┌──────────── Manager container (this project) ─────────────┐
                │  control loop (asyncio)              FastAPI (same process) │
   docker.sock ─┤  ① discover → ② collect → ③ decide → ④ act   /healthz       │
   GraphQL  ────┤        ▲          │             │            /readyz /metrics│
   :9999 scrape ┤   state (sqlite)  │             │            /  (status UI)  │
                └──────────────────┼─────────────┼───────────────────────────┘
                                   scrape :9999   docker run / stop / restart / rm
                ┌──────────────────┴─────────────┴───────────────────────────┐
   same host →  │ connector × N (per Remote Network)   janus      log-shipper │
                └────────────────────────────────────────────────────────────┘
```

**Actuation boundary.** All Docker lifecycle operations go through the `Actuator` protocol (`src/fc/actuator/base.py`). The decision engine never calls Docker directly; it only receives and returns pure domain models. A multi-host or cloud backend can be introduced by implementing a new `Actuator` without touching any decision logic.

**Decision unit.** The Remote Network. Twingate load-balances across Connectors in the same Remote Network, so the scaling unit is the Connector count within one Remote Network. Aggregates, watermark comparisons, and scale decisions are all computed per RN.

**State.** SQLite (`FC_STATE_PATH`) holds only cooldown timestamps and action history. Connector inventory is rediscovered every cycle by querying the Docker socket and the Twingate API; it is never stored. Manual changes to the host are therefore picked up automatically on the next cycle.

---

## Control-loop phases

One cycle runs every `poll_interval_seconds`. A `cycle_id` (UUID hex) is generated at the start and bound onto every log line for the duration of the cycle. The cycle is wrapped so any unhandled error becomes a `loop.cycle.error` log line and the loop survives; a clean cycle ends with a `loop.cycle.complete` heartbeat.

### Phase 1 — Discover

`ControlLoop._discover` (`src/fc/loop.py`)

Lists all FC-managed containers from the Docker actuator (via the `twingate.fc.managed` label), then fetches the authoritative Connector list from the Twingate GraphQL API. Enriches each container with `twingate_state` and `last_heartbeat_at` by joining on the `twingate.fc.connector_id` label (falls back to a name match for seed containers). Reads the cordon set from SQLite and marks any cordoned Connector accordingly.

Failures here abort the whole cycle (`loop.cycle.error`), because without an inventory or a Twingate state no safe decision is possible.

### Phase 2 — Collect

`ControlLoop._collect` (`src/fc/loop.py`)

Runs every enabled collector against every Connector. Collector failures are isolated per Connector per collector source: a `collect.error` is logged and `fc_collect_errors_total` is incremented, but collection continues for all other Connectors. A collector returning `None` (no sample this cycle) is simply omitted.

Three collector sources are available (toggled in the YAML policy):

| Source | What it measures | Toggle |
|---|---|---|
| `docker_stats` | CPU (normalized to per-effective-core utilization) and memory via the Docker stats API | `collectors.docker_stats` |
| `stdout_metrics` | CPU and memory parsed from the Connector container's stdout (custom image only) | `collectors.stdout_metrics` |
| `prometheus` | Tunnel throughput (bytes/sec) scraped from the Connector's `:9999` Prometheus endpoint | `collectors.prometheus` |

CPU normalization: raw Docker CPU percent is per-single-core and unbounded. The collectors divide by the number of effective CPU cores so the result is a 0–100 per-core utilization comparable to the `cpu_high_pct` / `cpu_low_pct` watermarks.

### Phase 3 — Decide

`ControlLoop._decide_scale` and `ControlLoop._decide_and_log_health` (`src/fc/loop.py`)

Groups Connectors by Remote Network, resolves the per-RN policy, fetches cooldown timestamps from SQLite, and calls the pure decider functions:

- `decide_scale` (`src/fc/engine/decider.py`) — produces one `ScaleDecision` per RN.
- `decide_health` (`src/fc/engine/decider.py`) — produces zero or more `HealthAction` objects for unhealthy Connectors.

The decider performs no I/O. The loop passes in all necessary state (aggregates, cooldowns, restart counts) and then acts on the returned decisions.

Per-RN isolation: a failure in the decide/act block for one Remote Network is caught, logged as `loop.rn.error`, and skipped. The remaining Remote Networks continue, and the heartbeat, fleet gauges, and status snapshot are still written at the end of the cycle.

### Phase 4 — Act

`ControlLoop._act_scale` and `ControlLoop._act_health` (`src/fc/loop.py`)

Executes the decisions produced in Phase 3. Scale actions use the three-step provision and drain-before-delete sequences described below. Health actions use the restart-before-replace sequence. All actuation is serialized under `_action_lock` (an `asyncio.Lock`), which also serializes manual override requests arriving from the FastAPI layer.

---

## Nine non-negotiable design rules

These rules are enforced at the enforcement points listed. Violating them is not a recoverable error; they are invariants the code never relaxes.

**Rule 1 — The Twingate API does not deploy compute.**
Provisioning is always three ordered steps: `connectorCreate` → `connectorGenerateTokens` → `docker run` with those tokens. Deprovisioning is the reverse: `connectorDelete` → `drain_grace_seconds` wait → stop/remove container. Tokens are unique per Connector and are never reused across containers or stored anywhere except the new container's environment.
*Enforced in:* `ControlLoop._provision_one` and `_deprovision_one` (`src/fc/loop.py`).

**Rule 2 — Hard floor per Remote Network.**
`scale_down_count` in `src/fc/engine/policy.py` returns `0` when `current <= min_connectors`, and `min_connectors` has a Pydantic `ge=2` constraint on both `RemoteNetworkDefaults` and the resolved merged model. The floor cannot be overridden below 2.
*Enforced in:* `src/fc/engine/policy.py:scale_down_count`, `src/fc/config.py:RemoteNetworkDefaults`.

**Rule 3 — Asymmetric, sustained-window triggers.**
Scale-up is evaluated on the short `scale_up_window_seconds` aggregate; scale-down on the longer `scale_down_window_seconds` aggregate. Both directions consult persisted cooldown timestamps from SQLite so a manager restart cannot reset a cooldown. Scale-up always takes precedence: the decider checks high load before low load and never removes capacity while any signal is hot.
*Enforced in:* `src/fc/engine/decider.py:decide_scale`, `src/fc/engine/policy.py:cooldown_remaining`.

**Rule 4 — Drain before delete.**
Scale-down order: pick victim → `connectorDelete` (the controller stops routing new connections) → wait `drain_grace_seconds` → `actuator.deprovision` (stop + remove container). Health replace order: provision a new Connector first, then drain-delete the old one, so capacity never dips during a replace. If the new Connector fails to provision, the old one is left in place.
*Enforced in:* `ControlLoop._deprovision_one` and `_replace_one` (`src/fc/loop.py`).

**Rule 5 — Never fight janus.**
Before generating a health action, `decide_health` checks `connector.janus_locked`. A locked Connector is skipped entirely — no restart, no replace. The janus lock is detected during discovery by checking for the `janus_lock_label` Docker label on the container.
*Enforced in:* `src/fc/engine/decider.py:decide_health`, discovery logic in `src/fc/loop.py`.

**Rule 6 — Structured logging with standard fields.**
Every log line is JSON (via structlog) with at minimum `ts`, `level`, `event` (constant from `src/fc/observability/events.py`), and `cycle_id`. The `cycle_id` correlates all signals, decisions, and actions of one cycle. The `loop.cycle.complete` line is the heartbeat — its absence signals a silent or stuck manager.
*Enforced in:* `src/fc/observability/events.py`, `src/fc/loop.py`.

**Rule 7 — The manager is observable from outside.**
`/healthz`, `/readyz`, and `/metrics` are always served. `fc_last_successful_cycle_timestamp_seconds` is updated at the end of every clean cycle so `time() - value > N` is a viable staleness alert.
*Enforced in:* `src/fc/api/app.py`, `src/fc/observability/metrics.py`.

**Rule 8 — CPU is normalized before comparison.**
Raw Docker CPU percent is per single core and unbounded. Collectors divide by the number of effective CPU cores to produce a 0–100 per-core value before storing it in `ResourceSample.cpu_pct_norm`. Memory is advisory (`mem_ceiling_bytes: 0` means no memory-based action); CPU and tunnel throughput are the scale triggers.
*Enforced in:* `src/fc/collectors/docker_stats.py`, `src/fc/models.py:ResourceSample`.

**Rule 9 — The actuator is an interface.**
`src/fc/actuator/base.py` defines `Actuator` as a `typing.Protocol`. The engine, decider, aggregator, and collectors never import the Docker actuator directly. The Docker-specific implementation lives entirely in `src/fc/actuator/docker_actuator.py`.
*Enforced in:* `src/fc/actuator/base.py`.

---

## Provision and deprovision sequences

### Provision (scale-up or replace)

```
1. twingate.create_connector(rn_id, name)
        → returns ManagedConnector with connector_id
2. twingate.generate_tokens(connector_id)
        → returns ConnectorTokens (accessToken, refreshToken)
3. actuator.provision(rn_id, connector_id, name, tokens, mem_limit_bytes=...)
        → starts container with tokens in env; sets management labels
```

A failure at step 1 or 2 logs `action.provision.fail`, increments `fc_twingate_api_errors_total`, records the failure in SQLite, and returns `None`; the cycle continues. A failure at step 3 logs `action.provision.fail`, increments `fc_docker_api_errors_total`, and returns `None`. Tokens are never logged, persisted to SQLite, or surfaced in the API or status UI.

### Deprovision (scale-down or replace's old-side)

```
1. twingate.delete_connector(connector_id)
        → controller stops routing new connections to this Connector
2. sleep(drain_grace_seconds)
        → existing connections drain
3. actuator.deprovision(connector)
        → container stop + remove
```

A failure at step 1 aborts the sequence (the container stays running; a future cycle may retry). A failure at step 3 after a successful step 1 leaves an orphaned container that no longer receives connections; it will be picked up again on the next cycle.

### Health remediation — restart before replace

For each unhealthy Connector (Twingate state `DEAD_*` or Docker health `unhealthy`) that is not janus-locked:

1. Check `count_recent_restarts(connector_id, since=now - restart_window_seconds)` from SQLite.
2. If `restart_count < max_restarts`: emit `action.restart`, call `actuator.restart`, record.
3. If `restart_count >= max_restarts`: run the full provision sequence for a new Connector, then the full deprovision sequence for the old one; emit `action.replace`.

---

## Core domain models

All models are Pydantic v2 (`src/fc/models.py`). No model ever holds secret material.

| Model | Description |
|---|---|
| `ManagedConnector` | A Connector under FC management. Carries `connector_id`, `rn_id`, `container_id` (may be `None` mid-provision), `twingate_state`, `last_heartbeat_at`, `docker_health`, `janus_locked`, and `cordoned`. Rediscovered every cycle; never persisted. |
| `ResourceSample` | A single point-in-time signal from one collector for one Connector. Fields: `connector_id`, `source` (`CollectorSource`), `ts`, `cpu_pct_norm` (normalized 0–100), `mem_bytes`, `mem_pct`, `throughput_bps`. |
| `ScaleDecision` | The decider's verdict for one RN: `rn_id`, `direction` (`UP`/`DOWN`/`NONE`), `count`, `reason`, and `metrics` (triggering windowed aggregates for audit). |
| `HealthAction` | A remediation decision for one Connector: `connector_id`, `rn_id`, `kind` (`"restart"` or `"replace"`), `reason`. |
| `ActionRecord` | A persisted row in SQLite's `action_history` table: `ts`, `rn_id`, `action`, `count`, `reason`, `outcome`, `actor` (`"auto"` or `"manual"`). |
| `RemoteNetworkView` | The per-RN rollup the decider receives: `rn_id`, `name`, `connectors`, `aggregates`. |

---

## SQLite state

`StateStore` (`src/fc/state.py`) wraps a SQLite file (`FC_STATE_PATH`). All access goes through `asyncio.to_thread` so the synchronous `sqlite3` calls never block the event loop. The database uses WAL journal mode and a 5-second busy timeout.

**Schema tables:**

| Table | Purpose |
|---|---|
| `cooldowns` | One row per Remote Network: `rn_id` (PK), `last_up_ts`, `last_down_ts`. Timestamps are ISO-8601 UTC strings. UPSERT on write. |
| `action_history` | Append-only log of every action FC took. Columns: `id`, `ts`, `rn_id`, `connector_id` (internal; used by `count_recent_restarts`), `action`, `count`, `reason`, `outcome`, `actor`. Indexed on `ts` and `(connector_id, action, ts)`. |
| `cordons` | One row per cordoned Connector: `connector_id` (PK), `ts`. Cleared automatically when a Connector is deprovisioned. |

The Connector inventory is **not** stored; only the three tables above are persisted.

---

## Actuator protocol

`Actuator` (`src/fc/actuator/base.py`) is a `typing.Protocol` with four methods:

| Method | Signature | Purpose |
|---|---|---|
| `provision` | `(rn_id, connector_id, name, tokens, *, mem_limit_bytes=None) -> str` | Start a Connector's container with tokens and management labels; return the container id. |
| `deprovision` | `(connector: ManagedConnector) -> None` | Stop and remove a Connector's container. A logical-only Connector (no container) is a no-op. |
| `restart` | `(connector: ManagedConnector) -> None` | Restart a Connector's container in place, preserving its env/tokens. |
| `list_managed` | `() -> list[ManagedConnector]` | List all FC-managed containers as `ManagedConnector` objects. |

The Docker implementation is `src/fc/actuator/docker_actuator.py`. It is the only place that knows about the Docker label scheme and Docker-specific error types (`DockerActuatorError`). The engine never imports it directly.
