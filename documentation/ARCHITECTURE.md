# Architecture

## Control-plane model

FC is a single-process container that runs on the same Docker host as the Connectors it manages. There is no external scheduler, no separate worker, and no multi-host coordination in v1. A single asyncio event loop drives both the autoscaling control loop and the uvicorn/FastAPI server via `asyncio.gather`.

```
                ┌──────────── Manager container (this project) ─────────────┐
                │  control loop (asyncio)              FastAPI (same process) │
   docker.sock ─┤  ① discover → ② collect → ③ decide → ④ act   /healthz       │
   GraphQL  ────┤        ▲          │             │            /readyz /metrics│
                │   state (sqlite)  │             │            /  (status UI)  │
                └──────────────────┼─────────────┼───────────────────────────┘
                                   docker stats   docker run / stop / restart / rm
                                   + stdout logs
                ┌──────────────────┴─────────────┴───────────────────────────┐
   same host →  │ connector × N (the one Remote Network)  janus   log-shipper │
                └────────────────────────────────────────────────────────────┘
```

**One FC, one Remote Network (Key Design Rule N1).** FC is strictly 1:1 with a single Remote Network — the policy is one flat model carrying that RN's id, and there is no per-RN iteration anywhere. To manage more Remote Networks, run more FC instances.

**Actuation boundary.** All Docker lifecycle operations go through the `Actuator` protocol (`src/fc/actuator/base.py`). The decision engine never calls Docker directly; it only receives and returns pure domain models. A multi-host or cloud backend can be introduced by implementing a new `Actuator` without touching any decision logic.

**Decision unit.** The single managed Remote Network. Twingate load-balances across Connectors in the same Remote Network, so the scaling unit is the Connector count within that one Remote Network. Aggregates, watermark comparisons, and scale decisions are all computed for it; there is no per-RN iteration (Rule N1).

**State.** SQLite (`FC_STATE_PATH`) holds only cooldown timestamps and action history. Connector inventory is rediscovered every cycle by querying the Docker socket and the Twingate API; it is never stored. Manual changes to the host are therefore picked up automatically on the next cycle.

---

## Control-loop phases

One cycle runs every `poll_interval_seconds`. A `cycle_id` (UUID hex) is generated at the start and bound onto every log line for the duration of the cycle. The cycle is wrapped so any unhandled error becomes a `loop.cycle.error` log line and the loop survives; a clean cycle ends with a `loop.cycle.complete` heartbeat.

### Phase 1 — Discover

`ControlLoop._discover` (`src/fc/loop.py`)

Lists all FC-managed containers from the Docker actuator (via the `twingate.fc.managed` label), then fetches the authoritative Connector list from the Twingate GraphQL API. Enriches each container with `twingate_state` and `last_heartbeat_at` by joining on the `twingate.fc.connector_id` label. Reads the cordon set from SQLite and marks any cordoned Connector accordingly.

Failures here abort the whole cycle (`loop.cycle.error`), because without an inventory or a Twingate state no safe decision is possible.

### Phase 2 — Collect

`ControlLoop._collect` (`src/fc/loop.py`)

Runs every enabled collector against every Connector. Collector failures are isolated per Connector per collector source: a `collect.error` is logged and `fc_collect_errors_total` is incremented, but collection continues for all other Connectors. A collector returning `None` (no sample this cycle) is simply omitted.

Two collector sources are available (toggled in the YAML policy):

| Source | What it measures | Toggle |
|---|---|---|
| `docker_stats` | CPU (normalized to per-effective-core utilization), memory, and a NIC-delta throughput fallback via the Docker stats API — the universal source, works on any image | `collectors.docker_stats` |
| `stdout_metrics` | CPU, memory, and tunnel throughput parsed from the Connector container's stdout (custom image only) — the primary throughput signal when available | `collectors.stdout_metrics` |

CPU normalization: raw Docker CPU percent is per-single-core and unbounded. The collectors divide by the number of effective CPU cores so the result is a 0–100 per-core utilization comparable to the `scale_metrics.cpu.high_pct` / `low_pct` watermarks (a percentage of the prescribed 1-core limit — Rule N2).

Docker health is read from the authoritative container inspect field `State.Health.Status` (plus `FailingStreak`), not parsed from the list status string. A shared `InspectCache` (`src/fc/docker_inspect.py`) performs at most one inspect per Connector per cycle and is consumed by both the actuator and the `stdout_metrics` collector.

### Phase 3 — Decide

`ControlLoop._decide_scale` and `ControlLoop._decide_and_log_health` (`src/fc/loop.py`)

Reduces each scale metric over its own window/aggregation, fetches cooldown timestamps from SQLite, and calls the pure decider functions for the single managed Remote Network:

- `decide_scale` (`src/fc/engine/decider.py`) — produces one `ScaleDecision`. It combines per-connector high-watermark crossings according to `scale_up_trigger` (`any`/`mean`/`quorum`) and records `connectors_over_high_watermark` / `hot_connector_max` (and `quorum_threshold`) on every decision.
- `decide_health` (`src/fc/engine/decider.py`) — produces zero or more `HealthAction` objects for unhealthy Connectors, applying the startup-grace and continuous-unhealth gates and skipping cordoned and mid-replace Connectors.

The decider performs no I/O. The loop passes in all necessary state (aggregates, cooldowns, restart counts, first-seen / first-unhealthy timestamps, pending-replace ids) and then acts on the returned decisions.

Isolation: a failure in the decide/act block is caught, logged as `loop.rn.error`, and skipped — the heartbeat, fleet gauges, and status snapshot are still written at the end of the cycle.

### Phase 4 — Act

`ControlLoop._act_scale` and `ControlLoop._act_health` (`src/fc/loop.py`)

Executes the decisions produced in Phase 3. Scale actions use the three-step provision and drain-before-delete sequences described below. Health actions use the restart-before-replace sequence. All actuation is serialized under `_action_lock` (an `asyncio.Lock`), which also serializes manual override requests arriving from the FastAPI layer.

---

## Nine non-negotiable design rules

These rules are enforced at the enforcement points listed. Violating them is not a recoverable error; they are invariants the code never relaxes.

**Rule 1 — The Twingate API does not deploy compute.**
Provisioning is always three ordered steps: `connectorCreate` → `connectorGenerateTokens` → `docker run` with those tokens. Deprovisioning is the reverse: `connectorDelete` → `drain_grace_seconds` wait → stop/remove container. Tokens are unique per Connector and are never reused across containers or stored anywhere except the new container's environment.
*Enforced in:* `ControlLoop._provision_one` and `_deprovision_one` (`src/fc/loop.py`).

**Rule 2 — Hard floor.**
`scale_down_count` in `src/fc/engine/policy.py` returns `0` when `current <= min_connectors`, and `min_connectors` has a Pydantic `ge=2` constraint on the flat `Policy` model. The floor cannot be set below 2 (in YAML or via an `FC_POLICY__*` env override). A Remote Network found below its floor is filled back up before anything else, independent of load and ungated by the up-cooldown.
*Enforced in:* `src/fc/engine/policy.py:scale_down_count` / `floor_fill_count`, `src/fc/config.py:Policy`.

**Rule 3 — Per-metric, sustained-window triggers.**
Each scale metric (CPU, throughput) is reduced over its *own* trailing window with its own aggregation mode (`avg`/`min`/`pNN`) before comparison — one window per metric drives both directions (there is no separate up-/down-window). Both directions consult persisted cooldown timestamps from SQLite so a manager restart cannot reset a cooldown. Scale-up always takes precedence: the decider checks high load before low load and never removes capacity while the fleet is hot. How per-connector crossings combine into a fleet scale-up is set by `scale_up_trigger` (`any`/`mean`/`quorum`).
*Enforced in:* `src/fc/engine/decider.py:decide_scale`, `src/fc/engine/aggregator.py`, `src/fc/engine/policy.py:cooldown_remaining`.

**Rule 4 — Drain before delete.**
Scale-down order: pick victim → `connectorDelete` (the controller stops routing new connections) → wait `drain_grace_seconds` → `actuator.deprovision` (stop + remove container). Health replace is **cycle-spanning and wait-for-healthy**: provision the replacement first (`_begin_replace`) and register a pending replace; only on a *later* cycle, once the replacement reports `ALIVE`/healthy, is the old Connector drained and deleted (`_process_pending_replaces`). Capacity never dips. If the replacement does not become healthy within `replace_health_timeout_seconds`, FC emits an alertable `health.replace_timeout`, tears down the failed (never-healthy) replacement, and leaves the old traffic-serving Connector in place to retry next cycle (fail-forward). A Connector is acted on for health only after it has been continuously unhealthy past `unhealthy_threshold_seconds`. Token reuse for a *sequential* same-logical-connector replacement is permitted; two concurrent active containers sharing one token is forbidden (Rule 1).
*Enforced in:* `ControlLoop._deprovision_one`, `_begin_replace`, and `_process_pending_replaces` (`src/fc/loop.py`).

**Rule 5 — Tolerate janus.**
janus has **no lock mechanism** — it upgrades a Connector container whenever a newer image is published. FC does not coordinate with janus via a lock or marker. Instead it (a) *enrols* every Connector it provisions by stamping janus's auto-update labels (`janus.autoupdate.enable=true` + `janus.autoupdate.interval=<seconds>`, gated by the `janus:` policy block), and (b) *absorbs* the brief container recreate a janus upgrade causes (same token, new image) via the `startup_grace_seconds` and `unhealthy_threshold_seconds` windows rather than remediating during that window.
*Enforced in:* `DockerActuator.provision` (label stamping) in `src/fc/actuator/docker_actuator.py`; the grace/unhealthy gates in `src/fc/engine/decider.py:decide_health`.

**Rule 6 — Structured logging with standard fields.**
Every log line is JSON (via structlog) with at minimum `ts`, `level`, `event` (constant from `src/fc/observability/events.py`), and `cycle_id`. The `cycle_id` correlates all signals, decisions, and actions of one cycle. The `loop.cycle.complete` line is the heartbeat — its absence signals a silent or stuck manager.
*Enforced in:* `src/fc/observability/events.py`, `src/fc/loop.py`.

**Rule 7 — The manager is observable from outside.**
`/healthz`, `/readyz`, and `/metrics` are always served. `fc_last_successful_cycle_timestamp_seconds` is updated at the end of every clean cycle so `time() - value > N` is a viable staleness alert.
*Enforced in:* `src/fc/api/app.py`, `src/fc/observability/metrics.py`.

**Rule 8 — CPU is normalized before comparison.**
Raw Docker CPU percent is per single core and unbounded. Collectors divide by the number of effective CPU cores to produce a 0–100 per-core value before storing it in `ResourceSample.cpu_pct_norm`, compared against the prescribed 1-core limit (Rule N2). Memory is advisory only (there is no mem-ceiling knob); CPU and tunnel throughput are the scale triggers.
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
3. actuator.provision(rn_id, connector_id, name, tokens)
        → starts container with tokens in env; sets management labels;
          applies the hard-coded 1 vCPU / 2 GB limits (Rule N2)
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

### Health remediation — restart before replace (cycle-spanning)

A Connector is considered for remediation only when it is unhealthy (Twingate state `DEAD_*` or Docker health `unhealthy`), **not** cordoned (cordon is an operator hand-off — FC takes its hands off entirely), not already mid-replace, past its `startup_grace_seconds`, and has been *continuously* unhealthy past `unhealthy_threshold_seconds`. The `startup_grace_seconds` / `unhealthy_threshold_seconds` gates are also what let FC tolerate a janus upgrade-recreate without remediating (Rule 5). Then:

1. Check `count_recent_restarts(connector_id, since=now - restart_window_seconds)` from SQLite.
2. If `restart_count < max_restarts`: emit `action.restart` (carrying `reason`, `state`, and a bounded `sample`), call `actuator.restart`, record.
3. If `restart_count >= max_restarts`: **begin a replace** — provision a net-new Connector first and register a pending replace (`health.replace_pending`); the old Connector is **not** torn down yet.

The replace then completes across cycles in `_process_pending_replaces`:

- Once the replacement reports `ALIVE`/healthy, drain + delete the old Connector and emit `action.replace`.
- If the replacement has not become healthy within `replace_health_timeout_seconds`, emit `health.replace_timeout` (alertable, **once**), tear down the failed never-healthy replacement, and release the pending slot — the old, traffic-serving Connector is left running and re-evaluated next cycle (fail-forward retry).

Each `fc_health_actions_total{kind, reason_class}` increment classifies the action by a bounded reason class (e.g. `dead_no_relays`, `docker_unhealthy`).

---

## Core domain models

All models are Pydantic v2 (`src/fc/models.py`). No model ever holds secret material.

| Model | Description |
|---|---|
| `ManagedConnector` | A Connector under FC management. Carries `connector_id`, `rn_id`, `container_id` (may be `None` mid-provision), `twingate_state`, `last_heartbeat_at`, `docker_health` (from inspect `State.Health.Status`), `docker_failing_streak` (inspect `FailingStreak`), and `cordoned`. Rediscovered every cycle; never persisted. |
| `ResourceSample` | A single point-in-time signal from one collector for one Connector. Fields: `connector_id`, `source` (`CollectorSource`), `ts`, `cpu_pct_norm` (normalized 0–100), `mem_bytes`, `mem_pct`, `throughput_bps`. |
| `ScaleDecision` | The decider's verdict for one RN: `rn_id`, `direction` (`UP`/`DOWN`/`NONE`), `count`, `reason`, and `metrics` (triggering windowed aggregates for audit). |
| `HealthAction` | A remediation decision for one Connector: `connector_id`, `rn_id`, `kind` (`"restart"` or `"replace"`), `reason`. |
| `ActionRecord` | A persisted row in SQLite's `action_history` table: `ts`, `rn_id`, `action`, `count`, `reason`, `outcome`, `actor` (`"auto"` or `"manual"`). |

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
| `provision` | `(rn_id, connector_id, name, tokens) -> str` | Start a Connector's container with tokens and management labels, applying the hard-coded 1 vCPU / 2 GB limits (Rule N2); return the container id. |
| `deprovision` | `(connector: ManagedConnector) -> None` | Stop and remove a Connector's container. A logical-only Connector (no container) is a no-op. |
| `restart` | `(connector: ManagedConnector) -> None` | Restart a Connector's container in place, preserving its env/tokens. |
| `list_managed` | `() -> list[ManagedConnector]` | List all FC-managed containers as `ManagedConnector` objects. |

The protocol is **backend-agnostic** — three implementations satisfy it identically, and the engine/loop only ever call the four methods (Rule #9):

| Backend | Implementation | Compute unit | `restart` semantics | Collector |
|---|---|---|---|---|
| Docker (default) | `actuator/docker_actuator.py` | one container per Connector | in-place `container.restart()` (same token) | `docker_stats` / `stdout_metrics` (Docker socket) |
| AWS ECS | `actuator/ecs_actuator.py` | one `RunTask` task per Connector | `StopTask` → **wait for STOPPED** → `RunTask` reusing the same token (never two active) | `cloudwatch_logs` (no socket) |
| Azure ACI | `actuator/aci_actuator.py` | one container group per Connector | in-place `POST .../restart` (same token) | `azure_logs` (no socket) |

The explicit `FC_PLATFORM` env var (`docker`/`ecs`/`aci`; no auto-detection, since FC deletes compute) selects the backend; `actuator/factory.py::build_platform` constructs the matching actuator + collector set + readiness probe. Each backend wraps its failures in a typed `ActuatorError` subclass (`DockerActuatorError`/`EcsActuatorError`/`AciActuatorError`) that the loop catches uniformly. All three apply the prescribed 1 vCPU / 2 GB sizing (Rule N2) and reuse the FC identity keys (Docker labels ↔ cloud tags). The single-use-token invariant (Rule #1 — never two active containers/tasks/groups sharing one token) holds across all three. The cloud backends are documented per-platform in `docs/platforms/ecs.md` and `docs/platforms/aci.md` (API mapping, least-privilege IAM/role, settings, collection).
