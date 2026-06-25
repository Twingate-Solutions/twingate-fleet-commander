# Configuration Reference

FC has two distinct configuration surfaces:

- **Environment variables / secrets** — process configuration and secrets, sourced from the environment (or an optional `.env` file). Never put secrets in the YAML file.
- **YAML policy** — the non-secret autoscaling policy, loaded from `FC_CONFIG_PATH`. Controls watermarks, windows, cooldowns, fleet bounds, collector toggles, and Docker label keys.

Both surfaces are validated at startup via Pydantic v2. A bad configuration causes an immediate exit with a human-readable error message that contains no secret values.

---

## Environment variables

Source: `src/fc/config.py:Settings` (pydantic-settings, reads from environment and `.env`). See [../.env.example](../.env.example) for a template.

`TWINGATE_NETWORK` and `TWINGATE_API_KEY` are required. All others have defaults.

| Variable | Type | Default | Description |
|---|---|---|---|
| `TWINGATE_NETWORK` | `str` | — **(required)** | Network slug for `https://<slug>.twingate.com`. The GraphQL endpoint is derived from this value. |
| `TWINGATE_API_KEY` | `SecretStr` | — **(required)** | Twingate Admin API key. Requires Admin or DevOps role to create and delete Connectors. Stored as `SecretStr`; never rendered in logs, `repr`, or exceptions. |
| `FC_CONFIG_PATH` | `str` | `/app/config/config.yaml` | Filesystem path to the YAML policy file. |
| `FC_STATE_PATH` | `str` | `/app/state/fc.sqlite3` | Filesystem path to the SQLite state database (cooldowns + action history). |
| `FC_LOG_LEVEL` | `str` | `info` | Log verbosity. One of: `debug`, `info`, `warning`, `error`. Case-insensitive. |
| `DOCKER_HOST` | `str` | `unix:///var/run/docker.sock` | Docker socket or remote Docker daemon URL. |
| `FC_OVERRIDE_ENABLED` | `bool` | `false` | Gate for the manual scale/cordon override endpoints (`POST /api/overrides/*`). When `false` those endpoints return `403`. |
| `FC_OVERRIDE_SECRET` | `SecretStr\|None` | `None` | Shared secret required in the `X-FC-Override-Secret` header when override endpoints are called. Must be set and at least 16 characters when `FC_OVERRIDE_ENABLED=true`; validation fails fast at startup otherwise. |

**Startup validation:**
- `FC_LOG_LEVEL` must be one of `debug`, `info`, `warning`, `error`.
- When `FC_OVERRIDE_ENABLED=true`, `FC_OVERRIDE_SECRET` must be set and at least 16 characters.

---

## YAML policy

Source: `src/fc/config.py:Policy`, loaded by `load_policy(FC_CONFIG_PATH)`. See [../config/config.example.yaml](../config/config.example.yaml) for an annotated template.

Unknown keys at any level are rejected (`extra="forbid"`) so a typo in the YAML fails fast at startup rather than being silently ignored.

**One FC instance manages exactly one Remote Network (Key Design Rule N1).** The policy is therefore a single **flat** model carrying that Remote Network's id directly — there is no `defaults` block and no `remote_networks[]` list, and there is no per-RN merge/override resolution. To manage more Remote Networks, run more FC instances.

**Every policy knob is environment-overridable.** Each field can be set via an `FC_POLICY__<FIELD>` environment variable (nested fields use `__`, e.g. `FC_POLICY__SCALE_METRICS__CPU__HIGH_PCT`). Precedence is **env → YAML → field default**: an env var wins over the YAML file, which wins over the model's built-in default. This makes it possible to twist one knob in `docker-compose` without editing the mounted file.

### Top-level keys

| Key | Type | Default | Description |
|---|---|---|---|
| `remote_network_id` | `str` | — **(required)** | The base64 id of the single Remote Network this FC instance manages (from the Twingate Admin API). |
| `remote_network_name` | `str \| None` | `None` | Optional human-friendly name for the managed Remote Network (display only). |
| `poll_interval_seconds` | `int >= 1` | — | How often the control loop runs one full cycle. |
| `connector_image` | `str` | — | Docker image used when provisioning new Connectors. |
| `min_connectors` | `int >= 2` | — | Hard redundancy floor. Scale-down never reduces the Remote Network below this count. Cannot be set below 2. |
| `max_connectors` | `int >= 2`, `>= min_connectors` | — | Scale-up ceiling. Provisioning stops when the count reaches this value. |
| `scale_step` | `int >= 1` | — | Number of Connectors to add or remove per scale action. |
| `scale_metrics` | object | — | Per-metric scale triggers (CPU + throughput). See below. |
| `scale_up_trigger` | `"any" \| "mean" \| "quorum"` | `quorum` | How per-connector high-watermark crossings combine into one fleet scale-up decision. See *Scale-up trigger* below. |
| `quorum_fraction` | `float`, `0 < x <= 1` | `0.5` | Fraction of Connectors that must be over a high watermark under `quorum` mode; the integer threshold is `max(1, ceil(quorum_fraction * current_count))`. |
| `scale_up_cooldown_seconds` | `int >= 0` | — | Minimum elapsed time between two consecutive scale-up actions. Persisted in SQLite so a manager restart cannot reset it. |
| `scale_down_cooldown_seconds` | `int >= 0` | — | Minimum elapsed time between two consecutive scale-down actions. |
| `drain_grace_seconds` | `int >= 0` | — | Seconds to wait after `connectorDelete` (which stops the controller routing new connections) before stopping and removing the container. |
| `max_restarts` | `int >= 1` | — | Maximum in-place restarts of a Connector within `restart_window_seconds` before the decider escalates from restart to replace. |
| `restart_window_seconds` | `int >= 1` | — | Rolling window over which restart counts are evaluated for restart-before-replace escalation. The effective window is printed in the replace `reason` string. |
| `startup_grace_seconds` | `int >= 0` | `90` | Grace window after FC first sees a Connector before a *never-heartbeated* `DEAD_NO_HEARTBEAT` Connector is treated as dead, so a freshly provisioned Connector is not restarted before its first heartbeat registers. `0` disables grace. |
| `unhealthy_threshold_seconds` | `int >= 0` | `60` | A Connector must be *continuously* unhealthy for at least this long before any health remediation (restart/replace) fires, so a brief blip never triggers an action. The timer resets the moment the Connector recovers. `0` disables the gate (act on the first unhealthy observation). |
| `replace_health_timeout_seconds` | `int >= 1` | `300` | Bound on the cycle-spanning wait-for-healthy replace: after the replacement is provisioned, FC waits up to this long for it to report `ALIVE`/healthy before giving up on that attempt. On timeout the failed replacement is torn down and the old Connector is left in place to retry next cycle. |
| `collectors` | object | — | Enable/disable flags for each signal collector. See below. |
| `labels` | object | — | Docker label keys FC sets and reads to identify managed Connectors. See below. |
| `janus` | object | (enabled) | Janus auto-update enrolment for provisioned Connectors. See *Janus enrolment* below. |

> **Container resource limits are not configurable (Key Design Rule N2).** Every provisioned Connector is given the prescribed per-connector limits — **1 vCPU / 2 GB** — hard-coded by the actuator (Docker `NanoCpus` + `Memory`; ECS `cpu=1024`/`memory=2048`; ACI `1.0` core / `2.0` GB). The Connector data path is effectively single-threaded, so a 1-vCPU limit lets a saturated Connector read ~100% normalized CPU (against two cores it would top out near ~50% and the watermark could never fire) — FC scales horizontally instead. The CPU watermark (`scale_metrics.cpu.high_pct` / `low_pct`) is therefore a percentage of that one effective core. There is no `mem_ceiling_bytes` knob; memory is advisory only and never a scale trigger.

### `collectors`

| Key | Type | Description |
|---|---|---|
| `docker_stats` | `bool` | Collect CPU (normalized), memory, and a NIC-delta throughput fallback via the Docker stats API. Works with any Connector image — the universal source. |
| `stdout_metrics` | `bool` | Parse CPU, memory, and tunnel throughput from the Connector's stdout (custom image only). The primary throughput signal when available. Opt-in; disabled by default in the example config. |

### `labels`

| Key | Type | Description |
|---|---|---|
| `managed` | `str` | Label key FC sets on every container it provisions to identify it as managed (e.g. `twingate.fc.managed`). Discovery filters on this label. |
| `remote_network` | `str` | Label key FC sets to record the Remote Network id on a container (e.g. `twingate.fc.rn`). |
| `connector_id` | `str` | Label key FC sets to record the logical Twingate Connector id on a container (e.g. `twingate.fc.connector_id`). Used as the join key back to the Twingate API. |

### `janus` — janus enrolment

janus (the connector version-updater sidecar) has **no lock mechanism** — it upgrades a container whenever a newer image is published (Key Design Rule #5). FC does **not** coordinate with janus via a lock or skip Connectors for it. Instead, when janus is enabled, FC *enrols* every Connector it provisions by stamping the janus auto-update labels on it, and *absorbs* the brief container recreate a janus upgrade causes via the `startup_grace_seconds` and `unhealthy_threshold_seconds` windows.

| Key | Type | Default | Description |
|---|---|---|---|
| `enabled` | `bool` | `true` | When `true`, stamp `janus.autoupdate.enable=true` and `janus.autoupdate.interval=<interval_seconds>` on every provisioned Connector (alongside the `twingate.fc.*` labels). When `false`, no janus labels are stamped. |
| `interval_seconds` | `int >= 1` | `86400` | The auto-update interval (seconds) written into the `janus.autoupdate.interval` label. |

Both fields are env-overridable via `FC_POLICY__JANUS__ENABLED` / `FC_POLICY__JANUS__INTERVAL_SECONDS`.

### `scale_metrics` — per-metric scale triggers

Each scale metric carries its own watermarks, **its own sustained window**, and **its own time-aggregation mode**, so CPU can react on a short window while throughput reacts on a longer one (Key Design Rule #3, expressed per-metric). There is **one window per metric**, and that same windowed value drives *both* the high (scale-up) and low (scale-down) tests — there is no separate up-window / down-window.

#### `scale_metrics.cpu`

| Key | Type | Constraint | Description |
|---|---|---|---|
| `high_pct` | `float` | `0–100`, `> low_pct` | Normalized per-effective-core CPU utilization (0–100) at or above which a Connector counts as over the CPU high watermark. |
| `low_pct` | `float` | `0–100`, `< high_pct` | Normalized CPU utilization at or below which the CPU signal counts as low (for scale-down). |
| `window_seconds` | `int` | `>= 1` | Length of the trailing window over which CPU is reduced before comparison. |
| `agg` | `str` | `avg` \| `min` \| `pNN` | Time-aggregation mode applied over the window (default `avg`). See *Aggregation modes* below. |

#### `scale_metrics.throughput`

| Key | Type | Constraint | Description |
|---|---|---|---|
| `high_mbps` | `float` | `>= 0`, `> low_mbps` | Per-connector tunnel throughput (Mbps) at or above which a Connector counts as over the throughput high watermark. |
| `low_mbps` | `float` | `>= 0`, `< high_mbps` | Per-connector throughput (Mbps) at or below which the throughput signal counts as low. |
| `window_seconds` | `int` | `>= 1` | Length of the trailing window over which throughput is reduced before comparison. |
| `agg` | `str` | `avg` \| `min` \| `pNN` | Time-aggregation mode applied over the window (default `avg`). |

#### Aggregation modes (`agg`)

Each metric's windowed samples are reduced to a single value using its `agg` mode before comparison to the watermarks:

| Mode | Meaning |
|---|---|
| `avg` | Mean of the samples in the window (default) — smooth, the usual choice. |
| `min` | Minimum sample in the window — approximates "stayed above the high watermark for the whole window" (the signal must *never* dip below to count as sustained-high). |
| `pNN` | The `NN`th percentile (e.g. `p95`), `NN` in `0–100` — tolerates brief outliers while still requiring sustained load. |

```yaml
scale_metrics:
  cpu:
    high_pct: 75        # normalized per-effective-core %
    low_pct: 25
    window_seconds: 300 # short — CPU reacts fast
    agg: avg
  throughput:
    high_mbps: 80       # per-connector tunnel throughput
    low_mbps: 10
    window_seconds: 900 # longer — throughput reacts slowly
    agg: p95
```

### Watermark and load semantics

A Connector is "over its high watermark" when its windowed CPU **or** its windowed throughput crosses the respective high watermark.

**High-load trigger (scale-up):** how per-connector crossings combine into one fleet scale-up decision is governed by `scale_up_trigger` (see below). Scale-up is always evaluated before scale-down in a cycle; if a scale-up fires, scale-down is not evaluated that cycle.

**Low-load trigger (scale-down):** unchanged and deliberately conservative — fires only when *every* present fleet-average signal is at or below its low watermark, with at least one signal present. An absent signal never counts as low; missing data never justifies removing capacity. Scale-down never consults the per-connector trigger.

### Scale-up trigger: `any` / `mean` / `quorum`

Connectors in a Remote Network load-balance *new* connections, but existing connections stay pinned where they landed — so a fleet can develop **one hot Connector** while the rest sit idle. `scale_up_trigger` chooses how the per-connector high-watermark crossings combine:

| Mode | Scales up when… | Trade-off |
|---|---|---|
| `any` | **any single** Connector is over its high watermark | Most reactive; one hot Connector adds capacity immediately, but prone to over-provisioning on a lone sticky Connector. |
| `mean` | the **fleet-average** windowed signal is over the high watermark | Smooth, but a single hot Connector is diluted by quiet ones and a real hotspot may never trigger. |
| `quorum` *(default)* | at least `max(1, ceil(quorum_fraction * current_count))` Connectors are over their high watermark | The middle ground: genuine fleet-wide load scales up, but a single sticky Connector does not. |

Chronic single-connector stickiness is usually a *load-balancing* problem, not a *capacity* one — adding a Connector does not help if clients stay pinned to the hot one. `quorum` is the default precisely because it resists that failure mode. The decision logs/metrics carry `connectors_over_high_watermark` and `hot_connector_max` (and `quorum_threshold` in quorum mode) so you can tune over time — a persistently high `hot_connector_max` with `connectors_over_high_watermark == 1` is the sticky-Connector signature (fix balancing, don't lower the quorum).

### Startup validation invariants

| Invariant | Error |
|---|---|
| `min_connectors >= 2` | Enforced by Pydantic `ge=2` on the flat `Policy` model |
| `max_connectors >= min_connectors` | Enforced by the `Policy` model validator |
| `cpu.high_pct > cpu.low_pct` | Enforced by the `CpuScaleMetric` model validator |
| `throughput.high_mbps > throughput.low_mbps` | Enforced by the `ThroughputScaleMetric` model validator |
| `agg` is `avg`, `min`, or `pNN` (0–100) | Enforced by the metric model validators |
| `0 < quorum_fraction <= 1` | Enforced by Pydantic `gt=0, le=1` on `Policy` |
| `FC_LOG_LEVEL` is a valid level | Enforced by `Settings` model validator |
| `FC_OVERRIDE_SECRET >= 16 chars` when overrides enabled | Enforced by `Settings` model validator |
| No unknown YAML keys or unknown `FC_POLICY__*` env keys | `extra="forbid"` on all policy models |
| Policy file exists and is valid YAML | Checked in `load_policy`; raises `ConfigError` |
