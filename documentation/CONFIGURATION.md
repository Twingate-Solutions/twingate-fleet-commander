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

### Top-level keys

| Key | Type | Description |
|---|---|---|
| `poll_interval_seconds` | `int >= 1` | How often the control loop runs one full cycle. |
| `connector_image` | `str` | Docker image used when provisioning new Connectors (e.g. `twingate/connector:1`). |
| `metrics_port` | `int` (1–65535) | The port on each Connector container that exposes the Prometheus metrics endpoint (default in the example: `9999`). |
| `collectors` | object | Enable/disable flags for each signal collector. See below. |
| `labels` | object | Docker label keys FC sets and reads to identify managed Connectors. See below. |
| `janus_lock_label` | `str` | Docker label key whose presence on a container signals a janus upgrade in progress. FC skips all scale and health actions on a locked Connector. |
| `defaults` | object | Default tunable values applied to every Remote Network unless overridden. See below. |
| `remote_networks` | `list` | Optional per-RN override blocks. Any key omitted inherits from `defaults`. See below. |

### `collectors`

| Key | Type | Description |
|---|---|---|
| `docker_stats` | `bool` | Collect CPU (normalized) and memory via the Docker stats API. Works with any Connector image. |
| `stdout_metrics` | `bool` | Parse CPU and memory from the Connector's stdout (custom image only). Opt-in; disabled by default in the example config. |
| `prometheus` | `bool` | Scrape tunnel throughput (bytes/sec) from `:<metrics_port>/metrics` on each Connector container. Primary throughput signal. |

### `labels`

| Key | Type | Description |
|---|---|---|
| `managed` | `str` | Label key FC sets on every container it provisions to identify it as managed (e.g. `twingate.fc.managed`). Discovery filters on this label. |
| `remote_network` | `str` | Label key FC sets to record the Remote Network id on a container (e.g. `twingate.fc.rn`). |
| `connector_id` | `str` | Label key FC sets to record the logical Twingate Connector id on a container (e.g. `twingate.fc.connector_id`). Used as the join key back to the Twingate API. |

### `defaults` — tunable fields

Applied to every Remote Network unless a `remote_networks` override provides a non-`null` value for a field.

| Key | Type | Constraint | Description |
|---|---|---|---|
| `min_connectors` | `int` | `>= 2` | Hard redundancy floor. Scale-down never reduces a Remote Network below this count. Cannot be set below 2 anywhere in the config. |
| `max_connectors` | `int` | `>= 2`, `>= min_connectors` | Scale-up ceiling. Provisioning stops when the count reaches this value. |
| `scale_step` | `int` | `>= 1` | Number of Connectors to add or remove per scale action. |
| `cpu_high_pct` | `float` | `0–100`, `> cpu_low_pct` | Normalized per-effective-core CPU utilization (0–100) at or above which scale-up is triggered in the up-window. |
| `cpu_low_pct` | `float` | `0–100`, `< cpu_high_pct` | Normalized CPU utilization at or below which scale-down may be triggered in the down-window. |
| `throughput_high_mbps` | `float` | `>= 0`, `> throughput_low_mbps` | Per-connector tunnel throughput (Mbps) at or above which scale-up is triggered. |
| `throughput_low_mbps` | `float` | `>= 0`, `< throughput_high_mbps` | Per-connector throughput (Mbps) at or below which scale-down may be triggered. |
| `mem_ceiling_bytes` | `int` | `>= 0` | Optional memory limit passed to the container at provision time. `0` means no limit (memory is advisory only and never used as a scale trigger). |
| `scale_up_window_seconds` | `int` | `>= 1` | Length of the rolling window over which CPU/throughput averages are computed for scale-up evaluation. Keep short to react quickly. |
| `scale_down_window_seconds` | `int` | `>= 1` | Length of the rolling window for scale-down evaluation. Keep longer than `scale_up_window_seconds` to remove capacity conservatively. |
| `scale_up_cooldown_seconds` | `int` | `>= 0` | Minimum elapsed time between two consecutive scale-up actions on the same Remote Network. Cooldown timestamps are persisted in SQLite so a manager restart cannot reset them. |
| `scale_down_cooldown_seconds` | `int` | `>= 0` | Minimum elapsed time between two consecutive scale-down actions on the same Remote Network. |
| `drain_grace_seconds` | `int` | `>= 0` | Seconds to wait after `connectorDelete` (which stops the controller routing new connections) before stopping and removing the container. Gives existing connections time to close. |
| `max_restarts` | `int` | `>= 1` | Maximum number of restarts for a Connector within `restart_window_seconds` before the decider escalates from restart to replace. |
| `restart_window_seconds` | `int` | `>= 1` | Rolling window over which restart counts are evaluated for restart-before-replace escalation. |

### Watermark and load semantics

**High-load trigger (scale-up):** fires when *any* available signal meets or exceeds its high watermark. One saturated resource is sufficient reason to add capacity.

**Low-load trigger (scale-down):** fires only when *all* available signals are at or below their low watermarks, and at least one signal is present. An absent signal never counts as low; missing data never justifies removing capacity.

Scale-up is always evaluated before scale-down in a cycle. If high load is detected, scale-down is not evaluated that cycle.

### `remote_networks` — per-RN overrides

Each entry must have `id` and `name`. All tunable fields are optional; an omitted field inherits from `defaults`. The `min_connectors` floor of 2 is re-enforced on the fully-merged result after override, so an override cannot breach it.

```yaml
remote_networks:
  - id: "UmVtb3RlTmV0d29yazoxMjM="   # base64 Remote Network id from the Twingate API
    name: "aws-prod"
    min_connectors: 3
    max_connectors: 10
    # all other fields inherit from defaults
```

### Override resolution

`Policy.resolve_remote_network(rn_id)` (`src/fc/config.py`) produces a fully-concrete `ResolvedRemoteNetwork`:

1. Start with all `defaults` fields.
2. If `rn_id` matches a `remote_networks` entry, overwrite each field for which the override provides a non-`None` value.
3. Validate the merged result (all constraints including `max_connectors >= min_connectors` and watermark ordering).
4. If `rn_id` is not in `remote_networks` (e.g. auto-discovered), pure defaults are used with `rn_id` as both `id` and `name`.

All configured overrides are eagerly resolved at startup (`policy.resolved_networks()`), so a per-RN override whose merged result violates an invariant fails fast rather than mid-cycle.

### Startup validation invariants

| Invariant | Error |
|---|---|
| `min_connectors >= 2` | Enforced by Pydantic `ge=2` on all definitions and the merged result |
| `max_connectors >= min_connectors` | Enforced by `RemoteNetworkDefaults` model validator and re-checked on merged result |
| `cpu_high_pct > cpu_low_pct` | Enforced by `RemoteNetworkDefaults` model validator |
| `throughput_high_mbps > throughput_low_mbps` | Enforced by `RemoteNetworkDefaults` model validator |
| `FC_LOG_LEVEL` is a valid level | Enforced by `Settings` model validator |
| `FC_OVERRIDE_SECRET >= 16 chars` when overrides enabled | Enforced by `Settings` model validator |
| No unknown YAML keys | `extra="forbid"` on all policy models |
| Policy file exists and is valid YAML | Checked in `load_policy`; raises `ConfigError` |
