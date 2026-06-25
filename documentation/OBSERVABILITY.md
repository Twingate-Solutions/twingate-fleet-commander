# Observability Reference

Observability is symmetric: FC watches the fleet; an external monitoring system watches FC. There are two paths:

- **Logs** (stdout, JSON) — zero-config path; anything that collects container stdout picks them up.
- **Health and metrics** (HTTP) — active scrape path; Prometheus, uptime checks, and readiness probes.

---

## Structured logs

FC uses structlog to emit one JSON object per line to stdout. Every line carries the standard fields below. The log level is set by `FC_LOG_LEVEL` (default `info`).

### Standard fields

| Field | Always present | Description |
|---|---|---|
| `ts` | yes | ISO-8601 UTC timestamp |
| `level` | yes | `debug`, `info`, `warning`, or `error` |
| `event` | yes | Event name constant from `src/fc/observability/events.py` |
| `cycle_id` | yes (within a cycle) | UUID hex that correlates all log lines from one discover→collect→decide→act cycle |
| `rn_id` | where relevant | Remote Network id |
| `connector_id` | where relevant | Logical Twingate Connector id |

No log line ever contains the API key, access tokens, refresh tokens, or full container environment variables.

### Event catalog

Source: `src/fc/observability/events.py`. Every `event` field value in a FC log line matches one of the constants below.

#### Cycle lifecycle

| Constant | Value | Level | Emitted when | Key fields beyond standard |
|---|---|---|---|---|
| `LOOP_CYCLE_START` | `loop.cycle.start` | info | Each control-loop cycle begins | `cycle_id` |
| `LOOP_CYCLE_COMPLETE` | `loop.cycle.complete` | info | Each cycle completes cleanly — **this is the heartbeat line** | `cycle_id`, `duration_ms`, `rn_count` |
| `LOOP_CYCLE_ERROR` | `loop.cycle.error` | error | An unhandled error aborted a cycle; loop logs and continues | `cycle_id`, `error` |
| `LOOP_RN_ERROR` | `loop.rn.error` | error | An unhandled error while deciding/acting on one Remote Network; that RN is skipped and the cycle continues with the rest | `rn_id`, `error` |

The absence of `loop.cycle.complete` lines is the primary indicator of a stuck or crashed manager. The `loop.rn.error` event signals per-RN isolation: one bad Remote Network never aborts the whole cycle.

#### Discovery

| Constant | Value | Level | Emitted when | Key fields |
|---|---|---|---|---|
| `DISCOVER_RESULT` | `discover.result` | info | Fleet discovery finishes | `fleet_size`, `per_rn` (counts per RN) |

#### Collection

| Constant | Value | Level | Emitted when | Key fields |
|---|---|---|---|---|
| `COLLECT_SAMPLE` | `collect.sample` | debug | A single resource sample was taken | `connector_id`, `source` |
| `COLLECT_ERROR` | `collect.error` | warning | A collector failed for one Connector (isolated; cycle continues) | `connector_id`, `source`, `error` |

#### Decision

| Constant | Value | Level | Emitted when | Key fields |
|---|---|---|---|---|
| `DECIDE_SCALE_UP` | `decide.scale_up` | info | A scale-up is decided for a Remote Network | `rn_id`, `count`, `reason`, `metrics` |
| `DECIDE_SCALE_DOWN` | `decide.scale_down` | info | A scale-down is decided | `rn_id`, `count`, `reason`, `metrics` |
| `DECIDE_NO_ACTION` | `decide.no_action` | info | Steady state — no scaling action this cycle for the RN | `rn_id`, `reason` |
| `DECIDE_COOLDOWN_SKIP` | `decide.cooldown_skip` | info | A valid scaling action was suppressed by an active cooldown | `rn_id`, `seconds_remaining`, `reason` |

#### Actions

| Constant | Value | Level | Emitted when | Key fields |
|---|---|---|---|---|
| `ACTION_PROVISION_START` | `action.provision.start` | info | Provisioning a Connector began | `rn_id`, `name`, `actor` |
| `ACTION_PROVISION_SUCCESS` | `action.provision.success` | info | Connector provisioned successfully | `rn_id`, `connector_id`, `name`, `actor` |
| `ACTION_PROVISION_FAIL` | `action.provision.fail` | error | Provisioning failed | `rn_id`, `error`, `actor` |
| `ACTION_DEPROVISION_START` | `action.deprovision.start` | info | Drain + remove began | `rn_id`, `connector_id`, `drain_grace`, `actor` |
| `ACTION_DEPROVISION_SUCCESS` | `action.deprovision.success` | info | Connector drained and removed | `rn_id`, `connector_id`, `actor` |
| `ACTION_DEPROVISION_FAIL` | `action.deprovision.fail` | error | Deprovisioning failed | `rn_id`, `connector_id`, `error`, `actor` |
| `ACTION_RESTART` | `action.restart` | info | Connector restarted in place | `rn_id`, `connector_id`, `restart_count` |
| `ACTION_REPLACE` | `action.replace` | info | Connector replaced after repeated restart failures | `rn_id`, `old_connector_id`, `new_connector_id`, `old_removed` |
| `ACTION_CORDON` | `action.cordon` | info | Connector cordoned or un-cordoned via manual override | `connector_id`, `cordoned`, `actor=manual`, `rn_id` |

#### Health

| Constant | Value | Level | Emitted when | Key fields |
|---|---|---|---|---|
| `HEALTH_CONNECTOR_DEAD` | `health.connector_dead` | warning | Twingate reports a Connector in a `DEAD_*` state | `connector_id`, `state` |
| `HEALTH_UNHEALTHY` | `health.unhealthy` | warning | Connector's Docker health is `unhealthy` | `connector_id` |

#### Janus

| Constant | Value | Level | Emitted when | Key fields |
|---|---|---|---|---|
| `JANUS_LOCK_ENGAGED` | `janus.lock_engaged` | info | A Connector was skipped because the janus upgrade lock is set | `connector_id` |

#### Config and external-dependency errors

| Constant | Value | Level | Emitted when | Key fields |
|---|---|---|---|---|
| `CONFIG_RELOAD` | `config.reload` | info | The YAML policy was (re)loaded | `path` |
| `TWINGATE_API_ERROR` | `twingate_api.error` | error | A Twingate GraphQL call failed | `operation`, `error` |
| `DOCKER_API_ERROR` | `docker_api.error` | error | A Docker API call failed | `op`, `error` |

---

## Self-metrics

Source: `src/fc/observability/metrics.py`. Prometheus text exposition is served at `GET /metrics`.

The registry is private (not the global prometheus_client default), so constructing the manager in a test never raises "Duplicated timeseries". No label ever carries a secret or a token.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fc_loop_iterations_total` | Counter | — | Total control-loop cycles attempted (incremented at the start of every cycle including ones that error). |
| `fc_last_successful_cycle_timestamp_seconds` | Gauge | — | Unix timestamp of the last cleanly-completed cycle. **Primary staleness detector**: alert when `time() - value > N`. |
| `fc_connectors` | Gauge | `rn`, `state` | Number of managed Connectors, grouped by Remote Network id and state. The `state` label is the Twingate state (`ALIVE`, `DEAD_NO_HEARTBEAT`, etc.) when available, the Docker health string otherwise, or `unknown`. Cleared and repopulated each cycle so stale combinations disappear. |
| `fc_scale_actions_total` | Counter | `rn`, `direction` | Scale actions that succeeded (`direction` is `up` or `down`). Incremented once per Connector provisioned or deprovisioned. |
| `fc_restarts_total` | Counter | `rn` | Successful in-place Connector restarts, by Remote Network. |
| `fc_replacements_total` | Counter | `rn` | Successful full replacements (new provisioned and old removed), by Remote Network. An incomplete replace (new provisioned but old not removed) is not counted. |
| `fc_seconds_since_last_action` | Gauge | `rn` | Seconds since the most recent scale action (up or down) in a Remote Network. Derived from the SQLite cooldown timestamps; unset for RNs with no recorded action. |
| `fc_twingate_api_errors_total` | Counter | — | Total Twingate Admin API call failures across all operations. |
| `fc_docker_api_errors_total` | Counter | — | Total Docker API call failures. |
| `fc_collect_errors_total` | Counter | `collector` | Collector failures, by collector source name (`docker_stats`, `stdout_metrics`, `prometheus`). |

### HTTP endpoints

All three endpoints are served by the same uvicorn process as the status UI. Every response carries hardening headers: `Content-Security-Policy: default-src 'self'`, `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`.

| Endpoint | HTTP status | Body | Purpose |
|---|---|---|---|
| `GET /healthz` | `200` always | `{"status": "ok"}` | Liveness. The process is up and the asyncio event loop is responsive. Use for container restart policies. |
| `GET /readyz` | `200` (ready) or `503` (not ready) | `{"ready": bool, "docker": bool, "twingate": bool}` | Readiness. Both the Docker socket and the Twingate Admin API must be reachable. Only boolean flags are returned; no error strings or internal details are exposed. |
| `GET /metrics` | `200` | Prometheus text exposition | Self-metrics for scraping. Content-type is `text/plain; version=0.0.4; charset=utf-8`. |

---

## Egress examples and alert rules

### AWS ECS / CloudWatch Logs

Add the `awslogs` log driver to the ECS task definition. All stdout lands in the specified log group automatically:

```json
"logConfiguration": {
  "logDriver": "awslogs",
  "options": {
    "awslogs-group": "/twingate/fc",
    "awslogs-region": "us-east-1",
    "awslogs-stream-prefix": "fc"
  }
}
```

Build CloudWatch metric filters on the JSON `event` field to detect missing heartbeats (no `loop.cycle.complete` for N minutes) or count `action.provision.fail` occurrences.

### Azure Container Instances / Container Insights

When Container Insights is enabled, stdout is collected into the `ContainerLogV2` table in Log Analytics. Query by `event` value:

```kusto
ContainerLogV2
| where LogMessage has '"event":"loop.cycle.complete"'
| summarize last_heartbeat = max(TimeGenerated) by ContainerName
| where last_heartbeat < ago(5m)
```

Build Log Analytics alert rules on the absence of heartbeat lines or the presence of error events.

### Datadog

Run the Datadog agent alongside the stack with log collection enabled. Use autodiscovery labels on the FC container so its JSON lines parse into first-class Datadog attributes (`event`, `rn_id`, `connector_id`, etc.):

Add to the `datadog-agent` compose service:

```yaml
  datadog-agent:
    image: gcr.io/datadoghq/agent:latest
    environment:
      DD_API_KEY: "${DD_API_KEY}"
      DD_LOGS_ENABLED: "true"
      DD_LOGS_CONFIG_CONTAINER_COLLECT_ALL: "true"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - /proc/:/host/proc/:ro
      - /sys/fs/cgroup/:/host/sys/fs/cgroup:ro
```

Label the FC service so its logs are tagged and parsed as JSON:

```yaml
    labels:
      com.datadoghq.ad.logs: '[{"source":"fc","service":"fleet-commander"}]'
```

Build log-based monitors, for example:
- No `loop.cycle.complete` from `service:fleet-commander` in 5 minutes.
- Count of `event:action.provision.fail` > 3 in 15 minutes.

Alternatively, scrape `/metrics` via the Datadog OpenMetrics integration for metric-based monitors.

### Prometheus

Scrape the manager directly:

```yaml
scrape_configs:
  - job_name: "fc"
    static_configs:
      - targets: ["fc-host:8080"]
    metrics_path: /metrics
```

### Recommended alert rules

```yaml
groups:
  - name: fc
    rules:
      - alert: FcCycleStale
        expr: time() - fc_last_successful_cycle_timestamp_seconds > 180
        for: 0m
        labels: { severity: critical }
        annotations:
          summary: "FC has not completed a cycle in >3m"

      - alert: FcDown
        expr: up{job="fc"} == 0
        for: 1m
        labels: { severity: critical }
        annotations:
          summary: "FC scrape target is unreachable"

      - alert: FcFloorBreached
        expr: sum by (rn) (fc_connectors) < 2
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "A Remote Network has fewer than 2 Connectors"

      - alert: FcProvisionFailures
        expr: increase(fc_twingate_api_errors_total[15m]) > 5
        for: 0m
        labels: { severity: warning }
        annotations:
          summary: "Elevated Twingate API errors — provisioning may be failing"
```

`FcCycleStale` is the most important alert. A stuck or crashed manager produces no `loop.cycle.complete` log lines and does not update `fc_last_successful_cycle_timestamp_seconds`; this alert fires within 3 minutes of the last clean cycle.

`FcFloorBreached` detects a fleet that has dropped below the minimum redundancy threshold. Under normal operation the hard floor prevents this from happening via autoscaling; this alert catches external interference (e.g. manual container removal) or a provisioning failure during a replace.
