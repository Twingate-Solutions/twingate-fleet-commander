# Fleet Commander (FC)

> ⚠️ **Example project — provided as-is, with no support or warranty.** Fleet Commander is published as a reference example to build from, not a supported product. Nothing here is guaranteed and no support is attached to it. It was developed with help from an LLM-based coding assistant. Review the code and test it yourself before using it in any critical or production environment. Your use is governed by the Apache License 2.0, including its "AS IS", no-warranty (Section 7), and limitation-of-liability (Section 8) terms.

A single-host, containerized control plane for a fleet of [Twingate](https://www.twingate.com/)
Connectors. FC runs a continuous async control loop that discovers managed Connector
containers, collects load and liveness signals, and **autoscales each Remote Network** —
provisioning Connectors when sustained load is high, draining them when it is low, and
restarting or replacing unhealthy ones. It actuates by driving the local Docker socket;
the Twingate GraphQL Admin API is the bookkeeping layer that creates/deletes the logical
Connector and mints its tokens.

Observability is symmetric: **FC watches the fleet, and your monitoring system watches FC**
via structured stdout logs and a Prometheus `/metrics` endpoint.

> **Reference docs:** This README is the quickstart. The full architecture, configuration
> reference, and observability/event catalog live in [`documentation/`](documentation/):
> [Architecture](documentation/ARCHITECTURE.md) ·
> [Configuration](documentation/CONFIGURATION.md) ·
> [Observability](documentation/OBSERVABILITY.md).

---

## Quickstart — stand up a host

The fastest path is the bootstrap script, which installs Docker, lays down config, mints
the seed Connectors' tokens via the Twingate API, brings the stack up, and waits until the
manager is healthy. It is **idempotent** — safe to re-run.

```bash
git clone <this-repo> fleet-commander
cd fleet-commander

# Interactive (prompts for network slug, API key, seed Remote Network id):
./deploy/bootstrap.sh

# Or non-interactive:
TWINGATE_NETWORK=acme \
TWINGATE_API_KEY=tgp_xxxxxxxx \
SEED_RN_ID=UmVtb3RlTmV0d29yazoxMjM= \
./deploy/bootstrap.sh
```

When it finishes, the status UI + health/metrics are on **port 8080**:

- `http://<host>:8080/` — status UI
- `http://<host>:8080/healthz` — liveness
- `http://<host>:8080/readyz` — readiness (Docker socket + Twingate API reachable)
- `http://<host>:8080/metrics` — Prometheus exposition

### Manual / non-bootstrap path

```bash
cp .env.example .env                          # set TWINGATE_NETWORK + TWINGATE_API_KEY (+ seed tokens)
cp config/config.example.yaml config/config.yaml
docker compose up -d                          # manager + seed connectors + janus
docker compose --profile shipping up -d       # also start the optional log-shipper
```

Seed connectors need per-connector tokens in `.env` (`SEED1_ACCESS_TOKEN`, …). `bootstrap.sh`
mints these for you (`connectorCreate` → `connectorGenerateTokens`); if you skip bootstrap,
mint them yourself or set `FC_SKIP_SEED=1` and let FC provision up to the floor on its own.

### First-boot on a cloud VM

Thin cloud-init / user-data snippets that fetch and run `bootstrap.sh` live in
[`deploy/cloud-init/`](deploy/cloud-init/) for **AWS EC2, Azure VM, GCP Compute Engine, and
generic Proxmox**. See [`deploy/cloud-init/README.md`](deploy/cloud-init/README.md).

---

## What comes up

| Service | Role | Notes |
|---|---|---|
| `fc` | the manager (this project) | control loop + status UI + `/metrics`; mounts the Docker socket |
| `connector-seed-1/2` | seed Connectors | FC discovers and scales from these (labels match `config.yaml`) |
| `janus` | Connector version updater | FC yields to it via the upgrade-lock label; never fights it |
| `log-shipper` | stdout → SIEM (optional) | behind the `shipping` compose profile |

### ⚠️ The Docker socket is root-equivalent

FC mounts `/var/run/docker.sock` so its actuator can run/stop/remove Connector containers.
Anything that can reach the daemon socket can control the host. Treat the FC host as a
trusted control-plane node:

- Bind port 8080 to a trusted/loopback interface behind a TLS-terminating proxy; never
  expose the status UI / override endpoints to the public internet.
- For hardened deployments, front the daemon with a **read-mostly socket proxy** that
  allowlists only the container-lifecycle calls FC needs (a commented `socket-proxy`
  service is included in `docker-compose.yml`, enabled via `--profile hardened`).
- The config volume is mounted read-only so a compromised loop can't rewrite policy.

See [`documentation/ARCHITECTURE.md`](documentation/ARCHITECTURE.md) → *Nine non-negotiable design rules* (Rule 9, the actuator interface) and the control-plane trust model.

---

## Observability egress — wire FC into your monitoring

FC emits **one JSON event per line to stdout** (every decision/action plus a per-cycle
`loop.cycle.complete` heartbeat — a silent manager is detectable by the *absence* of the
heartbeat) and exposes Prometheus self-metrics on `/metrics`. Both paths are always on.

### AWS — CloudWatch Logs

When running under ECS, use the `awslogs` log driver in the task definition; all stdout
lands in a log group automatically:

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

Build a metric filter + alarm on the JSON (e.g. alarm when no `loop.cycle.complete` line
appears for N minutes, or `{ $.event = "action.provision.fail" }`).

### Azure — Monitor / Container Insights

With Container Insights enabled, stdout is collected into `ContainerLogV2`. Filter on
`LogMessage has '"event":"loop.cycle.complete"'` (heartbeat) or by `event` value and build
Log Analytics alert rules.

### Datadog

Run the Datadog agent with `DD_LOGS_ENABLED=true` and use container autodiscovery labels so
the JSON parses into first-class attributes. Label the `fc` service:

```yaml
    labels:
      com.datadoghq.ad.logs: '[{"source":"fc","service":"fleet-commander"}]'
```

Then build log-based monitors (e.g. "no `loop.cycle.complete` in 5m"). Full agent compose
snippet in [`documentation/OBSERVABILITY.md`](documentation/OBSERVABILITY.md) → *Egress Examples*.

### Prometheus — scrape `/metrics`

```yaml
scrape_configs:
  - job_name: "fc"
    metrics_path: /metrics
    static_configs:
      - targets: ["fc-host:8080"]
```

**The alert rules that matter:**

```yaml
groups:
  - name: fc
    rules:
      - alert: FcCycleStale            # the heartbeat freshness check
        expr: time() - fc_last_successful_cycle_timestamp_seconds > 180
        for: 0m
        labels: { severity: critical }
        annotations: { summary: "FC has not completed a control cycle in >3m" }
      - alert: FcDown                  # /healthz / scrape target down
        expr: up{job="fc"} == 0
        for: 1m
        labels: { severity: critical }
      - alert: FcFloorBreached         # an RN below its hard floor
        expr: sum by (rn) (fc_connectors) < 2
        for: 2m
        labels: { severity: critical }
      - alert: FcProvisionFailures     # repeated Twingate API / provision failures
        expr: increase(fc_twingate_api_errors_total[15m]) > 5
        for: 0m
        labels: { severity: warning }
```

Key self-metrics: `fc_last_successful_cycle_timestamp_seconds` (staleness),
`fc_connectors{rn,state}` (fleet size), `fc_scale_actions_total{rn,direction}`,
`fc_restarts_total{rn}`, `fc_replacements_total{rn}`, `fc_twingate_api_errors_total`,
`fc_docker_api_errors_total`, `fc_collect_errors_total{collector}`. No metric label ever
contains a secret. Full catalog: [`documentation/OBSERVABILITY.md`](documentation/OBSERVABILITY.md).

---

## Configuration

- **Secrets** (env / `.env`): `TWINGATE_NETWORK`, `TWINGATE_API_KEY`, seed tokens. See
  [`.env.example`](.env.example). Never commit a real `.env`.
- **Policy** (non-secret YAML): watermarks, windows, cooldowns, per-RN floors/ceilings,
  collector toggles, labels. See [`config/config.example.yaml`](config/config.example.yaml).

Full reference: [`documentation/CONFIGURATION.md`](documentation/CONFIGURATION.md).

---

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the development setup and the four
quality gates (`ruff check`, `ruff format --check`, `mypy`, `pytest`) that CI
enforces.

## License

Licensed under the Apache License, Version 2.0 — see [`LICENSE`](LICENSE). The
software is provided on an "AS IS" basis, without warranties or conditions of
any kind (see the disclaimer at the top of this README).
