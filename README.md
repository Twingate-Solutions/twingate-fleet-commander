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

The fastest path is the bootstrap script, which installs Docker, lays down config, brings
the stack up, and waits until the manager is healthy. FC then **self-provisions** its
Connectors — there are no seed Connectors to mint tokens for. It is **idempotent** — safe
to re-run.

```bash
git clone <this-repo> fleet-commander
cd fleet-commander

# Interactive (prompts for network slug + API key):
./deploy/bootstrap.sh

# Or non-interactive:
TWINGATE_NETWORK=acme \
TWINGATE_API_KEY=tgp_xxxxxxxx \
./deploy/bootstrap.sh
```

When it finishes, the status UI + health/metrics are on **port 8080**, bound to **loopback
by default** (reach it via an SSH tunnel — `ssh -L 8080:localhost:8080 <host>` — or front it
with a TLS proxy; see [Manual overrides](#manual-overrides-optional-off-by-default)):

- `http://localhost:8080/` — status UI
- `http://localhost:8080/healthz` — liveness
- `http://localhost:8080/readyz` — readiness (Docker socket + Twingate API reachable)
- `http://localhost:8080/metrics` — Prometheus exposition

### Manual / non-bootstrap path

```bash
cp .env.example .env                          # set TWINGATE_NETWORK + TWINGATE_API_KEY
cp config/config.example.yaml config/config.yaml   # defaults to the custom connector image
docker compose up -d                          # manager + janus; FC provisions the connectors
docker compose --profile shipping up -d       # also start the optional log-shipper
```

**No seed Connectors, no connector tokens in `.env`.** FC brings the Remote Network up to
`min_connectors` from empty and scales on load, minting tokens via the Twingate API and
injecting them straight into the containers it runs. Just start the manager and it fills the
floor itself.

For other topologies (minimal, hardened socket-proxy, Docker Desktop, cloud VM), see the
thin variants in [`deploy/compose/`](deploy/compose/).

### Tearing down (clean — no orphans)

FC `docker run`s the Connector containers directly (they are **not** compose-managed), so a
plain `docker compose down` would stop only `fc`/`janus` and leave every Connector container
running with its logical Connector still registered in the tenant. Tear the fleet down
**first**, then stop the stack:

```bash
docker compose exec fc fc-teardown      # drain + remove every managed Connector, then exit
docker compose --profile shipping down -v
```

`fc-teardown` drains and removes every Connector FC manages (`connectorDelete` → drain grace →
stop/rm), **bypassing the `min_connectors` floor** because the whole deployment is going away.
It is a deliberate, explicit command — a routine manager restart (config reload, janus
upgrade, crash) never tears down the data plane, so Connectors keep serving across FC
restarts. A normal `docker compose restart fc` is always safe.

### First-boot on a cloud VM

Thin cloud-init / user-data snippets that fetch and run `bootstrap.sh` live in
[`deploy/cloud-init/`](deploy/cloud-init/) for **AWS EC2, Azure VM, GCP Compute Engine, and
generic Proxmox**. See [`deploy/cloud-init/README.md`](deploy/cloud-init/README.md).

---

## What comes up

| Service | Role | Notes |
|---|---|---|
| `fc` | the manager (this project) | control loop + status UI + `/metrics`; mounts the Docker socket. Self-provisions the Connectors (no seed services) — fills the Remote Network to `min_connectors` and scales on load |
| `janus` | Connector version updater | no lock — FC enrols the Connectors it provisions with janus auto-update labels and tolerates the brief upgrade-recreate via the grace windows |
| `log-shipper` | connector logs → S3-compatible bucket (optional) | behind the `shipping` compose profile; see [above](#optional-ship-connector-analytics-to-a-non-aws-bucket) |

### ⚠️ The Docker socket is root-equivalent

FC mounts `/var/run/docker.sock` so its actuator can run/stop/remove Connector containers.
Anything that can reach the daemon socket can control the host. Treat the FC host as a
trusted control-plane node:

- Port 8080 binds to **loopback by default**; expose the status UI / override endpoints
  publicly only behind a TLS-terminating proxy, never directly on the public internet.
- For hardened deployments, front the daemon with a **read-mostly socket proxy** that
  allowlists only the container-lifecycle calls FC needs — see
  [`deploy/compose/socket-proxy-hardened.yml`](deploy/compose/socket-proxy-hardened.yml)
  (FC talks to it via `DOCKER_HOST=tcp://socket-proxy:2375` and never mounts the raw socket).
  Note the proxy reduces the reachable Docker API *surface* (no exec/secrets/images/swarm)
  but does **not** make its network safe: allowing `containers/create` still permits a
  privileged-bind container that owns the host, so the proxy network remains a trust
  boundary that only FC may reach.
- The config volume is mounted read-only so a compromised loop can't rewrite policy.

See [`documentation/ARCHITECTURE.md`](documentation/ARCHITECTURE.md) → *Nine non-negotiable design rules* (Rule 9, the actuator interface) and the control-plane trust model.

### Compute backends — Docker, AWS ECS, Azure ACI

FC actuates one compute backend, chosen explicitly by `FC_PLATFORM` (`docker` default, `ecs`, or `aci` — no auto-detection, since FC deletes compute). The same control loop, decision engine, and observability drive all three behind the backend-agnostic `Actuator` interface (Rule #9):

| `FC_PLATFORM` | Backend | Compute unit | Metrics source | Install extra |
|---|---|---|---|---|
| `docker` | local Docker | one container per Connector | Docker socket (`docker_stats` / `stdout_metrics`) | — |
| `ecs` | AWS ECS (`RunTask`, 1:1) | one task per Connector | CloudWatch Logs | `pip install -e '.[ecs]'` |
| `aci` | Azure Container Instances | one container group per Connector | Log Analytics | `pip install -e '.[aci]'` |

Every backend applies the prescribed **1 vCPU / 2 GB** sizing (Rule N2) and upholds the single-use-token rule (a token is never active on two containers/tasks/groups at once; cloud "restart" relaunches the **same** token sequentially). Per-platform setup — API mapping, least-privilege IAM/Azure roles, settings, and collection — is in [`documentation/platforms/ecs.md`](documentation/platforms/ecs.md) and [`documentation/platforms/aci.md`](documentation/platforms/aci.md).

On AWS the `docker` backend also runs well on **one large EC2 instance** packed with Connector containers — often the cheapest path to saturating a single network pipe. Instance sizing, host tuning, and the network-ceiling monitoring FC can't see itself are covered in [`documentation/platforms/ec2.md`](documentation/platforms/ec2.md).

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

- **Secrets** (env / `.env`): `TWINGATE_NETWORK`, `TWINGATE_API_KEY`, the optional override
  secret, and the optional `TWINGATE_SHIPPER_*` block. See [`.env.example`](.env.example).
  Never commit a real `.env`. (No connector tokens — FC mints those itself.)
- **Policy** (non-secret YAML): the connector image (defaults to the custom image),
  watermarks, windows, cooldowns, floor/ceiling, collector toggles, labels, and the `janus:`
  enrolment block. See [`config/config.example.yaml`](config/config.example.yaml).

Full reference: [`documentation/CONFIGURATION.md`](documentation/CONFIGURATION.md).

### Scale-up trigger: any / mean / quorum (the sticky-connector problem)

Connectors in a Remote Network load-balance *new* connections, but existing ones stay
pinned where they landed — so a fleet can end up with **one hot Connector** while the
rest sit idle. The `scale_up_trigger` policy knob chooses how per-connector
high-watermark crossings combine into one fleet scale-up decision:

- **`any`** — scale up if *any single* Connector is over its high watermark (most
  reactive; one hot Connector adds capacity).
- **`mean`** — scale up on the *fleet-average* windowed signal; smooth, but one hot
  Connector can be **diluted** by quiet ones and never trigger.
- **`quorum`** *(default)* — scale up only when a configurable fraction of Connectors
  are hot (`quorum_fraction`, default `0.5`; threshold `= max(1, ceil(fraction × count))`).

Why not always scale on one hot Connector? Adding a Connector only helps if new
connections land on it. If clients stay pinned to the hot one, the new Connector sits
idle and the bottleneck remains — so **chronic single-connector stickiness is usually a
load-balancing problem, not a capacity one.** Watch `connectors_over_high_watermark` and
`hot_connector_max` in the decision logs/metrics: a persistently high `hot_connector_max`
with only one Connector over the watermark is the sticky-Connector signature (fix
balancing, don't lower the quorum). Tune `scale_up_trigger`/`quorum_fraction` over time.
**Scale-down is unchanged** — it stays deliberately conservative, removing capacity only
when *every* present signal is at/below its low watermark (the whole fleet is quiet).

---

## Optional: ship connector analytics to a non-AWS bucket

Twingate's connector log-shipper natively targets AWS S3. The bundled
`log-shipper` service (behind the `shipping` compose profile, **off by default**) lets you
push connector real-time analytics / stdout to **any S3-compatible bucket** — e.g. Google
Cloud Storage via its S3 interoperability endpoint.

- The shipper reads container **log files**, so it mounts `/var/lib/docker/containers:ro`
  (not the Docker socket).
- Configure it with the `TWINGATE_SHIPPER_*` block in [`.env.example`](.env.example). It
  gets a **scoped** env set — FC's `TWINGATE_API_KEY` is never handed to the shipper.
- **GCS (S3 interop):** set `TWINGATE_SHIPPER_S3_ENDPOINT_URL=https://storage.googleapis.com`
  and use a **GCS HMAC key pair** (Cloud Storage → Settings → Interoperability) as
  `TWINGATE_SHIPPER_S3_ACCESS_KEY_ID` / `TWINGATE_SHIPPER_S3_SECRET_ACCESS_KEY`.
- Set `TWINGATE_SHIPPER_DOCKER_CONTAINER_NAME_FILTER` to a substring of the connector image
  you provision — the shipper's default `twingate/connector` does **not** match the custom
  image (`twingate-custom-connector-container`).

```bash
docker compose --profile shipping up -d   # start FC + janus + the shipper
```

---

## Manual overrides (optional, off by default)

The status UI can expose guarded manual controls — **scale ±1**, **cordon/uncordon** a
Connector, and **replace** a Connector — for operators who need to act between autoscaler
cycles. They are **disabled by default** (`FC_OVERRIDE_ENABLED=false`). When enabled they
require a shared secret (`FC_OVERRIDE_SECRET`, ≥16 chars) sent in the
`X-FC-Override-Secret` request header and constant-time compared.

- **Replace** is a **net-new, wait-for-healthy** operation: FC provisions a fresh
  Connector, waits for it to report `ALIVE`/healthy, and only then drains and deletes the
  target — so the fleet never dips below the floor mid-replace.
- Every override is actuated through the same floor/ceiling- and drain-respecting paths as
  the autoscaler and is written to the action history with `actor=manual`.
- **Debounced:** while one override is in flight, a concurrent override is rejected rather
  than queued, so rapid clicking can't stack unbounded provisions/drains. A drain's grace
  wait no longer blocks the control loop or the status UI — the connector's removal runs as a
  background task, so the fleet view updates immediately and the rest of the fleet keeps being
  monitored during a drain.
- An in-flight replace **suspends autoscaling** for the Remote Network until it finishes, so
  the autoscaler can't drain the brand-new replacement while the replace is still settling.

> ⚠️ **Floor-fill:** manually removing or replacing a Connector below `min_connectors`
> only triggers FC to **auto-fill the Remote Network back up to the floor on the next
> cycle**. To run the Remote Network smaller, lower `min_connectors` and restart the
> manager instead — the floor outranks a manual scale-down.
>
> ⚠️ **The override secret travels in a request header in clear text.** It is a static,
> non-expiring bearer credential (rotate it by redeploying with a new `FC_OVERRIDE_SECRET`).
> Enable overrides only behind a TLS-terminating proxy (or over loopback), and ensure any
> proxy in front does not log the `X-FC-Override-Secret` header. The status UI / `/metrics`
> port binds to **`127.0.0.1` by default** in `docker-compose.yml`; public exposure is
> opt-in and should always sit behind TLS. Reach a loopback-bound manager over an SSH tunnel
> (`ssh -L 8080:localhost:8080 <host>`).

---

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the development setup and the four
quality gates (`ruff check`, `ruff format --check`, `mypy`, `pytest`) that CI
enforces.

## License

Licensed under the Apache License, Version 2.0 — see [`LICENSE`](LICENSE). The
software is provided on an "AS IS" basis, without warranties or conditions of
any kind (see the disclaimer at the top of this README).
