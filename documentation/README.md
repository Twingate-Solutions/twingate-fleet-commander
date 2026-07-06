# Fleet Commander — Documentation Index

Fleet Commander (FC) is a single-host, containerized control plane that autoscales a fleet of Twingate Connectors on a Docker host. It runs a continuous asyncio control loop (discover → collect → decide → act) and a FastAPI server (health, metrics, status UI) in the same process. FC drives the local Docker socket for lifecycle operations and uses the Twingate GraphQL Admin API as the logical bookkeeping layer; SQLite persists only cooldown timers and action history. The manager exposes structured JSON logs, `/healthz`, `/readyz`, and a Prometheus `/metrics` endpoint so an external monitoring system can supervise the manager with the same tools the manager uses to supervise the fleet.

## Contents

| Document | What it covers |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Control-plane model, control-loop phases, design rules, provision/deprovision sequences, domain models, SQLite state, and the Actuator protocol |
| [CONFIGURATION.md](CONFIGURATION.md) | Every environment variable and YAML policy key, types, defaults, validation invariants, and per-RN override resolution |
| [OBSERVABILITY.md](OBSERVABILITY.md) | Structured-log standard fields, complete event catalog, self-metric catalog, health/metrics API, and egress examples for CloudWatch, Azure, Datadog, and Prometheus |
| [host-tuning.md](host-tuning.md) | Host-global kernel/Docker tuning for a busy host (conntrack, socket buffers, BBR, daemon defaults) — the standalone, compose-friendly companion to what FC already stamps per connector |

## Platform deployment guides

Per-backend setup for each `FC_PLATFORM`. The decision engine, policy, and observability are identical across all of them — only the compute backend and metrics source differ.

| Guide | Backend |
|---|---|
| [platforms/ec2.md](platforms/ec2.md) | `docker` on **one large AWS EC2 instance** — instance sizing, host tuning, and the network ceiling FC can't see itself |
| [platforms/ecs.md](platforms/ecs.md) | `ecs` — AWS ECS/Fargate, one task per Connector, least-privilege IAM |
| [platforms/aci.md](platforms/aci.md) | `aci` — Azure Container Instances, one container group per Connector |

## Root files

- [../README.md](../README.md) — quick-start, deployment, and component overview
- [../config/config.example.yaml](../config/config.example.yaml) — annotated YAML policy template
- [../.env.example](../.env.example) — environment variable template
