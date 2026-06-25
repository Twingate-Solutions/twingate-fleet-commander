# Compose examples

The repo-root [`docker-compose.yml`](../../docker-compose.yml) is the **single, rich,
heavily-commented canonical example** (manager + janus + optional log-shipper, with the
full security/why-it's-shaped-this-way notes). The files here are **thin variants** for
specific situations — each links back to the canonical file rather than re-explaining it.

All of them assume FC's **no-seed self-provisioning** model: you start only the manager
(plus optional sidecars) and FC fills the Remote Network up to `min_connectors` and scales
on load by minting tokens via the Twingate API and injecting them into the containers it
runs. There are no seed-connector services and no `SEED*_ACCESS_TOKEN` env vars.

| File | Use case |
|---|---|
| [`minimal.yml`](minimal.yml) | "Just autoscale my Remote Network; I ship connector logs myself." `fc` only (+ socket, config, state volume). |
| [`full-with-sidecars.yml`](full-with-sidecars.yml) | `fc` + `janus` (auto-update enrolment) + `log-shipper` (behind the `shipping` profile). The everything example. |
| [`socket-proxy-hardened.yml`](socket-proxy-hardened.yml) | Keeps the raw Docker socket out of FC: a read-mostly socket proxy fronts the daemon and FC talks to it via `DOCKER_HOST=tcp://socket-proxy:2375`, allowlisting only the lifecycle calls FC needs. |
| [`desktop-dev.yml`](desktop-dev.yml) | Docker Desktop (Windows/macOS) socket-permission workaround (`group_add: ["0"]`). Layer it on top of `minimal.yml`. Dev only. |
| [`cloud-vm.yml`](cloud-vm.yml) | Unattended cloud VM: `restart: always`, loopback binding, instance-role creds. Pairs with [`../cloud-init/`](../cloud-init/). |

## Running a variant

Run from the **repo root** (the relative `build:`/volume paths resolve from there). First
lay down the config + secrets the same way as the canonical quickstart:

```bash
cp .env.example .env                                  # set TWINGATE_NETWORK + TWINGATE_API_KEY
cp config/config.example.yaml config/config.yaml      # defaults to the custom connector image

docker compose -f deploy/compose/minimal.yml up -d
# or, the full stack with the optional shipper:
docker compose -f deploy/compose/full-with-sidecars.yml --profile shipping up -d
# Docker Desktop, layered onto the minimal base:
docker compose -f deploy/compose/minimal.yml -f deploy/compose/desktop-dev.yml up -d
```

> **Validate before deploying:** `docker compose -f deploy/compose/<file>.yml config` renders
> the merged, variable-substituted config so you can eyeball it without starting anything.
