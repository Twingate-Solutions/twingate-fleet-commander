# Cloud-init / user-data snippets

Each file here is a thin first-boot script for "a Linux box with Docker". They all
do the same three things and then hand off to [`../bootstrap.sh`](../bootstrap.sh),
which is the single source of truth for standing up the stack:

1. Install git + curl.
2. Clone (or copy) this repository to `/opt/fleet-commander`.
3. Run `deploy/bootstrap.sh` non-interactively, feeding it the Twingate network,
   API key, and seed Remote Network id via environment variables.

Because `bootstrap.sh` is idempotent, re-running any of these is safe.

## Before you boot

Provide three values to first boot (how you inject them differs per platform —
see each file). **Treat `TWINGATE_API_KEY` as a secret**: prefer your cloud's
secret store (AWS Secrets Manager / SSM, Azure Key Vault, GCP Secret Manager)
over inlining it in user-data, which is often readable from instance metadata.

| Variable | Meaning |
|---|---|
| `TWINGATE_NETWORK` | network slug for `https://<slug>.twingate.com` |
| `TWINGATE_API_KEY` | Admin/DevOps API key (used to mint seed connector tokens) |
| `SEED_RN_ID` | Remote Network id the seed connectors join |

Set `FC_SKIP_SEED=1` if you'd rather let FC provision the fleet up to the floor
itself instead of starting seed connectors from compose.

| File | Platform | How values are passed |
|---|---|---|
| [`aws-ec2.yaml`](aws-ec2.yaml) | AWS EC2 | cloud-config; pull the API key from SSM Parameter Store at boot |
| [`azure-vm.yaml`](azure-vm.yaml) | Azure VM | cloud-config; custom-data |
| [`gcp-compute.yaml`](gcp-compute.yaml) | GCP Compute Engine | cloud-config via the `user-data` metadata key |
| [`proxmox-generic.sh`](proxmox-generic.sh) | Proxmox / any plain VM | a plain `#!/usr/bin/env bash` user-data / first-boot script |

Replace `REPO_URL` in each file with this repository's clone URL (or bake the repo
into your image and drop the clone step).
