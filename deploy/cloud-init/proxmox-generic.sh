#!/usr/bin/env bash
#
# Proxmox / generic-VM first-boot bootstrap for Fleet Commander.
#
# Use this on any plain Linux VM that doesn't use cloud-config YAML (e.g. a Proxmox
# VM/CT). Drop it in as a user-data / first-boot script, or run it by hand once.
# It is a thin wrapper that clones the repo and hands off to deploy/bootstrap.sh
# (which installs Docker and brings up the stack). Idempotent — safe to re-run.
#
# Provide secrets via the environment (don't hardcode the API key in an image):
#   TWINGATE_NETWORK=acme \
#   TWINGATE_API_KEY=tgp_xxxxxxxx \
#   ./proxmox-generic.sh
#
# FC self-provisions its Connectors; set the Remote Network it manages in
# config/config.yaml (remote_network_id). There are no seed connectors.

set -euo pipefail

REPO_URL="${REPO_URL:-REPO_URL}"   # set to this repo's clone URL, or bake the repo into the image
INSTALL_DIR="${INSTALL_DIR:-/opt/fleet-commander}"

# --- minimal prerequisites (bootstrap.sh handles Docker itself) ---
if ! command -v git >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update && apt-get install -y git curl
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y git curl
  else
    echo "install git + curl, then re-run" >&2
    exit 1
  fi
fi

# --- get the code ---
if [ -d "${INSTALL_DIR}/.git" ]; then
  git -C "${INSTALL_DIR}" pull --ff-only || true
elif [ ! -d "${INSTALL_DIR}" ]; then
  git clone "${REPO_URL}" "${INSTALL_DIR}"
fi

# --- hand off to the real bootstrap (passes through TWINGATE_NETWORK / TWINGATE_API_KEY) ---
exec "${INSTALL_DIR}/deploy/bootstrap.sh"
