#!/usr/bin/env bash
#
# bootstrap.sh — stand up a Fleet Commander host from scratch.
#
# Idempotent and safe to re-run. It will:
#   1. Verify (and, if missing, install) Docker Engine + the compose plugin.
#   2. Lay down .env and config/config.yaml from the committed examples.
#   3. Collect TWINGATE_NETWORK + TWINGATE_API_KEY (from the environment or by prompt).
#   4. Pull images and `docker compose up -d` (the manager + janus).
#   5. Poll the manager's /healthz and /readyz until both are green.
#
# FC self-provisions its Connectors: it brings the Remote Network up to
# `min_connectors` from empty and scales on load, minting tokens via the Twingate
# API and injecting them straight into the containers it runs. There are NO seed
# Connectors and NO connector tokens in .env.
#
# Usage:
#   ./deploy/bootstrap.sh
#   TWINGATE_NETWORK=acme TWINGATE_API_KEY=tgp_... ./deploy/bootstrap.sh
#
# Environment overrides (all optional; prompted when needed and a TTY is available):
#   TWINGATE_NETWORK   network slug for https://<slug>.twingate.com
#   TWINGATE_API_KEY   Admin/DevOps API key (FC uses it to create/delete connectors)
#   FC_HEALTH_URL      base URL to poll (default http://localhost:8080)
#   FC_WAIT_TIMEOUT    seconds to wait for health (default 180)
#   FC_TUNE_HOST       set to 1 to also apply host-global kernel/Docker tuning
#                      (deploy/tune-host.sh) for a large, busy host. Off by
#                      default so small/dev hosts are untouched. FC already stamps
#                      the per-connector limits (nofile, ports, log rotation)
#                      itself, so this is only for the host-global sysctls.

set -euo pipefail

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
ENV_FILE="${REPO_ROOT}/.env"
ENV_EXAMPLE="${REPO_ROOT}/.env.example"
CONFIG_FILE="${REPO_ROOT}/config/config.yaml"
CONFIG_EXAMPLE="${REPO_ROOT}/config/config.example.yaml"

HEALTH_URL="${FC_HEALTH_URL:-http://localhost:8080}"
WAIT_TIMEOUT="${FC_WAIT_TIMEOUT:-180}"

# ── Logging (to stderr; never echo secrets) ───────────────────────────────────
log()  { printf '\033[0;32m[bootstrap]\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[0;33m[bootstrap] WARN:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[0;31m[bootstrap] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# ── Prerequisites ──────────────────────────────────────────────────────────────
require_root_or_sudo() {
  if [ "$(id -u)" -eq 0 ]; then
    SUDO=""
  elif command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
  else
    die "need root or sudo to install packages"
  fi
}

install_docker() {
  if command -v docker >/dev/null 2>&1; then
    log "docker present: $(docker --version)"
  else
    log "docker not found — installing via get.docker.com"
    require_root_or_sudo
    local tmp
    tmp="$(mktemp)"
    curl -fsSL https://get.docker.com -o "$tmp"
    ${SUDO} sh "$tmp"
    rm -f "$tmp"
    ${SUDO} systemctl enable --now docker >/dev/null 2>&1 || true
  fi

  if docker compose version >/dev/null 2>&1; then
    log "compose present: $(docker compose version | head -n1)"
  else
    die "the Docker Compose v2 plugin is required (got: $(docker --version)). Install docker-compose-plugin and re-run."
  fi
}

# ── .env helpers ────────────────────────────────────────────────────────────
# Read the value of KEY from .env (last assignment wins, "" if unset/blank).
get_env() {
  local key="$1"
  [ -f "$ENV_FILE" ] || { printf ''; return 0; }
  grep -E "^${key}=" "$ENV_FILE" | tail -n1 | cut -d= -f2- || true
}

# Set KEY=VALUE in .env, replacing in place or appending. Re-tightens perms.
set_env() {
  local key="$1" value="$2" tmp
  tmp="$(mktemp)"
  if grep -qE "^${key}=" "$ENV_FILE"; then
    # Pass the value via ENVIRON (not `-v`, which backslash-escape-processes it),
    # so secrets containing backslashes are written verbatim.
    SET_ENV_KEY="$key" SET_ENV_VAL="$value" awk '
      BEGIN { FS = "="; k = ENVIRON["SET_ENV_KEY"] }
      $1 == k { print k "=" ENVIRON["SET_ENV_VAL"]; next }
      { print }
    ' "$ENV_FILE" >"$tmp"
  else
    cat "$ENV_FILE" >"$tmp"
    printf '%s=%s\n' "$key" "$value" >>"$tmp"
  fi
  mv "$tmp" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
}

# Prompt for a value if it is empty and we have a TTY; otherwise fail.
prompt_if_empty() {
  local current="$1" label="$2" secret="$3" answer
  if [ -n "$current" ]; then
    printf '%s' "$current"
    return 0
  fi
  if [ ! -t 0 ]; then
    die "${label} is required but not set and no TTY is available to prompt"
  fi
  if [ "$secret" = "secret" ]; then
    read -r -s -p "Enter ${label}: " answer >&2
    printf '\n' >&2
  else
    read -r -p "Enter ${label}: " answer >&2
  fi
  printf '%s' "$answer"
}

# ── Scaffolding ────────────────────────────────────────────────────────────
ensure_files() {
  if [ ! -f "$ENV_FILE" ]; then
    [ -f "$ENV_EXAMPLE" ] || die "missing ${ENV_EXAMPLE}"
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    log "created .env from .env.example"
  fi
  if [ ! -f "$CONFIG_FILE" ]; then
    [ -f "$CONFIG_EXAMPLE" ] || die "missing ${CONFIG_EXAMPLE}"
    cp "$CONFIG_EXAMPLE" "$CONFIG_FILE"
    log "created config/config.yaml from the example"
  fi
}

collect_secrets() {
  local network api_key
  network="$(prompt_if_empty "${TWINGATE_NETWORK:-$(get_env TWINGATE_NETWORK)}" "TWINGATE_NETWORK (network slug)" plain)"
  api_key="$(prompt_if_empty "${TWINGATE_API_KEY:-$(get_env TWINGATE_API_KEY)}" "TWINGATE_API_KEY" secret)"
  [ -n "$network" ] || die "TWINGATE_NETWORK is required"
  [ -n "$api_key" ] || die "TWINGATE_API_KEY is required"
  set_env TWINGATE_NETWORK "$network"
  set_env TWINGATE_API_KEY "$api_key"
  log "Twingate network: ${network} (API key set, not shown)"
  warn "set the Remote Network FC manages in config/config.yaml (remote_network_id) before relying on autoscaling"
}

# ── Bring up the stack ─────────────────────────────────────────────────────
compose_up() {
  log "pulling images"
  ( cd "$REPO_ROOT" && docker compose pull --quiet 2>/dev/null || true )
  log "starting the stack (manager + janus; FC self-provisions the connectors)"
  ( cd "$REPO_ROOT" && docker compose up -d --build )
}

wait_healthy() {
  local deadline endpoint url
  deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
  for endpoint in healthz readyz; do
    url="${HEALTH_URL}/${endpoint}"
    log "waiting for ${url} ..."
    while :; do
      if curl -fsS --max-time 5 "$url" >/dev/null 2>&1; then
        log "${endpoint} is green"
        break
      fi
      if [ "$(date +%s)" -ge "$deadline" ]; then
        warn "timed out waiting for ${url} after ${WAIT_TIMEOUT}s"
        warn "check logs with:  docker compose -f ${REPO_ROOT}/docker-compose.yml logs fc"
        return 1
      fi
      sleep 3
    done
  done
  log "manager is healthy — status UI at ${HEALTH_URL}/ (loopback by default)"
}

# ── Optional host tuning ──────────────────────────────────────────────────────
tune_host() {
  if [ "${FC_TUNE_HOST:-0}" != "1" ]; then
    return 0
  fi
  local script="${SCRIPT_DIR}/tune-host.sh"
  [ -x "$script" ] || [ -f "$script" ] || { warn "FC_TUNE_HOST=1 but ${script} not found — skipping"; return 0; }
  log "FC_TUNE_HOST=1 — applying host-global kernel/Docker tuning"
  # Runs before the stack comes up so a daemon restart (if enabled) precedes it.
  bash "$script" || warn "host tuning reported an error — continuing"
}

main() {
  log "Fleet Commander — host bootstrap"
  install_docker
  tune_host
  ensure_files
  collect_secrets
  compose_up
  wait_healthy
  log "done."
}

main "$@"
