#!/usr/bin/env bash
#
# bootstrap.sh — stand up a Fleet Commander host from scratch.
#
# Idempotent and safe to re-run. It will:
#   1. Verify (and, if missing, install) Docker Engine + the compose plugin and jq.
#   2. Lay down .env and config/config.yaml from the committed examples.
#   3. Collect TWINGATE_NETWORK + TWINGATE_API_KEY (from the environment or by prompt).
#   4. Mint per-connector tokens for the seed Connectors via the Twingate GraphQL API
#      (connectorCreate -> connectorGenerateTokens) and write them into .env.
#      Skipped if the tokens are already present, or entirely if FC_SKIP_SEED=1.
#   5. Pull images and `docker compose up -d`.
#   6. Poll the manager's /healthz and /readyz until both are green.
#
# Usage:
#   ./deploy/bootstrap.sh
#   TWINGATE_NETWORK=acme TWINGATE_API_KEY=tgp_... SEED_RN_ID=UmVt... ./deploy/bootstrap.sh
#   FC_SKIP_SEED=1 ./deploy/bootstrap.sh        # don't mint/start seed connectors
#
# Environment overrides (all optional; prompted when needed and a TTY is available):
#   TWINGATE_NETWORK   network slug for https://<slug>.twingate.com
#   TWINGATE_API_KEY   Admin/DevOps API key (used only to mint seed tokens; written to .env)
#   SEED_RN_ID         Remote Network id the seed connectors join
#   FC_SKIP_SEED      "1" to skip seed minting and start only the manager + janus
#   FC_HEALTH_URL     base URL to poll (default http://localhost:8080)
#   FC_WAIT_TIMEOUT   seconds to wait for health (default 180)

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

install_jq() {
  if command -v jq >/dev/null 2>&1; then
    return 0
  fi
  log "jq not found — installing"
  require_root_or_sudo
  if command -v apt-get >/dev/null 2>&1; then
    ${SUDO} apt-get update -qq && ${SUDO} apt-get install -y -qq jq
  elif command -v dnf >/dev/null 2>&1; then
    ${SUDO} dnf install -y -q jq
  elif command -v yum >/dev/null 2>&1; then
    ${SUDO} yum install -y -q jq
  elif command -v apk >/dev/null 2>&1; then
    ${SUDO} apk add --no-cache jq
  else
    die "could not install jq automatically; install it and re-run"
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
    awk -v k="$key" -v v="$value" '
      BEGIN { FS = "=" }
      $1 == k { print k "=" v; next }
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
  # Export for the GraphQL helpers below.
  TWINGATE_NETWORK="$network"
  TWINGATE_API_KEY="$api_key"
  log "Twingate network: ${TWINGATE_NETWORK} (API key set, not shown)"
}

# ── GraphQL (seed token minting) ──────────────────────────────────────────────
GQL_ENDPOINT=""

graphql() {
  # $1 = GraphQL document, $2 = JSON object of variables.
  local query="$1" variables="$2" body
  body="$(jq -n --arg q "$query" --argjson v "$variables" '{query: $q, variables: $v}')"
  curl -fsS -X POST "$GQL_ENDPOINT" \
    -H "X-API-KEY: ${TWINGATE_API_KEY}" \
    -H "Content-Type: application/json" \
    --data "$body"
}

# Find an existing connector id by name (echoes id or empty). Tolerates filter quirks.
find_connector_id() {
  local name="$1" resp
  # shellcheck disable=SC2016  # $name is a GraphQL variable, not a shell expansion
  resp="$(graphql \
    'query($name: String) { connectors(first: 100, filter: { name: { eq: $name } }) { edges { node { id name } } } }' \
    "$(jq -n --arg name "$name" '{name: $name}')" 2>/dev/null || true)"
  printf '%s' "$resp" | jq -r --arg name "$name" \
    '.data.connectors.edges[]? | select(.node.name == $name) | .node.id' 2>/dev/null | head -n1 || true
}

# Create connector $name in $SEED_RN_ID if absent; echo its id.
create_connector() {
  local name="$1" existing resp id
  existing="$(find_connector_id "$name")"
  if [ -n "$existing" ]; then
    log "reusing existing connector '${name}' (${existing})"
    printf '%s' "$existing"
    return 0
  fi
  # shellcheck disable=SC2016  # $rn/$name are GraphQL variables, not shell expansions
  resp="$(graphql \
    'mutation($rn: ID!, $name: String) { connectorCreate(remoteNetworkId: $rn, name: $name) { ok error entity { id } } }' \
    "$(jq -n --arg rn "$SEED_RN_ID" --arg name "$name" '{rn: $rn, name: $name}')")"
  if [ "$(printf '%s' "$resp" | jq -r '.data.connectorCreate.ok')" != "true" ]; then
    die "connectorCreate failed for '${name}': $(printf '%s' "$resp" | jq -r '.data.connectorCreate.error // .errors // "unknown"')"
  fi
  id="$(printf '%s' "$resp" | jq -r '.data.connectorCreate.entity.id')"
  log "created connector '${name}' (${id})"
  printf '%s' "$id"
}

# Mint tokens for connector id $1; sets ${2}_ACCESS_TOKEN / ${2}_REFRESH_TOKEN in .env.
mint_tokens() {
  local connector_id="$1" prefix="$2" resp access refresh
  # shellcheck disable=SC2016  # $id is a GraphQL variable, not a shell expansion
  resp="$(graphql \
    'mutation($id: ID!) { connectorGenerateTokens(connectorId: $id) { ok error connectorTokens { accessToken refreshToken } } }' \
    "$(jq -n --arg id "$connector_id" '{id: $id}')")"
  if [ "$(printf '%s' "$resp" | jq -r '.data.connectorGenerateTokens.ok')" != "true" ]; then
    die "connectorGenerateTokens failed for ${connector_id}: $(printf '%s' "$resp" | jq -r '.data.connectorGenerateTokens.error // "unknown"')"
  fi
  access="$(printf '%s' "$resp" | jq -r '.data.connectorGenerateTokens.connectorTokens.accessToken')"
  refresh="$(printf '%s' "$resp" | jq -r '.data.connectorGenerateTokens.connectorTokens.refreshToken')"
  [ -n "$access" ] && [ "$access" != "null" ] || die "no access token returned for ${connector_id}"
  set_env "${prefix}_ACCESS_TOKEN" "$access"
  set_env "${prefix}_REFRESH_TOKEN" "$refresh"
  log "minted tokens for ${prefix} (values written to .env, not shown)"
}

provision_seeds() {
  if [ "${FC_SKIP_SEED:-0}" = "1" ]; then
    warn "FC_SKIP_SEED=1 — skipping seed connectors; will start manager + janus only"
    return 0
  fi
  if [ -n "$(get_env SEED1_ACCESS_TOKEN)" ] && [ -n "$(get_env SEED2_ACCESS_TOKEN)" ]; then
    log "seed tokens already present in .env — skipping minting"
    return 0
  fi

  install_jq
  GQL_ENDPOINT="https://${TWINGATE_NETWORK}.twingate.com/api/graphql/"

  local rn id1 id2
  rn="$(prompt_if_empty "${SEED_RN_ID:-$(get_env SEED_RN_ID)}" "SEED_RN_ID (Remote Network id for the seed connectors)" plain)"
  [ -n "$rn" ] || die "SEED_RN_ID is required to mint seed tokens (or set FC_SKIP_SEED=1)"
  set_env SEED_RN_ID "$rn"
  SEED_RN_ID="$rn"

  log "minting seed connector tokens against ${GQL_ENDPOINT}"
  id1="$(create_connector "fc-seed-1")"
  mint_tokens "$id1" "SEED1"
  id2="$(create_connector "fc-seed-2")"
  mint_tokens "$id2" "SEED2"
}

# ── Bring up the stack ─────────────────────────────────────────────────────
compose_up() {
  log "pulling images"
  ( cd "$REPO_ROOT" && docker compose pull --quiet 2>/dev/null || true )
  log "starting the stack"
  if [ "${FC_SKIP_SEED:-0}" = "1" ]; then
    ( cd "$REPO_ROOT" && docker compose up -d --build fc janus )
  else
    ( cd "$REPO_ROOT" && docker compose up -d --build )
  fi
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
  log "manager is healthy — status UI at ${HEALTH_URL}/"
}

main() {
  log "Fleet Commander — host bootstrap"
  install_docker
  ensure_files
  collect_secrets
  provision_seeds
  compose_up
  wait_healthy
  log "done."
}

main "$@"
