#!/usr/bin/env bash
#
# tune-host.sh — apply host-level kernel/Docker tuning for a Fleet Commander host
# that runs many Connector containers at high connection/throughput.
#
# WHAT THIS DOES (and does NOT do):
#   FC already stamps the *per-connector* limits it can control directly onto the
#   containers it launches — the ``nofile`` ulimit, the ephemeral source-port
#   range, and json-file log rotation (see the connector_* keys in config.yaml).
#   Those need nothing here. This script sets the *host-global* kernel knobs that
#   are NOT per-container-namespaced and therefore cannot be stamped by FC:
#   connection-tracking table size, socket buffers, backlogs, and BBR/fq. It also
#   optionally sets a Docker daemon default nofile as a belt-and-suspenders
#   backstop for anything created before FC stamps its own.
#
# It is idempotent and safe to re-run. It is OPT-IN: bootstrap.sh only calls it
# when FC_TUNE_HOST=1. For a compose-only host (no bootstrap), run it directly,
# or copy the equivalent commands from documentation/host-tuning.md.
#
# Usage:
#   sudo ./deploy/tune-host.sh                 # sysctls + conntrack
#   FC_TUNE_DAEMON_ULIMITS=1 sudo ./deploy/tune-host.sh   # also daemon nofile
#
# This tuning suits a large, busy, internet-bound host. It is unnecessary (and
# the socket-buffer sizes are wasteful) on small or dev hosts.

set -euo pipefail

SYSCTL_FILE="/etc/sysctl.d/99-twingate-connector.conf"
CONNTRACK_HASHSIZE="${FC_CONNTRACK_HASHSIZE:-524288}"
DAEMON_JSON="/etc/docker/daemon.json"
DEFAULT_NOFILE="${FC_DEFAULT_NOFILE:-131072}"

log()  { printf '\033[0;32m[tune-host]\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[0;33m[tune-host] WARN:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[0;31m[tune-host] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

require_root_or_sudo() {
  if [ "$(id -u)" -eq 0 ]; then
    SUDO=""
  elif command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
  else
    die "need root or sudo to write kernel/Docker config"
  fi
}

write_sysctls() {
  log "writing ${SYSCTL_FILE}"
  ${SUDO} tee "${SYSCTL_FILE}" >/dev/null <<'EOF'
# Fleet Commander host tuning (host-global; per-connector limits are stamped by FC).
# ---- File descriptors (system-wide ceilings; must be >= container nofile) ----
fs.file-max = 4194304
fs.nr_open  = 2097152

# ---- Connection tracking (fills at high PPS with many flows) ----
net.netfilter.nf_conntrack_max = 2097152

# ---- Socket buffers (high bandwidth-delay product over internet paths) ----
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
net.ipv4.tcp_rmem = 4096 87380 134217728
net.ipv4.tcp_wmem = 4096 65536 134217728

# ---- Backlogs ----
net.core.netdev_max_backlog = 250000
net.core.somaxconn = 65535

# ---- Throughput over lossy/latent internet links ----
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
EOF
  # nf_conntrack must be loaded for its sysctl to exist; load + persist.
  ${SUDO} modprobe nf_conntrack 2>/dev/null || warn "could not modprobe nf_conntrack (may be built-in)"
  log "applying sysctls"
  ${SUDO} sysctl --system >/dev/null
}

set_conntrack_hashsize() {
  local path="/sys/module/nf_conntrack/parameters/hashsize"
  if [ -w "$path" ] || [ "$(id -u)" -eq 0 ] || command -v sudo >/dev/null 2>&1; then
    log "setting nf_conntrack hashsize=${CONNTRACK_HASHSIZE}"
    echo "${CONNTRACK_HASHSIZE}" | ${SUDO} tee "$path" >/dev/null 2>&1 \
      || warn "could not set conntrack hashsize now (module may not be loaded yet)"
    echo "options nf_conntrack hashsize=${CONNTRACK_HASHSIZE}" \
      | ${SUDO} tee /etc/modprobe.d/nf_conntrack.conf >/dev/null
  fi
}

set_daemon_ulimits() {
  # Optional belt-and-suspenders host-wide default. FC stamps nofile per connector
  # already, so this only matters for containers created before FC, or non-FC ones.
  if [ "${FC_TUNE_DAEMON_ULIMITS:-0}" != "1" ]; then
    return 0
  fi
  if [ -f "$DAEMON_JSON" ]; then
    warn "${DAEMON_JSON} already exists — not overwriting; merge default-ulimits.nofile=${DEFAULT_NOFILE} manually"
    return 0
  fi
  log "writing ${DAEMON_JSON} with default nofile=${DEFAULT_NOFILE}"
  ${SUDO} mkdir -p /etc/docker
  ${SUDO} tee "$DAEMON_JSON" >/dev/null <<EOF
{
  "default-ulimits": {
    "nofile": { "Name": "nofile", "Soft": ${DEFAULT_NOFILE}, "Hard": ${DEFAULT_NOFILE} }
  }
}
EOF
  log "restarting docker to apply daemon defaults"
  ${SUDO} systemctl restart docker || warn "could not restart docker — restart it manually"
}

main() {
  log "Fleet Commander — host tuning (host-global kernel/Docker knobs)"
  require_root_or_sudo
  write_sysctls
  set_conntrack_hashsize
  set_daemon_ulimits
  log "done. NOTE: CPU governor, C-states, and NIC/IRQ tuning are hardware/host"
  log "specific and are NOT applied here — see documentation/platforms/ec2.md §3.2."
}

main "$@"
