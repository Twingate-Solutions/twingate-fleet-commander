# Host tuning for a busy Fleet Commander host

When FC packs many Connector containers onto one Docker host pushing real
internet-bound traffic, the **host kernel** — not FC — becomes the limit for
things like connection tracking and socket buffers. This page is the standalone,
copy-pasteable version of that tuning for operators who run FC **without**
`bootstrap.sh` (e.g. a plain `docker compose up` on your own host).

> **You may not need any of this.** On a light, in-VPC, or dev host the defaults
> are fine. Apply this only when a large, busy host is approaching its network or
> connection ceilings. On AWS EC2 specifically, also see the instance-sizing and
> NIC/CPU tuning in [`platforms/ec2.md`](platforms/ec2.md).

## What FC already handles (nothing to do here)

FC stamps the **per-connector** limits it can control directly onto every
container it launches, from the `connector_*` policy keys (see
[`CONFIGURATION.md`](CONFIGURATION.md)):

| Concern | How FC handles it | Knob |
|---|---|---|
| Open file descriptors (~8 FDs/tunnel → connection ceiling) | `nofile` ulimit on the container | `connector_nofile` |
| Ephemeral source-port exhaustion (outbound connections) | `net.ipv4.ip_local_port_range` sysctl on the container | `connector_ephemeral_port_range` |
| Unbounded connector logs filling the disk | `json-file` log rotation on the container | `connector_log_max_size` / `connector_log_max_file` |

These are **per-network-namespace / per-container** settings, so FC sets them and
you don't. To change them, edit `config/config.yaml` and restart FC (new
connectors get the new value; existing ones adopt it when FC next recreates them).

## What only the host can set (this page)

The settings below are **host-global** kernel knobs — they are not
per-container-namespaced, so FC cannot stamp them. Apply them once per host.

### Option A — run the script

The repo ships an idempotent script that does everything on this page:

```bash
sudo ./deploy/tune-host.sh
# also set a host-wide Docker daemon default nofile backstop (restarts Docker):
FC_TUNE_DAEMON_ULIMITS=1 sudo ./deploy/tune-host.sh
```

(If you use `bootstrap.sh`, set `FC_TUNE_HOST=1` and it calls this for you.)

### Option B — apply the commands by hand

**1. Kernel sysctls** — connection tracking, socket buffers, backlogs, and BBR:

```bash
sudo tee /etc/sysctl.d/99-twingate-connector.conf >/dev/null <<'EOF'
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
sudo modprobe nf_conntrack
sudo sysctl --system
```

**2. conntrack hash-bucket size** — a module parameter, not a sysctl:

```bash
echo 524288 | sudo tee /sys/module/nf_conntrack/parameters/hashsize
echo "options nf_conntrack hashsize=524288" | sudo tee /etc/modprobe.d/nf_conntrack.conf  # persist
```

**3. (optional) Docker daemon default `nofile`** — a belt-and-suspenders host-wide
backstop. FC already sets `nofile` per connector, so this only affects containers
created before FC (or non-FC containers on the box):

```bash
sudo tee /etc/docker/daemon.json >/dev/null <<'EOF'
{
  "default-ulimits": {
    "nofile": { "Name": "nofile", "Soft": 131072, "Hard": 131072 }
  }
}
EOF
sudo systemctl restart docker
```

## Verifying

```bash
# host-wide FD usage: allocated / unused / max
cat /proc/sys/fs/file-nr
# a connector's effective limits and sysctls (PID 1 is the connector)
docker exec <connector> sh -c 'cat /proc/1/limits | grep "open files"'
docker exec <connector> sh -c 'cat /proc/sys/net/ipv4/ip_local_port_range'
# conntrack usage vs max
sudo conntrack -C 2>/dev/null; sysctl net.netfilter.nf_conntrack_max
```

## Not covered here (hardware / cloud-specific)

CPU frequency governor, C-state disabling, and NIC (ENA) queue/ring/IRQ tuning are
host-hardware and often cloud-specific, so they are neither in `tune-host.sh` nor
stamped by FC. For AWS EC2 they're documented in
[`platforms/ec2.md`](platforms/ec2.md) §3.2.
