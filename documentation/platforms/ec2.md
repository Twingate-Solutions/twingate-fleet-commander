# Fleet Commander on a single large EC2 instance (Docker backend)

Fleet Commander's most native shape is its **default `docker` backend running on one
large EC2 instance**: FC mounts the local Docker socket, `docker run`s a fleet of
Connector containers on that host, and autoscales them against load. This is a
different AWS deployment than the **ECS/Fargate** backend in
[`ecs.md`](ecs.md) — here there is no ECS control plane, just one beefy box packed
with containers.

> **Which AWS path?**
> - **This doc (`FC_PLATFORM=docker` on EC2)** — one host, containers on the local
>   Docker socket. Cheapest per Gbps, simplest mental model, single point of failure.
>   Best when you want to saturate one big instance's network pipe.
> - **[`ecs.md`](ecs.md) (`FC_PLATFORM=ecs`)** — one Fargate/ECS task per Connector,
>   no host to manage or tune, spreads across the cluster's capacity. Best when you
>   want managed compute and don't want to own kernel/NIC tuning.
>
> The decision engine, policy, Twingate bookkeeping, and observability are identical
> across both — only the compute backend differs.

This guide adapts an internal EC2 sizing study to FC. The original study sized a
**manual** multi-connector deployment (hand-written compose blocks, hand-minted
tokens, `cpuset` pinning). **FC replaces all of that** — it mints its own tokens,
`docker run`s connectors dynamically (they are *not* compose-managed), and applies
the prescribed **1 vCPU / 2 GB** limit (Rule N2) via Docker `NanoCpus`/`Memory`. So
what carries over here is **instance sizing, host tuning, and monitoring** — not the
manual provisioning mechanics.

---

## 1. Why one big box — the three stacked ceilings

You hit the **lowest** of three ceilings. Understanding which one binds explains why
FC scales *horizontally* (more connectors) rather than giving each connector more
cores (Rule N2).

| # | Ceiling | Scope | Notes |
|---|---|---|---|
| 1 | **Per-connector single-thread crypto/PPS throughput** | Per connector | The Connector data plane runs on **one thread** (the 2nd only does ACL downloads / heartbeats). More cores do *nothing* for a single connector — only a faster core does. Usually **PPS-bound** (small packets hurt). This is exactly why FC caps each connector at 1 vCPU and adds *more connectors* instead. |
| 2 | **Instance internet-bound bandwidth ≈ 50% of baseline** | Whole instance | Connector→relay/client traffic crosses the Internet Gateway. **Containers do not raise this — one box = one network ceiling.** Needs a 32+ vCPU, network-optimized instance to clear the flat 5 Gbps floor. |
| 3 | **AWS single-flow cap = 5 Gbps per 5-tuple** | Per user flow | Combined with Twingate's per-user pinning, one heavy user is bounded by `min(one connector's single thread, 5 Gbps)`. No number of connectors helps a single user. |

**Why FC runs many connectors** even though they don't add bandwidth: to spread
Twingate's per-user logins across many single-thread data planes so the *aggregate*
climbs toward ceiling #2, and to absorb the fact that Twingate's login-time balancing
is approximate — the hottest connector runs well above the mean. FC's
`scale_up_trigger` (`any`/`mean`/`quorum`) is the knob that decides how that skew
turns into a scale-up (see the README's *sticky-connector* section).

> ⚠️ **Ceiling #2 is invisible to FC.** FC's collectors see per-connector CPU and
> tunnel throughput — **not** the host's ENA bandwidth allowance. As you approach the
> instance network cap, FC will keep reading connectors as "busy" and keep scaling
> toward `max_connectors`, but aggregate throughput won't climb because the pipe is
> full. **You must watch the ENA allowance metrics in [§5](#5-monitoring-the-ceiling-fc-cant-see) yourself** — they are the
> only signal that says "stop adding connectors, you've hit the box."

---

## 2. Picking the instance

Internet-bound throughput ≈ `max(50% × baseline, 5 Gbps)` for 32+ vCPU instances; a
flat 5 Gbps for smaller ones. Pricing is **approximate** (us-east-1, On-Demand,
Linux) — verify in the AWS calculator.

| Instance | vCPU (phys cores) | RAM | Processor / clock | Baseline | Internet-bound (~50%) | Meets 10 Gbps internet? | ~$/hr | Notes |
|---|---|---|---|---|---|---|---|---|
| c6i.8xlarge | 32 (16) | 64 GB | Ice Lake, 3.5 GHz turbo | 12.5 Gbps | ~6.25 Gbps | ❌ (in-VPC only) | ~$1.36 | Great cores, but network caps it under target. Fine if traffic is **in-VPC**, not internet. |
| m5.8xlarge | 32 (16) | 128 GB | Skylake/Cascade ~3.1 GHz | 10 Gbps | ~5 Gbps | ❌ (in-VPC only) | ~$1.54 | Slow cores, sub-target. Not recommended. |
| c5.9xlarge | 36 (18) | 72 GB | Skylake ~3.0/3.5 GHz | 12 Gbps | ~6 Gbps | ❌ (in-VPC only) | ~$1.53 | Same story with a touch more network. |
| **c5n.9xlarge** | 36 (18) | 96 GB | Skylake ~3.0/3.5 GHz | 50 Gbps | **~25 Gbps** | ✅ | ~$1.94 | Meets target with headroom, but older/slower core than c6in. |
| **c6in.8xlarge** ⭐ | 32 (16) | 64 GB | **Ice Lake, 3.5 GHz turbo** | 50 Gbps | **~25 Gbps** | ✅ | ~$1.81 | **Recommended default.** Fast cores + network-optimized; 2.5× headroom over 10 Gbps for skew. |
| m5.16xlarge | 64 (32) | 256 GB | Skylake/Cascade ~3.1 GHz | 20 Gbps | **~10 Gbps** | ✅ (barely) | ~$3.07 | Just meets target; slow cores, 256 GB you won't use, ~2× the cost. Avoid. |
| c6in.16xlarge | 64 (32) | 128 GB | Ice Lake, 3.5 GHz turbo | 100 Gbps | **~50 Gbps** | ✅ (large headroom) | ~$3.63 | If you need >25 Gbps aggregate or more cores for many connectors. |

**Recommendation: `c6in.8xlarge`.** Highest sustained clock in the set (the
single-thread data plane loves it), network-optimized so the internet-bound cap lands
at ~25 Gbps (2.5× a 10 Gbps target → comfortable skew headroom), 16 fast physical
cores, ~40% cheaper than m5.16xlarge. Step up to **`c6in.16xlarge`** only if you need
more aggregate throughput or more cores. Use **x86 / non-Graviton** — the study
benchmarked against x86; arm64 sizing is not characterized here.

> **Memory is not the constraint.** At FC's 2 GB-per-connector limit, 64 GB holds
> ~14 connectors' worth of cap with room for the host, FC, and janus — far more RAM
> headroom than the network ceiling will ever let you use. Don't pay for the 128–256
> GB tiers for this workload.

---

## 3. Host preparation

`bootstrap.sh` installs Docker and brings the stack up on a bare Amazon Linux 2023 /
Ubuntu AMI with **no host tuning** — that is fine for light or in-VPC loads. The
tuning below matters only when you are pushing a real internet-bound host toward its
network ceiling. **Everything in [§3.2](#32-cpu--nic-tuning-advanced-optional) is optional.**

### 3.1 Connection-capacity limits (mostly handled by FC)

Three limits cap how many connections a connector can carry — and none of them are
visible to FC's CPU/throughput scale triggers (a connector out of FDs, ports, or
disk keeps looking "not busy" while it refuses connections). **FC now stamps all
three itself** on every connector it provisions, from `connector_*` policy keys
(see [`../CONFIGURATION.md`](../CONFIGURATION.md)), so on the Docker backend there is
nothing you *must* do here:

| Limit | Why it caps connections | Policy key (default) |
|---|---|---|
| Open file descriptors | ~8 FDs per client tunnel | `connector_nofile` (`131072` ≈ 16k tunnels) |
| Ephemeral source ports | outbound flows exhaust the ~28k default range | `connector_ephemeral_port_range` (`"10240 65535"`) |
| Log disk usage | always-on ANALYTICS logs grow unbounded | `connector_log_max_size` / `connector_log_max_file` (`20m` × `5`) |

Tune any of them by editing `config/config.yaml` and restarting FC (new connectors
get the new value; existing ones adopt it when FC next recreates them).

**Host-global** kernel knobs that FC *can't* stamp per-connector — connection
tracking (`nf_conntrack_max`), socket buffers, backlogs, BBR, and the system-wide FD
ceilings — still matter on a busy box and are the biggest lever after the
per-connector limits above. They live in one place: **[`../host-tuning.md`](../host-tuning.md)**
(run `sudo ./deploy/tune-host.sh`, or `FC_TUNE_HOST=1 ./deploy/bootstrap.sh`, or apply
the commands by hand). Verify per connector:

```bash
docker exec <connector> sh -c 'cat /proc/1/limits | grep "open files"'
docker exec <connector> sh -c 'cat /proc/sys/net/ipv4/ip_local_port_range'
cat /proc/sys/fs/file-nr          # allocated / unused / max (host-wide)
```

> **conntrack bites harder than FDs** at high PPS with many flows — the host tuning
> in [`../host-tuning.md`](../host-tuning.md) matters as much as the per-connector limits.

### 3.2 CPU & NIC tuning (advanced, optional)

The data plane is single-threaded, so keeping cores at max clock and NIC interrupts
off the hot path is where the real single-thread wins are. Apply these only when
benchmarking shows CPU (not the network cap) is your binding ceiling.

**Force the performance governor** (stop cores clocking down):

```bash
sudo apt-get install -y linux-tools-$(uname -r) || sudo yum install -y kernel-tools
sudo cpupower frequency-set -g performance
```

**Disable deep C-states** for consistent turbo (edit `/etc/default/grub`, append to
`GRUB_CMDLINE_LINUX`: `intel_idle.max_cstate=1 processor.max_cstate=1`, regenerate
grub, reboot).

**ENA queue + ring tuning** (align queues to cores, deepen rings, keep offloads on):

```bash
sudo ethtool -L eth0 combined 16          # match physical core count (16 on c6in.8xlarge)
sudo ethtool -G eth0 rx 8192 tx 8192      # maximize ring buffers
sudo ethtool -k eth0 | egrep 'tso|gso|gro'  # offloads should stay ON
```

**IRQ affinity** — keep NIC interrupts off the connector cores so crypto isn't
preempted:

```bash
sudo systemctl disable --now irqbalance
# then pin ENA IRQs to a couple of housekeeping cores via /proc/irq/<n>/smp_affinity_list
```

> **Skipped on purpose: `cpuset` pinning and NUMA.** The source study pinned each
> connector to a physical core's SMT siblings. **FC does not pin** — it sets
> `NanoCpus=1` and lets the scheduler place containers. For the recommended
> single-socket `c6in.8xlarge` the network cap (~25 Gbps) binds well before SMT
> contention matters, so pinning buys little. If you deliberately choose a 2-socket
> box (e.g. m5.16xlarge), be aware FC has no NUMA affinity control today.

---

## 4. Sizing the fleet — set `max_connectors`, don't hand-count

The source study computed a fixed connector count. **FC autoscales that count for
you** against the CPU/throughput watermarks — you do not hand-size it. What you *do*
set is the ceiling, and to set it sensibly you need one number the study also calls
for: **measured per-connector throughput** (it is not published; it's clock- and
packet-mix-dependent).

1. Bring FC up with `min_connectors` (e.g. 2) on your chosen instance.
2. Drive load through a tunnel with `iperf3` — run **both** a large-packet (1500/9001
   MTU) profile and a small-packet profile (`-l 64`) to expose the PPS limit, not
   just best-case Gbps. Watch the connector's CPU in FC's status UI / `docker stats`
   and the ENA allowance metrics (§5).
3. `per_connector_Gbps` = the aggregate at which one connector's hot thread saturates.
4. Set the ceiling to cover the target plus skew, but never above the network cap:

   ```
   max_connectors ≈ (target aggregate Gbps ÷ per_connector_Gbps) × skew (1.3–1.5)
   ```

   Then sanity-check it lands **under** the instance internet-bound cap (§2). If the
   arithmetic wants more connectors than the network cap can feed, the box — not the
   config — is your limit; see [§6](#6-high-availability--the-single-host-limit).

Provision the ceiling for the **hottest** connector, not the mean — Twingate's
per-login split is approximate, so the busiest connector can run 1.5–2× average, and
FC's `quorum`/`any` triggers exist precisely to react to that skew.

---

## 5. Monitoring the ceiling FC can't see

FC's own `/metrics` cover the fleet (`fc_connectors`, `fc_scale_actions_total`,
`hot_connector_max`, cycle heartbeat — see [`../OBSERVABILITY.md`](../OBSERVABILITY.md)).
On a big EC2 box you must **pair them with the host's ENA allowance counters**, which
are the only signal for ceiling #2. AWS exposes **no CloudWatch metric** for these by
default — read them via `ethtool -S eth0` or the CloudWatch agent:

| ENA metric | Meaning | What to do |
|---|---|---|
| `bw_out_allowance_exceeded` / `bw_in_allowance_exceeded` | Hit the instance **bandwidth** cap | You've reached the box's wall — more connectors won't help; scale to a bigger instance or a second host |
| `pps_allowance_exceeded` | Hit the **packets-per-second** cap (common for the single-thread data plane) | Same wall, PPS-bound; jumbo frames on the in-VPC side (below) can relieve it |
| `conntrack_allowance_exceeded` | Connection-tracking table full | Raise `nf_conntrack_max` (§3.1) |
| `linklocal_allowance_exceeded` | Throttling to metadata/DNS/NTP | Usually a DNS/metadata storm — investigate, don't scale |

**The decision rule:** if FC shows connectors hot **and** ENA shows
`bw_out_allowance_exceeded`/`pps_allowance_exceeded`, adding connectors is futile —
you are network-bound, not capacity-bound. If connectors are hot and ENA is clean,
FC's scale-up is doing the right thing.

> **Jumbo frames (in-VPC side only).** Fewer packets for the same bytes directly
> relieves the PPS-bound single thread. The VPC supports MTU 9001; the **internet
> side stays 1500**. Mind Twingate's encapsulation overhead and test before
> committing: `sudo ip link set dev eth0 mtu 9001`.

---

## 6. High availability — the single-host limit

**A single FC host is a single point of failure and a single network ceiling**, and
FC does not lift either today:

- **One box = one network cap.** ~25 Gbps internet-bound on `c6in.8xlarge` is a hard
  wall no number of connectors can exceed.
- **You cannot split one Remote Network across two FC hosts.** Rule **N1** is *one FC
  instance : one Remote Network*, and connectors within a Remote Network auto-cluster.
  Running two FC hosts against the **same** Remote Network would have them fight over
  the same fleet (each discovering and acting on connectors it didn't create). FC has
  no leader election or cross-host coordination.

So the source study's "split connectors across ≥2 AZs for HA + 2× ceiling" pattern
**does not map onto FC as-is** for a single Remote Network. Your options today:

- **Accept the single-host tradeoff** — simplest; the box is a control-plane node you
  monitor and can rebuild from `bootstrap.sh` quickly (connectors keep serving across
  an FC restart; only autoscaling pauses while FC is down).
- **Use separate Remote Networks per host** — run one FC per box, each 1:1 with its
  own Remote Network, and split resources/traffic across those Remote Networks at the
  Twingate policy layer. This gives AZ redundancy and doubles the aggregate ceiling,
  but it is a *topology* decision (two independent fleets), not FC-level HA.
- **Use the ECS backend** ([`ecs.md`](ecs.md)) — Fargate spreads tasks across the
  cluster's AZs, sidestepping the single-host ceiling, at the cost of managed-compute
  pricing and no host-level tuning.

---

## 7. Quick-start checklist

- [ ] Launch **`c6in.8xlarge`** (x86), Amazon Linux 2023 or Ubuntu, in a public
      subnet (or with a NAT path for internet-bound traffic).
- [ ] *(optional, busy host)* Apply host-global kernel tuning — `sudo ./deploy/tune-host.sh` or `FC_TUNE_HOST=1 ./deploy/bootstrap.sh` (see [`../host-tuning.md`](../host-tuning.md)).
- [ ] Run [`deploy/cloud-init/aws-ec2.yaml`](../../deploy/cloud-init/aws-ec2.yaml) as user-data, or `deploy/bootstrap.sh` by hand (API key from SSM, **not** inlined).
- [ ] Set `remote_network_id` in `config/config.yaml`; leave `FC_PLATFORM=docker` (the default). FC stamps the per-connector FD limit itself (`connector_nofile`, default 131072) — raise it here if a connector will carry >~16k tunnels (§3.1).
- [ ] *(optional, advanced)* `cpupower frequency-set -g performance`, disable deep C-states, tune ENA queues/rings/IRQs (§3.2).
- [ ] Benchmark **one** connector (large + small packet) → set `max_connectors` = count × skew, under the network cap (§4).
- [ ] Wire ENA allowance metrics into CloudWatch **alongside** FC's `/metrics` (§5).
- [ ] Decide the HA posture (§6) — a single host is a single point of failure by design.
