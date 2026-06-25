"""Default collector: normalized CPU/mem/net from aiodocker container stats.

Works on both the official ``twingate/connector`` image and the custom image,
because it reads container resource counters rather than anything the connector
emits. Each cycle it takes a single-shot ``stats`` reading and computes:

* **CPU** — per-effective-core utilization (0-100) from the change in the
  container's CPU usage versus the change in total system CPU usage between this
  cycle and the previous one (the previous snapshot is retained per container).
* **memory** — usage bytes and percent (percent is ``None`` when the stats
  payload carries no limit; see Key Design Rule #8).
* **throughput** — a NIC-level bytes/sec fallback from the change in summed
  ``rx_bytes``/``tx_bytes``. This is the fallback signal; ``prometheus`` is the
  primary, tunnel-level throughput source.

Because deltas are computed cycle-over-cycle, the very first reading for a
container yields ``None`` CPU and throughput (no prior snapshot) while still
reporting memory.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog

from fc.collectors.base import (
    CollectorError,
    compute_mem_pct,
    normalize_cpu_deltas,
    throughput_from_totals,
)
from fc.models import CollectorSource, ManagedConnector, ResourceSample

if TYPE_CHECKING:
    import aiodocker

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class _Snapshot:
    """The previous cycle's CPU and network counters for one container."""

    cpu_total: int
    system_total: int
    net_total: int
    ts: datetime


def _latest_stat(stats: object) -> dict[str, Any]:
    """Normalize an aiodocker ``stats`` result to a single stat dict.

    ``stats(stream=False)`` returns a one-element list; some call paths return
    the dict directly. An empty/unknown shape is a :class:`CollectorError`.
    """
    if isinstance(stats, list):
        if not stats:
            raise CollectorError("empty stats payload")
        candidate = stats[-1]
    else:
        candidate = stats
    if not isinstance(candidate, dict):
        raise CollectorError(f"unexpected stats payload type: {type(candidate).__name__}")
    return candidate


def _parse_docker_ts(value: object) -> datetime:
    """Parse a Docker ``read`` timestamp, tolerating nanosecond precision.

    Docker stamps stats with an RFC3339 ``read`` time whose fractional seconds
    may have up to nine digits, which ``datetime.fromisoformat`` rejects. The
    fraction is truncated to microseconds. Falls back to ``now`` (UTC) on any
    parse failure so a malformed timestamp never aborts collection.
    """
    if isinstance(value, str) and value:
        text = value.replace("Z", "+00:00")
        if "." in text:
            head, _, tail = text.partition(".")
            frac = tail
            offset = ""
            for sign in ("+", "-"):
                idx = tail.find(sign)
                if idx != -1:
                    frac, offset = tail[:idx], tail[idx:]
                    break
            text = f"{head}.{frac[:6]}{offset}"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            logger.debug("docker_stats.bad_read_ts", value=value)
    return datetime.now(UTC)


def _sum_network_bytes(stat: dict[str, Any]) -> int:
    """Sum rx+tx bytes across every interface in a stats payload."""
    networks = stat.get("networks") or {}
    total = 0
    for iface in networks.values():
        if isinstance(iface, dict):
            total += int(iface.get("rx_bytes", 0)) + int(iface.get("tx_bytes", 0))
    return total


class DockerStatsCollector:
    """Collects normalized CPU/memory/network samples via aiodocker stats.

    Retains the previous reading per container id so CPU and throughput can be
    derived as deltas across the poll interval (a truer signal for scaling than
    Docker's ~1s internal window).
    """

    source = CollectorSource.DOCKER_STATS

    def __init__(self, docker: "aiodocker.Docker") -> None:
        """Build the collector.

        Args:
            docker: The shared aiodocker client used for all stats reads.
        """
        self._docker = docker
        self._prev: dict[str, _Snapshot] = {}

    async def collect(self, connector: ManagedConnector) -> ResourceSample | None:
        """Sample one Connector's container, or ``None`` if it has no container.

        Args:
            connector: The Connector to sample.

        Returns:
            A normalized :class:`ResourceSample`, or ``None`` for a
            logical-only Connector (no ``container_id``).

        Raises:
            CollectorError: When the Docker stats read fails or is malformed.
        """
        if connector.container_id is None:
            return None
        container_id = connector.container_id

        try:
            container = await self._docker.containers.get(container_id)
            raw = await container.stats(stream=False)
        except CollectorError:
            raise
        except Exception as exc:  # aiodocker raises a variety of errors
            raise CollectorError(f"docker stats read failed: {type(exc).__name__}") from exc

        stat = _latest_stat(raw)
        ts = _parse_docker_ts(stat.get("read"))

        cpu_stats = stat.get("cpu_stats") or {}
        cpu_total = int((cpu_stats.get("cpu_usage") or {}).get("total_usage", 0))
        system_total = int(cpu_stats.get("system_cpu_usage", 0))
        net_total = _sum_network_bytes(stat)

        memory_stats = stat.get("memory_stats") or {}
        mem_bytes = int(memory_stats.get("usage", 0))
        mem_limit_raw = memory_stats.get("limit")
        mem_limit = int(mem_limit_raw) if isinstance(mem_limit_raw, int) else None

        prev = self._prev.get(container_id)
        if prev is None:
            cpu_norm: float | None = None
            throughput: float | None = None
        else:
            cpu_norm = normalize_cpu_deltas(
                cpu_delta=cpu_total - prev.cpu_total,
                system_delta=system_total - prev.system_total,
            )
            throughput = throughput_from_totals(prev.net_total, prev.ts, net_total, ts)

        self._prev[container_id] = _Snapshot(
            cpu_total=cpu_total,
            system_total=system_total,
            net_total=net_total,
            ts=ts,
        )

        return ResourceSample(
            connector_id=connector.connector_id,
            source=CollectorSource.DOCKER_STATS,
            ts=ts,
            cpu_pct_norm=cpu_norm,
            mem_bytes=mem_bytes,
            mem_pct=compute_mem_pct(usage=mem_bytes, limit=mem_limit),
            throughput_bps=throughput,
        )
