"""Opt-in collector: parse the custom image's stdout metrics JSON lines.

The custom connector image writes a metrics JSON line to stdout every 60s,
sharing the stream with ordinary service logs and (when
``TWINGATE_LOG_ANALYTICS=v2``) ``ANALYTICS`` traffic lines. This collector tails
the container's logs over the Docker API — an independent read that does not
interfere with the log-shipper — classifies each line, and maps the most recent
metrics payload onto a normalized :class:`~fc.models.ResourceSample`.

It is opt-in and custom-image only: on the official image no metrics lines
appear and :meth:`StdoutMetricsCollector.collect` returns ``None``.

Normalization mirrors ``docker_stats``: the payload's ``cpu_pct`` is per single
core and unbounded, so it is divided by the container's effective core count
(from inspect) to per-effective-core utilization. Memory percent is taken
straight from the payload, which already reports ``null`` when no limit is set.
"""

import json
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog

from fc.collectors.base import (
    CollectorError,
    effective_cores_from_inspect,
    normalize_raw_cpu_pct,
)
from fc.models import CollectorSource, ManagedConnector, ResourceSample

if TYPE_CHECKING:
    import aiodocker

logger = structlog.get_logger(__name__)

# The reliable selector: lines whose JSON has this event value are metrics.
_METRICS_MARKER = "[metrics] "
_METRICS_EVENT = "metrics"

# Bounded tail so a busy connector's log volume never stalls the cycle; the
# image emits one metrics line per minute, so a few hundred lines comfortably
# covers a typical poll interval.
_LOG_TAIL = 400


def parse_metrics_line(line: str) -> dict[str, Any] | None:
    """Classify one stdout line and return its metrics payload, or ``None``.

    A line qualifies only if it contains the ``[metrics] `` marker and the JSON
    after the marker parses and carries ``"event": "metrics"``. ``ANALYTICS``
    lines, ordinary service logs, non-metrics events, and malformed JSON all
    return ``None``.

    Args:
        line: A single raw stdout line from the container.

    Returns:
        The decoded metrics object, or ``None`` if the line is not a metrics
        line.
    """
    marker = line.find(_METRICS_MARKER)
    if marker == -1:
        return None
    payload = line[marker + len(_METRICS_MARKER) :].strip()
    try:
        decoded = json.loads(payload)
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, dict) or decoded.get("event") != _METRICS_EVENT:
        return None
    return decoded


def _iter_lines(log_result: object) -> list[str]:
    """Flatten an aiodocker ``log`` result into individual non-empty lines."""
    if isinstance(log_result, str):
        chunks = [log_result]
    elif isinstance(log_result, list):
        chunks = [c for c in log_result if isinstance(c, str)]
    else:
        return []
    lines: list[str] = []
    for chunk in chunks:
        lines.extend(piece for piece in chunk.splitlines() if piece.strip())
    return lines


def _parse_payload_ts(value: object) -> datetime:
    """Parse the metrics payload's ISO-8601 ``ts``; fall back to now (UTC)."""
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            logger.debug("stdout_metrics.bad_ts", value=value)
    return datetime.now(UTC)


def _payload_throughput(payload: dict[str, Any]) -> float | None:
    """Derive bytes/sec from the payload's per-interval net deltas."""
    interval = payload.get("interval_sec")
    if not isinstance(interval, (int, float)) or interval <= 0:
        return None
    rx = payload.get("net_rx_bytes_delta", 0)
    tx = payload.get("net_tx_bytes_delta", 0)
    if not isinstance(rx, (int, float)) or not isinstance(tx, (int, float)):
        return None
    return (rx + tx) / interval


class StdoutMetricsCollector:
    """Collects samples from the custom image's stdout metrics lines.

    Stateless across cycles (each metrics line carries its own per-interval
    deltas), so no previous snapshot is retained.
    """

    source = CollectorSource.STDOUT_METRICS

    def __init__(self, docker: "aiodocker.Docker", *, host_cpus: int | None = None) -> None:
        """Build the collector.

        Args:
            docker: The shared aiodocker client for log tails and inspects.
            host_cpus: The host's online core count, used to resolve effective
                cores when a container has no CPU limit. Defaults to the
                manager host's CPU count.
        """
        self._docker = docker
        self._host_cpus = host_cpus if host_cpus and host_cpus > 0 else (os.cpu_count() or 1)

    async def collect(self, connector: ManagedConnector) -> ResourceSample | None:
        """Return the latest stdout-metrics sample for a Connector, or ``None``.

        ``None`` is returned for a logical-only Connector and when the log tail
        contains no metrics line (e.g. the official image, or none emitted yet
        this window).

        Args:
            connector: The Connector to sample.

        Returns:
            A normalized :class:`ResourceSample`, or ``None``.

        Raises:
            CollectorError: When the Docker log/inspect read fails.
        """
        if connector.container_id is None:
            return None

        try:
            container = await self._docker.containers.get(connector.container_id)
            raw_log = await container.log(stdout=True, stderr=False, tail=_LOG_TAIL)
        except Exception as exc:
            raise CollectorError(f"docker log read failed: {type(exc).__name__}") from exc

        payload = self._latest_metrics(raw_log)
        if payload is None:
            return None

        try:
            inspect = await container.show()
        except Exception as exc:
            raise CollectorError(f"docker inspect failed: {type(exc).__name__}") from exc
        effective_cores = effective_cores_from_inspect(inspect, host_cpus=self._host_cpus)

        cpu_pct_raw = payload.get("cpu_pct")
        cpu_norm = (
            normalize_raw_cpu_pct(cpu_pct=float(cpu_pct_raw), effective_cores=effective_cores)
            if isinstance(cpu_pct_raw, (int, float))
            else None
        )

        mem_bytes_raw = payload.get("mem_bytes")
        mem_bytes = int(mem_bytes_raw) if isinstance(mem_bytes_raw, (int, float)) else None
        mem_pct_raw = payload.get("mem_pct")
        mem_pct = float(mem_pct_raw) if isinstance(mem_pct_raw, (int, float)) else None

        return ResourceSample(
            connector_id=connector.connector_id,
            source=CollectorSource.STDOUT_METRICS,
            ts=_parse_payload_ts(payload.get("ts")),
            cpu_pct_norm=cpu_norm,
            mem_bytes=mem_bytes,
            mem_pct=mem_pct,
            throughput_bps=_payload_throughput(payload),
        )

    def _latest_metrics(self, raw_log: object) -> dict[str, Any] | None:
        """Return the most recent metrics payload in a raw log result."""
        latest: dict[str, Any] | None = None
        for line in _iter_lines(raw_log):
            parsed = parse_metrics_line(line)
            if parsed is not None:
                latest = parsed
        return latest
