"""Shared parsing of the custom connector image's ``[metrics]`` log lines.

The custom connector image writes one metrics JSON line each interval — to
**stderr** as of the 2026-06 image update, keeping it off the stdout stream that
carries ``ANALYTICS`` + service logs — in the same format regardless of where it
runs. (The transport is stderr for the Docker collector and the combined
stdout+stderr cloud log streams for CloudWatch/Azure.) Three collectors consume that
format from different transports — the Docker log API
(:mod:`fc.collectors.stdout_metrics`), CloudWatch Logs
(:mod:`fc.collectors.cloudwatch_logs`), and Azure Monitor
(:mod:`fc.collectors.azure_logs`) — so the line classifier, the documented
schema mapping, and the payload → :class:`~fc.models.ResourceSample` builder all
live here, in one tested place, instead of being re-implemented per transport.

The helpers are pure (no I/O): they take a raw line or a decoded payload and an
already-resolved effective-core count, and return primitives or a
:class:`ResourceSample`. CPU normalization mirrors ``docker_stats`` — the
payload's ``cpu_pct`` is per single core and unbounded, so it is divided by the
container's effective core count to per-effective-core utilization (0..100).
"""

import json
from datetime import UTC, datetime
from typing import Any

import structlog

from fc.collectors.base import normalize_raw_cpu_pct
from fc.models import CollectorSource, ResourceSample

logger = structlog.get_logger(__name__)

#: The marker that precedes the JSON object on a metrics line.
METRICS_MARKER = "[metrics] "
#: The ``event`` value a genuine metrics line carries.
METRICS_EVENT = "metrics"

#: The custom-image metrics schema this layer maps (per the custom image's
#: ``metrics.md``). A parsed metrics line carrying none of these is schema drift
#: — the upstream payload changed and every field would map to ``None``.
KNOWN_FIELDS = frozenset(
    {
        "cpu_pct",
        "mem_bytes",
        "mem_pct",
        "net_rx_bytes_delta",
        "net_tx_bytes_delta",
        "interval_sec",
    }
)


def parse_metrics_line(line: str) -> dict[str, Any] | None:
    """Classify one log line and return its metrics payload, or ``None``.

    A line qualifies only if it contains the ``[metrics] `` marker and the JSON
    after the marker parses and carries ``"event": "metrics"``. ``ANALYTICS``
    lines, ordinary service logs, non-metrics events, and malformed JSON all
    return ``None``.

    Args:
        line: A single raw log line from the connector.

    Returns:
        The decoded metrics object, or ``None`` if the line is not a metrics
        line.
    """
    marker = line.find(METRICS_MARKER)
    if marker == -1:
        return None
    payload = line[marker + len(METRICS_MARKER) :].strip()
    try:
        decoded = json.loads(payload)
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, dict) or decoded.get("event") != METRICS_EVENT:
        return None
    return decoded


def latest_metrics_payload(lines: list[str]) -> dict[str, Any] | None:
    """Return the most recent metrics payload across a list of raw log lines."""
    latest: dict[str, Any] | None = None
    for line in lines:
        parsed = parse_metrics_line(line)
        if parsed is not None:
            latest = parsed
    return latest


def has_known_fields(payload: dict[str, Any]) -> bool:
    """Return whether a parsed metrics payload carries any mapped schema field."""
    return not KNOWN_FIELDS.isdisjoint(payload.keys())


def parse_payload_ts(value: object) -> datetime:
    """Parse the metrics payload's ISO-8601 ``ts``; fall back to now (UTC)."""
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            logger.debug("metrics_payload.bad_ts", value=value)
    return datetime.now(UTC)


def payload_throughput(payload: dict[str, Any]) -> float | None:
    """Derive bytes/sec from the payload's per-interval net deltas."""
    interval = payload.get("interval_sec")
    if not isinstance(interval, (int, float)) or interval <= 0:
        return None
    rx = payload.get("net_rx_bytes_delta", 0)
    tx = payload.get("net_tx_bytes_delta", 0)
    if not isinstance(rx, (int, float)) or not isinstance(tx, (int, float)):
        return None
    return (rx + tx) / interval


def build_sample_from_payload(
    payload: dict[str, Any],
    *,
    connector_id: str,
    effective_cores: float,
    source: CollectorSource,
) -> ResourceSample:
    """Map a decoded metrics payload onto a normalized :class:`ResourceSample`.

    Args:
        payload: A decoded metrics object (from :func:`parse_metrics_line`).
        connector_id: The logical Connector id the sample belongs to.
        effective_cores: The container/task's effective core count, used to
            normalize the raw per-single-core ``cpu_pct``.
        source: The collector source to stamp on the sample.

    Returns:
        A normalized :class:`ResourceSample`; individual fields are ``None`` when
        the payload omits them or carries an unexpected type.
    """
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
        connector_id=connector_id,
        source=source,
        ts=parse_payload_ts(payload.get("ts")),
        cpu_pct_norm=cpu_norm,
        mem_bytes=mem_bytes,
        mem_pct=mem_pct,
        throughput_bps=payload_throughput(payload),
    )
