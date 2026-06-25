"""Primary throughput collector: scrape the connector ``:9999`` metrics endpoint.

This is the truest scaling signal: ``twingate_inbound_bytes_total`` and
``twingate_outbound_bytes_total`` count the bytes Twingate is actually brokering
through the tunnel (summed across transports), which is more meaningful than
NIC-level counters. Throughput is the change in the summed byte totals divided
by the wall-clock interval since the previous scrape (retained per connector).

The endpoint requires the Connector to have been started with
``TWINGATE_METRICS_PORT`` set. When it is absent or unreachable, the collector
degrades gracefully — it logs ``collect.error`` and returns ``None`` so the
control loop falls back to a resource collector's NIC delta and never aborts
the cycle (Key Design Rule: one bad Connector never breaks a cycle).
"""

from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import structlog

from fc.collectors.base import throughput_from_totals
from fc.models import CollectorSource, ManagedConnector, ResourceSample

logger = structlog.get_logger(__name__)

# Metric names parsed from the exposition (transport label is summed over).
_INBOUND = "twingate_inbound_bytes_total"
_OUTBOUND = "twingate_outbound_bytes_total"
_UPTIME = "twingate_connector_uptime_seconds"

# Short per-target scrape timeout: a slow endpoint must not stall the cycle.
_SCRAPE_TIMEOUT = 5.0


def _utcnow() -> datetime:
    """Return the current UTC time (injectable default for the collector clock)."""
    return datetime.now(UTC)


def _metric_base_name(token: str) -> str:
    """Return the metric name without its ``{label="..."}`` suffix."""
    brace = token.find("{")
    return token[:brace] if brace != -1 else token


def parse_prometheus_metrics(text: str) -> dict[str, float | None]:
    """Parse the connector ``/metrics`` text exposition into the fields we use.

    Sums ``twingate_inbound_bytes_total`` and ``twingate_outbound_bytes_total``
    across all transport label values and reads
    ``twingate_connector_uptime_seconds``. Comment/``HELP``/``TYPE`` lines and
    unparseable samples are ignored.

    Args:
        text: The raw Prometheus text exposition body.

    Returns:
        A mapping with ``inbound_bytes`` and ``outbound_bytes`` (floats,
        defaulting to ``0``) and ``uptime_seconds`` (float or ``None`` when the
        gauge is absent).
    """
    inbound = 0.0
    outbound = 0.0
    uptime: float | None = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        # A sample line is "<name>[{labels}] <value>"; value is the last token.
        try:
            name_part, value_part = line.rsplit(maxsplit=1)
            value = float(value_part)
        except ValueError:
            continue
        base = _metric_base_name(name_part)
        if base == _INBOUND:
            inbound += value
        elif base == _OUTBOUND:
            outbound += value
        elif base == _UPTIME:
            uptime = value

    return {"inbound_bytes": inbound, "outbound_bytes": outbound, "uptime_seconds": uptime}


class PrometheusCollector:
    """Scrapes the connector's tunnel-throughput metrics over HTTP.

    Retains the previous summed byte total and scrape time per connector so
    throughput can be derived as a rate. The first scrape establishes the
    baseline and reports ``None`` throughput.
    """

    source = CollectorSource.PROMETHEUS

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        port: int,
        host_resolver: Callable[[ManagedConnector], str | None] | None = None,
        timeout: float = _SCRAPE_TIMEOUT,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        """Build the collector.

        Args:
            client: Shared :class:`httpx.AsyncClient` used for scrapes.
            port: The connector metrics port (``metrics_port`` from policy).
            host_resolver: Maps a Connector to the host/DNS name to scrape;
                defaults to the Connector's name (its container name on the
                shared Docker network). Returning ``None`` skips the scrape.
            timeout: Per-scrape timeout in seconds.
            clock: Callable returning the current time; injectable for tests.
        """
        self._client = client
        self._port = port
        self._host_resolver = host_resolver or (lambda c: c.name)
        self._timeout = timeout
        self._clock = clock
        self._prev: dict[str, tuple[int, datetime]] = {}

    async def collect(self, connector: ManagedConnector) -> ResourceSample | None:
        """Scrape one Connector's tunnel throughput, or ``None`` on degrade.

        Returns ``None`` for a logical-only Connector, an unresolvable host, or
        any scrape failure (connection error, timeout, or non-200). Failures
        are logged as ``collect.error`` and never raised.

        Args:
            connector: The Connector to scrape.

        Returns:
            A :class:`ResourceSample` carrying ``throughput_bps`` (CPU/memory
            are ``None`` — Prometheus is throughput-only), or ``None``.
        """
        if connector.container_id is None:
            return None
        host = self._host_resolver(connector)
        if not host:
            return None
        url = f"http://{host}:{self._port}/metrics"

        try:
            response = await self._client.get(url, timeout=self._timeout)
        except httpx.HTTPError as exc:
            logger.warning(
                "collect.error",
                collector=self.source.value,
                connector_id=connector.connector_id,
                error=type(exc).__name__,
            )
            return None

        if response.status_code != 200:
            logger.warning(
                "collect.error",
                collector=self.source.value,
                connector_id=connector.connector_id,
                status=response.status_code,
            )
            return None

        parsed = parse_prometheus_metrics(response.text)
        total = int((parsed["inbound_bytes"] or 0) + (parsed["outbound_bytes"] or 0))
        now = self._clock()

        prev = self._prev.get(connector.container_id)
        if prev is None:
            throughput: float | None = None
        else:
            prev_total, prev_ts = prev
            throughput = throughput_from_totals(prev_total, prev_ts, total, now)
        self._prev[connector.container_id] = (total, now)

        return ResourceSample(
            connector_id=connector.connector_id,
            source=CollectorSource.PROMETHEUS,
            ts=now,
            cpu_pct_norm=None,
            mem_bytes=None,
            mem_pct=None,
            throughput_bps=throughput,
        )
