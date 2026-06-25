"""Tests for :class:`fc.collectors.prometheus.PrometheusCollector`.

Covers parsing the Twingate Connector ``/metrics`` text exposition (summing
inbound/outbound byte counters across transports, reading uptime), deriving
tunnel throughput as a delta across two scrapes, the first-scrape baseline, and
graceful degradation when the endpoint is missing or returns a non-200 — the
collector must return ``None`` and never raise out of a cycle.
"""

from datetime import UTC, datetime, timedelta

import httpx
import respx

from fc.collectors.prometheus import PrometheusCollector, parse_prometheus_metrics
from fc.models import CollectorSource, ManagedConnector

CID = "abc123"

EXPOSITION = """\
# HELP twingate_inbound_bytes_total Inbound tunnel bytes
# TYPE twingate_inbound_bytes_total counter
twingate_inbound_bytes_total{transport="TCP"} 1000
twingate_inbound_bytes_total{transport="UDP"} 500
# TYPE twingate_outbound_bytes_total counter
twingate_outbound_bytes_total{transport="TCP"} 2000
twingate_outbound_bytes_total{transport="UDP"} 1500
# TYPE twingate_connector_uptime_seconds gauge
twingate_connector_uptime_seconds 3600
"""


def _connector(container_id: str | None = CID, name: str = "fc-aws-1") -> ManagedConnector:
    return ManagedConnector(
        connector_id="Q29ubmVjdG9yOjE=",
        name=name,
        rn_id="rn-1",
        container_id=container_id,
    )


class _Clock:
    """A deterministic, advanceable clock for throughput-interval tests."""

    def __init__(self, start: datetime) -> None:
        self._now = start

    def advance(self, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)

    def __call__(self) -> datetime:
        return self._now


def _exposition(*, inbound: int, outbound: int) -> str:
    return (
        f'twingate_inbound_bytes_total{{transport="TCP"}} {inbound}\n'
        f'twingate_outbound_bytes_total{{transport="TCP"}} {outbound}\n'
        "twingate_connector_uptime_seconds 100\n"
    )


# --- parse_prometheus_metrics ----------------------------------------------


def test_parse_sums_transports_and_reads_uptime() -> None:
    parsed = parse_prometheus_metrics(EXPOSITION)
    assert parsed["inbound_bytes"] == 1500  # 1000 + 500
    assert parsed["outbound_bytes"] == 3500  # 2000 + 1500
    assert parsed["uptime_seconds"] == 3600.0


def test_parse_missing_metrics_default_zero_and_none() -> None:
    parsed = parse_prometheus_metrics("# just a comment\n")
    assert parsed["inbound_bytes"] == 0
    assert parsed["outbound_bytes"] == 0
    assert parsed["uptime_seconds"] is None


# --- collect ---------------------------------------------------------------


@respx.mock
async def test_first_scrape_has_no_throughput() -> None:
    respx.get("http://fc-aws-1:9999/metrics").mock(
        return_value=httpx.Response(200, text=_exposition(inbound=1000, outbound=2000))
    )
    async with httpx.AsyncClient() as client:
        collector = PrometheusCollector(
            client, port=9999, clock=_Clock(datetime(2026, 6, 24, tzinfo=UTC))
        )
        sample = await collector.collect(_connector())

    assert sample is not None
    assert sample.source is CollectorSource.PROMETHEUS
    assert sample.throughput_bps is None
    assert sample.cpu_pct_norm is None
    assert sample.mem_bytes is None


@respx.mock
async def test_second_scrape_derives_throughput() -> None:
    route = respx.get("http://fc-aws-1:9999/metrics")
    route.side_effect = [
        httpx.Response(200, text=_exposition(inbound=1000, outbound=2000)),
        httpx.Response(200, text=_exposition(inbound=6000, outbound=7000)),
    ]
    clock = _Clock(datetime(2026, 6, 24, tzinfo=UTC))
    async with httpx.AsyncClient() as client:
        collector = PrometheusCollector(client, port=9999, clock=clock)
        conn = _connector()
        await collector.collect(conn)  # baseline: total 3000
        clock.advance(10)
        sample = await collector.collect(conn)  # total 13000 -> +10000 over 10s

    assert sample is not None
    assert sample.throughput_bps == 1000.0


@respx.mock
async def test_unreachable_endpoint_degrades_to_none() -> None:
    respx.get("http://fc-aws-1:9999/metrics").mock(side_effect=httpx.ConnectError("refused"))
    async with httpx.AsyncClient() as client:
        collector = PrometheusCollector(
            client, port=9999, clock=_Clock(datetime(2026, 6, 24, tzinfo=UTC))
        )
        sample = await collector.collect(_connector())

    assert sample is None  # graceful: no exception bubbles out


@respx.mock
async def test_non_200_degrades_to_none() -> None:
    respx.get("http://fc-aws-1:9999/metrics").mock(return_value=httpx.Response(503))
    async with httpx.AsyncClient() as client:
        collector = PrometheusCollector(
            client, port=9999, clock=_Clock(datetime(2026, 6, 24, tzinfo=UTC))
        )
        assert await collector.collect(_connector()) is None


async def test_logical_only_connector_returns_none() -> None:
    async with httpx.AsyncClient() as client:
        collector = PrometheusCollector(client, port=9999)
        assert await collector.collect(_connector(container_id=None)) is None
