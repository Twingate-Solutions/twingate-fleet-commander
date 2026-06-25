"""Tests for :class:`fc.collectors.stdout_metrics.StdoutMetricsCollector`.

Covers line classification (metrics vs ANALYTICS vs ordinary service logs),
the documented JSON schema mapping, CPU normalization against the container's
effective core count (from inspect), throughput from the per-interval net
deltas, choosing the most recent metrics line, and the graceful no-metrics and
logical-only cases.
"""

import json
from typing import Any

from fc.collectors.stdout_metrics import StdoutMetricsCollector, parse_metrics_line
from fc.models import CollectorSource, ManagedConnector

CID = "abc123"


def _metrics_line(**overrides: Any) -> str:
    payload: dict[str, Any] = {
        "ts": "2026-06-24T12:00:00Z",
        "event": "metrics",
        "cgroup_v": 2,
        "iface": "eth0",
        "cpu_pct": 150.0,
        "mem_bytes": 256,
        "mem_limit_bytes": 1024,
        "mem_pct": 25.0,
        "net_rx_bytes_total": 100_000,
        "net_tx_bytes_total": 50_000,
        "net_rx_bytes_delta": 36_000,
        "net_tx_bytes_delta": 24_000,
        "interval_sec": 60,
    }
    payload.update(overrides)
    return f"[2026-06-24 12:00:00] [metrics] {json.dumps(payload)}"


def _connector(container_id: str | None = CID) -> ManagedConnector:
    return ManagedConnector(
        connector_id="Q29ubmVjdG9yOjE=",
        name="fc-aws-1",
        rn_id="rn-1",
        container_id=container_id,
    )


class _FakeContainer:
    def __init__(self, container_id: str, lines: list[str], inspect: dict[str, Any]) -> None:
        self.id = container_id
        self._lines = lines
        self._inspect = inspect

    async def log(self, *, stdout: bool = False, stderr: bool = False, **kwargs: Any) -> list[str]:
        assert stdout is True
        assert stderr is False
        return self._lines

    async def show(self, **kwargs: Any) -> dict[str, Any]:
        return self._inspect


class _FakeContainers:
    def __init__(self, container: _FakeContainer) -> None:
        self._container = container

    async def get(self, container_id: str, **kwargs: Any) -> _FakeContainer:
        return self._container


class _FakeDocker:
    def __init__(self, container: _FakeContainer) -> None:
        self.containers = _FakeContainers(container)


def _docker(lines: list[str], *, nano_cpus: int = 2_000_000_000) -> _FakeDocker:
    inspect = {"HostConfig": {"NanoCpus": nano_cpus}}
    return _FakeDocker(_FakeContainer(CID, lines, inspect))


# --- parse_metrics_line classification -------------------------------------


def test_parse_keeps_metrics_line() -> None:
    parsed = parse_metrics_line(_metrics_line())
    assert parsed is not None
    assert parsed["event"] == "metrics"
    assert parsed["cpu_pct"] == 150.0


def test_parse_skips_analytics_line() -> None:
    line = '[2026-06-24 12:00:00] ANALYTICS {"flow": "tcp", "bytes": 999}'
    assert parse_metrics_line(line) is None


def test_parse_skips_ordinary_service_log() -> None:
    line = "[2026-06-24 12:00:00] [INFO] connector started; connected to relay"
    assert parse_metrics_line(line) is None


def test_parse_skips_non_metrics_event_json() -> None:
    # Has the [metrics] marker shape spoofed but the JSON event is not metrics.
    line = '[2026-06-24 12:00:00] [metrics] {"event":"heartbeat","cpu_pct":10}'
    assert parse_metrics_line(line) is None


def test_parse_skips_malformed_json() -> None:
    line = "[2026-06-24 12:00:00] [metrics] {not valid json"
    assert parse_metrics_line(line) is None


# --- collect ---------------------------------------------------------------


async def test_collect_maps_schema_and_normalizes_cpu() -> None:
    docker = _docker([_metrics_line()], nano_cpus=2_000_000_000)  # 2 effective cores
    collector = StdoutMetricsCollector(docker, host_cpus=8)  # type: ignore[arg-type]

    sample = await collector.collect(_connector())

    assert sample is not None
    assert sample.source is CollectorSource.STDOUT_METRICS
    assert sample.cpu_pct_norm == 75.0  # 150 / 2 effective cores
    assert sample.mem_bytes == 256
    assert sample.mem_pct == 25.0
    # (36_000 + 24_000) / 60s = 1000 bytes/sec
    assert sample.throughput_bps == 1000.0


async def test_collect_mem_pct_null_passthrough() -> None:
    docker = _docker([_metrics_line(mem_limit_bytes=None, mem_pct=None)])
    collector = StdoutMetricsCollector(docker, host_cpus=8)  # type: ignore[arg-type]

    sample = await collector.collect(_connector())

    assert sample is not None
    assert sample.mem_pct is None


async def test_collect_uses_most_recent_metrics_line() -> None:
    older = _metrics_line(cpu_pct=20.0)
    service = "[2026-06-24 12:00:30] [INFO] noise"
    newer = _metrics_line(cpu_pct=200.0)
    docker = _docker([older, service, newer], nano_cpus=2_000_000_000)
    collector = StdoutMetricsCollector(docker, host_cpus=8)  # type: ignore[arg-type]

    sample = await collector.collect(_connector())

    assert sample is not None
    assert sample.cpu_pct_norm == 100.0  # 200 / 2 cores, from the latest line


async def test_collect_no_metrics_line_returns_none() -> None:
    docker = _docker(["[2026-06-24 12:00:00] [INFO] just logs", "ANALYTICS foo"])
    collector = StdoutMetricsCollector(docker, host_cpus=8)  # type: ignore[arg-type]

    assert await collector.collect(_connector()) is None


async def test_collect_logical_only_connector_returns_none() -> None:
    docker = _docker([_metrics_line()])
    collector = StdoutMetricsCollector(docker, host_cpus=8)  # type: ignore[arg-type]

    assert await collector.collect(_connector(container_id=None)) is None
