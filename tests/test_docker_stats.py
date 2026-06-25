"""Tests for :class:`fc.collectors.docker_stats.DockerStatsCollector`.

The collector is exercised against a fake aiodocker surface (no live Docker).
Coverage: normalized CPU from cycle-over-cycle deltas (single- and multi-core),
memory with and without a limit, NIC-based throughput from network deltas, the
first-reading baseline (no delta yet), and graceful handling of a logical-only
Connector with no container.
"""

from typing import Any

from fc.collectors.docker_stats import DockerStatsCollector
from fc.models import CollectorSource, ManagedConnector

CID = "abc123"


def _connector(container_id: str | None = CID) -> ManagedConnector:
    return ManagedConnector(
        connector_id="Q29ubmVjdG9yOjE=",
        name="fc-aws-1",
        rn_id="rn-1",
        container_id=container_id,
    )


def _stats(
    *,
    read: str,
    cpu_total: int,
    system: int,
    online: int = 4,
    mem_usage: int = 512,
    mem_limit: int | None = 1024,
    rx: int = 0,
    tx: int = 0,
) -> dict[str, Any]:
    memory_stats: dict[str, Any] = {"usage": mem_usage}
    if mem_limit is not None:
        memory_stats["limit"] = mem_limit
    return {
        "read": read,
        "cpu_stats": {
            "cpu_usage": {"total_usage": cpu_total},
            "system_cpu_usage": system,
            "online_cpus": online,
        },
        "memory_stats": memory_stats,
        "networks": {"eth0": {"rx_bytes": rx, "tx_bytes": tx}},
    }


class _FakeContainer:
    def __init__(self, container_id: str, payloads: list[dict[str, Any]]) -> None:
        self.id = container_id
        self._payloads = payloads
        self._calls = 0

    async def stats(self, *, stream: bool = True) -> list[dict[str, Any]]:
        payload = self._payloads[min(self._calls, len(self._payloads) - 1)]
        self._calls += 1
        # aiodocker returns a one-element list for stream=False.
        return [payload]


class _FakeContainers:
    def __init__(self, container: _FakeContainer) -> None:
        self._container = container

    async def get(self, container_id: str, **kwargs: Any) -> _FakeContainer:
        assert container_id == self._container.id
        return self._container


class _FakeDocker:
    def __init__(self, container: _FakeContainer) -> None:
        self.containers = _FakeContainers(container)


def _docker(payloads: list[dict[str, Any]]) -> _FakeDocker:
    return _FakeDocker(_FakeContainer(CID, payloads))


async def test_first_reading_has_no_cpu_or_throughput_but_has_memory() -> None:
    docker = _docker([_stats(read="2026-06-24T12:00:00Z", cpu_total=1000, system=10_000)])
    collector = DockerStatsCollector(docker)  # type: ignore[arg-type]

    sample = await collector.collect(_connector())

    assert sample is not None
    assert sample.source is CollectorSource.DOCKER_STATS
    assert sample.cpu_pct_norm is None  # no prior reading -> no delta
    assert sample.throughput_bps is None
    assert sample.mem_bytes == 512
    assert sample.mem_pct == 50.0


async def test_second_reading_computes_normalized_cpu_multi_core() -> None:
    # cpu advances 2000, system advances 4000 -> (2000/4000)*100 = 50% per core.
    docker = _docker(
        [
            _stats(read="2026-06-24T12:00:00Z", cpu_total=1000, system=10_000),
            _stats(read="2026-06-24T12:00:30Z", cpu_total=3000, system=14_000),
        ]
    )
    collector = DockerStatsCollector(docker)  # type: ignore[arg-type]
    conn = _connector()

    await collector.collect(conn)
    sample = await collector.collect(conn)

    assert sample is not None
    assert sample.cpu_pct_norm == 50.0


async def test_second_reading_computes_normalized_cpu_single_core() -> None:
    # Full single core: cpu delta == system delta -> 100%.
    docker = _docker(
        [
            _stats(read="2026-06-24T12:00:00Z", cpu_total=0, system=0, online=1),
            _stats(read="2026-06-24T12:00:30Z", cpu_total=5000, system=5000, online=1),
        ]
    )
    collector = DockerStatsCollector(docker)  # type: ignore[arg-type]
    conn = _connector()

    await collector.collect(conn)
    sample = await collector.collect(conn)

    assert sample is not None
    assert sample.cpu_pct_norm == 100.0


async def test_throughput_from_network_deltas() -> None:
    # rx+tx advance by 30_000 bytes over 30s -> 1000 bytes/sec.
    docker = _docker(
        [
            _stats(read="2026-06-24T12:00:00Z", cpu_total=0, system=0, rx=1000, tx=2000),
            _stats(read="2026-06-24T12:00:30Z", cpu_total=0, system=1, rx=21_000, tx=12_000),
        ]
    )
    collector = DockerStatsCollector(docker)  # type: ignore[arg-type]
    conn = _connector()

    await collector.collect(conn)
    sample = await collector.collect(conn)

    assert sample is not None
    assert sample.throughput_bps == 1000.0


async def test_memory_pct_none_when_no_limit() -> None:
    docker = _docker(
        [_stats(read="2026-06-24T12:00:00Z", cpu_total=0, system=0, mem_usage=777, mem_limit=None)]
    )
    collector = DockerStatsCollector(docker)  # type: ignore[arg-type]

    sample = await collector.collect(_connector())

    assert sample is not None
    assert sample.mem_bytes == 777
    assert sample.mem_pct is None


async def test_logical_only_connector_returns_none() -> None:
    docker = _docker([_stats(read="2026-06-24T12:00:00Z", cpu_total=0, system=0)])
    collector = DockerStatsCollector(docker)  # type: ignore[arg-type]

    assert await collector.collect(_connector(container_id=None)) is None
