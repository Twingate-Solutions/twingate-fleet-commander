"""Tests for :class:`fc.collectors.cloudwatch_logs.CloudWatchLogsCollector`.

Exercised against a fake aioboto3 session (no AWS SDK installed). Coverage:
parsing the custom image's ``[metrics]`` lines out of CloudWatch log events,
CPU normalization against the prescribed 1 vCPU task sizing, the graceful
no-log-group / not-found / no-metrics cases, and the derived log-stream name.
"""

import json
from typing import Any

from fc.collectors.cloudwatch_logs import CloudWatchLogsCollector
from fc.models import CollectorSource, ManagedConnector
from fc.platform import EcsSettings

CID = "Q29ubmVjdG9yOjE="
TASK_ARN = "arn:aws:ecs:us-east-1:123:task/fc-cluster/abcdef0123456789"


def _metrics_line(**overrides: Any) -> str:
    payload: dict[str, Any] = {
        "ts": "2026-06-24T12:00:00Z",
        "event": "metrics",
        "cpu_pct": 75.0,
        "mem_bytes": 256,
        "mem_pct": 25.0,
        "net_rx_bytes_delta": 36_000,
        "net_tx_bytes_delta": 24_000,
        "interval_sec": 60,
    }
    payload.update(overrides)
    return f"[2026-06-24 12:00:00] [metrics] {json.dumps(payload)}"


def _connector(container_id: str | None = TASK_ARN) -> ManagedConnector:
    return ManagedConnector(
        connector_id=CID, name="fc-one", rn_id="rn-1", container_id=container_id
    )


class ResourceNotFoundError(Exception):
    """Mimics botocore's CloudWatch not-found error class (name contains ResourceNotFound)."""


class _FakeLogs:
    def __init__(self, events: list[str] | None, *, raise_exc: Exception | None = None) -> None:
        self._events = events
        self._raise = raise_exc
        self.calls: list[dict[str, Any]] = []

    async def get_log_events(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self._raise is not None:
            raise self._raise
        return {"events": [{"message": m} for m in (self._events or [])]}


class _ClientCM:
    def __init__(self, client: _FakeLogs) -> None:
        self._client = client

    async def __aenter__(self) -> _FakeLogs:
        return self._client

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeSession:
    def __init__(self, logs: _FakeLogs) -> None:
        self._logs = logs

    def client(self, service: str, **kwargs: Any) -> _ClientCM:
        assert service == "logs"
        return _ClientCM(self._logs)


def _settings(**overrides: Any) -> EcsSettings:
    params: dict[str, Any] = {
        "cluster": "fc-cluster",
        "subnets": ["subnet-a"],
        "region": "us-east-1",
        "log_group": "/fc/connectors",
        "log_stream_prefix": "fc",
        "container_name": "connector",
    }
    params.update(overrides)
    return EcsSettings(**params)


def _collector(logs: _FakeLogs, **overrides: Any) -> CloudWatchLogsCollector:
    return CloudWatchLogsCollector(_FakeSession(logs), settings=_settings(**overrides))


async def test_collect_parses_metrics_and_normalizes_cpu() -> None:
    logs = _FakeLogs([_metrics_line()])
    collector = _collector(logs)

    sample = await collector.collect(_connector())

    assert sample is not None
    assert sample.source is CollectorSource.CLOUDWATCH_LOGS
    assert sample.cpu_pct_norm == 75.0  # 75 / 1 prescribed core
    assert sample.mem_bytes == 256
    assert sample.throughput_bps == 1000.0  # (36000 + 24000) / 60s
    # The stream is derived as <prefix>/<container>/<task-id>.
    assert logs.calls[0]["logStreamName"] == "fc/connector/abcdef0123456789"
    assert logs.calls[0]["logGroupName"] == "/fc/connectors"


async def test_collect_uses_latest_metrics_line() -> None:
    logs = _FakeLogs(
        [_metrics_line(cpu_pct=20.0), "[ts] [INFO] noise", _metrics_line(cpu_pct=200.0)]
    )
    collector = _collector(logs)
    sample = await collector.collect(_connector())
    assert sample is not None
    assert sample.cpu_pct_norm == 100.0  # 200 / 1 core, clamped to 100, the latest line


async def test_collect_no_log_group_returns_none() -> None:
    logs = _FakeLogs([_metrics_line()])
    collector = _collector(logs, log_group=None)
    assert await collector.collect(_connector()) is None
    assert logs.calls == []  # short-circuited before any API call


async def test_collect_taskless_connector_returns_none() -> None:
    logs = _FakeLogs([_metrics_line()])
    collector = _collector(logs)
    assert await collector.collect(_connector(container_id=None)) is None


async def test_collect_missing_stream_degrades_to_none() -> None:
    logs = _FakeLogs(None, raise_exc=ResourceNotFoundError("no such log stream"))
    collector = _collector(logs)
    # A not-found (task hasn't logged yet) degrades gracefully, not an error.
    assert await collector.collect(_connector()) is None


async def test_collect_no_metrics_line_returns_none() -> None:
    logs = _FakeLogs(["[ts] [INFO] starting", "ANALYTICS {}"])
    collector = _collector(logs)
    assert await collector.collect(_connector()) is None
