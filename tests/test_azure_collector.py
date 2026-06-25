"""Tests for :class:`fc.collectors.azure_logs.AzureLogsCollector`.

Exercised against an ``httpx.MockTransport`` (no Azure SDK installed) and a stub
token provider. Coverage: parsing the custom image's ``[metrics]`` lines out of a
Log Analytics query response, CPU normalization against the prescribed 1 vCPU
sizing, the bearer auth + KQL query, and the graceful no-workspace / 404 / no-
metrics cases.
"""

import json
from collections.abc import Callable
from typing import Any

import httpx

from fc.collectors.azure_logs import AzureLogsCollector
from fc.models import CollectorSource, ManagedConnector
from fc.platform import AciSettings

CID = "Q29ubmVjdG9yOjE="
GROUP = "fc-abc123"
WORKSPACE = "ws-guid"


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


def _connector(container_id: str | None = GROUP) -> ManagedConnector:
    return ManagedConnector(connector_id=CID, name=GROUP, rn_id="rn-1", container_id=container_id)


def _table(messages: list[str]) -> dict[str, Any]:
    return {
        "tables": [
            {
                "columns": [{"name": "Message", "type": "string"}],
                "rows": [[m] for m in messages],
            }
        ]
    }


async def _token_provider(scope: str) -> str:
    return "fake-bearer"


_Handler = Callable[[httpx.Request], httpx.Response]


class _Recorder:
    def __init__(self, handler: _Handler) -> None:
        self.requests: list[httpx.Request] = []
        self._handler = handler

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._handler(request)


def _client(handler: _Handler) -> tuple[httpx.AsyncClient, _Recorder]:
    recorder = _Recorder(handler)
    return httpx.AsyncClient(transport=httpx.MockTransport(recorder)), recorder


def _settings(**overrides: Any) -> AciSettings:
    params: dict[str, Any] = {
        "subscription_id": "sub-123",
        "resource_group": "fc-rg",
        "region": "eastus",
        "log_analytics_workspace_id": WORKSPACE,
    }
    params.update(overrides)
    return AciSettings(**params)


def _collector(http: httpx.AsyncClient, **overrides: Any) -> AzureLogsCollector:
    return AzureLogsCollector(http, _token_provider, settings=_settings(**overrides))


async def test_collect_parses_metrics_and_normalizes_cpu() -> None:
    # Rows arrive newest-first; the first metrics line is the most recent.
    http, rec = _client(lambda r: httpx.Response(200, json=_table([_metrics_line()])))
    collector = _collector(http)

    sample = await collector.collect(_connector())

    assert sample is not None
    assert sample.source is CollectorSource.AZURE_MONITOR
    assert sample.cpu_pct_norm == 75.0  # 75 / 1 prescribed core
    assert sample.throughput_bps == 1000.0

    req = rec.requests[0]
    assert req.method == "POST"
    assert req.url.path.endswith(f"/workspaces/{WORKSPACE}/query")
    assert req.headers["Authorization"] == "Bearer fake-bearer"
    query = json.loads(req.content)["query"]
    assert GROUP in query
    assert "ContainerInstanceLog_CL" in query


async def test_collect_takes_first_metrics_row_newest_first() -> None:
    rows = [_metrics_line(cpu_pct=200.0), "[ts] [INFO] noise", _metrics_line(cpu_pct=20.0)]
    http, _ = _client(lambda r: httpx.Response(200, json=_table(rows)))
    collector = _collector(http)
    sample = await collector.collect(_connector())
    assert sample is not None
    assert sample.cpu_pct_norm == 100.0  # 200 / 1 core, clamped to 100, the newest (first) row


async def test_collect_no_workspace_returns_none() -> None:
    http, rec = _client(lambda r: httpx.Response(200, json=_table([_metrics_line()])))
    collector = _collector(http, log_analytics_workspace_id=None)
    assert await collector.collect(_connector()) is None
    assert rec.requests == []  # short-circuited before any query


async def test_collect_groupless_connector_returns_none() -> None:
    http, _ = _client(lambda r: httpx.Response(200, json=_table([_metrics_line()])))
    collector = _collector(http)
    assert await collector.collect(_connector(container_id=None)) is None


async def test_collect_404_degrades_to_none() -> None:
    http, _ = _client(lambda r: httpx.Response(404))
    collector = _collector(http)
    assert await collector.collect(_connector()) is None


async def test_collect_no_metrics_rows_returns_none() -> None:
    http, _ = _client(lambda r: httpx.Response(200, json=_table(["[ts] [INFO] just logs"])))
    collector = _collector(http)
    assert await collector.collect(_connector()) is None
