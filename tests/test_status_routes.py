"""Tests for the status API and guarded override endpoints (``fc.api.routes``).

Covers the read-only status payload (snapshot + actions + events + config), the
HTML page rendering, and the override gate: disabled by default (403), enabled
but unauthenticated (401), and enabled + correct secret (acts and audits). A
fake operator stands in for the control loop.
"""

import asyncio
from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

from fc.api.app import Readiness, create_app
from fc.api.routes import create_status_router
from fc.config import Policy
from fc.models import ActionRecord, ScaleDirection
from fc.observability.metrics import Metrics
from fc.state import StateStore
from fc.status import (
    ConnectorStatus,
    EventBuffer,
    FleetSnapshot,
    RemoteNetworkStatus,
    StatusState,
)

NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)
SECRET = "override-secret-abcdef"  # >= 16 chars

_POLICY = Policy.model_validate(
    {
        "poll_interval_seconds": 30,
        "connector_image": "twingate/connector:1",
        "metrics_port": 9999,
        "collectors": {"docker_stats": True, "stdout_metrics": False, "prometheus": True},
        "labels": {
            "managed": "twingate.fc.managed",
            "remote_network": "twingate.fc.rn",
            "connector_id": "twingate.fc.connector_id",
        },
        "janus_lock_label": "twingate.janus.upgrading",
        "defaults": {
            "min_connectors": 2,
            "max_connectors": 6,
            "scale_step": 1,
            "cpu_high_pct": 75.0,
            "cpu_low_pct": 25.0,
            "throughput_high_mbps": 80.0,
            "throughput_low_mbps": 10.0,
            "mem_ceiling_bytes": 0,
            "scale_up_window_seconds": 300,
            "scale_down_window_seconds": 1200,
            "scale_up_cooldown_seconds": 600,
            "scale_down_cooldown_seconds": 1800,
            "drain_grace_seconds": 120,
            "max_restarts": 3,
            "restart_window_seconds": 600,
        },
        "remote_networks": [{"id": "rn-1", "name": "aws-prod"}],
    }
)


class FakeOperator:
    """Records manual override calls instead of touching real infrastructure."""

    def __init__(self) -> None:
        self.scale_calls: list[tuple[str, ScaleDirection]] = []
        self.cordon_calls: list[tuple[str, bool]] = []
        self.scale_result = True

    async def manual_scale(self, rn_id: str, direction: ScaleDirection) -> bool:
        self.scale_calls.append((rn_id, direction))
        return self.scale_result

    async def manual_cordon(self, connector_id: str, cordoned: bool) -> bool:
        self.cordon_calls.append((connector_id, cordoned))
        return True


def _snapshot() -> FleetSnapshot:
    return FleetSnapshot(
        cycle_id="cyc-1",
        ts=NOW,
        remote_networks=[
            RemoteNetworkStatus(
                rn_id="rn-1",
                name="aws-prod",
                count=2,
                min_connectors=2,
                max_connectors=6,
                connectors=[
                    ConnectorStatus(
                        connector_id="c1",
                        name="c1",
                        twingate_state="ALIVE",
                        docker_health="healthy",
                        janus_locked=False,
                        cordoned=False,
                        cpu_pct_norm=42.0,
                        throughput_bps=1_000_000.0,
                        mem_bytes=None,
                    )
                ],
            )
        ],
    )


def _build_client(
    tmp_path: Path,
    *,
    override_enabled: bool = False,
    with_snapshot: bool = True,
) -> tuple[TestClient, FakeOperator, StateStore]:
    state = StateStore(tmp_path / "state.sqlite3")
    asyncio.run(state.init())
    asyncio.run(
        state.record_action(
            ActionRecord(
                ts=NOW,
                rn_id="rn-1",
                action="provision",
                count=1,
                reason="scale up",
                outcome="success",
            ),
            connector_id="c1",
        )
    )
    status = StatusState()
    if with_snapshot:
        status.publish(_snapshot())
    events = EventBuffer()
    events.processor(None, "info", {"event": "loop.cycle.complete", "ts": NOW.isoformat()})
    operator = FakeOperator()
    router = create_status_router(
        status=status,
        events=events,
        state=state,
        policy=_POLICY,
        operator=operator,
        override_enabled=override_enabled,
        override_secret=SECRET if override_enabled else None,
    )
    readiness = Readiness(docker_probe=_ok_probe, twingate_probe=_ok_probe)
    app = create_app(metrics=Metrics(), readiness=readiness, status_router=router)
    return TestClient(app), operator, state


async def _ok_probe() -> object:
    return "ok"


def test_status_api_returns_full_payload(tmp_path: Path) -> None:
    client, _operator, _state = _build_client(tmp_path)
    with client:
        resp = client.get("/api/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["snapshot"]["remote_networks"][0]["name"] == "aws-prod"
    assert body["snapshot"]["remote_networks"][0]["connectors"][0]["twingate_state"] == "ALIVE"
    assert body["actions"][0]["action"] == "provision"
    assert any(e["event"] == "loop.cycle.complete" for e in body["events"])
    assert body["config"]["poll_interval_seconds"] == 30
    assert body["overrides_enabled"] is False


def test_status_api_no_snapshot_yet(tmp_path: Path) -> None:
    client, _operator, _state = _build_client(tmp_path, with_snapshot=False)
    with client:
        resp = client.get("/api/status")
    assert resp.status_code == 200
    assert resp.json()["snapshot"] is None


def test_status_page_renders_html(tmp_path: Path) -> None:
    client, _operator, _state = _build_client(tmp_path)
    with client:
        resp = client.get("/")
    assert resp.status_code == 200
    assert "Fleet Commander" in resp.text
    assert "/static/app.js" in resp.text


def test_overrides_disabled_returns_403(tmp_path: Path) -> None:
    client, operator, _state = _build_client(tmp_path, override_enabled=False)
    with client:
        resp = client.post("/api/overrides/scale", json={"rn_id": "rn-1", "direction": "up"})
    assert resp.status_code == 403
    assert operator.scale_calls == []


def test_overrides_enabled_requires_secret(tmp_path: Path) -> None:
    client, operator, _state = _build_client(tmp_path, override_enabled=True)
    with client:
        missing = client.post("/api/overrides/scale", json={"rn_id": "rn-1", "direction": "up"})
        wrong = client.post(
            "/api/overrides/scale",
            json={"rn_id": "rn-1", "direction": "up"},
            headers={"X-FC-Override-Secret": "nope"},
        )
    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert operator.scale_calls == []


def test_override_scale_with_secret_acts(tmp_path: Path) -> None:
    client, operator, _state = _build_client(tmp_path, override_enabled=True)
    with client:
        resp = client.post(
            "/api/overrides/scale",
            json={"rn_id": "rn-1", "direction": "down"},
            headers={"X-FC-Override-Secret": SECRET},
        )
    assert resp.status_code == 200
    assert resp.json()["acted"] is True
    assert operator.scale_calls == [("rn-1", ScaleDirection.DOWN)]


def test_override_cordon_with_secret_acts(tmp_path: Path) -> None:
    client, operator, _state = _build_client(tmp_path, override_enabled=True)
    with client:
        resp = client.post(
            "/api/overrides/cordon",
            json={"connector_id": "c1", "cordoned": True},
            headers={"X-FC-Override-Secret": SECRET},
        )
    assert resp.status_code == 200
    assert operator.cordon_calls == [("c1", True)]


def test_override_scale_rejects_bad_direction(tmp_path: Path) -> None:
    client, _operator, _state = _build_client(tmp_path, override_enabled=True)
    with client:
        resp = client.post(
            "/api/overrides/scale",
            json={"rn_id": "rn-1", "direction": "sideways"},
            headers={"X-FC-Override-Secret": SECRET},
        )
    assert resp.status_code == 422  # pydantic Literal validation
