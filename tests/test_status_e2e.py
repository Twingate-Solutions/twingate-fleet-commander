"""Playwright E2E tests for the status UI (headless).

The FastAPI app is served by a real uvicorn instance in a background thread and
driven through a headless Chromium browser, exercising the actual template +
vanilla JS render path:

* the fleet view renders the published snapshot (Remote Network, counts,
  connector state) from a mocked backend, and
* with overrides enabled, clicking a scale button issues the expected
  authenticated request and the UI reflects the updated fleet.

Skipped automatically if Playwright/Chromium is unavailable so the rest of the
suite still runs.
"""

import asyncio
import socket
import threading
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import uvicorn
from fastapi import FastAPI

from fc.api.app import Readiness, create_app

if TYPE_CHECKING:
    from playwright.sync_api import Browser
from fc.api.routes import create_status_router
from fc.config import Policy
from fc.models import ScaleDirection
from fc.observability.metrics import Metrics
from fc.state import StateStore
from fc.status import (
    ConnectorStatus,
    EventBuffer,
    FleetSnapshot,
    RemoteNetworkStatus,
    StatusState,
)

playwright_api = pytest.importorskip("playwright.sync_api")

NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)
SECRET = "override-secret-abcdef"
_STATIC_DIR = Path(__file__).resolve().parents[1] / "src" / "fc" / "web" / "static"

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
    """Records override calls; scaling up grows the published snapshot."""

    def __init__(self, status: StatusState) -> None:
        self._status = status
        self.scale_calls: list[tuple[str, ScaleDirection]] = []

    async def manual_scale(self, rn_id: str, direction: ScaleDirection) -> bool:
        self.scale_calls.append((rn_id, direction))
        snap = self._status.get()
        assert snap is not None
        rn = snap.remote_networks[0]
        new_count = rn.count + (1 if direction is ScaleDirection.UP else -1)
        connectors = list(rn.connectors)
        if direction is ScaleDirection.UP:
            connectors.append(_connector(f"c{new_count}"))
        updated = rn.model_copy(update={"count": new_count, "connectors": connectors})
        self._status.publish(snap.model_copy(update={"remote_networks": [updated]}))
        return True

    async def manual_cordon(self, connector_id: str, cordoned: bool) -> bool:
        return True


def _connector(cid: str) -> ConnectorStatus:
    return ConnectorStatus(
        connector_id=cid,
        name=cid,
        twingate_state="ALIVE",
        docker_health="healthy",
        janus_locked=False,
        cordoned=False,
        cpu_pct_norm=40.0,
        throughput_bps=1_000_000.0,
        mem_bytes=None,
    )


def _snapshot(count: int = 2) -> FleetSnapshot:
    return FleetSnapshot(
        cycle_id="cyc-1",
        ts=NOW,
        remote_networks=[
            RemoteNetworkStatus(
                rn_id="rn-1",
                name="aws-prod",
                count=count,
                min_connectors=2,
                max_connectors=6,
                connectors=[_connector(f"c{i + 1}") for i in range(count)],
            )
        ],
    )


def _run_sync(coro: object) -> object:
    """Run a coroutine to completion in a dedicated thread.

    The Playwright sync API drives its own event loop in the test thread, so
    ``asyncio.run`` cannot be used there; running on a fresh thread sidesteps
    the "loop already running" conflict.
    """
    box: list[object] = []

    def _worker() -> None:
        box.append(asyncio.run(coro))  # type: ignore[arg-type]

    thread = threading.Thread(target=_worker)
    thread.start()
    thread.join()
    return box[0] if box else None


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _ServerThread:
    """Runs a uvicorn server in a daemon thread for the duration of a test."""

    def __init__(self, app: FastAPI, port: int) -> None:
        self._server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        )
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def __enter__(self) -> "_ServerThread":
        self._thread.start()
        for _ in range(100):
            if self._server.started:
                break
            time.sleep(0.05)
        else:
            raise RuntimeError("uvicorn did not start in time")
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=5)


async def _ok_probe() -> object:
    return "ok"


def _make_app(tmp_path: Path, *, override_enabled: bool) -> tuple[FastAPI, FakeOperator]:
    state = StateStore(tmp_path / "state.sqlite3")
    _run_sync(state.init())
    status = StatusState()
    status.publish(_snapshot())
    events = EventBuffer()
    events.processor(None, "info", {"event": "loop.cycle.complete", "ts": NOW.isoformat()})
    operator = FakeOperator(status)
    router = create_status_router(
        status=status,
        events=events,
        state=state,
        policy=_POLICY,
        operator=operator,
        override_enabled=override_enabled,
        override_secret=SECRET if override_enabled else None,
    )
    app = create_app(
        metrics=Metrics(),
        readiness=Readiness(docker_probe=_ok_probe, twingate_probe=_ok_probe),
        status_router=router,
        static_dir=_STATIC_DIR,
    )
    return app, operator


@pytest.fixture(scope="module")
def browser() -> Iterator["Browser"]:
    with playwright_api.sync_playwright() as pw:
        try:
            instance = pw.chromium.launch(headless=True)
        except Exception as exc:  # pragma: no cover - environment guard
            pytest.skip(f"chromium unavailable: {exc}")
        yield instance
        instance.close()


def test_status_page_renders_fleet(browser: "Browser", tmp_path: Path) -> None:
    app, _operator = _make_app(tmp_path, override_enabled=False)
    port = _free_port()
    with _ServerThread(app, port):
        page = browser.new_page()
        try:
            page.goto(f"http://127.0.0.1:{port}/", wait_until="networkidle")
            page.wait_for_selector('[data-testid="rn-rn-1"]')
            assert "aws-prod" in page.inner_text('[data-testid="rn-rn-1"]')
            assert "ALIVE" in page.inner_text('[data-testid="connector-c1"]')
            assert page.inner_text('[data-testid="count-rn-1"]').startswith("2")
            # Overrides disabled → the panel stays hidden.
            assert page.is_hidden('[data-testid="overrides-panel"]')
        finally:
            page.close()


def test_scale_override_updates_ui(browser: "Browser", tmp_path: Path) -> None:
    app, operator = _make_app(tmp_path, override_enabled=True)
    port = _free_port()
    with _ServerThread(app, port):
        page = browser.new_page()
        try:
            page.goto(f"http://127.0.0.1:{port}/", wait_until="networkidle")
            page.wait_for_selector('[data-testid="overrides-panel"]', state="visible")
            page.fill("#override-secret", SECRET)
            page.click('[data-testid="scale-up-rn-1"]')
            # The fake operator grows the snapshot; the UI refreshes to 3. Wait
            # on the new connector row appearing rather than wait_for_function:
            # the page's strict CSP (no 'unsafe-eval') blocks eval-based waits,
            # which is the intended hardening.
            page.wait_for_selector('[data-testid="connector-c3"]')
            assert page.inner_text('[data-testid="count-rn-1"]').startswith("3")
            assert operator.scale_calls == [("rn-1", ScaleDirection.UP)]
            assert page.locator('[data-testid="connector-c3"]').count() == 1
        finally:
            page.close()
