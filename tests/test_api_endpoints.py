"""Tests for the health/metrics API (``fc.api.app``).

Covers liveness always-ok, readiness reflecting both dependency probes (and
flipping to 503 when either is unreachable), the readiness body exposing only
booleans (no internal detail), and the ``/metrics`` Prometheus exposition.
"""

from collections.abc import Awaitable, Callable

from fastapi.testclient import TestClient

from fc.api.app import Readiness, create_app
from fc.observability.metrics import Metrics


def _probe(ok: bool) -> Callable[[], Awaitable[object]]:
    async def probe() -> object:
        if not ok:
            raise RuntimeError("unreachable")
        return "ok"

    return probe


def _client(*, docker_ok: bool = True, twingate_ok: bool = True) -> TestClient:
    readiness = Readiness(
        docker_probe=_probe(docker_ok),
        twingate_probe=_probe(twingate_ok),
    )
    app = create_app(metrics=Metrics(), readiness=readiness)
    return TestClient(app)


def test_healthz_always_ok() -> None:
    with _client() as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readyz_ready_when_both_reachable() -> None:
    with _client(docker_ok=True, twingate_ok=True) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json() == {"ready": True, "docker": True, "twingate": True}


def test_readyz_not_ready_when_docker_unreachable() -> None:
    with _client(docker_ok=False, twingate_ok=True) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body == {"ready": False, "docker": False, "twingate": True}


def test_readyz_not_ready_when_twingate_unreachable() -> None:
    with _client(docker_ok=True, twingate_ok=False) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 503
    assert resp.json()["twingate"] is False


def test_readyz_body_exposes_only_booleans() -> None:
    with _client(docker_ok=False, twingate_ok=False) as client:
        resp = client.get("/readyz")
    # No error strings or internal detail — only the boolean reachability map.
    assert set(resp.json().keys()) == {"ready", "docker", "twingate"}
    assert all(isinstance(v, bool) for v in resp.json().values())


def test_metrics_endpoint_renders_exposition() -> None:
    metrics = Metrics()
    metrics.loop_iterations.inc()
    readiness = Readiness(docker_probe=_probe(True), twingate_probe=_probe(True))
    app = create_app(metrics=metrics, readiness=readiness)
    with TestClient(app) as client:
        resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "fc_loop_iterations_total" in resp.text


def test_security_headers_present_on_responses() -> None:
    # Every response carries the hardening headers; the CSP forbids inline/eval
    # script so a future injection regression on the status page is blocked.
    with _client() as client:
        resp = client.get("/healthz")
    csp = resp.headers["content-security-policy"]
    assert "default-src 'self'" in csp
    assert "'unsafe-inline'" not in csp and "'unsafe-eval'" not in csp
    assert resp.headers["x-content-type-options"] == "nosniff"
    assert resp.headers["x-frame-options"] == "DENY"
    assert resp.headers["referrer-policy"] == "no-referrer"
