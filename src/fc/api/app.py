"""FastAPI application factory: ``/healthz``, ``/readyz``, ``/metrics``, status UI.

These endpoints make the manager observable from outside (Key Design Rule #7):
liveness, readiness, and a Prometheus exposition of the self-metrics. The status
UI and any guarded manual-override routes mount onto the same app
via :func:`create_app`'s ``status_router`` hook, so the whole surface is served
by one uvicorn process alongside the control loop.

Readiness is deliberately shallow in what it *reveals*: ``/readyz`` returns only
boolean reachability for the Docker socket and the Twingate API — never an error
message or internal detail — so probing it can't leak topology or credentials.
"""

from collections.abc import Awaitable, Callable
from pathlib import Path

import structlog
from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from fc.observability.metrics import Metrics

logger = structlog.get_logger(__name__)

#: Security headers applied to every response. The status page bootstraps from a
#: non-executable JSON ``<script type="application/json">`` block and loads its
#: logic from same-origin ``/static/app.js``, so ``default-src 'self'`` needs no
#: ``'unsafe-inline'`` — a CSP regression that introduced an inline/injected
#: script would then be blocked. The frame/referrer headers harden the privileged
#: override surface against clickjacking and referrer leakage.
_SECURITY_HEADERS: dict[str, str] = {
    "Content-Security-Policy": (
        "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; "
        "object-src 'none'; form-action 'self'"
    ),
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
}


class Readiness:
    """Probes the two external dependencies the manager must reach to act.

    A readiness probe is allowed to fail without the process being unhealthy:
    the Twingate API or Docker socket may be transiently unreachable while the
    loop keeps running and simply skips actions that cycle. The probes are
    injected so they can be pointed at the real clients in ``main`` and at fakes
    in tests.
    """

    def __init__(
        self,
        *,
        docker_probe: Callable[[], Awaitable[object]],
        twingate_probe: Callable[[], Awaitable[object]],
    ) -> None:
        """Build the readiness checker.

        Args:
            docker_probe: Awaitable that succeeds iff the Docker socket is
                reachable (e.g. ``docker.version``).
            twingate_probe: Awaitable that succeeds iff the Twingate Admin API
                is reachable.
        """
        self._docker_probe = docker_probe
        self._twingate_probe = twingate_probe

    async def _probe(self, probe: Callable[[], Awaitable[object]], name: str) -> bool:
        """Run one probe, swallowing and logging any failure as not-ready."""
        try:
            await probe()
        except Exception as exc:
            logger.warning("readiness.probe_failed", probe=name, error=type(exc).__name__)
            return False
        return True

    async def check(self) -> tuple[bool, dict[str, bool]]:
        """Return overall readiness and the per-dependency booleans.

        Returns:
            ``(ready, {"docker": bool, "twingate": bool})`` where ``ready`` is
            the logical AND of both checks. No detail beyond the booleans is
            exposed.
        """
        docker_ok = await self._probe(self._docker_probe, "docker")
        twingate_ok = await self._probe(self._twingate_probe, "twingate")
        return (docker_ok and twingate_ok), {"docker": docker_ok, "twingate": twingate_ok}


def create_app(
    *,
    metrics: Metrics,
    readiness: Readiness,
    status_router: APIRouter | None = None,
    static_dir: Path | None = None,
) -> FastAPI:
    """Build the FastAPI app serving health, metrics, and (optionally) status.

    Args:
        metrics: The manager self-metrics; rendered at ``/metrics``.
        readiness: The readiness checker backing ``/readyz``.
        status_router: Optional router carrying the status UI and override
            endpoints; mounted when provided.
        static_dir: Optional directory of static assets served at ``/static``.

    Returns:
        The configured :class:`~fastapi.FastAPI` application.
    """
    app = FastAPI(title="Fleet Commander", docs_url=None, redoc_url=None)

    @app.middleware("http")
    async def _security_headers(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Attach the standard security headers to every response."""
        response = await call_next(request)
        for header, value in _SECURITY_HEADERS.items():
            response.headers.setdefault(header, value)
        return response

    if static_dir is not None and static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        """Liveness: the process is up and the event loop is responsive."""
        return JSONResponse({"status": "ok"})

    @app.get("/readyz")
    async def readyz() -> JSONResponse:
        """Readiness: the Docker socket and the Twingate API are both reachable."""
        ready, checks = await readiness.check()
        body = {"ready": ready, **checks}
        return JSONResponse(body, status_code=200 if ready else 503)

    @app.get("/metrics")
    async def metrics_endpoint() -> Response:
        """Prometheus text exposition of the manager's self-metrics."""
        body, content_type = metrics.render()
        return Response(content=body, media_type=content_type)

    if status_router is not None:
        app.include_router(status_router)

    return app
