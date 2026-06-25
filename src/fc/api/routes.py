"""Status API routes and optional guarded manual-override endpoints.

Two surfaces, both mounted onto the manager's FastAPI app:

* **Read-only status** — ``GET /`` renders the Jinja2 status page and
  ``GET /api/status`` returns the same data as JSON (the page polls it): the
  single managed Remote Network's fleet view with counts against floor/ceiling,
  each Connector's Twingate state + Docker health + latest sample, the recent
  action history (from ``state.py``), a tail of recent observability events
  (from the in-memory :class:`~fc.status.EventBuffer`), and the effective config.

* **Guarded overrides** — ``POST /api/overrides/scale`` and
  ``/api/overrides/cordon``. These are **disabled by default**: when
  ``fc_override_enabled`` is false they return ``403``; when enabled they
  require a matching ``X-FC-Override-Secret`` header (constant-time compared)
  and return ``401`` otherwise. Every override is actuated through the same
  floor/ceiling- and drain-respecting paths as the autoscaler and is audited
  with ``actor=manual``.
"""

import hmac
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from fc.config import Policy
from fc.models import ScaleDirection
from fc.state import StateStore
from fc.status import EventBuffer, FleetOperator, StatusState

#: Directory holding the Jinja2 templates and static assets (``fc/web``).
_WEB_DIR = Path(__file__).resolve().parent.parent / "web"

#: Header carrying the manual-override shared secret.
_OVERRIDE_HEADER = "X-FC-Override-Secret"


class ScaleOverrideRequest(BaseModel):
    """Body for a manual scale override."""

    rn_id: str
    direction: Literal["up", "down"]


class CordonOverrideRequest(BaseModel):
    """Body for a manual cordon override."""

    connector_id: str
    cordoned: bool = True


class ReplaceOverrideRequest(BaseModel):
    """Body for a manual per-connector replace override."""

    connector_id: str


def _effective_config(policy: Policy) -> dict[str, object]:
    """Summarize the non-secret effective config for the status surface."""
    return {
        "poll_interval_seconds": policy.poll_interval_seconds,
        "connector_image": policy.connector_image,
        "collectors": policy.collectors.model_dump(),
        "remote_network": {
            "id": policy.remote_network_id,
            "name": policy.remote_network_name or policy.remote_network_id,
            "min_connectors": policy.min_connectors,
            "max_connectors": policy.max_connectors,
            "scale_metrics": policy.scale_metrics.model_dump(),
        },
    }


def create_status_router(
    *,
    status: StatusState,
    events: EventBuffer,
    state: StateStore,
    policy: Policy,
    operator: FleetOperator,
    override_enabled: bool = False,
    override_secret: str | None = None,
) -> APIRouter:
    """Build the status + override router.

    Args:
        status: Shared snapshot store published by the control loop.
        events: In-memory recent-events ring buffer.
        state: SQLite store, read for the recent action history.
        policy: The effective policy (for the config view).
        operator: The control surface (the loop) the overrides drive.
        override_enabled: Whether the override endpoints are active.
        override_secret: Shared secret required in the override header when
            overrides are enabled.

    Returns:
        The configured :class:`~fastapi.APIRouter`.
    """
    router = APIRouter()
    templates = Jinja2Templates(directory=str(_WEB_DIR / "templates"))

    async def _status_payload() -> dict[str, object]:
        snapshot = status.get()
        actions = await state.recent_actions(limit=25)
        return {
            "snapshot": snapshot.model_dump(mode="json") if snapshot is not None else None,
            "actions": [a.model_dump(mode="json") for a in actions],
            "events": events.tail(limit=40),
            "config": _effective_config(policy),
            "overrides_enabled": override_enabled,
        }

    def _check_override_auth(secret_header: str | None) -> None:
        """Raise 403 when overrides are disabled, 401 on a bad/missing secret."""
        if not override_enabled:
            raise HTTPException(status_code=403, detail="manual overrides are disabled")
        expected = override_secret or ""
        provided = secret_header or ""
        if not expected or not hmac.compare_digest(provided, expected):
            raise HTTPException(status_code=401, detail="invalid override secret")

    @router.get("/", response_class=HTMLResponse)
    async def status_page(request: Request) -> HTMLResponse:
        """Render the glanceable fleet status page."""
        payload = await _status_payload()
        return templates.TemplateResponse(request, "status.html", {"data": payload})

    @router.get("/api/status")
    async def status_api() -> JSONResponse:
        """Return the full status payload as JSON (the page polls this)."""
        return JSONResponse(await _status_payload())

    @router.post("/api/overrides/scale")
    async def override_scale(
        body: ScaleOverrideRequest,
        x_fc_override_secret: str | None = Header(default=None),
    ) -> JSONResponse:
        """Manually scale a Remote Network by one Connector (guarded)."""
        _check_override_auth(x_fc_override_secret)
        direction = ScaleDirection.UP if body.direction == "up" else ScaleDirection.DOWN
        acted = await operator.manual_scale(body.rn_id, direction)
        return JSONResponse({"acted": acted, "rn_id": body.rn_id, "direction": body.direction})

    @router.post("/api/overrides/cordon")
    async def override_cordon(
        body: CordonOverrideRequest,
        x_fc_override_secret: str | None = Header(default=None),
    ) -> JSONResponse:
        """Cordon or un-cordon a Connector (guarded)."""
        _check_override_auth(x_fc_override_secret)
        acted = await operator.manual_cordon(body.connector_id, body.cordoned)
        return JSONResponse(
            {"ok": acted, "connector_id": body.connector_id, "cordoned": body.cordoned}
        )

    @router.post("/api/overrides/replace")
    async def override_replace(
        body: ReplaceOverrideRequest,
        x_fc_override_secret: str | None = Header(default=None),
    ) -> JSONResponse:
        """Replace a Connector via the cycle-spanning net-new path (guarded).

        Provisions a net-new replacement and waits for it to become healthy
        before draining the target, so the floor is never breached. Returns
        ``acted=False`` if the Connector is not in the fleet, a replace is
        already in flight, or provisioning failed.
        """
        _check_override_auth(x_fc_override_secret)
        acted = await operator.manual_replace(body.connector_id)
        return JSONResponse({"acted": acted, "connector_id": body.connector_id})

    return router
