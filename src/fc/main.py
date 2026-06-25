"""Process entrypoint.

Builds every dependency from :class:`~fc.config.Settings` + the YAML
:class:`~fc.config.Policy`, then runs the control loop and the uvicorn server
together under one event loop via :func:`asyncio.gather`. A SIGINT/SIGTERM
handler flips a shared stop event so both the loop and the server drain cleanly
before the process exits (resources — the shared HTTP client and the Docker
client — are closed in a ``finally``).

This module performs real I/O (opens the Docker socket and an HTTP client) and
is exercised end-to-end by the deployment playbook rather than the unit suite;
the testable seams — the loop, the collectors, the API, the metrics — are all
constructed here but unit-tested in isolation against mocks.
"""

import asyncio
import contextlib
import signal
from pathlib import Path

import httpx
import structlog
import uvicorn

from fc.actuator.factory import build_platform
from fc.api.app import Readiness, create_app
from fc.api.routes import create_status_router
from fc.config import Policy, Settings, load_policy
from fc.engine.aggregator import Aggregator
from fc.loop import ControlLoop
from fc.observability.logging import configure_logging
from fc.observability.metrics import Metrics
from fc.state import StateStore
from fc.status import EventBuffer, StatusState
from fc.twingate.client import TwingateClient

logger = structlog.get_logger(__name__)

#: Port the in-process uvicorn server binds for UI + health + metrics.
_HTTP_PORT = 8080

#: Directory of static UI assets served at ``/static``.
_STATIC_DIR = Path(__file__).resolve().parent / "web" / "static"


def _retention_seconds(policy: Policy) -> int:
    """Pick the aggregator retention: the longest per-metric window in play.

    The aggregator must keep samples at least as long as the longest metric
    window so a metric's reduction never drops data it still needs.
    """
    return max(
        policy.scale_metrics.cpu.window_seconds,
        policy.scale_metrics.throughput.window_seconds,
    )


async def _amain() -> None:
    """Async entrypoint: build dependencies and run loop + server together."""
    settings = Settings()  # type: ignore[call-arg]  # fields come from env
    event_buffer = EventBuffer()
    configure_logging(settings.fc_log_level, extra_processors=[event_buffer.processor])
    policy = load_policy(settings.fc_config_path)
    logger.info("config.reload", path=settings.fc_config_path)

    state = StateStore(settings.fc_state_path)
    await state.init()

    http = httpx.AsyncClient()
    twingate = TwingateClient(settings.twingate_network, settings.twingate_api_key, client=http)
    # The actuator + collectors are chosen by FC_PLATFORM (docker | ecs | aci);
    # the factory wires the matching backend and (for docker) the shared inspect
    # cache. compute_probe feeds /readyz; aclose tears down any backend client.
    platform = build_platform(settings, policy, http=http)
    actuator = platform.actuator
    collectors = platform.collectors
    aggregator = Aggregator(retention_seconds=_retention_seconds(policy))
    metrics = Metrics()
    status_state = StatusState()

    loop = ControlLoop(
        policy=policy,
        twingate=twingate,
        actuator=actuator,
        collectors=collectors,
        aggregator=aggregator,
        state=state,
        metrics=metrics,
        status=status_state,
    )

    readiness = Readiness(
        docker_probe=platform.compute_probe,
        twingate_probe=twingate.list_remote_networks,
    )
    status_router = create_status_router(
        status=status_state,
        events=event_buffer,
        state=state,
        policy=policy,
        operator=loop,
        override_enabled=settings.fc_override_enabled,
        override_secret=(
            settings.fc_override_secret.get_secret_value()
            if settings.fc_override_secret is not None
            else None
        ),
    )
    app = create_app(
        metrics=metrics,
        readiness=readiness,
        status_router=status_router,
        static_dir=_STATIC_DIR,
    )

    server = uvicorn.Server(
        uvicorn.Config(app, host="0.0.0.0", port=_HTTP_PORT, log_config=None, access_log=False)
    )

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event, server)

    if settings.fc_override_enabled:
        # The override secret travels in a request header in clear text. The
        # server binds 0.0.0.0 inside the container; the loopback/TLS boundary is
        # enforced by the compose port mapping (127.0.0.1 by default). Make the
        # requirement loud so an operator who exposes the port does so knowingly.
        logger.warning(
            "manager.overrides_enabled",
            detail=(
                "manual override endpoints are ENABLED — the shared secret is sent in the "
                "X-FC-Override-Secret header; expose this port only behind a TLS proxy or "
                "over loopback, never plain HTTP on a public interface"
            ),
        )
    logger.info("manager.start", http_port=_HTTP_PORT, poll_interval=policy.poll_interval_seconds)
    try:
        await asyncio.gather(loop.run_forever(stop_event), server.serve())
    finally:
        await platform.aclose()
        await http.aclose()
        logger.info("manager.stop")


def _install_signal_handlers(stop_event: asyncio.Event, server: uvicorn.Server) -> None:
    """Wire SIGINT/SIGTERM to a graceful stop, tolerating platforms without it.

    On platforms where the event loop cannot register signal handlers (e.g.
    Windows during local development), this is a no-op and the process relies on
    the default KeyboardInterrupt path.
    """
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        stop_event.set()
        # uvicorn's Server exposes ``should_exit`` to break its serve loop.
        server.should_exit = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, AttributeError, ValueError):
            loop.add_signal_handler(sig, _request_stop)


def run() -> None:
    """Console-script entrypoint (``fc``)."""
    asyncio.run(_amain())


if __name__ == "__main__":
    run()
