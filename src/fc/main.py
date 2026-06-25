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

import aiodocker
import httpx
import structlog
import uvicorn

from fc.actuator.docker_actuator import DockerActuator
from fc.api.app import Readiness, create_app
from fc.api.routes import create_status_router
from fc.collectors.base import Collector
from fc.collectors.docker_stats import DockerStatsCollector
from fc.collectors.prometheus import PrometheusCollector
from fc.collectors.stdout_metrics import StdoutMetricsCollector
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
    """Pick the aggregator retention: the longest scale-down window in play.

    The aggregator must keep samples at least as long as the longest decision
    window so the down-window reduction never drops data it still needs.
    """
    windows = [policy.defaults.scale_down_window_seconds]
    windows += [
        rn.scale_down_window_seconds
        for rn in policy.remote_networks
        if rn.scale_down_window_seconds is not None
    ]
    return max(windows)


def _build_collectors(
    policy: Policy, docker: aiodocker.Docker, http: httpx.AsyncClient
) -> list[Collector]:
    """Construct the enabled collectors per the policy toggles, in priority order."""
    collectors: list[Collector] = []
    if policy.collectors.docker_stats:
        collectors.append(DockerStatsCollector(docker))
    if policy.collectors.stdout_metrics:
        collectors.append(StdoutMetricsCollector(docker))
    if policy.collectors.prometheus:
        collectors.append(PrometheusCollector(http, port=policy.metrics_port))
    return collectors


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
    docker = aiodocker.Docker(url=settings.docker_host)
    twingate = TwingateClient(settings.twingate_network, settings.twingate_api_key, client=http)
    actuator = DockerActuator(
        docker,
        network=settings.twingate_network,
        image=policy.connector_image,
        labels=policy.labels,
        metrics_port=policy.metrics_port,
        janus_lock_label=policy.janus_lock_label,
    )
    collectors = _build_collectors(policy, docker, http)
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
        docker_probe=docker.version,
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

    logger.info("manager.start", http_port=_HTTP_PORT, poll_interval=policy.poll_interval_seconds)
    try:
        await asyncio.gather(loop.run_forever(stop_event), server.serve())
    finally:
        await http.aclose()
        await docker.close()
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
