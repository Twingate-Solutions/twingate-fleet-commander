"""aiodocker implementation of the ``Actuator`` protocol.

Runs the connector image with its token env, the ping-group sysctl the
connector needs, an ``unless-stopped`` restart policy, the FC management
labels (so the fleet can be rediscovered every cycle), the janus auto-update
enrolment labels (when janus is enabled), and Twingate's prescribed
per-connector resource limits (1 vCPU / 2 GB — Key Design Rule N2). Stop/remove
and in-place restart round out the lifecycle, and ``list_managed`` rediscovers
the fleet by the managed label.

All Docker-specific failures are wrapped in :class:`DockerActuatorError`, whose
message never contains a token: the access/refresh tokens are unwrapped from
their :class:`~pydantic.SecretStr` only into the container environment, never
into logs or exceptions.
"""

import json
from typing import TYPE_CHECKING, Any

import structlog

from fc.actuator.base import ActuatorError
from fc.config import Labels
from fc.docker_inspect import InspectCache
from fc.models import ManagedConnector
from fc.twingate.client import ConnectorTokens

if TYPE_CHECKING:
    import aiodocker

logger = structlog.get_logger(__name__)

_PING_GROUP_RANGE = "0 2147483647"

# Janus auto-update enrolment label keys (confirmed schema). Stamped on every
# provisioned Connector when janus is enabled so the janus sidecar adopts it for
# in-place version upgrades. These keys are fixed by janus, not configurable.
_JANUS_ENABLE_LABEL = "janus.autoupdate.enable"
_JANUS_INTERVAL_LABEL = "janus.autoupdate.interval"

# Twingate's prescribed per-connector resource limits (Key Design Rule N2).
# Stamped on every provisioned container so the CPU and throughput watermarks
# are measured against a known 1-core capacity envelope. The connector's data
# path is effectively single-threaded, so a 1-vCPU limit means a saturated
# connector reads ~100% normalized CPU (not ~50% against two cores) — the CPU
# watermark only becomes meaningful at this sizing. ``cpu_high_pct`` is a
# percentage of this one core. Scale out horizontally, not up.
_CONNECTOR_CPU_LIMIT_CORES = 1
_CONNECTOR_NANO_CPUS = _CONNECTOR_CPU_LIMIT_CORES * 1_000_000_000  # Docker NanoCpus
_CONNECTOR_MEM_LIMIT_BYTES = 2 * 1024 * 1024 * 1024  # 2 GiB


class DockerActuatorError(ActuatorError):
    """Raised when a Docker lifecycle operation fails.

    A backend-specific :class:`~fc.actuator.base.ActuatorError`; carries only the
    operation name and the failing exception's type — never a token, env value,
    or other secret.
    """


def _health_section(inspect: dict[str, Any]) -> dict[str, Any] | None:
    """Return the ``State.Health`` sub-object from an inspect, or ``None``.

    Docker only includes ``State.Health`` when the image/container defines a
    ``HEALTHCHECK``; its absence means health is not reported.
    """
    state = inspect.get("State")
    if not isinstance(state, dict):
        return None
    health = state.get("Health")
    return health if isinstance(health, dict) else None


def _health_from_inspect(inspect: dict[str, Any]) -> str | None:
    """Read authoritative health from ``State.Health.Status`` in an inspect.

    Returns ``"healthy"``, ``"unhealthy"``, or ``"starting"`` straight from the
    Docker inspect, and ``None`` when no healthcheck is configured (no
    ``State.Health`` object) — a container with no HEALTHCHECK is ignored by the
    health path, unchanged from the prior status-string behavior.
    """
    health = _health_section(inspect)
    if health is None:
        return None
    status = health.get("Status")
    if not isinstance(status, str) or status in ("", "none"):
        return None
    return status


def _failing_streak_from_inspect(inspect: dict[str, Any]) -> int | None:
    """Read ``State.Health.FailingStreak`` (consecutive failures), or ``None``."""
    health = _health_section(inspect)
    if health is None:
        return None
    streak = health.get("FailingStreak")
    return int(streak) if isinstance(streak, int) else None


def _name_from_summary(names: object) -> str:
    """Return a clean container name from the summary ``Names`` list."""
    if isinstance(names, list) and names and isinstance(names[0], str):
        return names[0].lstrip("/")
    return ""


class DockerActuator:
    """Drives the local Docker socket to manage Connector containers.

    The actuator owns the FC label scheme: it stamps every container it
    provisions with the managed/remote-network/connector-id labels and uses the
    managed label to rediscover the fleet.
    """

    def __init__(
        self,
        docker: "aiodocker.Docker",
        *,
        network: str,
        image: str,
        labels: Labels,
        janus_enabled: bool = True,
        janus_interval_seconds: int = 86400,
        inspect_cache: InspectCache | None = None,
    ) -> None:
        """Build the actuator.

        Args:
            docker: The shared aiodocker client.
            network: Twingate network slug, passed to the connector as
                ``TWINGATE_NETWORK``.
            image: Connector image reference used when provisioning.
            labels: The FC Docker label keys (managed/remote-network/
                connector-id).
            janus_enabled: When ``True``, :meth:`provision` stamps the janus
                auto-update enrolment labels (``janus.autoupdate.enable=true`` +
                ``janus.autoupdate.interval``) so the janus sidecar adopts the
                Connector for in-place version upgrades (Key Design Rule #5).
                janus has no lock — FC tolerates its brief recreate via the
                grace windows rather than coordinating with it.
            janus_interval_seconds: Value for the ``janus.autoupdate.interval``
                label when ``janus_enabled``.
            inspect_cache: Per-cycle inspect cache shared with the stdout-metrics
                collector so each container is inspected at most once per cycle
                (:meth:`list_managed` reads health from it and clears it each
                cycle). A private cache is built when none is supplied.
        """
        self._docker = docker
        self._network = network
        self._image = image
        self._labels = labels
        self._janus_enabled = janus_enabled
        self._janus_interval_seconds = janus_interval_seconds
        self._inspect = inspect_cache or InspectCache(docker)

    async def provision(
        self,
        rn_id: str,
        connector_id: str,
        name: str,
        tokens: ConnectorTokens,
    ) -> str:
        """Run a connector container with tokens, labels, sysctl, and limits.

        Always stamps Twingate's prescribed per-connector resource limits
        (1 vCPU / 2 GB — Key Design Rule N2) via the Docker ``NanoCpus`` and
        ``Memory`` host-config fields, so the CPU watermark is a percentage of a
        known 1-core envelope.

        Args:
            rn_id: Remote Network id (stamped as the remote-network label).
            connector_id: Logical Connector id (stamped as the connector-id
                label so the container can be joined back to the logical
                Connector).
            name: Container name.
            tokens: Single-use access/refresh tokens, injected only into env.

        Returns:
            The created container's id.

        Raises:
            DockerActuatorError: If the Docker run fails.
        """
        host_config: dict[str, Any] = {
            "RestartPolicy": {"Name": "unless-stopped"},
            "Sysctls": {"net.ipv4.ping_group_range": _PING_GROUP_RANGE},
            "NanoCpus": _CONNECTOR_NANO_CPUS,
            "Memory": _CONNECTOR_MEM_LIMIT_BYTES,
        }

        container_labels = {
            self._labels.managed: "true",
            self._labels.remote_network: rn_id,
            self._labels.connector_id: connector_id,
        }
        # Enrol the Connector with janus for in-place version upgrades. janus has
        # no lock — FC just stamps these labels and tolerates janus's brief
        # container recreate via the grace windows (Key Design Rule #5).
        if self._janus_enabled:
            container_labels[_JANUS_ENABLE_LABEL] = "true"
            container_labels[_JANUS_INTERVAL_LABEL] = str(self._janus_interval_seconds)

        config: dict[str, Any] = {
            "Image": self._image,
            "Env": [
                f"TWINGATE_NETWORK={self._network}",
                # Always-on connector analytics: emits ANALYTICS network-traffic
                # lines on stdout so the stdout_metrics collector and the optional
                # log-shipper have flow data to read. Hard-coded for every
                # connector FC provisions; the admin chooses whether to consume it.
                "TWINGATE_LOG_ANALYTICS=v2",
                f"TWINGATE_ACCESS_TOKEN={tokens.access_token.get_secret_value()}",
                f"TWINGATE_REFRESH_TOKEN={tokens.refresh_token.get_secret_value()}",
            ],
            "Labels": container_labels,
            "HostConfig": host_config,
        }

        try:
            container = await self._docker.containers.run(config=config, name=name)
        except Exception as exc:
            logger.error("docker_api.error", op="provision", error=type(exc).__name__)
            raise DockerActuatorError("failed to run connector container", op="provision") from exc

        # Lifecycle success/failure events (``action.provision.*``) are emitted by
        # the control loop, bound to the cycle_id (Key Design Rule #6). The
        # actuator stays a pure backend seam and does not log lifecycle events.
        return container.id

    async def deprovision(self, connector: ManagedConnector) -> None:
        """Stop then remove the connector's container (no-op if logical-only).

        Args:
            connector: The Connector whose container should be removed.

        Raises:
            DockerActuatorError: If stop/remove fails.
        """
        if connector.container_id is None:
            return
        try:
            container = await self._docker.containers.get(connector.container_id)
            await container.stop()
            await container.delete(force=True)
        except Exception as exc:
            logger.error(
                "docker_api.error",
                op="deprovision",
                connector_id=connector.connector_id,
                error=type(exc).__name__,
            )
            raise DockerActuatorError(
                "failed to remove connector container", op="deprovision"
            ) from exc

    async def restart(self, connector: ManagedConnector) -> None:
        """Restart the connector's container in place, preserving env/tokens.

        Args:
            connector: The Connector to restart.

        Raises:
            DockerActuatorError: If the restart fails or there is no container.
        """
        if connector.container_id is None:
            raise DockerActuatorError("cannot restart a logical-only connector", op="restart")
        try:
            container = await self._docker.containers.get(connector.container_id)
            await container.restart()
        except DockerActuatorError:
            raise
        except Exception as exc:
            logger.error(
                "docker_api.error",
                op="restart",
                connector_id=connector.connector_id,
                error=type(exc).__name__,
            )
            raise DockerActuatorError(
                "failed to restart connector container", op="restart"
            ) from exc

    async def list_managed(self) -> list[ManagedConnector]:
        """List FC-managed containers (by the managed label) as connectors.

        ``twingate_state`` and ``last_heartbeat_at`` are left ``None`` — they
        are authoritative only from the Twingate API and are filled in by the
        loop's discovery join. ``connector_id`` is read from the container label
        when present (FC-provisioned) and is empty for seed containers that
        predate FC.

        Returns:
            The managed Connectors discovered on the host.

        Raises:
            DockerActuatorError: If the list query fails.
        """
        managed_filter = json.dumps({"label": [f"{self._labels.managed}=true"]})
        try:
            containers = await self._docker.containers.list(all=True, filters=managed_filter)
        except Exception as exc:
            logger.error("docker_api.error", op="list_managed", error=type(exc).__name__)
            raise DockerActuatorError(
                "failed to list managed containers", op="list_managed"
            ) from exc

        # Fresh per cycle: clear before refilling so each container is inspected
        # at most once this cycle (shared with the stdout-metrics collector).
        self._inspect.clear()
        result: list[ManagedConnector] = []
        for container in containers:
            labels = container["Labels"] or {}
            health, failing_streak = await self._read_health(container.id)
            result.append(
                ManagedConnector(
                    connector_id=labels.get(self._labels.connector_id, ""),
                    name=_name_from_summary(container["Names"]),
                    rn_id=labels.get(self._labels.remote_network, ""),
                    container_id=container.id,
                    docker_health=health,
                    docker_failing_streak=failing_streak,
                )
            )
        return result

    async def _read_health(self, container_id: str) -> tuple[str | None, int | None]:
        """Read authoritative health + failing streak from a container inspect.

        A per-container inspect failure is isolated (logged, treated as unknown
        health) so one bad container never aborts fleet discovery.
        """
        try:
            inspect = await self._inspect.inspect(container_id)
        except Exception as exc:
            logger.warning("docker_api.error", op="inspect", error=type(exc).__name__)
            return None, None
        return _health_from_inspect(inspect), _failing_streak_from_inspect(inspect)
