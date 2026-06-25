"""aiodocker implementation of the ``Actuator`` protocol.

Runs the connector image with its token env, the ping-group sysctl the
connector needs, an ``unless-stopped`` restart policy, the FC management
labels (so the fleet can be rediscovered every cycle), the metrics port, and an
optional memory limit. Stop/remove and in-place restart round out the
lifecycle, and ``list_managed`` rediscovers the fleet by the managed label.

All Docker-specific failures are wrapped in :class:`DockerActuatorError`, whose
message never contains a token: the access/refresh tokens are unwrapped from
their :class:`~pydantic.SecretStr` only into the container environment, never
into logs or exceptions.
"""

import json
from typing import TYPE_CHECKING, Any

import structlog

from fc.config import Labels
from fc.models import ManagedConnector
from fc.twingate.client import ConnectorTokens

if TYPE_CHECKING:
    import aiodocker

logger = structlog.get_logger(__name__)

_PING_GROUP_RANGE = "0 2147483647"


class DockerActuatorError(Exception):
    """Raised when a Docker lifecycle operation fails.

    Carries only the operation name and the failing exception's type — never a
    token, env value, or other secret.
    """

    def __init__(self, message: str, *, op: str) -> None:
        """Build the error with secret-free context.

        Args:
            message: Human-readable description (no secrets).
            op: The actuator operation that failed (e.g. ``"provision"``).
        """
        self.op = op
        super().__init__(f"{message} op={op}")


def _health_from_status(status: object) -> str | None:
    """Extract a health word from a container summary ``Status`` string.

    Docker embeds health in the status text, e.g. ``"Up 3 hours (healthy)"``.
    Returns ``"healthy"``, ``"unhealthy"``, ``"starting"``, or ``None`` when no
    health is reported (no healthcheck configured).
    """
    if not isinstance(status, str):
        return None
    lowered = status.lower()
    if "(healthy)" in lowered:
        return "healthy"
    if "(unhealthy)" in lowered:
        return "unhealthy"
    if "health: starting" in lowered or "(starting)" in lowered:
        return "starting"
    return None


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
        metrics_port: int,
        janus_lock_label: str | None = None,
    ) -> None:
        """Build the actuator.

        Args:
            docker: The shared aiodocker client.
            network: Twingate network slug, passed to the connector as
                ``TWINGATE_NETWORK``.
            image: Connector image reference used when provisioning.
            labels: The FC Docker label keys (managed/remote-network/
                connector-id).
            metrics_port: Port the connector exposes Prometheus metrics on,
                passed as ``TWINGATE_METRICS_PORT``.
            janus_lock_label: Label key whose presence on a container marks it
                as mid-upgrade by janus; :meth:`list_managed` sets
                ``janus_locked`` so the engine yields (Key Design Rule #5).
                Detecting the marker is a backend concern, so it lives here in
                the Docker actuator rather than in the decision engine.
        """
        self._docker = docker
        self._network = network
        self._image = image
        self._labels = labels
        self._metrics_port = metrics_port
        self._janus_lock_label = janus_lock_label

    async def provision(
        self,
        rn_id: str,
        connector_id: str,
        name: str,
        tokens: ConnectorTokens,
        *,
        mem_limit_bytes: int | None = None,
    ) -> str:
        """Run a connector container with tokens, labels, sysctl, and policy.

        Args:
            rn_id: Remote Network id (stamped as the remote-network label).
            connector_id: Logical Connector id (stamped as the connector-id
                label so the container can be joined back to the logical
                Connector).
            name: Container name.
            tokens: Single-use access/refresh tokens, injected only into env.
            mem_limit_bytes: Optional container memory limit in bytes.

        Returns:
            The created container's id.

        Raises:
            DockerActuatorError: If the Docker run fails.
        """
        host_config: dict[str, Any] = {
            "RestartPolicy": {"Name": "unless-stopped"},
            "Sysctls": {"net.ipv4.ping_group_range": _PING_GROUP_RANGE},
        }
        if mem_limit_bytes is not None and mem_limit_bytes > 0:
            host_config["Memory"] = mem_limit_bytes

        config: dict[str, Any] = {
            "Image": self._image,
            "Env": [
                f"TWINGATE_NETWORK={self._network}",
                f"TWINGATE_ACCESS_TOKEN={tokens.access_token.get_secret_value()}",
                f"TWINGATE_REFRESH_TOKEN={tokens.refresh_token.get_secret_value()}",
                f"TWINGATE_METRICS_PORT={self._metrics_port}",
            ],
            "Labels": {
                self._labels.managed: "true",
                self._labels.remote_network: rn_id,
                self._labels.connector_id: connector_id,
            },
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

        result: list[ManagedConnector] = []
        for container in containers:
            labels = container["Labels"] or {}
            janus_locked = bool(self._janus_lock_label) and self._janus_lock_label in labels
            result.append(
                ManagedConnector(
                    connector_id=labels.get(self._labels.connector_id, ""),
                    name=_name_from_summary(container["Names"]),
                    rn_id=labels.get(self._labels.remote_network, ""),
                    container_id=container.id,
                    docker_health=_health_from_status(container["Status"]),
                    janus_locked=janus_locked,
                )
            )
        return result
