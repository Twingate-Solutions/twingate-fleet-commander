"""The ``Actuator`` protocol: provision/deprovision/restart/list_managed.

This is the single seam between the decision engine and the compute backend.
The engine speaks only in terms of this protocol, so a multi-host or cloud
backend can be swapped in for the local-Docker implementation without touching
any decision logic (Key Design Rule #9).

The actuator owns the backend-specific identity scheme (Docker labels, cloud
tags, ...): the engine hands it a Remote Network id, the logical Connector id,
a name, and freshly minted tokens, and the actuator decides how to mark and
later rediscover the resulting compute.

Drain ordering for scale-down (``connectorDelete`` → grace → stop/remove) is
enforced by the *engine*, not here: :meth:`Actuator.deprovision` only performs
the compute-side stop/remove.
"""

from typing import Protocol, runtime_checkable

from fc.models import ManagedConnector
from fc.twingate.client import ConnectorTokens


@runtime_checkable
class Actuator(Protocol):
    """Drives the compute lifecycle of managed Connectors.

    Implementations must isolate all backend-specific failures behind a typed
    error so the control loop can catch, log, and continue.
    """

    async def provision(
        self,
        rn_id: str,
        connector_id: str,
        name: str,
        tokens: ConnectorTokens,
        *,
        mem_limit_bytes: int | None = None,
    ) -> str:
        """Start a Connector's compute with its tokens and management markers.

        Args:
            rn_id: The Remote Network the Connector belongs to.
            connector_id: The logical Twingate Connector id (created first), set
                as a marker so :meth:`list_managed` can join compute back to the
                logical Connector.
            name: The Connector/compute name.
            tokens: The freshly minted, single-use access/refresh token pair.
            mem_limit_bytes: Optional memory limit for the compute unit.

        Returns:
            The backend identifier of the started compute (e.g. container id).
        """
        ...

    async def deprovision(self, connector: ManagedConnector) -> None:
        """Stop and remove a Connector's compute.

        Assumes the logical Connector has already been deleted and drained by
        the engine. A logical-only Connector (no compute) is a no-op.
        """
        ...

    async def restart(self, connector: ManagedConnector) -> None:
        """Restart a Connector's compute in place, preserving its env/tokens."""
        ...

    async def list_managed(self) -> list[ManagedConnector]:
        """List all FC-managed compute units as :class:`ManagedConnector`s."""
        ...
