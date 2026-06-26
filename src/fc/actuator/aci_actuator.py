"""Azure Container Instances implementation of the ``Actuator`` protocol.

ACI's cleanest 1:1 fit for FC's per-connector single-use token model is **one
container group per logical Connector** (Key Design Rule #1): the group holds the
connector container with its token, and the group name *is* the connector's
backend identity. The actuator talks to the Azure Resource Manager REST API over
the project's existing :class:`httpx.AsyncClient` (no Azure management SDK
dependency); a bearer token is supplied by an injected async credential callable
so the test suite can drive it with an ``httpx.MockTransport`` and a stub token.

Lifecycle mapping:

* ``provision`` → ``PUT`` the container group with the token env (as Azure
  ``secureValue`` so it never reads back), the prescribed 1 vCPU / 2 GB sizing
  (Key Design Rule N2), and FC tags.
* ``restart`` → ``POST .../restart`` the group, which relaunches the *same*
  container definition (same token) in place — never two active sharing the
  token (the amended Rule #1).
* ``deprovision`` → ``DELETE`` the group (the loop performs ``connectorDelete`` +
  drain around this); a missing group is a no-op.
* ``list_managed`` → ``GET`` the resource group's container groups, filtered by
  the FC managed tag, mapping the instance-view state into ``docker_health``.

Tokens are unwrapped from their :class:`~pydantic.SecretStr` only into the
container group body, never into logs or :class:`AciActuatorError`.
"""

from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from fc.actuator.base import ActuatorError
from fc.config import Labels
from fc.models import ManagedConnector
from fc.platform import AciSettings
from fc.twingate.client import ConnectorTokens

#: Azure Resource Manager base URL and the container-instance API version.
_ARM_BASE = "https://management.azure.com"
_API_VERSION = "2023-05-01"
#: OAuth scope for the ARM management plane.
ARM_SCOPE = "https://management.azure.com/.default"

# Twingate's prescribed per-connector sizing (Key Design Rule N2): 1 vCPU / 2 GB.
_ACI_CPU_CORES = 1.0
_ACI_MEM_GB = 2.0

#: A bearer-token supplier: given an OAuth scope, return an access token.
TokenProvider = Callable[[str], Awaitable[str]]


class AciActuatorError(ActuatorError):
    """Raised when an ACI lifecycle operation fails (no token ever in message)."""


def _health_from_group(group: dict[str, Any]) -> str | None:
    """Map an ACI container group's instance-view state onto docker_health.

    ``Running`` maps to ``healthy``; ``Failed`` to ``unhealthy`` (so a crashed
    group escalates through the health path); anything else (or an absent
    instance view, common in list responses) maps to ``None`` so the Twingate
    liveness state drives remediation instead.
    """
    props = group.get("properties")
    if not isinstance(props, dict):
        return None
    instance_view = props.get("instanceView")
    if not isinstance(instance_view, dict):
        return None
    state = instance_view.get("state")
    if state == "Running":
        return "healthy"
    if state == "Failed":
        return "unhealthy"
    return None


class AciActuator:
    """Drives the ACI REST API to manage one container group per Connector."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        token_provider: TokenProvider,
        *,
        settings: AciSettings,
        network: str,
        image: str,
        labels: Labels,
    ) -> None:
        """Build the actuator.

        Args:
            http: The shared async HTTP client used for all ARM calls.
            token_provider: Async callable returning a bearer token for a scope;
                called with :data:`ARM_SCOPE`.
            settings: The validated ACI placement settings.
            network: Twingate network slug, injected as ``TWINGATE_NETWORK``.
            image: Connector image reference for the container group.
            labels: The FC identity keys, reused verbatim as Azure tag keys.
        """
        self._http = http
        self._token_provider = token_provider
        self._settings = settings
        self._network = network
        self._image = image
        self._labels = labels
        prefix = labels.managed.rsplit(".", 1)[0]
        self._name_tag = f"{prefix}.name"

    # -- helpers -------------------------------------------------------------

    def _group_url(self, name: str) -> str:
        """Build the ARM URL for a single container group."""
        s = self._settings
        return (
            f"{_ARM_BASE}/subscriptions/{s.subscription_id}"
            f"/resourceGroups/{s.resource_group}"
            f"/providers/Microsoft.ContainerInstance/containerGroups/{name}"
        )

    def _list_url(self) -> str:
        """Build the ARM URL listing the resource group's container groups."""
        s = self._settings
        return (
            f"{_ARM_BASE}/subscriptions/{s.subscription_id}"
            f"/resourceGroups/{s.resource_group}"
            f"/providers/Microsoft.ContainerInstance/containerGroups"
        )

    async def _headers(self) -> dict[str, str]:
        """Build the auth + content headers for an ARM request."""
        token = await self._token_provider(ARM_SCOPE)
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    async def _request(
        self,
        method: str,
        url: str,
        *,
        op: str,
        json_body: dict[str, Any] | None = None,
        ok: tuple[int, ...] = (200, 201, 202, 204),
    ) -> httpx.Response:
        """Issue one ARM request, raising :class:`AciActuatorError` off the path.

        Credential-acquisition failures, network failures, and unexpected status
        codes are all normalized to :class:`AciActuatorError` so the control loop
        catches a single backend error type. The response body is never surfaced
        (it may echo the request, which carries the secure token values on
        provision); credential exception text never carries the client secret.
        """
        try:
            headers = await self._headers()
        except Exception as exc:
            raise AciActuatorError(f"ACI auth failed: {type(exc).__name__}", op=op) from exc
        # ARM ``nextLink`` URLs already carry the api-version (and a skiptoken),
        # so only add the param when the URL does not already include it.
        params = None if "api-version=" in url else {"api-version": _API_VERSION}
        try:
            response = await self._http.request(
                method,
                url,
                params=params,
                headers=headers,
                json=json_body,
            )
        except httpx.HTTPError as exc:
            raise AciActuatorError(f"ACI request failed: {type(exc).__name__}", op=op) from exc
        if response.status_code not in ok:
            raise AciActuatorError(f"ACI request returned status {response.status_code}", op=op)
        return response

    def _container_group_body(
        self,
        rn_id: str,
        connector_id: str,
        name: str,
        tokens: ConnectorTokens,
    ) -> dict[str, Any]:
        """Build the container-group request body (tokens as secureValue)."""
        container: dict[str, Any] = {
            "name": self._settings.container_name,
            "properties": {
                "image": self._image,
                "resources": {"requests": {"cpu": _ACI_CPU_CORES, "memoryInGB": _ACI_MEM_GB}},
                "environmentVariables": [
                    {"name": "TWINGATE_NETWORK", "value": self._network},
                    # Always-on connector analytics: ANALYTICS stdout traffic lines
                    # for the stdout collector / log-shipper. Non-secret, so it uses
                    # a plain ``value`` (not ``secureValue``).
                    {"name": "TWINGATE_LOG_ANALYTICS", "value": "v2"},
                    {
                        "name": "TWINGATE_ACCESS_TOKEN",
                        "secureValue": tokens.access_token.get_secret_value(),
                    },
                    {
                        "name": "TWINGATE_REFRESH_TOKEN",
                        "secureValue": tokens.refresh_token.get_secret_value(),
                    },
                ],
            },
        }
        properties: dict[str, Any] = {
            "sku": "Standard",
            "osType": "Linux",
            "restartPolicy": "Always",
            "containers": [container],
        }
        if self._settings.subnet_id:
            properties["subnetIds"] = [{"id": self._settings.subnet_id}]
        return {
            "location": self._settings.region,
            "tags": {
                self._labels.managed: "true",
                self._labels.remote_network: rn_id,
                self._labels.connector_id: connector_id,
                self._name_tag: name,
            },
            "properties": properties,
        }

    # -- lifecycle -----------------------------------------------------------

    async def provision(
        self,
        rn_id: str,
        connector_id: str,
        name: str,
        tokens: ConnectorTokens,
    ) -> str:
        """``PUT`` a container group for one connector; return its name as the id.

        Raises:
            AciActuatorError: If the create call fails.
        """
        body = self._container_group_body(rn_id, connector_id, name, tokens)
        await self._request("PUT", self._group_url(name), op="provision", json_body=body)
        return name

    async def restart(self, connector: ManagedConnector) -> None:
        """``POST .../restart`` the group, reusing the same token in place.

        Raises:
            AciActuatorError: If the connector has no group, or the call fails.
        """
        if connector.container_id is None:
            raise AciActuatorError("cannot restart a group-less connector", op="restart")
        url = f"{self._group_url(connector.container_id)}/restart"
        await self._request("POST", url, op="restart")

    async def deprovision(self, connector: ManagedConnector) -> None:
        """``DELETE`` the connector's container group (no-op if absent).

        Assumes the loop has already performed ``connectorDelete`` + drain. A
        ``404`` (group already gone) is treated as success.

        Raises:
            AciActuatorError: If the delete call fails for any other reason.
        """
        if connector.container_id is None:
            return
        await self._request(
            "DELETE",
            self._group_url(connector.container_id),
            op="deprovision",
            ok=(200, 202, 204, 404),
        )

    async def list_managed(self) -> list[ManagedConnector]:
        """List FC-managed container groups (by tag) as :class:`ManagedConnector`s.

        ``twingate_state`` / ``last_heartbeat_at`` are left ``None`` (filled in by
        the loop's Twingate join). The group name is carried as ``container_id``.

        Raises:
            AciActuatorError: If the list call fails.
        """
        result: list[ManagedConnector] = []
        url: str | None = self._list_url()
        while url:
            response = await self._request("GET", url, op="list_managed", ok=(200,))
            try:
                payload = response.json()
            except ValueError as exc:
                raise AciActuatorError(
                    "ACI list returned non-JSON body", op="list_managed"
                ) from exc

            groups = payload.get("value") if isinstance(payload, dict) else None
            for group in groups or []:
                if not isinstance(group, dict):
                    continue
                tags = group.get("tags")
                tags = tags if isinstance(tags, dict) else {}
                if tags.get(self._labels.managed) != "true":
                    continue
                connector_id = tags.get(self._labels.connector_id)
                if not connector_id:
                    # Managed-but-untagged: skip rather than collapse onto an
                    # empty id (FC always stamps the id tag, so this is foreign).
                    continue
                group_name = group.get("name")
                result.append(
                    ManagedConnector(
                        connector_id=str(connector_id),
                        name=str(tags.get(self._name_tag) or group_name or ""),
                        rn_id=str(tags.get(self._labels.remote_network, "")),
                        container_id=str(group_name) if group_name else None,
                        docker_health=_health_from_group(group),
                        docker_failing_streak=None,
                    )
                )
            # ARM list responses paginate via ``nextLink`` (already api-versioned).
            next_link = payload.get("nextLink") if isinstance(payload, dict) else None
            url = next_link if isinstance(next_link, str) and next_link else None
        return result
