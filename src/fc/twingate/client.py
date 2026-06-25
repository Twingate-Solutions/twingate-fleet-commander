"""Async ``httpx`` GraphQL client for the Twingate Admin API.

Covers connector list/get/create/delete, token generation, and remote-network
listing with ``X-API-KEY`` auth and backoff.

The client owns a single shared :class:`httpx.AsyncClient` with connection
pooling and explicit timeouts. All GraphQL calls funnel through
:meth:`TwingateClient._execute`, which applies exponential backoff with jitter
on HTTP ``429`` and ``5xx`` responses and raises :class:`TwingateApiError` on
exhaustion, non-retryable HTTP errors, or a top-level GraphQL ``errors`` array.

No secret material is ever logged or placed in an exception message: the API
key lives only in the ``X-API-KEY`` request header, and minted connector tokens
are wrapped in :class:`pydantic.SecretStr` so they cannot leak via ``repr``.
"""

import asyncio
import random
from datetime import datetime
from typing import Any

import httpx
import structlog
from pydantic import BaseModel, SecretStr

from fc.models import ConnectorState, ManagedConnector

logger = structlog.get_logger(__name__)

# Maximum number of characters of an upstream GraphQL ``error`` string that we
# include in logs and exceptions. Bounds log volume; never contains secrets.
_MAX_ERROR_LEN = 500

# Page size for Relay-style pagination on list queries.
_PAGE_SIZE = 50

# Safety cap on pagination: bounds memory and prevents an infinite loop if the
# API ever returns ``hasNextPage=true`` with a non-advancing cursor.
# _MAX_PAGES * _PAGE_SIZE is the largest set a single list call will collect.
_MAX_PAGES = 1000

# Default HTTP timeouts for the shared client (seconds).
_DEFAULT_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=10.0)


_LIST_CONNECTORS_QUERY = f"""
query ListConnectors($after: String) {{
  connectors(first: {_PAGE_SIZE}, after: $after) {{
    edges {{
      node {{
        id
        name
        state
        lastHeartbeatAt
        version
        hostname
        remoteNetwork {{ id name }}
      }}
    }}
    pageInfo {{ hasNextPage endCursor }}
  }}
}}
"""

# Single-node lookup by id. Assumes the schema exposes a top-level
# ``connector(id: ID!)`` field returning the same node fields as the list
# query; if the connector does not exist the field resolves to ``null`` and
# :meth:`TwingateClient.get_connector` returns ``None``.
_GET_CONNECTOR_QUERY = """
query GetConnector($id: ID!) {
  connector(id: $id) {
    id
    name
    state
    lastHeartbeatAt
    version
    hostname
    remoteNetwork { id name }
  }
}
"""

_CREATE_CONNECTOR_MUTATION = """
mutation CreateConnector($rnId: ID!, $name: String) {
  connectorCreate(remoteNetworkId: $rnId, name: $name) {
    ok
    error
    entity { id name remoteNetwork { id name } }
  }
}
"""

_GEN_TOKENS_MUTATION = """
mutation GenTokens($id: ID!) {
  connectorGenerateTokens(connectorId: $id) {
    ok
    error
    connectorTokens { accessToken refreshToken }
  }
}
"""

_DELETE_CONNECTOR_MUTATION = """
mutation DeleteConnector($id: ID!) {
  connectorDelete(id: $id) { ok error }
}
"""

_LIST_REMOTE_NETWORKS_QUERY = f"""
query ListRemoteNetworks($after: String) {{
  remoteNetworks(first: {_PAGE_SIZE}, after: $after) {{
    edges {{ node {{ id name }} }}
    pageInfo {{ hasNextPage endCursor }}
  }}
}}
"""


class TwingateApiError(Exception):
    """Raised when a Twingate Admin API call fails.

    Carries operation context (operation name, HTTP status, and the upstream
    GraphQL ``error`` string when present) for diagnosis. It never contains the
    API key or any connector token: only the operation name, HTTP status, and
    the (truncated) upstream error message are included.
    """

    def __init__(
        self,
        message: str,
        *,
        op_name: str,
        status: int | None = None,
        error: str | None = None,
    ) -> None:
        """Build the error with structured, secret-free context.

        Args:
            message: Human-readable description of the failure.
            op_name: GraphQL operation name (e.g. ``"CreateConnector"``).
            status: HTTP status code, when the failure was HTTP-level.
            error: Upstream GraphQL ``error`` string, when present.
        """
        self.op_name = op_name
        self.status = status
        self.error = error
        parts = [message, f"op={op_name}"]
        if status is not None:
            parts.append(f"status={status}")
        if error:
            parts.append(f"error={error[:_MAX_ERROR_LEN]}")
        super().__init__(" ".join(parts))


class ConnectorTokens(BaseModel):
    """Freshly minted access/refresh token pair for a single Connector.

    Both fields are :class:`pydantic.SecretStr` so the raw token values never
    appear in ``repr``/``str`` output or in logs. Unwrap with
    ``.get_secret_value()`` only at the point of injection into Docker env.
    """

    access_token: SecretStr
    refresh_token: SecretStr


class RemoteNetwork(BaseModel):
    """A Twingate Remote Network identifier/name pair."""

    id: str
    name: str


def _parse_heartbeat(value: str | None) -> datetime | None:
    """Parse a nullable ISO-8601 heartbeat timestamp into a ``datetime``.

    Returns ``None`` when the value is missing or unparseable, so a malformed
    timestamp from the API never aborts a discovery cycle.

    Args:
        value: ISO-8601 string (optionally ``Z``-suffixed) or ``None``.

    Returns:
        The parsed :class:`datetime`, or ``None`` if absent/invalid.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        logger.debug("twingate_api.bad_heartbeat", value=value)
        return None


def _parse_state(value: str | None) -> ConnectorState | None:
    """Coerce a GraphQL ``state`` string into :class:`ConnectorState`.

    Unknown or missing states map to ``None`` (logged at debug) rather than
    raising, so an API enum that FC does not yet recognize never breaks
    discovery.

    Args:
        value: The raw ``state`` string from the API, or ``None``.

    Returns:
        The matching :class:`ConnectorState`, or ``None`` if unrecognized.
    """
    if not value:
        return None
    try:
        return ConnectorState(value)
    except ValueError:
        logger.debug("twingate_api.unknown_state", state=value)
        return None


def _node_to_connector(node: dict[str, Any]) -> ManagedConnector:
    """Map a GraphQL connector node onto a :class:`ManagedConnector`.

    Only the API-known fields are populated. ``container_id``,
    ``docker_health``, and ``cordoned`` are not knowable from the Admin API and
    keep their model defaults.

    Args:
        node: The ``node`` object from a ``connectors`` edge or single lookup.

    Returns:
        The corresponding :class:`ManagedConnector`.
    """
    remote_network = node.get("remoteNetwork") or {}
    try:
        connector_id = node["id"]
        name = node["name"]
    except KeyError as exc:
        raise TwingateApiError(
            "connector node missing required field",
            op_name="parse",
            error=f"missing field {exc}",
        ) from exc
    return ManagedConnector(
        connector_id=connector_id,
        name=name,
        rn_id=remote_network.get("id", ""),
        twingate_state=_parse_state(node.get("state")),
        last_heartbeat_at=_parse_heartbeat(node.get("lastHeartbeatAt")),
    )


class TwingateClient:
    """Async GraphQL client for the Twingate Admin API.

    Owns a single shared :class:`httpx.AsyncClient` (unless one is injected for
    testing) and centralizes auth, retry/backoff, and GraphQL error mapping for
    all Connector and Remote Network operations.
    """

    def __init__(
        self,
        network: str,
        api_key: str | SecretStr,
        *,
        client: httpx.AsyncClient | None = None,
        max_retries: int = 5,
        base_backoff: float = 0.5,
        max_backoff: float = 30.0,
    ) -> None:
        """Construct the client.

        Args:
            network: Twingate network slug; the endpoint becomes
                ``https://<network>.twingate.com/api/graphql/``.
            api_key: Admin/DevOps API key. Accepted as ``str`` or
                :class:`pydantic.SecretStr` and always stored as ``SecretStr``;
                its value is only ever sent in the ``X-API-KEY`` header.
            client: Optional pre-built :class:`httpx.AsyncClient` (used by
                tests/respx). When provided, the caller owns its lifecycle and
                :meth:`aclose` will not close it.
            max_retries: Maximum number of attempts before giving up on a
                retryable (``429``/``5xx``) failure.
            base_backoff: Base delay (seconds) for exponential backoff.
            max_backoff: Cap (seconds) on any single backoff delay.
        """
        if max_retries < 1:
            raise ValueError(f"max_retries must be >= 1, got {max_retries}")
        self._network = network
        self._api_key: SecretStr = api_key if isinstance(api_key, SecretStr) else SecretStr(api_key)
        self._endpoint = f"https://{network}.twingate.com/api/graphql/"
        self._max_retries = max_retries
        self._base_backoff = base_backoff
        self._max_backoff = max_backoff

        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT)

    async def __aenter__(self) -> "TwingateClient":
        """Enter the async context manager, returning ``self``."""
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Exit the async context manager, closing an owned HTTP client."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close the underlying HTTP client if this instance owns it.

        An injected client is left open for the caller to manage.
        """
        if self._owns_client:
            await self._client.aclose()

    @property
    def _headers(self) -> dict[str, str]:
        """Request headers, including the secret ``X-API-KEY`` value."""
        return {
            "X-API-KEY": self._api_key.get_secret_value(),
            "Content-Type": "application/json",
        }

    def _backoff_delay(self, attempt: int) -> float:
        """Compute an exponential backoff delay with jitter for an attempt.

        The delay is ``min(max_backoff, base_backoff * 2**attempt)`` plus
        random jitter in ``[0, base_backoff)``.

        Args:
            attempt: Zero-based attempt index that just failed.

        Returns:
            The delay in seconds to sleep before the next attempt.
        """
        capped = min(self._max_backoff, self._base_backoff * (2**attempt))
        jitter = random.uniform(0.0, self._base_backoff)
        return float(min(self._max_backoff, capped + jitter))

    async def _execute(
        self, query: str, variables: dict[str, Any], *, op_name: str
    ) -> dict[str, Any]:
        """Execute a GraphQL operation with retry/backoff and error mapping.

        POSTs ``{"query", "variables"}`` to the endpoint with the ``X-API-KEY``
        header. Retries on HTTP ``429`` and ``5xx`` up to ``max_retries``
        attempts with exponential backoff and jitter. Raises on a non-retryable
        HTTP status, on retry exhaustion, on a top-level GraphQL ``errors``
        array, and on transport-level failures.

        Args:
            query: The GraphQL query/mutation document.
            variables: Variable bindings for the operation.
            op_name: Operation name for logging and error context.

        Returns:
            The GraphQL ``data`` object.

        Raises:
            TwingateApiError: On any HTTP, transport, or GraphQL-level failure.
        """
        payload = {"query": query, "variables": variables}
        last_status: int | None = None

        for attempt in range(self._max_retries):
            try:
                response = await self._client.post(
                    self._endpoint, json=payload, headers=self._headers
                )
            except httpx.HTTPError as exc:
                # Transport-level failure (DNS, connect, read timeout, ...).
                last_status = None
                if attempt + 1 < self._max_retries:
                    logger.warning(
                        "twingate_api.transport_retry",
                        op_name=op_name,
                        attempt=attempt,
                        error=type(exc).__name__,
                    )
                    await asyncio.sleep(self._backoff_delay(attempt))
                    continue
                logger.error(
                    "twingate_api.error",
                    op_name=op_name,
                    status=None,
                    error=type(exc).__name__,
                )
                raise TwingateApiError(
                    "transport error", op_name=op_name, error=type(exc).__name__
                ) from exc

            status = response.status_code
            last_status = status

            if status == 429 or 500 <= status < 600:
                if attempt + 1 < self._max_retries:
                    logger.warning(
                        "twingate_api.retry",
                        op_name=op_name,
                        status=status,
                        attempt=attempt,
                    )
                    await asyncio.sleep(self._backoff_delay(attempt))
                    continue
                logger.error("twingate_api.error", op_name=op_name, status=status)
                raise TwingateApiError("retryable status exhausted", op_name=op_name, status=status)

            if status >= 400:
                # Non-retryable client error (4xx other than 429).
                logger.error("twingate_api.error", op_name=op_name, status=status)
                raise TwingateApiError("non-retryable HTTP status", op_name=op_name, status=status)

            return self._parse_graphql_body(response, op_name=op_name)

        # Loop completed without returning or raising (max_retries <= 0 or
        # every attempt continued). Surface a deterministic error.
        logger.error("twingate_api.error", op_name=op_name, status=last_status)
        raise TwingateApiError("request not completed", op_name=op_name, status=last_status)

    def _parse_graphql_body(self, response: httpx.Response, *, op_name: str) -> dict[str, Any]:
        """Decode a 2xx GraphQL response body and surface top-level errors.

        Args:
            response: A successful (2xx) HTTP response.
            op_name: Operation name for error context.

        Returns:
            The GraphQL ``data`` object.

        Raises:
            TwingateApiError: If the body is not valid JSON, contains a
                top-level ``errors`` array, or omits ``data``.
        """
        try:
            body: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error("twingate_api.error", op_name=op_name, status=response.status_code)
            raise TwingateApiError(
                "invalid JSON response",
                op_name=op_name,
                status=response.status_code,
            ) from exc

        errors = body.get("errors")
        if errors:
            message = _first_graphql_error(errors)
            logger.error("twingate_api.error", op_name=op_name, error=message)
            raise TwingateApiError("graphql errors", op_name=op_name, error=message)

        data: dict[str, Any] | None = body.get("data")
        if data is None:
            logger.error("twingate_api.error", op_name=op_name, error="no data")
            raise TwingateApiError("response missing data", op_name=op_name, error="no data")
        return data

    def _check_mutation(self, payload: dict[str, Any] | None, *, op_name: str) -> None:
        """Validate a mutation payload's ``ok``/``error`` envelope.

        Args:
            payload: The mutation result object (e.g. the ``connectorCreate``
                payload), or ``None`` if the field was absent.
            op_name: Operation name for error context.

        Raises:
            TwingateApiError: When the payload is missing or ``ok`` is falsey.
        """
        if not payload:
            raise TwingateApiError("mutation payload missing", op_name=op_name, error="no payload")
        if not payload.get("ok", False):
            error = payload.get("error")
            logger.error("twingate_api.error", op_name=op_name, error=error)
            raise TwingateApiError("mutation returned ok=false", op_name=op_name, error=error)

    async def list_connectors(self) -> list[ManagedConnector]:
        """List all Connectors across the network, following pagination.

        Walks the Relay ``pageInfo.endCursor``/``hasNextPage`` cursor until the
        full set is collected.

        Returns:
            Every Connector mapped to :class:`ManagedConnector`.

        Raises:
            TwingateApiError: On any API-level failure.
        """
        connectors: list[ManagedConnector] = []
        after: str | None = None
        for _ in range(_MAX_PAGES):
            data = await self._execute(
                _LIST_CONNECTORS_QUERY, {"after": after}, op_name="ListConnectors"
            )
            conn = data.get("connectors") or {}
            for edge in conn.get("edges") or []:
                node = edge.get("node")
                if node:
                    connectors.append(_node_to_connector(node))
            page_info = conn.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                return connectors
            after = page_info.get("endCursor")
        raise TwingateApiError(
            f"pagination exceeded {_MAX_PAGES} pages",
            op_name="ListConnectors",
            error="cursor did not terminate",
        )

    async def get_connector(self, connector_id: str) -> ManagedConnector | None:
        """Fetch a single Connector by id.

        Assumes a top-level ``connector(id: ID!)`` field. A ``null`` result
        (unknown id) yields ``None``.

        Args:
            connector_id: The Twingate Connector id.

        Returns:
            The :class:`ManagedConnector`, or ``None`` if not found.

        Raises:
            TwingateApiError: On any API-level failure.
        """
        data = await self._execute(
            _GET_CONNECTOR_QUERY, {"id": connector_id}, op_name="GetConnector"
        )
        node = data.get("connector")
        if not node:
            return None
        return _node_to_connector(node)

    async def create_connector(self, rn_id: str, name: str | None = None) -> ManagedConnector:
        """Create a logical Connector in a Remote Network.

        This only creates the bookkeeping entity; provisioning the running
        container is a separate step performed by the actuator.

        Args:
            rn_id: Target Remote Network id.
            name: Optional Connector name; the API auto-names when ``None``.

        Returns:
            The newly created Connector as a :class:`ManagedConnector` (state
            and heartbeat are unknown at creation time).

        Raises:
            TwingateApiError: When the mutation fails or returns ``ok=false``.
        """
        data = await self._execute(
            _CREATE_CONNECTOR_MUTATION,
            {"rnId": rn_id, "name": name},
            op_name="CreateConnector",
        )
        payload = data.get("connectorCreate")
        self._check_mutation(payload, op_name="CreateConnector")
        assert payload is not None  # narrowed by _check_mutation
        entity = payload.get("entity") or {}
        remote_network = entity.get("remoteNetwork") or {}
        try:
            entity_id = entity["id"]
        except KeyError as exc:
            raise TwingateApiError(
                "connectorCreate entity missing id",
                op_name="CreateConnector",
                error=f"missing field {exc}",
            ) from exc
        return ManagedConnector(
            connector_id=entity_id,
            name=entity.get("name", name or ""),
            rn_id=remote_network.get("id", rn_id),
        )

    async def generate_tokens(self, connector_id: str) -> ConnectorTokens:
        """Mint a fresh access/refresh token pair for a Connector.

        Tokens are unique per Connector and must never be reused across
        containers. They are returned wrapped in :class:`pydantic.SecretStr`.

        Args:
            connector_id: The Connector to mint tokens for.

        Returns:
            The minted :class:`ConnectorTokens`.

        Raises:
            TwingateApiError: When the mutation fails or returns ``ok=false``.
        """
        data = await self._execute(_GEN_TOKENS_MUTATION, {"id": connector_id}, op_name="GenTokens")
        payload = data.get("connectorGenerateTokens")
        self._check_mutation(payload, op_name="GenTokens")
        assert payload is not None  # narrowed by _check_mutation
        tokens = payload.get("connectorTokens") or {}
        try:
            access_token = tokens["accessToken"]
            refresh_token = tokens["refreshToken"]
        except KeyError as exc:
            raise TwingateApiError(
                "connectorGenerateTokens payload missing token",
                op_name="GenTokens",
                error=f"missing field {exc}",
            ) from exc
        return ConnectorTokens(
            access_token=SecretStr(access_token),
            refresh_token=SecretStr(refresh_token),
        )

    async def delete_connector(self, connector_id: str) -> None:
        """Delete a logical Connector.

        Deleting signals the controller to stop routing new connections to the
        Connector; the actuator handles draining and removing the container.

        Args:
            connector_id: The Connector to delete.

        Raises:
            TwingateApiError: When the mutation fails or returns ``ok=false``.
        """
        data = await self._execute(
            _DELETE_CONNECTOR_MUTATION,
            {"id": connector_id},
            op_name="DeleteConnector",
        )
        payload = data.get("connectorDelete")
        self._check_mutation(payload, op_name="DeleteConnector")

    async def list_remote_networks(self) -> list[RemoteNetwork]:
        """List all Remote Networks, following pagination.

        Returns:
            Every Remote Network as a :class:`RemoteNetwork`.

        Raises:
            TwingateApiError: On any API-level failure.
        """
        networks: list[RemoteNetwork] = []
        after: str | None = None
        for _ in range(_MAX_PAGES):
            data = await self._execute(
                _LIST_REMOTE_NETWORKS_QUERY,
                {"after": after},
                op_name="ListRemoteNetworks",
            )
            rn = data.get("remoteNetworks") or {}
            for edge in rn.get("edges") or []:
                node = edge.get("node")
                if not node:
                    continue
                try:
                    networks.append(RemoteNetwork(id=node["id"], name=node["name"]))
                except KeyError as exc:
                    raise TwingateApiError(
                        "remote network node missing required field",
                        op_name="ListRemoteNetworks",
                        error=f"missing field {exc}",
                    ) from exc
            page_info = rn.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                return networks
            after = page_info.get("endCursor")
        raise TwingateApiError(
            f"pagination exceeded {_MAX_PAGES} pages",
            op_name="ListRemoteNetworks",
            error="cursor did not terminate",
        )


def _first_graphql_error(errors: list[dict[str, Any]]) -> str:
    """Extract a readable message from a GraphQL ``errors`` array.

    Args:
        errors: The top-level ``errors`` list from a GraphQL response.

    Returns:
        The first error's ``message``, or a generic fallback.
    """
    if errors and isinstance(errors[0], dict):
        message = errors[0].get("message")
        if isinstance(message, str):
            return message
    return "unknown graphql error"
