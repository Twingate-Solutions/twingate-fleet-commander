"""Tests for the async Twingate GraphQL client (``fc.twingate.client``).

Uses ``respx`` to mock the ``httpx`` transport. ``asyncio_mode=auto`` is set in
``pyproject.toml`` so ``async def test_*`` functions run directly without an
explicit marker. These tests cover pagination, mutation error mapping,
SecretStr token handling, retry/backoff behavior, and the hard requirement that
the API key never leaks into exceptions or logs.
"""

from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
import respx
import structlog

from fc.models import ConnectorState, ManagedConnector
from fc.twingate.client import (
    ConnectorTokens,
    RemoteNetwork,
    TwingateApiError,
    TwingateClient,
)

ENDPOINT = "https://acme.twingate.com/api/graphql/"
SENTINEL_KEY = "tgp_SECRET_DO_NOT_LOG"


def _gql(data: dict[str, Any]) -> httpx.Response:
    """Build a 200 GraphQL success response wrapping ``data``."""
    return httpx.Response(200, json={"data": data})


def make_client(**kwargs: Any) -> TwingateClient:
    """Construct a client against the ``acme`` network with the sentinel key."""
    return TwingateClient("acme", SENTINEL_KEY, **kwargs)


def _connector_node(
    connector_id: str,
    name: str,
    rn_id: str,
    state: str | None = "ALIVE",
    heartbeat: str | None = "2026-06-24T12:00:00Z",
) -> dict[str, Any]:
    """Build a GraphQL connector node for list/get responses."""
    return {
        "id": connector_id,
        "name": name,
        "state": state,
        "lastHeartbeatAt": heartbeat,
        "version": "1.0.0",
        "hostname": "host-1",
        "remoteNetwork": {"id": rn_id, "name": f"rn-{rn_id}"},
    }


@respx.mock
async def test_list_connectors_paginates_and_maps() -> None:
    """list_connectors follows pageInfo and maps every node to the model."""
    page1 = _gql(
        {
            "connectors": {
                "edges": [
                    {"node": _connector_node("c-1", "conn-1", "rn-a")},
                    {"node": _connector_node("c-2", "conn-2", "rn-a", state="DEAD_NO_RELAYS")},
                ],
                "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
            }
        }
    )
    page2 = _gql(
        {
            "connectors": {
                "edges": [
                    {"node": _connector_node("c-3", "conn-3", "rn-b", heartbeat=None)},
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        }
    )
    route = respx.post(ENDPOINT).mock(side_effect=[page1, page2])

    async with make_client() as client:
        result = await client.list_connectors()

    assert route.call_count == 2
    assert [c.connector_id for c in result] == ["c-1", "c-2", "c-3"]
    assert all(isinstance(c, ManagedConnector) for c in result)
    assert result[0].rn_id == "rn-a"
    assert result[0].twingate_state is ConnectorState.ALIVE
    assert result[0].last_heartbeat_at is not None
    assert result[1].twingate_state is ConnectorState.DEAD_NO_RELAYS
    assert result[2].rn_id == "rn-b"
    assert result[2].last_heartbeat_at is None

    # The second request carried the cursor from page 1.
    second_request_body = route.calls[1].request.content.decode()
    assert "c1" in second_request_body


@respx.mock
async def test_list_connectors_unknown_state_maps_to_none() -> None:
    """An unrecognized state string degrades to None instead of raising."""
    page = _gql(
        {
            "connectors": {
                "edges": [{"node": _connector_node("c-9", "conn-9", "rn-a", state="WAT")}],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        }
    )
    respx.post(ENDPOINT).mock(return_value=page)

    async with make_client() as client:
        result = await client.list_connectors()

    assert result[0].twingate_state is None


@respx.mock
async def test_get_connector_found_and_missing() -> None:
    """get_connector returns a model when present and None when null."""
    respx.post(ENDPOINT).mock(
        side_effect=[
            _gql({"connector": _connector_node("c-1", "conn-1", "rn-a")}),
            _gql({"connector": None}),
        ]
    )
    async with make_client() as client:
        found = await client.get_connector("c-1")
        missing = await client.get_connector("c-x")

    assert found is not None
    assert found.connector_id == "c-1"
    assert missing is None


@respx.mock
async def test_create_connector_success() -> None:
    """A successful create returns a ManagedConnector from the entity."""
    respx.post(ENDPOINT).mock(
        return_value=_gql(
            {
                "connectorCreate": {
                    "ok": True,
                    "error": None,
                    "entity": {
                        "id": "c-new",
                        "name": "conn-new",
                        "remoteNetwork": {"id": "rn-a", "name": "rn-a"},
                    },
                }
            }
        )
    )
    async with make_client() as client:
        conn = await client.create_connector("rn-a", "conn-new")

    assert isinstance(conn, ManagedConnector)
    assert conn.connector_id == "c-new"
    assert conn.name == "conn-new"
    assert conn.rn_id == "rn-a"


@respx.mock
async def test_create_connector_ok_false_raises() -> None:
    """ok=false on create raises TwingateApiError carrying the error string."""
    respx.post(ENDPOINT).mock(
        return_value=_gql(
            {"connectorCreate": {"ok": False, "error": "rn not found", "entity": None}}
        )
    )
    async with make_client() as client:
        with pytest.raises(TwingateApiError) as exc_info:
            await client.create_connector("rn-bad")

    assert exc_info.value.op_name == "CreateConnector"
    assert "rn not found" in str(exc_info.value)


@respx.mock
async def test_generate_tokens_wraps_secrets() -> None:
    """Tokens are returned as SecretStr and never appear in repr/str."""
    access = "ACCESS_TOKEN_VALUE_abc123"
    refresh = "REFRESH_TOKEN_VALUE_def456"
    respx.post(ENDPOINT).mock(
        return_value=_gql(
            {
                "connectorGenerateTokens": {
                    "ok": True,
                    "error": None,
                    "connectorTokens": {
                        "accessToken": access,
                        "refreshToken": refresh,
                    },
                }
            }
        )
    )
    async with make_client() as client:
        tokens = await client.generate_tokens("c-1")

    assert isinstance(tokens, ConnectorTokens)
    assert tokens.access_token.get_secret_value() == access
    assert tokens.refresh_token.get_secret_value() == refresh
    # The raw secret values must not leak via repr or str.
    assert access not in repr(tokens)
    assert refresh not in repr(tokens)
    assert access not in str(tokens)
    assert refresh not in str(tokens)


@respx.mock
async def test_delete_connector_success_and_failure() -> None:
    """delete succeeds on ok=true and raises on ok=false."""
    respx.post(ENDPOINT).mock(
        side_effect=[
            _gql({"connectorDelete": {"ok": True, "error": None}}),
            _gql({"connectorDelete": {"ok": False, "error": "no such connector"}}),
        ]
    )
    async with make_client() as client:
        await client.delete_connector("c-1")  # should not raise
        with pytest.raises(TwingateApiError):
            await client.delete_connector("c-x")


@respx.mock
async def test_list_remote_networks_paginates() -> None:
    """list_remote_networks aggregates across pages into RemoteNetwork models."""
    page1 = _gql(
        {
            "remoteNetworks": {
                "edges": [{"node": {"id": "rn-a", "name": "Net A"}}],
                "pageInfo": {"hasNextPage": True, "endCursor": "p1"},
            }
        }
    )
    page2 = _gql(
        {
            "remoteNetworks": {
                "edges": [{"node": {"id": "rn-b", "name": "Net B"}}],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        }
    )
    respx.post(ENDPOINT).mock(side_effect=[page1, page2])

    async with make_client() as client:
        networks = await client.list_remote_networks()

    assert networks == [
        RemoteNetwork(id="rn-a", name="Net A"),
        RemoteNetwork(id="rn-b", name="Net B"),
    ]


@respx.mock
async def test_top_level_graphql_errors_raise() -> None:
    """A top-level GraphQL errors array maps to TwingateApiError."""
    respx.post(ENDPOINT).mock(
        return_value=httpx.Response(
            200, json={"errors": [{"message": "field 'connectors' is deprecated"}]}
        )
    )
    async with make_client() as client:
        with pytest.raises(TwingateApiError) as exc_info:
            await client.list_connectors()

    assert "deprecated" in str(exc_info.value)


@respx.mock
async def test_non_retryable_4xx_raises_without_retrying(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 400 fails immediately without consuming retries or sleeping."""
    sleep_mock = AsyncMock()
    monkeypatch.setattr("fc.twingate.client.asyncio.sleep", sleep_mock)
    route = respx.post(ENDPOINT).mock(return_value=httpx.Response(400, json={}))

    async with make_client() as client:
        with pytest.raises(TwingateApiError) as exc_info:
            await client.list_connectors()

    assert exc_info.value.status == 400
    assert route.call_count == 1
    sleep_mock.assert_not_awaited()


@respx.mock
async def test_backoff_then_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two retryable statuses then a 200 succeeds; sleep is awaited between."""
    sleep_mock = AsyncMock()
    monkeypatch.setattr("fc.twingate.client.asyncio.sleep", sleep_mock)
    success = _gql(
        {
            "connectors": {
                "edges": [{"node": _connector_node("c-1", "conn-1", "rn-a")}],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        }
    )
    respx.post(ENDPOINT).mock(
        side_effect=[
            httpx.Response(429, json={}),
            httpx.Response(503, json={}),
            success,
        ]
    )
    async with make_client() as client:
        result = await client.list_connectors()

    assert [c.connector_id for c in result] == ["c-1"]
    assert sleep_mock.await_count == 2  # one sleep per retry


@respx.mock
async def test_backoff_exhausted_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Persistent 503 raises after max_retries with the expected sleep count."""
    sleep_mock = AsyncMock()
    monkeypatch.setattr("fc.twingate.client.asyncio.sleep", sleep_mock)
    respx.post(ENDPOINT).mock(return_value=httpx.Response(503, json={}))

    async with make_client(max_retries=3) as client:
        with pytest.raises(TwingateApiError) as exc_info:
            await client.list_connectors()

    assert exc_info.value.status == 503
    # 3 attempts -> sleeps after attempts 0 and 1, none after the final.
    assert sleep_mock.await_count == 2


@respx.mock
async def test_api_key_never_leaks(monkeypatch: pytest.MonkeyPatch) -> None:
    """The sentinel API key appears in no exception message or captured log."""
    sleep_mock = AsyncMock()
    monkeypatch.setattr("fc.twingate.client.asyncio.sleep", sleep_mock)

    cap = structlog.testing.LogCapture()
    structlog.configure(processors=[cap])

    respx.post(ENDPOINT).mock(
        return_value=httpx.Response(200, json={"errors": [{"message": "boom"}]})
    )
    async with make_client() as client:
        with pytest.raises(TwingateApiError) as exc_info:
            await client.list_connectors()

    # Exception text carries no secret.
    assert SENTINEL_KEY not in str(exc_info.value)
    assert SENTINEL_KEY not in repr(exc_info.value)

    # No captured log line references the key anywhere in its fields.
    for entry in cap.entries:
        assert SENTINEL_KEY not in repr(entry)


@respx.mock
async def test_api_key_is_sent_in_header() -> None:
    """The key is transmitted only via the X-API-KEY header."""
    respx.post(ENDPOINT).mock(
        return_value=_gql(
            {
                "connectors": {
                    "edges": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        )
    )
    async with make_client() as client:
        await client.list_connectors()

    request = respx.calls.last.request
    assert request.headers["X-API-KEY"] == SENTINEL_KEY
    # The key must not be smuggled into the request body.
    assert SENTINEL_KEY not in request.content.decode()


def test_max_retries_below_one_rejected() -> None:
    """max_retries < 1 would never send a request; it is rejected up front."""
    with pytest.raises(ValueError, match="max_retries"):
        make_client(max_retries=0)


@respx.mock
async def test_malformed_connector_node_raises_typed_error() -> None:
    """A connector node missing a required field raises TwingateApiError, not
    a bare KeyError — the 'all API failures are typed' contract holds."""
    respx.post(ENDPOINT).mock(
        return_value=_gql(
            {
                "connectors": {
                    # node is missing the required "name" field.
                    "edges": [{"node": {"id": "c-1", "remoteNetwork": {"id": "rn-a"}}}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        )
    )
    async with make_client() as client:
        with pytest.raises(TwingateApiError) as exc_info:
            await client.list_connectors()
    assert "missing field" in str(exc_info.value)


@respx.mock
async def test_create_connector_missing_entity_id_raises_typed_error() -> None:
    """ok=true but a malformed entity (no id) raises TwingateApiError."""
    respx.post(ENDPOINT).mock(
        return_value=_gql({"connectorCreate": {"ok": True, "error": None, "entity": {"name": "x"}}})
    )
    async with make_client() as client:
        with pytest.raises(TwingateApiError) as exc_info:
            await client.create_connector("rn-a", "x")
    assert exc_info.value.op_name == "CreateConnector"


@respx.mock
async def test_generate_tokens_missing_token_raises_typed_error() -> None:
    """ok=true but a payload missing a token raises TwingateApiError (no KeyError)."""
    respx.post(ENDPOINT).mock(
        return_value=_gql(
            {
                "connectorGenerateTokens": {
                    "ok": True,
                    "error": None,
                    "connectorTokens": {"accessToken": "only-access"},
                }
            }
        )
    )
    async with make_client() as client:
        with pytest.raises(TwingateApiError) as exc_info:
            await client.generate_tokens("c-1")
    assert exc_info.value.op_name == "GenTokens"
