"""Tests for :class:`fc.actuator.aci_actuator.AciActuator`.

Exercised against an ``httpx.MockTransport`` (no Azure SDK installed) and a stub
token provider. Coverage: provision PUTs a container group with the prescribed
1 vCPU / 2 GB sizing, the token env as ``secureValue``, and FC tags; restart
POSTs ``.../restart`` (same token in place); deprovision DELETEs (404 tolerated);
list_managed filters by the managed tag and maps groups back to
:class:`ManagedConnector`. Bearer auth is attached and tokens never reach an
:class:`AciActuatorError`.
"""

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from fc.actuator.aci_actuator import AciActuator, AciActuatorError
from fc.actuator.base import Actuator
from fc.config import Labels
from fc.models import ManagedConnector
from fc.platform import AciSettings
from fc.twingate.client import ConnectorTokens

LABELS = Labels(
    managed="twingate.fc.managed",
    remote_network="twingate.fc.rn",
    connector_id="twingate.fc.connector_id",
)
NAME_TAG = "twingate.fc.name"
ACCESS = "tg_access_SECRET"
REFRESH = "tg_refresh_SECRET"


def _tokens() -> ConnectorTokens:
    return ConnectorTokens(access_token=SecretStr(ACCESS), refresh_token=SecretStr(REFRESH))


def _settings(**overrides: Any) -> AciSettings:
    params: dict[str, Any] = {
        "subscription_id": "sub-123",
        "resource_group": "fc-rg",
        "region": "eastus",
    }
    params.update(overrides)
    return AciSettings(**params)


async def _token_provider(scope: str) -> str:
    return "fake-bearer"


_Handler = Callable[[httpx.Request], httpx.Response]


class _Recorder:
    """Captures each request and serves a scripted response."""

    def __init__(self, handler: _Handler) -> None:
        self.requests: list[httpx.Request] = []
        self._handler = handler

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._handler(request)


def _client(handler: _Handler) -> tuple[httpx.AsyncClient, _Recorder]:
    recorder = _Recorder(handler)
    return httpx.AsyncClient(transport=httpx.MockTransport(recorder)), recorder


def _actuator(http: httpx.AsyncClient, **overrides: Any) -> AciActuator:
    return AciActuator(
        http,
        _token_provider,
        settings=_settings(**overrides),
        network="acme",
        image="ghcr.io/twingate-solutions/twingate-custom-connector-container:latest",
        labels=LABELS,
    )


def test_actuator_satisfies_protocol() -> None:
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    assert isinstance(_actuator(http), Actuator)


async def test_provision_puts_group_with_sizing_tokens_and_tags() -> None:
    http, rec = _client(lambda r: httpx.Response(201, json={"name": "fc-abc"}))
    actuator = _actuator(http)

    backend_id = await actuator.provision(
        rn_id="rn-1", connector_id="cid-1", name="fc-abc", tokens=_tokens()
    )

    assert backend_id == "fc-abc"
    req = rec.requests[0]
    assert req.method == "PUT"
    assert req.url.path.endswith("/containerGroups/fc-abc")
    assert req.url.params["api-version"] == "2023-05-01"
    assert req.headers["Authorization"] == "Bearer fake-bearer"

    body = json.loads(req.content)
    assert body["location"] == "eastus"
    tags = body["tags"]
    assert tags["twingate.fc.managed"] == "true"
    assert tags["twingate.fc.rn"] == "rn-1"
    assert tags["twingate.fc.connector_id"] == "cid-1"
    assert tags[NAME_TAG] == "fc-abc"

    container = body["properties"]["containers"][0]["properties"]
    # Prescribed 1 vCPU / 2 GB (Key Design Rule N2).
    assert container["resources"]["requests"] == {"cpu": 1.0, "memoryInGB": 2.0}
    env = {e["name"]: e for e in container["environmentVariables"]}
    assert env["TWINGATE_NETWORK"]["value"] == "acme"
    # Analytics is always-on, injected as a plain (non-secret) value.
    assert env["TWINGATE_LOG_ANALYTICS"]["value"] == "v2"
    # Tokens are injected as secureValue (write-only; never read back).
    assert env["TWINGATE_ACCESS_TOKEN"]["secureValue"] == ACCESS
    assert env["TWINGATE_REFRESH_TOKEN"]["secureValue"] == REFRESH
    assert "value" not in env["TWINGATE_ACCESS_TOKEN"]


async def test_provision_includes_subnet_when_set() -> None:
    http, rec = _client(lambda r: httpx.Response(200, json={}))
    actuator = _actuator(http, subnet_id="/subscriptions/sub/.../subnets/s")
    await actuator.provision(rn_id="rn-1", connector_id="c1", name="n1", tokens=_tokens())
    body = json.loads(rec.requests[0].content)
    assert body["properties"]["subnetIds"] == [{"id": "/subscriptions/sub/.../subnets/s"}]


async def test_provision_failure_raises_without_leaking_tokens() -> None:
    http, _ = _client(lambda r: httpx.Response(500, text="boom"))
    actuator = _actuator(http)
    try:
        await actuator.provision(rn_id="rn-1", connector_id="c1", name="n1", tokens=_tokens())
    except AciActuatorError as exc:
        assert ACCESS not in str(exc)
        assert REFRESH not in str(exc)
        assert exc.op == "provision"
    else:
        raise AssertionError("expected AciActuatorError")


async def test_restart_posts_restart_in_place() -> None:
    http, rec = _client(lambda r: httpx.Response(202))
    actuator = _actuator(http)
    connector = ManagedConnector(
        connector_id="cid-1", name="fc-abc", rn_id="rn-1", container_id="fc-abc"
    )

    await actuator.restart(connector)

    req = rec.requests[0]
    assert req.method == "POST"
    assert req.url.path.endswith("/containerGroups/fc-abc/restart")


async def test_restart_groupless_refuses() -> None:
    http, _ = _client(lambda r: httpx.Response(200))
    actuator = _actuator(http)
    connector = ManagedConnector(connector_id="c", name="n", rn_id="rn-1", container_id=None)
    with pytest.raises(AciActuatorError):
        await actuator.restart(connector)


async def test_deprovision_deletes_group() -> None:
    http, rec = _client(lambda r: httpx.Response(202))
    actuator = _actuator(http)
    connector = ManagedConnector(connector_id="cid", name="n", rn_id="rn-1", container_id="fc-abc")
    await actuator.deprovision(connector)
    assert rec.requests[0].method == "DELETE"
    assert rec.requests[0].url.path.endswith("/containerGroups/fc-abc")


async def test_deprovision_404_is_tolerated() -> None:
    http, _ = _client(lambda r: httpx.Response(404))
    actuator = _actuator(http)
    connector = ManagedConnector(connector_id="cid", name="n", rn_id="rn-1", container_id="gone")
    await actuator.deprovision(connector)  # must not raise


async def test_deprovision_groupless_is_noop() -> None:
    http, rec = _client(lambda r: httpx.Response(200))
    actuator = _actuator(http)
    connector = ManagedConnector(connector_id="c", name="n", rn_id="rn-1", container_id=None)
    await actuator.deprovision(connector)
    assert rec.requests == []


async def test_list_managed_filters_by_tag_and_maps() -> None:
    payload = {
        "value": [
            {
                "name": "fc-one",
                "tags": {
                    "twingate.fc.managed": "true",
                    "twingate.fc.rn": "rn-1",
                    "twingate.fc.connector_id": "cid-1",
                    NAME_TAG: "fc-one",
                },
                "properties": {"instanceView": {"state": "Running"}},
            },
            {
                "name": "fc-two",
                "tags": {
                    "twingate.fc.managed": "true",
                    "twingate.fc.connector_id": "cid-2",
                },
                "properties": {"instanceView": {"state": "Failed"}},
            },
            {
                "name": "someone-else",
                "tags": {"owner": "other"},  # not FC-managed
                "properties": {},
            },
        ]
    }
    http, rec = _client(lambda r: httpx.Response(200, json=payload))
    actuator = _actuator(http)

    managed = await actuator.list_managed()

    assert rec.requests[0].method == "GET"
    assert len(managed) == 2
    first = managed[0]
    assert first.container_id == "fc-one"
    assert first.connector_id == "cid-1"
    assert first.rn_id == "rn-1"
    assert first.name == "fc-one"
    assert first.docker_health == "healthy"
    assert managed[1].docker_health == "unhealthy"  # Failed → unhealthy


async def test_list_managed_follows_next_link() -> None:
    page2_url = (
        "https://management.azure.com/subscriptions/sub-123/resourceGroups/fc-rg"
        "/providers/Microsoft.ContainerInstance/containerGroups?api-version=2023-05-01&skiptoken=abc"
    )

    def _handler(request: httpx.Request) -> httpx.Response:
        if "skiptoken" in str(request.url):
            return httpx.Response(
                200,
                json={
                    "value": [
                        {
                            "name": "fc-two",
                            "tags": {
                                "twingate.fc.managed": "true",
                                "twingate.fc.connector_id": "cid-2",
                            },
                            "properties": {"instanceView": {"state": "Running"}},
                        }
                    ]
                },
            )
        return httpx.Response(
            200,
            json={
                "value": [
                    {
                        "name": "fc-one",
                        "tags": {
                            "twingate.fc.managed": "true",
                            "twingate.fc.connector_id": "cid-1",
                        },
                        "properties": {"instanceView": {"state": "Running"}},
                    }
                ],
                "nextLink": page2_url,
            },
        )

    http, rec = _client(_handler)
    actuator = _actuator(http)
    managed = await actuator.list_managed()
    assert {m.connector_id for m in managed} == {"cid-1", "cid-2"}
    # The second page was fetched via nextLink (no duplicated api-version param).
    assert len(rec.requests) == 2
    assert str(rec.requests[1].url).count("api-version=") == 1


async def test_list_managed_skips_untagged_connector_id() -> None:
    payload = {
        "value": [
            {
                "name": "fc-one",
                "tags": {"twingate.fc.managed": "true", "twingate.fc.connector_id": "cid-1"},
                "properties": {"instanceView": {"state": "Running"}},
            },
            {
                # Managed but missing the connector_id tag — skipped, not collapsed.
                "name": "fc-orphan",
                "tags": {"twingate.fc.managed": "true"},
                "properties": {"instanceView": {"state": "Running"}},
            },
        ]
    }
    http, _ = _client(lambda r: httpx.Response(200, json=payload))
    actuator = _actuator(http)
    managed = await actuator.list_managed()
    assert [m.connector_id for m in managed] == ["cid-1"]


async def test_request_normalizes_credential_failure() -> None:
    # A credential-provider failure must surface as AciActuatorError (not a raw
    # exception) so the loop's single backend-error path catches it.
    async def _failing_provider(scope: str) -> str:
        raise RuntimeError("token endpoint unreachable")

    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={})))
    actuator = AciActuator(
        http,
        _failing_provider,
        settings=_settings(),
        network="acme",
        image="img",
        labels=LABELS,
    )
    with pytest.raises(AciActuatorError) as excinfo:
        await actuator.list_managed()
    assert excinfo.value.op == "list_managed"
    assert "token endpoint unreachable" not in str(excinfo.value)
