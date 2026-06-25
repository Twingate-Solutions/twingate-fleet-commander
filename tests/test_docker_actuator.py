"""Tests for :class:`fc.actuator.docker_actuator.DockerActuator`.

Exercised against a fake aiodocker surface (no live Docker). Coverage:
provision sets the correct connector env (network + tokens + metrics port),
the FC management labels, the ping-group sysctl, the restart policy, and an
optional memory limit; deprovision stops then removes; restart preserves the
container; list_managed filters by the managed label and maps containers back
to :class:`ManagedConnector`. Token secrets must reach container env but never
an exception message.
"""

import json
from typing import Any

from fc.actuator.base import Actuator
from fc.actuator.docker_actuator import DockerActuator, DockerActuatorError
from fc.config import Labels
from fc.models import ManagedConnector
from fc.twingate.client import ConnectorTokens

LABELS = Labels(
    managed="twingate.fc.managed",
    remote_network="twingate.fc.rn",
    connector_id="twingate.fc.connector_id",
)
ACCESS = "tg_access_SECRET"
REFRESH = "tg_refresh_SECRET"


def _tokens() -> ConnectorTokens:
    from pydantic import SecretStr

    return ConnectorTokens(access_token=SecretStr(ACCESS), refresh_token=SecretStr(REFRESH))


class _RunContainer:
    def __init__(self, container_id: str) -> None:
        self.id = container_id
        self.stopped = False
        self.deleted = False
        self.restarted = False

    async def stop(self, **kwargs: Any) -> None:
        self.stopped = True

    async def delete(self, *, force: bool = False, **kwargs: Any) -> None:
        self.deleted = True

    async def restart(self, **kwargs: Any) -> None:
        self.restarted = True


class _ListContainer:
    """Mimics a listed aiodocker container (summary dict + ``.id``)."""

    def __init__(self, summary: dict[str, Any]) -> None:
        self._summary = summary
        self.id = summary["Id"]

    def __getitem__(self, key: str) -> Any:
        return self._summary[key]


class _FakeContainers:
    def __init__(self) -> None:
        self.run_calls: list[dict[str, Any]] = []
        self.run_names: list[str | None] = []
        self.list_kwargs: dict[str, Any] = {}
        self._existing: dict[str, _RunContainer] = {}
        self._listed: list[_ListContainer] = []
        self.run_should_raise = False

    async def run(self, config: dict[str, Any], *, name: str | None = None) -> _RunContainer:
        if self.run_should_raise:
            raise RuntimeError("daemon refused")
        self.run_calls.append(config)
        self.run_names.append(name)
        container = _RunContainer("new-container-id")
        self._existing[container.id] = container
        return container

    async def get(self, container_id: str, **kwargs: Any) -> _RunContainer:
        if container_id not in self._existing:
            self._existing[container_id] = _RunContainer(container_id)
        return self._existing[container_id]

    async def list(self, **kwargs: Any) -> list[_ListContainer]:
        self.list_kwargs = kwargs
        return self._listed


class _FakeDocker:
    def __init__(self) -> None:
        self.containers = _FakeContainers()


def _actuator(docker: _FakeDocker, **kwargs: Any) -> DockerActuator:
    params: dict[str, Any] = {
        "network": "acme",
        "image": "twingate/connector:1",
        "labels": LABELS,
        "metrics_port": 9999,
    }
    params.update(kwargs)
    return DockerActuator(docker, **params)  # type: ignore[arg-type]


def test_actuator_satisfies_protocol() -> None:
    assert isinstance(_actuator(_FakeDocker()), Actuator)


async def test_provision_sets_env_labels_sysctl_and_restart_policy() -> None:
    docker = _FakeDocker()
    actuator = _actuator(docker)

    container_id = await actuator.provision(
        rn_id="rn-1",
        connector_id="Q29ubmVjdG9yOjE=",
        name="fc-rn-1-abcd",
        tokens=_tokens(),
    )

    assert container_id == "new-container-id"
    assert docker.containers.run_names == ["fc-rn-1-abcd"]
    config = docker.containers.run_calls[0]

    assert config["Image"] == "twingate/connector:1"
    env = config["Env"]
    assert "TWINGATE_NETWORK=acme" in env
    assert f"TWINGATE_ACCESS_TOKEN={ACCESS}" in env
    assert f"TWINGATE_REFRESH_TOKEN={REFRESH}" in env
    assert "TWINGATE_METRICS_PORT=9999" in env

    labels = config["Labels"]
    assert labels["twingate.fc.managed"] == "true"
    assert labels["twingate.fc.rn"] == "rn-1"
    assert labels["twingate.fc.connector_id"] == "Q29ubmVjdG9yOjE="

    host_config = config["HostConfig"]
    assert host_config["Sysctls"]["net.ipv4.ping_group_range"] == "0 2147483647"
    assert host_config["RestartPolicy"] == {"Name": "unless-stopped"}
    assert "Memory" not in host_config  # no limit requested


async def test_provision_applies_optional_memory_limit() -> None:
    docker = _FakeDocker()
    actuator = _actuator(docker)

    await actuator.provision(
        rn_id="rn-1",
        connector_id="cid",
        name="c1",
        tokens=_tokens(),
        mem_limit_bytes=536_870_912,
    )

    assert docker.containers.run_calls[0]["HostConfig"]["Memory"] == 536_870_912


async def test_provision_failure_raises_without_leaking_tokens() -> None:
    docker = _FakeDocker()
    docker.containers.run_should_raise = True
    actuator = _actuator(docker)

    try:
        await actuator.provision(rn_id="rn-1", connector_id="cid", name="c1", tokens=_tokens())
    except DockerActuatorError as exc:
        message = str(exc)
        assert ACCESS not in message
        assert REFRESH not in message
    else:
        raise AssertionError("expected DockerActuatorError")


async def test_deprovision_stops_then_removes_container() -> None:
    docker = _FakeDocker()
    actuator = _actuator(docker)
    connector = ManagedConnector(
        connector_id="cid", name="c1", rn_id="rn-1", container_id="running-1"
    )

    await actuator.deprovision(connector)

    container = docker.containers._existing["running-1"]
    assert container.stopped is True
    assert container.deleted is True


async def test_deprovision_logical_only_is_noop() -> None:
    docker = _FakeDocker()
    actuator = _actuator(docker)
    connector = ManagedConnector(connector_id="cid", name="c1", rn_id="rn-1", container_id=None)

    await actuator.deprovision(connector)  # must not raise

    assert docker.containers._existing == {}


async def test_restart_restarts_container() -> None:
    docker = _FakeDocker()
    actuator = _actuator(docker)
    connector = ManagedConnector(
        connector_id="cid", name="c1", rn_id="rn-1", container_id="running-1"
    )

    await actuator.restart(connector)

    assert docker.containers._existing["running-1"].restarted is True


async def test_list_managed_filters_by_label_and_maps() -> None:
    docker = _FakeDocker()
    docker.containers._listed = [
        _ListContainer(
            {
                "Id": "c-100",
                "Names": ["/fc-rn-1-abcd"],
                "Status": "Up 3 hours (healthy)",
                "Labels": {
                    "twingate.fc.managed": "true",
                    "twingate.fc.rn": "rn-1",
                    "twingate.fc.connector_id": "Q29ubmVjdG9yOjE=",
                },
            }
        ),
        _ListContainer(
            {
                "Id": "c-200",
                "Names": ["/connector-seed-1"],
                "Status": "Up 5 minutes (unhealthy)",
                "Labels": {
                    "twingate.fc.managed": "true",
                    "twingate.fc.rn": "rn-2",
                },
            }
        ),
    ]
    actuator = _actuator(docker)

    managed = await actuator.list_managed()

    # Filter requested the managed label, against all containers.
    assert docker.containers.list_kwargs.get("all") is True
    assert json.loads(docker.containers.list_kwargs["filters"]) == {
        "label": ["twingate.fc.managed=true"]
    }

    assert len(managed) == 2
    first = managed[0]
    assert first.container_id == "c-100"
    assert first.name == "fc-rn-1-abcd"
    assert first.rn_id == "rn-1"
    assert first.connector_id == "Q29ubmVjdG9yOjE="
    assert first.docker_health == "healthy"

    second = managed[1]
    assert second.rn_id == "rn-2"
    assert second.connector_id == ""  # seed has no connector_id label yet
    assert second.docker_health == "unhealthy"
