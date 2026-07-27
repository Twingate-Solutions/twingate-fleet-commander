"""Tests for :class:`fc.actuator.ecs_actuator.EcsActuator`.

Exercised against a fake aioboto3 session (no AWS SDK installed). Coverage:
provision registers the task def with the prescribed 1 vCPU / 2 GB sizing and
runs a task with the token env override + FC tags; restart reuses the same token
and waits for the old task to reach STOPPED before launching the new one (never
two active), times out rather than risk a duplicate, and re-tags from FC's own
keys (dropping ``aws:`` system tags); deprovision stops the task; list_managed
filters by the managed tag, paginates ``nextToken``, and skips untagged tasks.
Token secrets reach the task env but never an exception message.
"""

from typing import Any

from pydantic import SecretStr

from fc.actuator.base import Actuator
from fc.actuator.ecs_actuator import EcsActuator, EcsActuatorError
from fc.config import Labels
from fc.models import ManagedConnector
from fc.platform import EcsSettings
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


def _settings(**overrides: Any) -> EcsSettings:
    params: dict[str, Any] = {
        "cluster": "fc-cluster",
        "subnets": ["subnet-a"],
        "security_groups": ["sg-1"],
        "region": "us-east-1",
        "log_group": "/fc/connectors",
    }
    params.update(overrides)
    return EcsSettings(**params)


class _FakeEcs:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.register_resp: dict[str, Any] = {
            "taskDefinition": {"taskDefinitionArn": "arn:aws:ecs:::task-definition/fc-connector:1"}
        }
        self.run_resp: dict[str, Any] = {"tasks": [{"taskArn": "arn:task:new"}]}
        self.describe_resp: dict[str, Any] = {"tasks": []}
        # When set, successive describe_tasks calls pop from this queue (last
        # entry repeats) so a test can model RUNNING → STOPPED transitions.
        self.describe_queue: list[dict[str, Any]] = []
        self.list_resp: dict[str, Any] = {"taskArns": []}
        # When set, successive list_tasks calls pop from this queue to model
        # ``nextToken`` pagination.
        self.list_queue: list[dict[str, Any]] = []
        self.run_should_raise = False

    async def register_task_definition(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("register", kwargs))
        return self.register_resp

    async def run_task(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("run", kwargs))
        if self.run_should_raise:
            raise RuntimeError("ecs refused")
        return self.run_resp

    async def describe_tasks(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("describe", kwargs))
        if self.describe_queue:
            return (
                self.describe_queue.pop(0)
                if len(self.describe_queue) > 1
                else self.describe_queue[0]
            )
        return self.describe_resp

    async def stop_task(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("stop", kwargs))
        return {}

    async def list_tasks(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("list", kwargs))
        if self.list_queue:
            return self.list_queue.pop(0) if len(self.list_queue) > 1 else self.list_queue[0]
        return self.list_resp


class _ClientCM:
    def __init__(self, client: _FakeEcs) -> None:
        self._client = client

    async def __aenter__(self) -> _FakeEcs:
        return self._client

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeSession:
    def __init__(self, ecs: _FakeEcs) -> None:
        self._ecs = ecs
        self.client_calls: list[tuple[str, dict[str, Any]]] = []

    def client(self, service: str, **kwargs: Any) -> _ClientCM:
        self.client_calls.append((service, kwargs))
        return _ClientCM(self._ecs)


def _actuator(ecs: _FakeEcs, *, stop_wait_attempts: int = 60, **overrides: Any) -> EcsActuator:
    return EcsActuator(
        _FakeSession(ecs),
        settings=_settings(**overrides),
        network="acme",
        image="ghcr.io/twingate-solutions/twingate-custom-connector-container:latest",
        labels=LABELS,
        stop_poll_interval_seconds=0.0,  # no real sleeps in tests
        stop_wait_attempts=stop_wait_attempts,
    )


def _env_map(env_list: list[dict[str, str]]) -> dict[str, str]:
    return {e["name"]: e["value"] for e in env_list}


def _tag_map(tag_list: list[dict[str, str]]) -> dict[str, str]:
    return {t["key"]: t["value"] for t in tag_list}


def test_actuator_satisfies_protocol() -> None:
    assert isinstance(_actuator(_FakeEcs()), Actuator)


async def test_provision_registers_sizing_and_runs_with_tokens_and_tags() -> None:
    ecs = _FakeEcs()
    actuator = _actuator(ecs)

    task_arn = await actuator.provision(
        rn_id="rn-1", connector_id="cid-1", name="fc-abc123", tokens=_tokens()
    )

    assert task_arn == "arn:task:new"

    register = next(kw for op, kw in ecs.calls if op == "register")
    # Prescribed 1 vCPU / 2 GB (Key Design Rule N2); 1024/2048 is Fargate-valid.
    assert register["cpu"] == "1024"
    assert register["memory"] == "2048"
    assert register["networkMode"] == "awsvpc"
    assert register["containerDefinitions"][0]["image"].endswith(
        "custom-connector-container:latest"
    )
    # Analytics is always-on, baked into the reused task definition's env.
    td_env = _env_map(register["containerDefinitions"][0]["environment"])
    assert td_env["TWINGATE_LOG_ANALYTICS"] == "v2"

    run = next(kw for op, kw in ecs.calls if op == "run")
    assert run["cluster"] == "fc-cluster"
    assert run["count"] == 1
    assert run["startedBy"] == "fc"
    env = _env_map(run["overrides"]["containerOverrides"][0]["environment"])
    assert env["TWINGATE_NETWORK"] == "acme"
    assert env["TWINGATE_ACCESS_TOKEN"] == ACCESS
    assert env["TWINGATE_REFRESH_TOKEN"] == REFRESH
    tags = _tag_map(run["tags"])
    assert tags["twingate.fc.managed"] == "true"
    assert tags["twingate.fc.rn"] == "rn-1"
    assert tags["twingate.fc.connector_id"] == "cid-1"
    assert tags[NAME_TAG] == "fc-abc123"
    net = run["networkConfiguration"]["awsvpcConfiguration"]
    assert net["subnets"] == ["subnet-a"]
    assert net["assignPublicIp"] == "DISABLED"


async def test_provision_enables_init_process_on_task_def() -> None:
    # ECS equivalent of Docker's --init: without it, processes a container script
    # orphans are reparented to the non-reaping connector at PID 1 and pile up as
    # zombies. initProcessEnabled is supported on Fargate as well as EC2.
    ecs = _FakeEcs()
    actuator = _actuator(ecs)

    await actuator.provision(rn_id="rn-1", connector_id="cid-1", name="fc-abc123", tokens=_tokens())

    register = next(kw for op, kw in ecs.calls if op == "register")
    linux_params = register["containerDefinitions"][0]["linuxParameters"]
    assert linux_params == {"initProcessEnabled": True}


async def test_provision_stamps_default_nofile_ulimit_on_task_def() -> None:
    ecs = _FakeEcs()
    actuator = _actuator(ecs)

    await actuator.provision(rn_id="rn-1", connector_id="cid-1", name="fc-abc123", tokens=_tokens())

    register = next(kw for op, kw in ecs.calls if op == "register")
    ulimits = register["containerDefinitions"][0]["ulimits"]
    assert ulimits == [{"name": "nofile", "softLimit": 131072, "hardLimit": 131072}]


async def test_provision_honors_configured_nofile_on_task_def() -> None:
    ecs = _FakeEcs()
    actuator = EcsActuator(
        _FakeSession(ecs),
        settings=_settings(),
        network="acme",
        image="ghcr.io/twingate-solutions/twingate-custom-connector-container:latest",
        labels=LABELS,
        nofile=262144,
    )

    await actuator.provision(rn_id="rn-1", connector_id="cid-1", name="fc-abc123", tokens=_tokens())

    register = next(kw for op, kw in ecs.calls if op == "register")
    ulimits = register["containerDefinitions"][0]["ulimits"]
    assert ulimits == [{"name": "nofile", "softLimit": 262144, "hardLimit": 262144}]


async def test_provision_reuses_one_task_definition() -> None:
    ecs = _FakeEcs()
    actuator = _actuator(ecs)
    await actuator.provision(rn_id="rn-1", connector_id="c1", name="n1", tokens=_tokens())
    await actuator.provision(rn_id="rn-1", connector_id="c2", name="n2", tokens=_tokens())
    # The token is never part of the task def, so one registration serves both.
    assert sum(1 for op, _ in ecs.calls if op == "register") == 1
    assert sum(1 for op, _ in ecs.calls if op == "run") == 2


async def test_provision_failure_raises_without_leaking_tokens() -> None:
    ecs = _FakeEcs()
    ecs.run_should_raise = True
    actuator = _actuator(ecs)
    try:
        await actuator.provision(rn_id="rn-1", connector_id="c1", name="n1", tokens=_tokens())
    except EcsActuatorError as exc:
        assert ACCESS not in str(exc)
        assert REFRESH not in str(exc)
    else:
        raise AssertionError("expected EcsActuatorError")


def _old_task(
    *, last_status: str = "STOPPED", tags: list[dict[str, str]] | None = None
) -> dict[str, Any]:
    return {
        "taskArn": "arn:task:old",
        "taskDefinitionArn": "arn:td:7",
        "lastStatus": last_status,
        "overrides": {
            "containerOverrides": [
                {
                    "name": "connector",
                    "environment": [
                        {"name": "TWINGATE_NETWORK", "value": "acme"},
                        {"name": "TWINGATE_ACCESS_TOKEN", "value": ACCESS},
                        {"name": "TWINGATE_REFRESH_TOKEN", "value": REFRESH},
                    ],
                }
            ]
        },
        "tags": tags
        if tags is not None
        else [{"key": "twingate.fc.connector_id", "value": "cid-1"}],
    }


async def test_restart_reuses_token_and_waits_for_stopped_before_run() -> None:
    ecs = _FakeEcs()
    # First describe reads the env (task RUNNING); the wait poll then sees STOPPED.
    ecs.describe_queue = [
        {"tasks": [_old_task(last_status="RUNNING")]},
        {"tasks": [_old_task(last_status="STOPPED")]},
    ]
    actuator = _actuator(ecs)
    connector = ManagedConnector(
        connector_id="cid-1", name="fc-abc", rn_id="rn-1", container_id="arn:task:old"
    )

    await actuator.restart(connector)

    ops = [op for op, _ in ecs.calls]
    # The old task is stopped, then a STOPPED status is confirmed, BEFORE run.
    assert ops.index("stop") < ops.index("run")
    # A describe (the wait poll) happens after the stop and before the run.
    describe_after_stop = [
        i for i, op in enumerate(ops) if op == "describe" and i > ops.index("stop")
    ]
    assert describe_after_stop and describe_after_stop[0] < ops.index("run")
    stop = next(kw for op, kw in ecs.calls if op == "stop")
    assert stop["task"] == "arn:task:old"
    run = next(kw for op, kw in ecs.calls if op == "run")
    env = _env_map(run["overrides"]["containerOverrides"][0]["environment"])
    # Same token reused on the replacement.
    assert env["TWINGATE_ACCESS_TOKEN"] == ACCESS
    assert env["TWINGATE_REFRESH_TOKEN"] == REFRESH
    assert run["taskDefinition"] == "arn:td:7"


async def test_restart_times_out_when_task_never_stops() -> None:
    ecs = _FakeEcs()
    # The old task stays RUNNING forever: restart must give up rather than launch
    # a second task sharing the token.
    ecs.describe_resp = {"tasks": [_old_task(last_status="RUNNING")]}
    actuator = _actuator(ecs, stop_wait_attempts=3)
    connector = ManagedConnector(
        connector_id="cid-1", name="fc-abc", rn_id="rn-1", container_id="arn:task:old"
    )
    try:
        await actuator.restart(connector)
    except EcsActuatorError as exc:
        assert exc.op == "restart"
    else:
        raise AssertionError("expected EcsActuatorError on stop timeout")
    # Must NOT have launched a replacement while the old task is still active.
    assert not any(op == "run" for op, _ in ecs.calls)


async def test_restart_drops_aws_system_tags() -> None:
    ecs = _FakeEcs()
    ecs.describe_resp = {
        "tasks": [
            _old_task(
                tags=[
                    {"key": "twingate.fc.managed", "value": "true"},
                    {"key": "twingate.fc.connector_id", "value": "cid-1"},
                    {"key": "aws:ecs:clusterName", "value": "fc-cluster"},  # reserved prefix
                ]
            )
        ]
    }
    actuator = _actuator(ecs)
    connector = ManagedConnector(
        connector_id="cid-1", name="fc-abc", rn_id="rn-1", container_id="arn:task:old"
    )
    await actuator.restart(connector)
    run = next(kw for op, kw in ecs.calls if op == "run")
    keys = {t["key"] for t in run["tags"]}
    # RunTask rejects ``aws:``-prefixed tags, so only FC's own keys are re-applied.
    assert "aws:ecs:clusterName" not in keys
    assert "twingate.fc.managed" in keys
    assert "twingate.fc.connector_id" in keys


async def test_restart_without_token_env_refuses() -> None:
    ecs = _FakeEcs()
    ecs.describe_resp = {"tasks": [{"taskArn": "arn:task:old", "overrides": {}}]}
    actuator = _actuator(ecs)
    connector = ManagedConnector(
        connector_id="cid-1", name="n", rn_id="rn-1", container_id="arn:task:old"
    )
    try:
        await actuator.restart(connector)
    except EcsActuatorError as exc:
        assert exc.op == "restart"
    else:
        raise AssertionError("expected EcsActuatorError")
    # Must not have stopped the task when it cannot recover the token.
    assert not any(op == "stop" for op, _ in ecs.calls)


async def test_deprovision_stops_task() -> None:
    ecs = _FakeEcs()
    actuator = _actuator(ecs)
    connector = ManagedConnector(
        connector_id="cid-1", name="n", rn_id="rn-1", container_id="arn:task:old"
    )
    await actuator.deprovision(connector)
    stop = next(kw for op, kw in ecs.calls if op == "stop")
    assert stop["task"] == "arn:task:old"


async def test_deprovision_taskless_is_noop() -> None:
    ecs = _FakeEcs()
    actuator = _actuator(ecs)
    connector = ManagedConnector(connector_id="cid", name="n", rn_id="rn-1", container_id=None)
    await actuator.deprovision(connector)
    assert ecs.calls == []


async def test_list_managed_filters_by_tag_and_maps() -> None:
    ecs = _FakeEcs()
    ecs.list_resp = {"taskArns": ["arn:task:1", "arn:task:2", "arn:task:3"]}
    ecs.describe_resp = {
        "tasks": [
            {
                "taskArn": "arn:task:1",
                "healthStatus": "HEALTHY",
                "tags": [
                    {"key": "twingate.fc.managed", "value": "true"},
                    {"key": "twingate.fc.rn", "value": "rn-1"},
                    {"key": "twingate.fc.connector_id", "value": "cid-1"},
                    {"key": NAME_TAG, "value": "fc-one"},
                ],
            },
            {
                "taskArn": "arn:task:2",
                "healthStatus": "UNHEALTHY",
                "tags": [
                    {"key": "twingate.fc.managed", "value": "true"},
                    {"key": "twingate.fc.connector_id", "value": "cid-2"},
                ],
            },
            {
                "taskArn": "arn:task:3",
                "healthStatus": "HEALTHY",
                "tags": [{"key": "someone-else", "value": "true"}],  # not FC-managed
            },
        ]
    }
    actuator = _actuator(ecs)

    managed = await actuator.list_managed()

    assert len(managed) == 2
    first = managed[0]
    assert first.container_id == "arn:task:1"
    assert first.connector_id == "cid-1"
    assert first.rn_id == "rn-1"
    assert first.name == "fc-one"
    assert first.docker_health == "healthy"
    assert managed[1].docker_health == "unhealthy"

    # ListTasks narrowed by the FC startedBy marker and RUNNING status.
    list_kw = next(kw for op, kw in ecs.calls if op == "list")
    assert list_kw["startedBy"] == "fc"
    assert list_kw["desiredStatus"] == "RUNNING"


async def test_list_managed_empty_returns_empty() -> None:
    ecs = _FakeEcs()
    ecs.list_resp = {"taskArns": []}
    actuator = _actuator(ecs)
    assert await actuator.list_managed() == []
    # No DescribeTasks when there are no tasks.
    assert not any(op == "describe" for op, _ in ecs.calls)


async def test_list_managed_follows_next_token() -> None:
    ecs = _FakeEcs()
    # Two pages of ListTasks, joined before DescribeTasks.
    ecs.list_queue = [
        {"taskArns": ["arn:task:1"], "nextToken": "more"},
        {"taskArns": ["arn:task:2"]},
    ]
    ecs.describe_resp = {
        "tasks": [
            {
                "taskArn": "arn:task:1",
                "tags": [
                    {"key": "twingate.fc.managed", "value": "true"},
                    {"key": "twingate.fc.connector_id", "value": "cid-1"},
                ],
            },
            {
                "taskArn": "arn:task:2",
                "tags": [
                    {"key": "twingate.fc.managed", "value": "true"},
                    {"key": "twingate.fc.connector_id", "value": "cid-2"},
                ],
            },
        ]
    }
    actuator = _actuator(ecs)
    managed = await actuator.list_managed()
    assert {m.connector_id for m in managed} == {"cid-1", "cid-2"}
    # The second ListTasks page was fetched via nextToken.
    list_calls = [kw for op, kw in ecs.calls if op == "list"]
    assert len(list_calls) == 2
    assert list_calls[1]["nextToken"] == "more"


async def test_list_managed_skips_untagged_connector_id() -> None:
    ecs = _FakeEcs()
    ecs.list_resp = {"taskArns": ["arn:task:1", "arn:task:2"]}
    ecs.describe_resp = {
        "tasks": [
            {
                "taskArn": "arn:task:1",
                "tags": [
                    {"key": "twingate.fc.managed", "value": "true"},
                    {"key": "twingate.fc.connector_id", "value": "cid-1"},
                ],
            },
            {
                # Managed but missing the connector_id tag — must be skipped, not
                # collapsed onto an empty id.
                "taskArn": "arn:task:2",
                "tags": [{"key": "twingate.fc.managed", "value": "true"}],
            },
        ]
    }
    actuator = _actuator(ecs)
    managed = await actuator.list_managed()
    assert [m.connector_id for m in managed] == ["cid-1"]
