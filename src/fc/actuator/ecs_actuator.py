"""aioboto3 implementation of the ``Actuator`` protocol for AWS ECS.

ECS has no Docker socket and FC's per-connector tokens are single-use, so the
actuator uses the **1:1 task model** (Key Design Rule #1): one ECS *task* per
logical Connector, launched with ``RunTask`` — never an ECS Service with a
``desiredCount`` (which would recycle a task definition's single token across
replacement tasks). The connector tokens travel as a ``RunTask`` *container
override* environment, so the base task definition is registered once and never
embeds a token.

Lifecycle mapping:

* ``provision`` → ``RegisterTaskDefinition`` (once, cached) + ``RunTask`` with
  the token env override, the prescribed 1 vCPU / 2 GB task sizing (Key Design
  Rule N2), and FC tags (the cloud equivalent of the Docker management labels).
* ``restart`` → read the running task's container-override env (the same token),
  ``StopTask`` it, then ``RunTask`` a replacement **with that same token**. The
  old task is stopped *before* the new one starts, so the two never run
  concurrently sharing the token (the amended Rule #1).
* ``deprovision`` → ``StopTask`` (the loop performs ``connectorDelete`` + drain
  around this).
* ``list_managed`` → ``ListTasks`` (narrowed by ``startedBy``) + ``DescribeTasks``
  with tags, filtered by the FC managed tag.

The aioboto3 session is injected; clients are opened per operation via
``session.client("ecs"|"sts")`` async context managers. No AWS SDK symbol is
imported at module load — the session is duck-typed — so the test suite needs no
AWS SDK installed. Tokens are unwrapped from their :class:`~pydantic.SecretStr`
only into the task environment, never into logs or :class:`EcsActuatorError`.
"""

import asyncio
from typing import Any

from fc.actuator.base import ActuatorError
from fc.config import Labels
from fc.models import ManagedConnector
from fc.platform import EcsSettings
from fc.twingate.client import ConnectorTokens

# Marks every FC-launched task so ``ListTasks`` can narrow to FC's own tasks
# before the tag filter (ECS ``ListTasks`` cannot filter by tag directly).
_STARTED_BY = "fc"

# ``DescribeTasks`` accepts at most 100 task ARNs per call, so ``list_managed``
# fans the discovered ARNs out in batches of this size.
_DESCRIBE_BATCH = 100

# ``restart`` stops the old task and must wait for it to actually reach STOPPED
# before relaunching the reused token — ``StopTask`` is asynchronous, so the
# token would otherwise be active in two tasks at once (Key Design Rule #1).
_STOP_POLL_INTERVAL_SECONDS = 2.0
_STOP_WAIT_ATTEMPTS = 60  # ~120s ceiling at the default 2s interval

# Twingate's prescribed per-connector sizing (Key Design Rule N2): 1 vCPU / 2 GB.
# ECS expresses CPU in 1024-units-per-vCPU and memory in MiB. 1024/2048 is a
# valid AWS Fargate CPU/memory combination (the 1-vCPU tier supports 2-8 GB) as
# well as on the EC2 launch type, so no per-launch-type special-casing is needed.
_TASK_CPU_UNITS = 1024
_TASK_MEM_MIB = 2048

# Default per-connector open-file-descriptor limit (see ``Policy.connector_nofile``).
# ~8 FDs per client tunnel, so this bounds concurrent connections per task; set on
# the container definition rather than inherited from the platform default.
_DEFAULT_CONNECTOR_NOFILE = 131072


class EcsActuatorError(ActuatorError):
    """Raised when an ECS lifecycle operation fails (no token ever in message)."""


def _tag_map(tags: object) -> dict[str, str]:
    """Build a ``{key: value}`` map from an ECS tag list (``[{key, value}]``)."""
    result: dict[str, str] = {}
    if isinstance(tags, list):
        for tag in tags:
            if isinstance(tag, dict):
                key = tag.get("key")
                value = tag.get("value")
                if isinstance(key, str) and isinstance(value, str):
                    result[key] = value
    return result


def _health_from_task(task: dict[str, Any]) -> str | None:
    """Map an ECS task ``healthStatus`` onto FC's docker_health vocabulary.

    ECS reports ``HEALTHY``/``UNHEALTHY`` only when the container defines a
    healthcheck; ``UNKNOWN`` (no healthcheck) maps to ``None`` so the Twingate
    liveness state drives the health path instead.
    """
    status = task.get("healthStatus")
    if status == "HEALTHY":
        return "healthy"
    if status == "UNHEALTHY":
        return "unhealthy"
    return None


class EcsActuator:
    """Drives ECS ``RunTask``/``StopTask`` to manage one task per Connector."""

    def __init__(
        self,
        session: Any,
        *,
        settings: EcsSettings,
        network: str,
        image: str,
        labels: Labels,
        nofile: int = _DEFAULT_CONNECTOR_NOFILE,
        stop_poll_interval_seconds: float = _STOP_POLL_INTERVAL_SECONDS,
        stop_wait_attempts: int = _STOP_WAIT_ATTEMPTS,
    ) -> None:
        """Build the actuator.

        Args:
            session: An ``aioboto3.Session`` (duck-typed); ``session.client(...)``
                must return an async-context-manager client.
            settings: The validated ECS placement settings.
            network: Twingate network slug, injected as ``TWINGATE_NETWORK``.
            image: Connector image reference for the task definition.
            labels: The FC identity keys, reused verbatim as ECS tag keys so the
                fleet is rediscovered the same way it is on Docker.
            nofile: Per-connector open-file-descriptor limit stamped as the
                container ``nofile`` ulimit (soft = hard) in the task definition.
            stop_poll_interval_seconds: Seconds between ``DescribeTasks`` polls
                while waiting for a stopped task to reach STOPPED on restart.
            stop_wait_attempts: Maximum number of those polls before a restart
                gives up (and refuses to launch a possible duplicate).
        """
        self._session = session
        self._settings = settings
        self._network = network
        self._image = image
        self._labels = labels
        self._nofile = nofile
        self._stop_poll_interval_seconds = stop_poll_interval_seconds
        self._stop_wait_attempts = stop_wait_attempts
        # Name tag key derived from the managed label's namespace so the cloud
        # tag scheme mirrors the Docker label scheme (e.g. ``twingate.fc.name``).
        prefix = labels.managed.rsplit(".", 1)[0]
        self._name_tag = f"{prefix}.name"
        # Registered lazily on first provision, then reused (the token is never
        # part of the task definition, so one revision serves every connector).
        self._task_def_arn: str | None = None

    def _client(self, service: str) -> Any:
        """Open an async-context-manager client for an AWS service."""
        return self._session.client(service, region_name=self._settings.region)

    # -- provisioning --------------------------------------------------------

    def _token_env(self, tokens: ConnectorTokens) -> list[dict[str, str]]:
        """Build the container-override environment carrying the tokens."""
        return [
            {"name": "TWINGATE_NETWORK", "value": self._network},
            {"name": "TWINGATE_ACCESS_TOKEN", "value": tokens.access_token.get_secret_value()},
            {"name": "TWINGATE_REFRESH_TOKEN", "value": tokens.refresh_token.get_secret_value()},
        ]

    def _tags(self, rn_id: str, connector_id: str, name: str) -> list[dict[str, str]]:
        """Build the FC management tag set for a task."""
        return [
            {"key": self._labels.managed, "value": "true"},
            {"key": self._labels.remote_network, "value": rn_id},
            {"key": self._labels.connector_id, "value": connector_id},
            {"key": self._name_tag, "value": name},
        ]

    def _network_configuration(self) -> dict[str, Any]:
        """Build the awsvpc network configuration for ``RunTask``."""
        return {
            "awsvpcConfiguration": {
                "subnets": list(self._settings.subnets),
                "securityGroups": list(self._settings.security_groups),
                "assignPublicIp": ("ENABLED" if self._settings.assign_public_ip else "DISABLED"),
            }
        }

    async def _ensure_task_def(self, ecs: Any) -> str:
        """Register (once) and return the connector task-definition reference.

        The task definition carries the image, the prescribed 1 vCPU / 2 GB
        sizing, the ``nofile`` ulimit, the awslogs configuration (when a log group
        is set), and a single non-secret ``TWINGATE_NETWORK`` env. Tokens are
        supplied per-run as a container override, so one revision is reused for
        every connector.
        """
        if self._task_def_arn is not None:
            return self._task_def_arn

        container_def: dict[str, Any] = {
            "name": self._settings.container_name,
            "image": self._image,
            "essential": True,
            "environment": [
                {"name": "TWINGATE_NETWORK", "value": self._network},
                # Always-on connector analytics (ANALYTICS stdout traffic lines) so
                # the stdout collector / log-shipper have flow data. Non-secret and
                # static, so it lives in the reused task definition.
                {"name": "TWINGATE_LOG_ANALYTICS", "value": "v2"},
            ],
            # Explicit open-file-descriptor limit (~8 FDs/tunnel) so the connection
            # ceiling per task is deterministic, not inherited from the platform
            # default. Soft = hard so the connector can use it all.
            "ulimits": [{"name": "nofile", "softLimit": self._nofile, "hardLimit": self._nofile}],
        }
        if self._settings.log_group:
            container_def["logConfiguration"] = {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": self._settings.log_group,
                    "awslogs-region": self._settings.region or "",
                    "awslogs-stream-prefix": self._settings.log_stream_prefix,
                },
            }

        params: dict[str, Any] = {
            "family": self._settings.task_family,
            "networkMode": "awsvpc",
            "requiresCompatibilities": [self._settings.launch_type],
            "cpu": str(_TASK_CPU_UNITS),
            "memory": str(_TASK_MEM_MIB),
            "containerDefinitions": [container_def],
        }
        if self._settings.task_role_arn:
            params["taskRoleArn"] = self._settings.task_role_arn
        if self._settings.execution_role_arn:
            params["executionRoleArn"] = self._settings.execution_role_arn

        resp = await ecs.register_task_definition(**params)
        arn = resp["taskDefinition"]["taskDefinitionArn"]
        self._task_def_arn = str(arn)
        return self._task_def_arn

    async def provision(
        self,
        rn_id: str,
        connector_id: str,
        name: str,
        tokens: ConnectorTokens,
    ) -> str:
        """Register the task def (once) and ``RunTask`` one connector task.

        Returns:
            The launched task ARN.

        Raises:
            EcsActuatorError: If task-def registration or ``RunTask`` fails, or
                ``RunTask`` returns no task.
        """
        try:
            async with self._client("ecs") as ecs:
                task_def = await self._ensure_task_def(ecs)
                resp = await ecs.run_task(
                    cluster=self._settings.cluster,
                    taskDefinition=task_def,
                    count=1,
                    launchType=self._settings.launch_type,
                    startedBy=_STARTED_BY,
                    networkConfiguration=self._network_configuration(),
                    overrides={
                        "containerOverrides": [
                            {
                                "name": self._settings.container_name,
                                "environment": self._token_env(tokens),
                            }
                        ]
                    },
                    tags=self._tags(rn_id, connector_id, name),
                )
        except EcsActuatorError:
            raise
        except Exception as exc:
            raise EcsActuatorError("failed to run connector task", op="provision") from exc

        tasks = resp.get("tasks") if isinstance(resp, dict) else None
        if not tasks:
            raise EcsActuatorError("run_task returned no task", op="provision")
        return str(tasks[0]["taskArn"])

    # -- restart -------------------------------------------------------------

    def _env_from_task(self, task: dict[str, Any]) -> list[dict[str, str]]:
        """Recover the connector's container-override env (its tokens) from a task.

        Raises:
            EcsActuatorError: If the task carries no reusable token environment,
                so a restart never silently launches a token-less connector.
        """
        overrides = task.get("overrides") or {}
        for override in overrides.get("containerOverrides") or []:
            if isinstance(override, dict) and override.get("name") == self._settings.container_name:
                env = override.get("environment")
                if env:
                    return list(env)
        raise EcsActuatorError("task has no token environment to reuse", op="restart")

    def _fc_tags_from_task(self, task: dict[str, Any]) -> list[dict[str, str]]:
        """Reselect only FC's own tags from a task (drop ``aws:`` system tags).

        ``RunTask`` rejects any tag with the reserved ``aws:`` prefix, so the
        replacement task is tagged from FC's known keys rather than echoing
        whatever ``DescribeTasks`` returned.
        """
        fc_keys = {
            self._labels.managed,
            self._labels.remote_network,
            self._labels.connector_id,
            self._name_tag,
        }
        tag_map = _tag_map(task.get("tags"))
        return [{"key": k, "value": v} for k, v in tag_map.items() if k in fc_keys]

    async def _wait_until_stopped(self, ecs: Any, task_arn: str) -> None:
        """Poll ``DescribeTasks`` until ``task_arn`` reaches STOPPED (or is gone).

        ``StopTask`` only *initiates* shutdown, so the task lingers (DEACTIVATING
        → STOPPING → STOPPED) for up to its ``stopTimeout``. Waiting here is what
        keeps the single token from being active in two tasks at once.

        Raises:
            EcsActuatorError: If the task has not reached STOPPED within the
                configured attempt budget (the restart then aborts rather than
                launch a possible duplicate).
        """
        for _ in range(self._stop_wait_attempts):
            desc = await ecs.describe_tasks(cluster=self._settings.cluster, tasks=[task_arn])
            tasks = desc.get("tasks") if isinstance(desc, dict) else None
            if not tasks or tasks[0].get("lastStatus") == "STOPPED":
                return
            await asyncio.sleep(self._stop_poll_interval_seconds)
        raise EcsActuatorError("old task did not reach STOPPED before timeout", op="restart")

    async def restart(self, connector: ManagedConnector) -> None:
        """Relaunch the connector's task, reusing the same token (never two active).

        Reads the running task's container-override env (the same token), stops
        the old task, **waits for it to reach STOPPED**, then runs a replacement
        with that env. Because ``StopTask`` is asynchronous, the wait — not the
        mere call order — is what guarantees the token is never active in two
        tasks at once (Key Design Rule #1, amended).

        Raises:
            EcsActuatorError: If the task is missing, carries no reusable token,
                does not stop within the timeout, or any ECS call fails.
        """
        if connector.container_id is None:
            raise EcsActuatorError("cannot restart a task-less connector", op="restart")
        try:
            async with self._client("ecs") as ecs:
                desc = await ecs.describe_tasks(
                    cluster=self._settings.cluster,
                    tasks=[connector.container_id],
                    include=["TAGS"],
                )
                tasks = desc.get("tasks") if isinstance(desc, dict) else None
                if not tasks:
                    raise EcsActuatorError("task not found for restart", op="restart")
                task = tasks[0]
                env = self._env_from_task(task)
                tags = self._fc_tags_from_task(task)
                task_def = task.get("taskDefinitionArn") or await self._ensure_task_def(ecs)

                # Stop the old task and WAIT for it to reach STOPPED before
                # launching the replacement, so the single token is never active
                # in two tasks simultaneously (StopTask is asynchronous).
                await ecs.stop_task(
                    cluster=self._settings.cluster,
                    task=connector.container_id,
                    reason="fc restart",
                )
                await self._wait_until_stopped(ecs, connector.container_id)
                await ecs.run_task(
                    cluster=self._settings.cluster,
                    taskDefinition=task_def,
                    count=1,
                    launchType=self._settings.launch_type,
                    startedBy=_STARTED_BY,
                    networkConfiguration=self._network_configuration(),
                    overrides={
                        "containerOverrides": [
                            {"name": self._settings.container_name, "environment": env}
                        ]
                    },
                    tags=tags,
                )
        except EcsActuatorError:
            raise
        except Exception as exc:
            raise EcsActuatorError("failed to restart connector task", op="restart") from exc

    # -- deprovision ---------------------------------------------------------

    async def deprovision(self, connector: ManagedConnector) -> None:
        """``StopTask`` the connector's task (no-op if task-less).

        Assumes the loop has already performed ``connectorDelete`` + drain.

        Raises:
            EcsActuatorError: If ``StopTask`` fails.
        """
        if connector.container_id is None:
            return
        try:
            async with self._client("ecs") as ecs:
                await ecs.stop_task(
                    cluster=self._settings.cluster,
                    task=connector.container_id,
                    reason="fc deprovision",
                )
        except Exception as exc:
            raise EcsActuatorError("failed to stop connector task", op="deprovision") from exc

    # -- discovery -----------------------------------------------------------

    async def list_managed(self) -> list[ManagedConnector]:
        """List FC-managed running tasks (by tag) as :class:`ManagedConnector`s.

        ``twingate_state`` / ``last_heartbeat_at`` are left ``None`` — they are
        authoritative only from the Twingate API and filled in by the loop's
        discovery join. The task ARN is carried as ``container_id``.

        Raises:
            EcsActuatorError: If the ``ListTasks``/``DescribeTasks`` query fails.
        """
        try:
            async with self._client("ecs") as ecs:
                arns = await self._list_task_arns(ecs)
                if not arns:
                    return []
                tasks: list[dict[str, Any]] = []
                for start in range(0, len(arns), _DESCRIBE_BATCH):
                    batch = arns[start : start + _DESCRIBE_BATCH]
                    desc = await ecs.describe_tasks(
                        cluster=self._settings.cluster,
                        tasks=batch,
                        include=["TAGS"],
                    )
                    tasks.extend(desc.get("tasks") or [] if isinstance(desc, dict) else [])
        except Exception as exc:
            raise EcsActuatorError("failed to list connector tasks", op="list_managed") from exc

        result: list[ManagedConnector] = []
        for task in tasks:
            tags = _tag_map(task.get("tags"))
            if tags.get(self._labels.managed) != "true":
                continue
            connector_id = tags.get(self._labels.connector_id)
            if not connector_id:
                # Managed-but-untagged: skip rather than collapse onto an empty
                # id (FC always stamps the id tag, so this is a foreign task).
                continue
            result.append(
                ManagedConnector(
                    connector_id=connector_id,
                    name=tags.get(self._name_tag, ""),
                    rn_id=tags.get(self._labels.remote_network, ""),
                    container_id=str(task.get("taskArn")) if task.get("taskArn") else None,
                    docker_health=_health_from_task(task),
                    docker_failing_streak=None,
                )
            )
        return result

    async def _list_task_arns(self, ecs: Any) -> list[str]:
        """Return every FC-started RUNNING task ARN, following ``nextToken``."""
        arns: list[str] = []
        next_token: str | None = None
        while True:
            kwargs: dict[str, Any] = {
                "cluster": self._settings.cluster,
                "startedBy": _STARTED_BY,
                "desiredStatus": "RUNNING",
            }
            if next_token:
                kwargs["nextToken"] = next_token
            listed = await ecs.list_tasks(**kwargs)
            if not isinstance(listed, dict):
                break
            arns.extend(listed.get("taskArns") or [])
            next_token = listed.get("nextToken")
            if not next_token:
                break
        return arns
