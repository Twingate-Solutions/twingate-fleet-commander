"""Domain models (Pydantic v2): connectors, samples, decisions, action records.

These are the authoritative in-memory shapes that flow through the control
loop: collectors emit :class:`ResourceSample`, discovery yields
:class:`ManagedConnector`, the aggregator reduces samples into per-metric
windowed values, and the decider produces :class:`ScaleDecision` and
:class:`HealthAction` objects that the actuator executes and the state store
records as :class:`ActionRecord` rows.

No secret material ever lives on these models; tokens and the API key are
write-only into Docker env and the GraphQL auth header.
"""

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel


class ConnectorState(StrEnum):
    """Twingate-reported liveness state of a Connector.

    Values mirror the GraphQL ``Connector.state`` enum verbatim so that a
    string returned by the Admin API can be coerced directly.
    """

    ALIVE = "ALIVE"
    DEAD_NO_HEARTBEAT = "DEAD_NO_HEARTBEAT"
    DEAD_HEARTBEAT_TOO_OLD = "DEAD_HEARTBEAT_TOO_OLD"
    DEAD_NO_RELAYS = "DEAD_NO_RELAYS"


class CollectorSource(StrEnum):
    """Identifies which collector produced a :class:`ResourceSample`."""

    DOCKER_STATS = "docker_stats"
    STDOUT_METRICS = "stdout_metrics"
    # Cloud log-based collectors: the custom image emits the same ``[metrics]``
    # stdout lines on every platform; on the cloud backends FC reads them from
    # the platform's log service instead of the Docker log API.
    CLOUDWATCH_LOGS = "cloudwatch_logs"
    AZURE_MONITOR = "azure_monitor"


class ResourceSample(BaseModel):
    """A single point-in-time signal for one Connector from one collector.

    CPU is already normalized to per-effective-core utilization (0..100) by
    the collector. Memory fields are advisory and may be ``None`` when the
    container has no memory limit. Throughput is tunnel bytes/sec, derived
    from the connector's stdout metrics (custom image) or a docker_stats
    NIC-delta fallback (universal).
    """

    connector_id: str
    source: CollectorSource
    ts: datetime
    cpu_pct_norm: float | None
    mem_bytes: int | None
    mem_pct: float | None
    throughput_bps: float | None


class ManagedConnector(BaseModel):
    """A Connector under FC management, rediscovered every control cycle.

    A Connector may be logical-only (``container_id is None``) while mid-
    provision, or fully realized with a running container. ``cordoned`` is a
    manual override that excludes it from autoscaling decisions.

    ``docker_health`` is the authoritative ``State.Health.Status`` read from the
    container inspect (``"healthy"``/``"unhealthy"``/``"starting"``), or ``None``
    when the container has no healthcheck; ``docker_failing_streak`` is the
    inspect's consecutive-failure counter, surfaced for observability.
    """

    connector_id: str
    name: str
    rn_id: str
    container_id: str | None = None
    twingate_state: ConnectorState | None = None
    last_heartbeat_at: datetime | None = None
    docker_health: str | None = None
    docker_failing_streak: int | None = None
    cordoned: bool = False


class ScaleDirection(StrEnum):
    """Direction of a scaling decision for a Remote Network."""

    UP = "up"
    DOWN = "down"
    NONE = "none"


class ScaleDecision(BaseModel):
    """The decider's verdict for one Remote Network in one cycle.

    ``count`` is the number of Connectors to add (UP) or remove (DOWN); it is
    ``0`` when ``direction`` is :attr:`ScaleDirection.NONE`. ``metrics`` carries
    the triggering windowed aggregates for audit and logging.
    """

    rn_id: str
    direction: ScaleDirection
    count: int
    reason: str
    metrics: dict[str, float]


class HealthAction(BaseModel):
    """A remediation action for a single unhealthy Connector.

    ``restart`` is attempted first; ``replace`` is only chosen after the
    configured ``max_restarts`` failures inside the restart window.
    """

    connector_id: str
    rn_id: str
    kind: Literal["restart", "replace"]
    reason: str


class ActionRecord(BaseModel):
    """A persisted record of an action FC took, for history and cooldowns.

    Stored in SQLite so decisions and cooldown timers survive a manager
    restart. ``actor`` distinguishes autoscaler actions from manual overrides.
    """

    ts: datetime
    rn_id: str
    action: str
    count: int
    reason: str
    outcome: Literal["success", "fail"]
    actor: Literal["auto", "manual"] = "auto"
