"""Unit tests for the domain models in :mod:`fc.models`."""

from datetime import UTC, datetime
from typing import Literal

from fc.models import (
    ActionRecord,
    CollectorSource,
    ConnectorState,
    HealthAction,
    ManagedConnector,
    RemoteNetworkView,
    ResourceSample,
    ScaleDecision,
    ScaleDirection,
)


def test_connector_state_mirrors_graphql_strings() -> None:
    """ConnectorState values match the Twingate GraphQL enum verbatim."""
    assert ConnectorState.ALIVE.value == "ALIVE"
    assert ConnectorState.DEAD_NO_HEARTBEAT.value == "DEAD_NO_HEARTBEAT"
    assert ConnectorState.DEAD_HEARTBEAT_TOO_OLD.value == "DEAD_HEARTBEAT_TOO_OLD"
    assert ConnectorState.DEAD_NO_RELAYS.value == "DEAD_NO_RELAYS"
    assert ConnectorState("ALIVE") is ConnectorState.ALIVE


def test_collector_source_values() -> None:
    """CollectorSource enumerates the three collector identifiers."""
    assert CollectorSource.DOCKER_STATS.value == "docker_stats"
    assert CollectorSource.STDOUT_METRICS.value == "stdout_metrics"
    assert CollectorSource.PROMETHEUS.value == "prometheus"


def test_scale_direction_values() -> None:
    """ScaleDirection enumerates up/down/none."""
    assert ScaleDirection.UP.value == "up"
    assert ScaleDirection.DOWN.value == "down"
    assert ScaleDirection.NONE.value == "none"


def test_resource_sample_roundtrip() -> None:
    """A fully-populated ResourceSample round-trips through model_dump."""
    sample = ResourceSample(
        connector_id="c1",
        source=CollectorSource.PROMETHEUS,
        ts=datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC),
        cpu_pct_norm=42.5,
        mem_bytes=1024,
        mem_pct=12.0,
        throughput_bps=1_000_000.0,
    )
    assert ResourceSample(**sample.model_dump()) == sample


def test_resource_sample_accepts_none_fields() -> None:
    """mem/cpu/throughput accept None (no limit / unavailable signal)."""
    sample = ResourceSample(
        connector_id="c1",
        source=CollectorSource.DOCKER_STATS,
        ts=datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC),
        cpu_pct_norm=None,
        mem_bytes=None,
        mem_pct=None,
        throughput_bps=None,
    )
    assert sample.cpu_pct_norm is None
    assert sample.mem_bytes is None
    assert sample.mem_pct is None
    assert sample.throughput_bps is None
    assert ResourceSample(**sample.model_dump()) == sample


def test_managed_connector_defaults() -> None:
    """Optional fields default to None/False as specified."""
    connector = ManagedConnector(connector_id="c1", name="conn-1", rn_id="rn1")
    assert connector.container_id is None
    assert connector.twingate_state is None
    assert connector.last_heartbeat_at is None
    assert connector.docker_health is None
    assert connector.janus_locked is False
    assert connector.cordoned is False
    assert ManagedConnector(**connector.model_dump()) == connector


def test_managed_connector_roundtrip_full() -> None:
    """A fully-populated ManagedConnector round-trips."""
    connector = ManagedConnector(
        connector_id="c1",
        name="conn-1",
        rn_id="rn1",
        container_id="deadbeef",
        twingate_state=ConnectorState.ALIVE,
        last_heartbeat_at=datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC),
        docker_health="healthy",
        janus_locked=True,
        cordoned=True,
    )
    assert ManagedConnector(**connector.model_dump()) == connector


def test_scale_decision_roundtrip() -> None:
    """ScaleDecision round-trips and keeps metrics as dict[str, float]."""
    decision = ScaleDecision(
        rn_id="rn1",
        direction=ScaleDirection.UP,
        count=2,
        reason="cpu sustained above high watermark",
        metrics={"avg_cpu": 88.0, "avg_throughput_mbps": 95.0},
    )
    assert ScaleDecision(**decision.model_dump()) == decision


def test_health_action_roundtrip() -> None:
    """HealthAction round-trips for both kinds."""
    kinds: tuple[Literal["restart", "replace"], ...] = ("restart", "replace")
    for kind in kinds:
        action = HealthAction(
            connector_id="c1",
            rn_id="rn1",
            kind=kind,
            reason="docker health unhealthy",
        )
        assert HealthAction(**action.model_dump()) == action


def test_action_record_roundtrip_and_default_actor() -> None:
    """ActionRecord round-trips; actor defaults to 'auto'."""
    record = ActionRecord(
        ts=datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC),
        rn_id="rn1",
        action="provision",
        count=1,
        reason="scale up",
        outcome="success",
    )
    assert record.actor == "auto"
    assert ActionRecord(**record.model_dump()) == record

    manual = ActionRecord(
        ts=datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC),
        rn_id="rn1",
        action="deprovision",
        count=1,
        reason="manual override",
        outcome="fail",
        actor="manual",
    )
    assert manual.actor == "manual"
    assert ActionRecord(**manual.model_dump()) == manual


def test_remote_network_view_roundtrip() -> None:
    """RemoteNetworkView round-trips with nested connectors and aggregates."""
    view = RemoteNetworkView(
        rn_id="rn1",
        name="aws-prod",
        connectors=[
            ManagedConnector(connector_id="c1", name="conn-1", rn_id="rn1"),
            ManagedConnector(connector_id="c2", name="conn-2", rn_id="rn1"),
        ],
        aggregates={"avg_cpu": 50.0, "count": 2.0},
    )
    assert RemoteNetworkView(**view.model_dump()) == view
