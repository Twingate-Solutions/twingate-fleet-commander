"""Tests for the decision engine (``fc.engine.decider``).

This is the heart of correctness, so the matrix is exercised explicitly:
scale-up only on a sustained high up-window, scale-down only on a sustained low
down-window, the hard floor and ceiling, up/down cooldown suppression, the
asymmetric-window split, and no action on mixed/quiet signals. For health:
dead/unhealthy → restart, repeated restarts → replace, janus-locked → skipped,
healthy → nothing.
"""

from datetime import UTC, datetime, timedelta

from fc.config import ResolvedRemoteNetwork
from fc.engine.aggregator import WindowAggregate
from fc.engine.decider import decide_health, decide_scale
from fc.models import ConnectorState, ManagedConnector, ScaleDirection
from fc.state import Cooldowns

NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)
NO_COOLDOWN = Cooldowns(last_up_ts=None, last_down_ts=None)

_DEFAULTS: dict[str, object] = {
    "id": "rn-1",
    "name": "rn-1",
    "min_connectors": 2,
    "max_connectors": 6,
    "scale_step": 1,
    "cpu_high_pct": 75.0,
    "cpu_low_pct": 25.0,
    "throughput_high_mbps": 80.0,
    "throughput_low_mbps": 10.0,
    "mem_ceiling_bytes": 0,
    "scale_up_window_seconds": 300,
    "scale_down_window_seconds": 1200,
    "scale_up_cooldown_seconds": 600,
    "scale_down_cooldown_seconds": 1800,
    "drain_grace_seconds": 120,
    "max_restarts": 3,
    "restart_window_seconds": 600,
}


def _policy(**overrides: object) -> ResolvedRemoteNetwork:
    return ResolvedRemoteNetwork.model_validate({**_DEFAULTS, **overrides})


def _agg(cpu: float | None = None, throughput: float | None = None) -> WindowAggregate:
    return WindowAggregate(
        avg_cpu_norm=cpu,
        avg_throughput_bps=throughput,
        sample_count=1 if (cpu is not None or throughput is not None) else 0,
        connectors_with_data=1 if throughput is not None else 0,
    )


def _connector(
    connector_id: str = "c1",
    *,
    state: ConnectorState | None = ConnectorState.ALIVE,
    docker_health: str | None = "healthy",
    janus_locked: bool = False,
    last_heartbeat_at: datetime | None = None,
) -> ManagedConnector:
    return ManagedConnector(
        connector_id=connector_id,
        name=connector_id,
        rn_id="rn-1",
        container_id=f"ctr-{connector_id}",
        twingate_state=state,
        docker_health=docker_health,
        janus_locked=janus_locked,
        last_heartbeat_at=last_heartbeat_at,
    )


# --- scale up --------------------------------------------------------------


def test_scale_up_on_sustained_high_under_ceiling() -> None:
    decision = decide_scale(
        policy=_policy(),
        up=_agg(cpu=85.0),
        down=_agg(cpu=85.0),
        current_count=3,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.UP
    assert decision.count == 1


def test_scale_up_suppressed_by_cooldown() -> None:
    cooldowns = Cooldowns(last_up_ts=NOW - timedelta(seconds=100), last_down_ts=None)
    decision = decide_scale(
        policy=_policy(),
        up=_agg(cpu=85.0),
        down=_agg(cpu=85.0),
        current_count=3,
        cooldowns=cooldowns,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.NONE
    assert decision.metrics["cooldown_seconds_remaining"] == 500.0


def test_scale_up_refused_at_ceiling() -> None:
    decision = decide_scale(
        policy=_policy(),
        up=_agg(cpu=85.0),
        down=_agg(cpu=85.0),
        current_count=6,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.NONE
    assert "ceiling" in decision.reason.lower()


def test_scale_up_triggers_on_throughput_alone() -> None:
    # 12.5 MB/s = 100 Mbit/s > 80; CPU quiet.
    decision = decide_scale(
        policy=_policy(),
        up=_agg(cpu=10.0, throughput=12_500_000),
        down=_agg(cpu=10.0, throughput=12_500_000),
        current_count=2,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.UP


# --- floor fill (active redundancy floor) ----------------------------------


def test_floor_fill_from_empty_provisions_to_min() -> None:
    # An empty RN with no load is filled straight up to min_connectors.
    decision = decide_scale(
        policy=_policy(min_connectors=2),
        up=_agg(),
        down=_agg(),
        current_count=0,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.UP
    assert decision.count == 2
    assert "floor" in decision.reason.lower()


def test_floor_fill_partial_below_floor() -> None:
    decision = decide_scale(
        policy=_policy(min_connectors=3),
        up=_agg(),
        down=_agg(),
        current_count=1,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.UP
    assert decision.count == 2  # 3 - 1


def test_floor_fill_ignores_up_cooldown() -> None:
    # Redundancy outranks the anti-flap cooldown: below floor still provisions.
    cooldowns = Cooldowns(last_up_ts=NOW - timedelta(seconds=1), last_down_ts=None)
    decision = decide_scale(
        policy=_policy(min_connectors=2),
        up=_agg(),
        down=_agg(),
        current_count=0,
        cooldowns=cooldowns,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.UP
    assert decision.count == 2


def test_floor_fill_takes_precedence_over_low_load() -> None:
    # Below floor + quiet signals: fill the floor, never emit a scale-down.
    decision = decide_scale(
        policy=_policy(min_connectors=2),
        up=_agg(cpu=1.0, throughput=1_000),
        down=_agg(cpu=1.0, throughput=1_000),
        current_count=1,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.UP
    assert decision.count == 1


def test_no_floor_fill_at_floor() -> None:
    decision = decide_scale(
        policy=_policy(min_connectors=2),
        up=_agg(),
        down=_agg(),
        current_count=2,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.NONE


# --- scale down ------------------------------------------------------------


def test_scale_down_on_sustained_low_above_floor() -> None:
    decision = decide_scale(
        policy=_policy(),
        up=_agg(cpu=10.0, throughput=100_000),
        down=_agg(cpu=10.0, throughput=100_000),  # 0.8 Mbit/s < 10
        current_count=4,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.DOWN
    assert decision.count == 1


def test_scale_down_refused_at_floor() -> None:
    decision = decide_scale(
        policy=_policy(),
        up=_agg(cpu=10.0, throughput=100_000),
        down=_agg(cpu=10.0, throughput=100_000),
        current_count=2,  # already at floor
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.NONE
    assert "floor" in decision.reason.lower()


def test_scale_down_suppressed_by_cooldown() -> None:
    cooldowns = Cooldowns(last_up_ts=None, last_down_ts=NOW - timedelta(seconds=600))
    decision = decide_scale(
        policy=_policy(),
        up=_agg(cpu=10.0, throughput=100_000),
        down=_agg(cpu=10.0, throughput=100_000),
        current_count=4,
        cooldowns=cooldowns,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.NONE
    assert decision.metrics["cooldown_seconds_remaining"] == 1200.0


# --- asymmetric windows ----------------------------------------------------


def test_scale_up_uses_up_window_even_if_down_window_quiet() -> None:
    # Recent spike (short up-window hot) but the long down-window is moderate.
    decision = decide_scale(
        policy=_policy(),
        up=_agg(cpu=90.0),
        down=_agg(cpu=40.0),
        current_count=3,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.UP


def test_no_scale_down_when_only_long_window_low_but_short_busy_does_not_block_up() -> None:
    # Down-window low but up-window high -> up wins (never down while hot).
    decision = decide_scale(
        policy=_policy(),
        up=_agg(cpu=90.0),
        down=_agg(cpu=10.0, throughput=100_000),
        current_count=4,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.UP


# --- no action -------------------------------------------------------------


def test_no_action_on_moderate_load() -> None:
    decision = decide_scale(
        policy=_policy(),
        up=_agg(cpu=50.0, throughput=5_000_000),
        down=_agg(cpu=50.0, throughput=5_000_000),
        current_count=3,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.NONE


def test_no_action_when_no_signal() -> None:
    decision = decide_scale(
        policy=_policy(),
        up=_agg(),
        down=_agg(),
        current_count=3,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.NONE


# --- health ----------------------------------------------------------------


def test_dead_connector_restarts() -> None:
    actions = decide_health(
        policy=_policy(),
        connectors=[_connector(state=ConnectorState.DEAD_NO_HEARTBEAT, docker_health="healthy")],
        restart_counts={},
        now=NOW,
    )
    assert len(actions) == 1
    assert actions[0].kind == "restart"
    assert actions[0].connector_id == "c1"


def test_unhealthy_docker_restarts() -> None:
    actions = decide_health(
        policy=_policy(),
        connectors=[_connector(state=ConnectorState.ALIVE, docker_health="unhealthy")],
        restart_counts={},
        now=NOW,
    )
    assert [a.kind for a in actions] == ["restart"]


def test_repeated_restart_failures_escalate_to_replace() -> None:
    actions = decide_health(
        policy=_policy(max_restarts=3),
        connectors=[_connector(state=ConnectorState.DEAD_NO_RELAYS)],
        restart_counts={"c1": 3},
        now=NOW,
    )
    assert [a.kind for a in actions] == ["replace"]


def test_janus_locked_connector_skipped() -> None:
    actions = decide_health(
        policy=_policy(),
        connectors=[
            _connector(state=ConnectorState.DEAD_NO_HEARTBEAT, janus_locked=True),
        ],
        restart_counts={},
        now=NOW,
    )
    assert actions == []


def test_healthy_connector_no_action() -> None:
    actions = decide_health(
        policy=_policy(),
        connectors=[_connector(state=ConnectorState.ALIVE, docker_health="healthy")],
        restart_counts={},
        now=NOW,
    )
    assert actions == []


# --- startup grace ---------------------------------------------------------


def test_startup_grace_skips_fresh_never_heartbeated_connector() -> None:
    # Just-provisioned: DEAD_NO_HEARTBEAT, never heartbeated, first seen now.
    actions = decide_health(
        policy=_policy(startup_grace_seconds=90),
        connectors=[_connector(state=ConnectorState.DEAD_NO_HEARTBEAT, last_heartbeat_at=None)],
        restart_counts={},
        now=NOW,
        first_seen={"c1": NOW},
    )
    assert actions == []


def test_startup_grace_expires_then_restarts() -> None:
    actions = decide_health(
        policy=_policy(startup_grace_seconds=90),
        connectors=[_connector(state=ConnectorState.DEAD_NO_HEARTBEAT, last_heartbeat_at=None)],
        restart_counts={},
        now=NOW,
        first_seen={"c1": NOW - timedelta(seconds=91)},
    )
    assert [a.kind for a in actions] == ["restart"]


def test_no_grace_for_connector_that_previously_heartbeated() -> None:
    # Had a heartbeat once, now DEAD — genuinely unhealthy, grace must not apply.
    actions = decide_health(
        policy=_policy(startup_grace_seconds=90),
        connectors=[
            _connector(
                state=ConnectorState.DEAD_NO_HEARTBEAT,
                last_heartbeat_at=NOW - timedelta(seconds=5),
            )
        ],
        restart_counts={},
        now=NOW,
        first_seen={"c1": NOW},
    )
    assert [a.kind for a in actions] == ["restart"]


def test_no_grace_for_unhealthy_docker_even_if_fresh() -> None:
    actions = decide_health(
        policy=_policy(startup_grace_seconds=90),
        connectors=[_connector(state=ConnectorState.ALIVE, docker_health="unhealthy")],
        restart_counts={},
        now=NOW,
        first_seen={"c1": NOW},
    )
    assert [a.kind for a in actions] == ["restart"]


def test_no_grace_without_first_seen_entry() -> None:
    # Backward-compatible: no first_seen map → prior behavior (restart).
    actions = decide_health(
        policy=_policy(startup_grace_seconds=90),
        connectors=[_connector(state=ConnectorState.DEAD_NO_HEARTBEAT, last_heartbeat_at=None)],
        restart_counts={},
        now=NOW,
    )
    assert [a.kind for a in actions] == ["restart"]
