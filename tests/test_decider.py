"""Tests for the decision engine (``fc.engine.decider``).

This is the heart of correctness, so the matrix is exercised explicitly:
scale-up only on sustained high load, scale-down only on sustained low load,
the hard floor and ceiling, up/down cooldown suppression, and no action on
mixed/quiet signals. Each scale metric is reduced over its own window before it
reaches the decider, so the decider receives one CPU value and one throughput
value (Key Design Rule #3, per-metric windows). For health: dead/unhealthy →
restart, repeated restarts → replace, cordoned → skipped, healthy → nothing.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from fc.config import Policy
from fc.engine.decider import decide_health, decide_scale
from fc.models import ConnectorState, ManagedConnector, ScaleDirection
from fc.state import Cooldowns

NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)
NO_COOLDOWN = Cooldowns(last_up_ts=None, last_down_ts=None)

_POLICY: dict[str, Any] = {
    "poll_interval_seconds": 30,
    "connector_image": "twingate/connector:1",
    "collectors": {"docker_stats": True, "stdout_metrics": False},
    "labels": {
        "managed": "twingate.fc.managed",
        "remote_network": "twingate.fc.rn",
        "connector_id": "twingate.fc.connector_id",
    },
    "remote_network_id": "rn-1",
    "remote_network_name": "rn-1",
    "min_connectors": 2,
    "max_connectors": 6,
    "scale_step": 1,
    "scale_metrics": {
        "cpu": {"high_pct": 75.0, "low_pct": 25.0, "window_seconds": 300, "agg": "avg"},
        "throughput": {"high_mbps": 80.0, "low_mbps": 10.0, "window_seconds": 1200, "agg": "avg"},
    },
    # The legacy fleet-average scale-up tests below pass only the fleet means, so
    # the base policy uses the ``mean`` trigger; the sticky-connector modes
    # (any/quorum) are exercised explicitly in their own section.
    "scale_up_trigger": "mean",
    "scale_up_cooldown_seconds": 600,
    "scale_down_cooldown_seconds": 1800,
    "drain_grace_seconds": 120,
    "max_restarts": 3,
    "restart_window_seconds": 600,
    # Disabled here so the existing health tests assert action on the first
    # unhealthy observation; the duration gate is exercised explicitly below.
    "unhealthy_threshold_seconds": 0,
}


def _policy(**overrides: object) -> Policy:
    return Policy.model_validate({**_POLICY, **overrides})


def _connector(
    connector_id: str = "c1",
    *,
    state: ConnectorState | None = ConnectorState.ALIVE,
    docker_health: str | None = "healthy",
    cordoned: bool = False,
    last_heartbeat_at: datetime | None = None,
) -> ManagedConnector:
    return ManagedConnector(
        connector_id=connector_id,
        name=connector_id,
        rn_id="rn-1",
        container_id=f"ctr-{connector_id}",
        twingate_state=state,
        docker_health=docker_health,
        cordoned=cordoned,
        last_heartbeat_at=last_heartbeat_at,
    )


# --- scale up --------------------------------------------------------------


def test_scale_up_on_sustained_high_under_ceiling() -> None:
    decision = decide_scale(
        policy=_policy(),
        cpu_value=85.0,
        throughput_value=None,
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
        cpu_value=85.0,
        throughput_value=None,
        current_count=3,
        cooldowns=cooldowns,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.NONE
    assert decision.metrics["cooldown_seconds_remaining"] == 500.0


def test_scale_up_refused_at_ceiling() -> None:
    decision = decide_scale(
        policy=_policy(),
        cpu_value=85.0,
        throughput_value=None,
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
        cpu_value=10.0,
        throughput_value=12_500_000,
        current_count=2,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.UP


# --- sticky-connector scale-up triggers (2i) -------------------------------
#
# Fleet spread: one hot connector at 100% CPU plus five at 10%. The fleet mean
# (~25%) is below the 75% high watermark, so ``mean`` does not scale up, but
# ``any`` does. ``quorum`` (0.5, 6 connectors → threshold 3) needs >= 3 hot.


def _spread(
    hot: int, *, total: int = 6, hot_cpu: float = 100.0, cool_cpu: float = 10.0
) -> dict[str, float]:
    """Build a per-connector CPU map: ``hot`` connectors hot, the rest cool."""
    return {f"c{i}": (hot_cpu if i < hot else cool_cpu) for i in range(total)}


def test_mean_mode_does_not_scale_up_when_one_hot_diluted() -> None:
    # Fleet mean (50%) sits between the low (25) and high (75) watermarks → mean
    # mode stays put even though one connector is pinned at 100%.
    cpu_by = _spread(hot=1, cool_cpu=40.0)
    mean_cpu = sum(cpu_by.values()) / len(cpu_by)
    decision = decide_scale(
        policy=_policy(scale_up_trigger="mean", max_connectors=12),
        cpu_value=mean_cpu,
        throughput_value=None,
        current_count=6,
        cooldowns=NO_COOLDOWN,
        now=NOW,
        cpu_by_connector=cpu_by,
        throughput_by_connector_bps={},
    )
    assert decision.direction is ScaleDirection.NONE
    assert decision.metrics["connectors_over_high_watermark"] == 1.0
    assert decision.metrics["hot_connector_max"] == 100.0


def test_any_mode_scales_up_on_single_hot_connector() -> None:
    cpu_by = _spread(hot=1)
    decision = decide_scale(
        policy=_policy(scale_up_trigger="any", max_connectors=12),
        cpu_value=sum(cpu_by.values()) / len(cpu_by),
        throughput_value=None,
        current_count=6,
        cooldowns=NO_COOLDOWN,
        now=NOW,
        cpu_by_connector=cpu_by,
        throughput_by_connector_bps={},
    )
    assert decision.direction is ScaleDirection.UP
    assert decision.metrics["connectors_over_high_watermark"] == 1.0
    assert "any:" in decision.reason


def test_quorum_mode_does_not_scale_up_below_threshold() -> None:
    # 0.5 * 6 → threshold 3; only 1 hot → no scale-up. cpu_value (the fleet mean)
    # sits between the watermarks so the conservative scale-down path is also a
    # no-op, isolating the quorum-gate assertion.
    cpu_by = _spread(hot=1)
    decision = decide_scale(
        policy=_policy(scale_up_trigger="quorum", quorum_fraction=0.5, max_connectors=12),
        cpu_value=50.0,
        throughput_value=None,
        current_count=6,
        cooldowns=NO_COOLDOWN,
        now=NOW,
        cpu_by_connector=cpu_by,
        throughput_by_connector_bps={},
    )
    assert decision.direction is ScaleDirection.NONE
    assert decision.metrics["quorum_threshold"] == 3.0
    assert decision.metrics["connectors_over_high_watermark"] == 1.0


def test_quorum_mode_scales_up_at_threshold() -> None:
    # 3 of 6 hot meets the 0.5 quorum threshold (3) → scale up.
    cpu_by = _spread(hot=3)
    decision = decide_scale(
        policy=_policy(scale_up_trigger="quorum", quorum_fraction=0.5, max_connectors=12),
        cpu_value=sum(cpu_by.values()) / len(cpu_by),
        throughput_value=None,
        current_count=6,
        cooldowns=NO_COOLDOWN,
        now=NOW,
        cpu_by_connector=cpu_by,
        throughput_by_connector_bps={},
    )
    assert decision.direction is ScaleDirection.UP
    assert decision.metrics["quorum_threshold"] == 3.0
    assert decision.metrics["connectors_over_high_watermark"] == 3.0
    assert "quorum:" in decision.reason and "3/6" in decision.reason


def test_over_watermark_counts_throughput_too() -> None:
    # A connector hot on throughput alone (100 Mbit/s > 80) counts as over.
    decision = decide_scale(
        policy=_policy(scale_up_trigger="any"),
        cpu_value=10.0,
        throughput_value=None,
        current_count=3,
        cooldowns=NO_COOLDOWN,
        now=NOW,
        cpu_by_connector={"c0": 10.0, "c1": 10.0},
        throughput_by_connector_bps={"c2": 12_500_000.0},
    )
    assert decision.direction is ScaleDirection.UP
    assert decision.metrics["connectors_over_high_watermark"] == 1.0


def test_over_watermark_metrics_present_on_no_action() -> None:
    # Observability: the sticky-connector counts ride along even when quiet.
    decision = decide_scale(
        policy=_policy(scale_up_trigger="quorum"),
        cpu_value=50.0,
        throughput_value=5_000_000,
        current_count=3,
        cooldowns=NO_COOLDOWN,
        now=NOW,
        cpu_by_connector={"c0": 50.0, "c1": 50.0, "c2": 50.0},
        throughput_by_connector_bps={},
    )
    assert decision.direction is ScaleDirection.NONE
    assert decision.metrics["connectors_over_high_watermark"] == 0.0
    assert decision.metrics["hot_connector_max"] == 50.0


def test_scale_down_unchanged_with_sticky_trigger() -> None:
    # Regression: scale-DOWN ignores the per-connector trigger entirely and
    # stays on the conservative fleet-mean low-load path (default quorum mode).
    decision = decide_scale(
        policy=_policy(scale_up_trigger="quorum"),
        cpu_value=10.0,
        throughput_value=100_000,  # 0.8 Mbit/s < 10
        current_count=4,
        cooldowns=NO_COOLDOWN,
        now=NOW,
        cpu_by_connector={"c0": 10.0, "c1": 10.0, "c2": 10.0, "c3": 10.0},
        throughput_by_connector_bps={},
    )
    assert decision.direction is ScaleDirection.DOWN
    assert decision.count == 1
    assert decision.metrics["connectors_over_high_watermark"] == 0.0


# --- floor fill (active redundancy floor) ----------------------------------


def test_floor_fill_from_empty_provisions_to_min() -> None:
    # An empty RN with no load is filled straight up to min_connectors.
    decision = decide_scale(
        policy=_policy(min_connectors=2),
        cpu_value=None,
        throughput_value=None,
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
        cpu_value=None,
        throughput_value=None,
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
        cpu_value=None,
        throughput_value=None,
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
        cpu_value=1.0,
        throughput_value=1_000,
        current_count=1,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.UP
    assert decision.count == 1


def test_no_floor_fill_at_floor() -> None:
    decision = decide_scale(
        policy=_policy(min_connectors=2),
        cpu_value=None,
        throughput_value=None,
        current_count=2,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.NONE


# --- scale down ------------------------------------------------------------


def test_scale_down_on_sustained_low_above_floor() -> None:
    decision = decide_scale(
        policy=_policy(),
        cpu_value=10.0,
        throughput_value=100_000,  # 0.8 Mbit/s < 10
        current_count=4,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.DOWN
    assert decision.count == 1


def test_scale_down_refused_at_floor() -> None:
    decision = decide_scale(
        policy=_policy(),
        cpu_value=10.0,
        throughput_value=100_000,
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
        cpu_value=10.0,
        throughput_value=100_000,
        current_count=4,
        cooldowns=cooldowns,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.NONE
    assert decision.metrics["cooldown_seconds_remaining"] == 1200.0


# --- scale-up precedence over scale-down -----------------------------------


def test_high_cpu_scales_up_even_if_throughput_low() -> None:
    # One hot signal is enough to add capacity; never scale down while hot.
    decision = decide_scale(
        policy=_policy(),
        cpu_value=90.0,
        throughput_value=100_000,  # low throughput
        current_count=4,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.UP


def test_low_requires_all_present_signals_low() -> None:
    # CPU is moderate (above its low watermark) → not "low" → no scale-down.
    decision = decide_scale(
        policy=_policy(),
        cpu_value=40.0,
        throughput_value=100_000,
        current_count=4,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.NONE


# --- no action -------------------------------------------------------------


def test_no_action_on_moderate_load() -> None:
    decision = decide_scale(
        policy=_policy(),
        cpu_value=50.0,
        throughput_value=5_000_000,
        current_count=3,
        cooldowns=NO_COOLDOWN,
        now=NOW,
    )
    assert decision.direction is ScaleDirection.NONE


def test_no_action_when_no_signal() -> None:
    decision = decide_scale(
        policy=_policy(),
        cpu_value=None,
        throughput_value=None,
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


def test_cordoned_unhealthy_connector_skipped() -> None:
    # A cordoned connector is a manual operator hand-off: FC excludes it from
    # all remediation, so an unhealthy cordoned connector yields no action while
    # an otherwise-identical non-cordoned one is restarted.
    cordoned = decide_health(
        policy=_policy(),
        connectors=[
            _connector(state=ConnectorState.DEAD_NO_HEARTBEAT, cordoned=True),
        ],
        restart_counts={},
        now=NOW,
    )
    assert cordoned == []

    not_cordoned = decide_health(
        policy=_policy(),
        connectors=[
            _connector(state=ConnectorState.DEAD_NO_HEARTBEAT, cordoned=False),
        ],
        restart_counts={},
        now=NOW,
    )
    assert [a.kind for a in not_cordoned] == ["restart"]


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


# --- unhealthy-duration gate (2g) ------------------------------------------


def test_unhealthy_below_threshold_skipped() -> None:
    # Unhealthy for only 30s with a 60s gate → no action yet (brief blip).
    actions = decide_health(
        policy=_policy(unhealthy_threshold_seconds=60),
        connectors=[_connector(state=ConnectorState.DEAD_HEARTBEAT_TOO_OLD)],
        restart_counts={},
        now=NOW,
        first_unhealthy={"c1": NOW - timedelta(seconds=30)},
    )
    assert actions == []


def test_unhealthy_past_threshold_acts() -> None:
    # Continuously unhealthy past the 60s gate → restart.
    actions = decide_health(
        policy=_policy(unhealthy_threshold_seconds=60),
        connectors=[_connector(state=ConnectorState.DEAD_HEARTBEAT_TOO_OLD)],
        restart_counts={},
        now=NOW,
        first_unhealthy={"c1": NOW - timedelta(seconds=61)},
    )
    assert [a.kind for a in actions] == ["restart"]


def test_unhealthy_threshold_without_entry_skips() -> None:
    # Gate enabled but no recorded first-unhealthy time (just went unhealthy this
    # cycle) → cannot confirm sustained unhealth → skip.
    actions = decide_health(
        policy=_policy(unhealthy_threshold_seconds=60),
        connectors=[_connector(state=ConnectorState.DEAD_HEARTBEAT_TOO_OLD)],
        restart_counts={},
        now=NOW,
        first_unhealthy={},
    )
    assert actions == []


def test_pending_replace_connector_skipped() -> None:
    # A Connector already mid-replace is not acted on again (no double replace),
    # even though its restart count would otherwise escalate to a replace.
    actions = decide_health(
        policy=_policy(max_restarts=3),
        connectors=[_connector(state=ConnectorState.DEAD_NO_RELAYS)],
        restart_counts={"c1": 3},
        now=NOW,
        pending_replace_ids={"c1"},
    )
    assert actions == []
