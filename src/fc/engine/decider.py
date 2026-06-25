"""Combine aggregates + policy + state into scale and health actions.

The decider is the safety-rail enforcement point. Given a Remote Network's
resolved policy, its short up-window and long down-window aggregates, the
current Connector count, and the persisted cooldown state, it emits exactly one
:class:`~fc.models.ScaleDecision` per RN and a list of
:class:`~fc.models.HealthAction`s for unhealthy Connectors.

Safety rails (CLAUDE.md Key Design Rules #2-#5) enforced here:

* **#2 Hard floor** — scale-down never drops below ``min_connectors``.
* **#3 Asymmetric, sustained windows + cooldowns** — scale-up is judged on the
  short up-window, scale-down on the long down-window, and each direction's
  cooldown can suppress an otherwise-valid action. Scale-up always takes
  precedence over scale-down (never remove capacity while any signal is hot).
* **#4 Restart-before-replace** — an unhealthy Connector is restarted until it
  has been restarted ``max_restarts`` times within the restart window, then
  escalated to a replace (the loop provisions the replacement before deleting
  the old one).
* **#5 Never fight janus** — a janus-locked Connector gets no health action.

The decider performs no I/O: the loop fetches cooldowns and restart counts from
:class:`~fc.state.StateStore` and passes them in, and actuates the returned
decisions (including the drain-before-delete sequence for scale-down).
"""

from datetime import datetime

from fc.config import ResolvedRemoteNetwork
from fc.engine import policy as rules
from fc.engine.aggregator import WindowAggregate
from fc.models import (
    ConnectorState,
    HealthAction,
    ManagedConnector,
    ScaleDecision,
    ScaleDirection,
)
from fc.state import Cooldowns


def _scale_metrics(
    up: WindowAggregate, down: WindowAggregate, current_count: int
) -> dict[str, float]:
    """Build the triggering-metrics dict, including only present aggregates."""
    metrics: dict[str, float] = {"current_count": float(current_count)}
    if up.avg_cpu_norm is not None:
        metrics["up_cpu_norm"] = up.avg_cpu_norm
    if up.avg_throughput_bps is not None:
        metrics["up_throughput_bps"] = up.avg_throughput_bps
    if down.avg_cpu_norm is not None:
        metrics["down_cpu_norm"] = down.avg_cpu_norm
    if down.avg_throughput_bps is not None:
        metrics["down_throughput_bps"] = down.avg_throughput_bps
    return metrics


def decide_scale(
    *,
    policy: ResolvedRemoteNetwork,
    up: WindowAggregate,
    down: WindowAggregate,
    current_count: int,
    cooldowns: Cooldowns,
    now: datetime,
) -> ScaleDecision:
    """Decide whether to scale a Remote Network up, down, or not at all.

    Args:
        policy: The resolved per-RN policy (watermarks, bounds, cooldowns).
        up: Aggregate over the short scale-up window.
        down: Aggregate over the long scale-down window.
        current_count: Current Connector count in the Remote Network.
        cooldowns: Persisted last up/down action timestamps.
        now: Current time.

    Returns:
        Exactly one :class:`ScaleDecision`. A :attr:`ScaleDirection.NONE`
        decision still carries a reason; cooldown-suppressed decisions include
        ``cooldown_seconds_remaining`` in ``metrics``.
    """
    metrics = _scale_metrics(up, down, current_count)

    # Key Design Rule #2 (active): a Remote Network below its floor — including
    # an empty one with no seed Connectors — is filled up to ``min_connectors``
    # before anything else, independent of load and not gated by the up-cooldown.
    # Restoring redundancy outranks both cost and the cooldown's anti-flap role.
    fill = rules.floor_fill_count(
        current=current_count,
        min_connectors=policy.min_connectors,
        max_connectors=policy.max_connectors,
    )
    if fill > 0:
        return ScaleDecision(
            rn_id=policy.id,
            direction=ScaleDirection.UP,
            count=fill,
            reason=(
                f"below floor (current={current_count}, min={policy.min_connectors}) — "
                f"provisioning {fill} to restore redundancy"
            ),
            metrics=metrics,
        )

    high = rules.is_high_load(
        cpu_norm=up.avg_cpu_norm,
        throughput_bps=up.avg_throughput_bps,
        cpu_high_pct=policy.cpu_high_pct,
        throughput_high_mbps=policy.throughput_high_mbps,
    )
    if high:
        count = rules.scale_up_count(
            current=current_count,
            max_connectors=policy.max_connectors,
            scale_step=policy.scale_step,
        )
        if count == 0:
            return ScaleDecision(
                rn_id=policy.id,
                direction=ScaleDirection.NONE,
                count=0,
                reason=(
                    f"high load but at ceiling (current={current_count}, "
                    f"max={policy.max_connectors})"
                ),
                metrics=metrics,
            )
        remaining = rules.cooldown_remaining(
            cooldowns.last_up_ts, policy.scale_up_cooldown_seconds, now
        )
        if remaining > 0:
            metrics["cooldown_seconds_remaining"] = remaining
            return ScaleDecision(
                rn_id=policy.id,
                direction=ScaleDirection.NONE,
                count=0,
                reason=f"scale-up suppressed by cooldown ({remaining:.0f}s remaining)",
                metrics=metrics,
            )
        return ScaleDecision(
            rn_id=policy.id,
            direction=ScaleDirection.UP,
            count=count,
            reason=f"sustained high load over {policy.scale_up_window_seconds}s up-window",
            metrics=metrics,
        )

    low = rules.is_low_load(
        cpu_norm=down.avg_cpu_norm,
        throughput_bps=down.avg_throughput_bps,
        cpu_low_pct=policy.cpu_low_pct,
        throughput_low_mbps=policy.throughput_low_mbps,
    )
    if low:
        count = rules.scale_down_count(
            current=current_count,
            min_connectors=policy.min_connectors,
            scale_step=policy.scale_step,
        )
        if count == 0:
            return ScaleDecision(
                rn_id=policy.id,
                direction=ScaleDirection.NONE,
                count=0,
                reason=(
                    f"low load but at floor (current={current_count}, min={policy.min_connectors})"
                ),
                metrics=metrics,
            )
        remaining = rules.cooldown_remaining(
            cooldowns.last_down_ts, policy.scale_down_cooldown_seconds, now
        )
        if remaining > 0:
            metrics["cooldown_seconds_remaining"] = remaining
            return ScaleDecision(
                rn_id=policy.id,
                direction=ScaleDirection.NONE,
                count=0,
                reason=f"scale-down suppressed by cooldown ({remaining:.0f}s remaining)",
                metrics=metrics,
            )
        return ScaleDecision(
            rn_id=policy.id,
            direction=ScaleDirection.DOWN,
            count=count,
            reason=f"sustained low load over {policy.scale_down_window_seconds}s down-window",
            metrics=metrics,
        )

    return ScaleDecision(
        rn_id=policy.id,
        direction=ScaleDirection.NONE,
        count=0,
        reason="load within watermarks; no action",
        metrics=metrics,
    )


def _is_unhealthy(connector: ManagedConnector) -> bool:
    """Return whether a Connector needs remediation.

    Unhealthy means Twingate reports a non-``ALIVE`` (DEAD_*) state, or the
    container's Docker health is ``unhealthy``.
    """
    state = connector.twingate_state
    if state is not None and state is not ConnectorState.ALIVE:
        return True
    return connector.docker_health == "unhealthy"


def _in_startup_grace(
    connector: ManagedConnector,
    first_seen: dict[str, datetime],
    grace_seconds: int,
    now: datetime,
) -> bool:
    """Return whether a Connector is still within its startup grace window.

    A freshly provisioned Connector is reported ``DEAD_NO_HEARTBEAT`` by the
    Twingate API for a short window before its first heartbeat registers.
    Remediating it then is counterproductive — a restart resets the connector
    and pushes the first heartbeat further out, risking a restart→replace loop
    on a Connector that is actually coming up fine. Grace applies only to that
    specific case: never-heartbeated (``last_heartbeat_at is None``) and in the
    ``DEAD_NO_HEARTBEAT`` state, and only while the time since FC first saw the
    Connector is under ``grace_seconds``. A Connector that previously had a
    heartbeat, or whose Docker health is failing, is never graced.

    Args:
        connector: The Connector under evaluation.
        first_seen: Map of connector id → the time FC first observed it.
        grace_seconds: The configured startup grace window length.
        now: Current time.

    Returns:
        ``True`` if remediation should be deferred this cycle.
    """
    if grace_seconds <= 0:
        return False
    if connector.twingate_state is not ConnectorState.DEAD_NO_HEARTBEAT:
        return False
    if connector.last_heartbeat_at is not None:
        return False
    if connector.docker_health == "unhealthy":
        return False
    seen_at = first_seen.get(connector.connector_id)
    if seen_at is None:
        return False
    return (now - seen_at).total_seconds() < grace_seconds


def decide_health(
    *,
    policy: ResolvedRemoteNetwork,
    connectors: list[ManagedConnector],
    restart_counts: dict[str, int],
    now: datetime,
    first_seen: dict[str, datetime] | None = None,
) -> list[HealthAction]:
    """Decide health remediation for the Connectors in a Remote Network.

    Args:
        policy: The resolved per-RN policy (for ``max_restarts`` and
            ``startup_grace_seconds``).
        connectors: The Remote Network's Connectors.
        restart_counts: Map of connector id → restart count within the restart
            window (fetched from the state store by the loop).
        now: Current time.
        first_seen: Map of connector id → the time FC first observed it, used to
            apply the startup grace window. When ``None`` (or a Connector is
            absent from it), no grace is applied and the prior behavior holds.

    Returns:
        One :class:`HealthAction` per unhealthy, non-janus-locked Connector not
        in its startup grace window; ``restart`` until ``max_restarts`` is
        reached in the window, then ``replace``.
    """
    seen = first_seen or {}
    actions: list[HealthAction] = []
    for connector in connectors:
        if connector.janus_locked:
            # Key Design Rule #5: never act on a Connector mid-upgrade.
            continue
        if not _is_unhealthy(connector):
            continue
        if _in_startup_grace(connector, seen, policy.startup_grace_seconds, now):
            # Freshly provisioned and not yet heartbeated — give it time to come
            # up rather than restarting a Connector that is starting fine.
            continue

        restarts = restart_counts.get(connector.connector_id, 0)
        state_label = (
            connector.twingate_state.value
            if connector.twingate_state is not None
            else f"docker:{connector.docker_health}"
        )
        if restarts >= policy.max_restarts:
            actions.append(
                HealthAction(
                    connector_id=connector.connector_id,
                    rn_id=connector.rn_id,
                    kind="replace",
                    reason=(
                        f"{state_label}; {restarts} restarts in "
                        f"{policy.restart_window_seconds}s window — replacing"
                    ),
                )
            )
        else:
            actions.append(
                HealthAction(
                    connector_id=connector.connector_id,
                    rn_id=connector.rn_id,
                    kind="restart",
                    reason=f"{state_label}; restart {restarts + 1}/{policy.max_restarts}",
                )
            )
    return actions
