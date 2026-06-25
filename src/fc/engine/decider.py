"""Combine per-metric windowed signals + policy + state into scale/health actions.

The decider is the safety-rail enforcement point. Given the single managed
Remote Network's policy, the per-metric windowed CPU and throughput values, the
current Connector count, and the persisted cooldown state, it emits exactly one
:class:`~fc.models.ScaleDecision` and a list of
:class:`~fc.models.HealthAction`s for unhealthy Connectors.

Safety rails (CLAUDE.md Key Design Rules #2-#5) enforced here:

* **#2 Hard floor** — scale-down never drops below ``min_connectors``.
* **#3 Per-metric sustained windows + cooldowns** — each scale metric (CPU,
  throughput) is reduced over its own window before comparison, and each
  direction's cooldown can suppress an otherwise-valid action. Scale-up always
  takes precedence over scale-down (never remove capacity while any signal is
  hot).
* **#4 Restart-before-replace** — an unhealthy Connector is restarted until it
  has been restarted ``max_restarts`` times within the restart window, then
  escalated to a replace (the loop provisions the replacement before deleting
  the old one).
* **#5 Tolerate janus** — janus has no lock; FC absorbs the brief container
  recreate of a janus upgrade via the startup-grace and unhealthy-duration
  windows rather than skipping any Connector.

The decider performs no I/O: the loop reduces the sample windows, fetches
cooldowns and restart counts from :class:`~fc.state.StateStore`, and passes them
in, then actuates the returned decisions (including the drain-before-delete
sequence for scale-down).
"""

import math
from datetime import datetime

from fc.config import Policy
from fc.engine import policy as rules
from fc.models import (
    ConnectorState,
    HealthAction,
    ManagedConnector,
    ScaleDecision,
    ScaleDirection,
)
from fc.state import Cooldowns


def _scale_metrics(
    cpu_value: float | None, throughput_value: float | None, current_count: int
) -> dict[str, float]:
    """Build the triggering-metrics dict, including only present signals."""
    metrics: dict[str, float] = {"current_count": float(current_count)}
    if cpu_value is not None:
        metrics["cpu_norm"] = cpu_value
    if throughput_value is not None:
        metrics["throughput_bps"] = throughput_value
    return metrics


def decide_scale(
    *,
    policy: Policy,
    cpu_value: float | None,
    throughput_value: float | None,
    current_count: int,
    cooldowns: Cooldowns,
    now: datetime,
    cpu_by_connector: dict[str, float] | None = None,
    throughput_by_connector_bps: dict[str, float] | None = None,
) -> ScaleDecision:
    """Decide whether to scale the Remote Network up, down, or not at all.

    Args:
        policy: The autoscaling policy (watermarks, bounds, cooldowns).
        cpu_value: Fleet-average per-effective-core normalized CPU, reduced over
            the CPU metric's window and aggregation, or ``None`` if no CPU
            signal. Drives the ``mean`` scale-up mode and the scale-down path.
        throughput_value: Fleet-average per-connector throughput in bytes/sec,
            reduced over the throughput metric's window and aggregation, or
            ``None``. Drives the ``mean`` scale-up mode and the scale-down path.
        current_count: Current Connector count in the Remote Network.
        cooldowns: Persisted last up/down action timestamps.
        now: Current time.
        cpu_by_connector: Per-connector windowed CPU (connector id → normalized
            percent), for the ``any``/``quorum`` scale-up modes. ``None`` is
            treated as an empty map (no connector hot), so callers that pass only
            the fleet means degrade gracefully.
        throughput_by_connector_bps: Per-connector windowed throughput
            (connector id → bytes/sec), for the ``any``/``quorum`` modes.
            ``None`` is treated as empty.

    Returns:
        Exactly one :class:`ScaleDecision`. A :attr:`ScaleDirection.NONE`
        decision still carries a reason; cooldown-suppressed decisions include
        ``cooldown_seconds_remaining`` in ``metrics``. Every decision carries
        ``connectors_over_high_watermark`` (and ``hot_connector_max`` when any
        connector reported CPU) for observability.
    """
    metrics = _scale_metrics(cpu_value, throughput_value, current_count)
    cpu_rule = policy.scale_metrics.cpu
    throughput_rule = policy.scale_metrics.throughput

    # Per-connector over-watermark counts: computed once and recorded on every
    # return path for observability (the sticky-connector signal), even when the
    # active trigger mode does not use them to gate the scale-up.
    cpu_by = cpu_by_connector or {}
    throughput_by = throughput_by_connector_bps or {}
    n_over, hot_max = rules.count_over_high_watermark(
        cpu_by_connector=cpu_by,
        throughput_by_connector_bps=throughput_by,
        cpu_high_pct=cpu_rule.high_pct,
        throughput_high_mbps=throughput_rule.high_mbps,
    )
    metrics["connectors_over_high_watermark"] = float(n_over)
    if hot_max is not None:
        metrics["hot_connector_max"] = hot_max

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
            rn_id=policy.remote_network_id,
            direction=ScaleDirection.UP,
            count=fill,
            reason=(
                f"below floor (current={current_count}, min={policy.min_connectors}) — "
                f"provisioning {fill} to restore redundancy"
            ),
            metrics=metrics,
        )

    # Determine the scale-up trigger by mode (the sticky-connector decision):
    # ``mean`` tests the fleet average (legacy), ``any`` reacts to one hot
    # connector, ``quorum`` (default) needs a fraction of connectors hot.
    match policy.scale_up_trigger:
        case "mean":
            high = rules.is_high_load(
                cpu_norm=cpu_value,
                throughput_bps=throughput_value,
                cpu_high_pct=cpu_rule.high_pct,
                throughput_high_mbps=throughput_rule.high_mbps,
            )
            up_reason = (
                f"sustained high load (mean: cpu {cpu_rule.window_seconds}s/{cpu_rule.agg}, "
                f"throughput {throughput_rule.window_seconds}s/{throughput_rule.agg})"
            )
        case "any":
            high = n_over >= 1
            up_reason = f"sustained high load (any: {n_over} connector(s) over high watermark)"
        case "quorum":
            threshold = max(1, math.ceil(policy.quorum_fraction * current_count))
            metrics["quorum_threshold"] = float(threshold)
            high = n_over >= threshold
            up_reason = (
                f"sustained high load (quorum: {n_over}/{current_count} over high "
                f"watermark, need {threshold})"
            )

    if high:
        count = rules.scale_up_count(
            current=current_count,
            max_connectors=policy.max_connectors,
            scale_step=policy.scale_step,
        )
        if count == 0:
            return ScaleDecision(
                rn_id=policy.remote_network_id,
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
                rn_id=policy.remote_network_id,
                direction=ScaleDirection.NONE,
                count=0,
                reason=f"scale-up suppressed by cooldown ({remaining:.0f}s remaining)",
                metrics=metrics,
            )
        return ScaleDecision(
            rn_id=policy.remote_network_id,
            direction=ScaleDirection.UP,
            count=count,
            reason=up_reason,
            metrics=metrics,
        )

    low = rules.is_low_load(
        cpu_norm=cpu_value,
        throughput_bps=throughput_value,
        cpu_low_pct=cpu_rule.low_pct,
        throughput_low_mbps=throughput_rule.low_mbps,
    )
    if low:
        count = rules.scale_down_count(
            current=current_count,
            min_connectors=policy.min_connectors,
            scale_step=policy.scale_step,
        )
        if count == 0:
            return ScaleDecision(
                rn_id=policy.remote_network_id,
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
                rn_id=policy.remote_network_id,
                direction=ScaleDirection.NONE,
                count=0,
                reason=f"scale-down suppressed by cooldown ({remaining:.0f}s remaining)",
                metrics=metrics,
            )
        return ScaleDecision(
            rn_id=policy.remote_network_id,
            direction=ScaleDirection.DOWN,
            count=count,
            reason=(
                f"sustained low load (cpu {cpu_rule.window_seconds}s/{cpu_rule.agg}, "
                f"throughput {throughput_rule.window_seconds}s/{throughput_rule.agg})"
            ),
            metrics=metrics,
        )

    return ScaleDecision(
        rn_id=policy.remote_network_id,
        direction=ScaleDirection.NONE,
        count=0,
        reason="load within watermarks; no action",
        metrics=metrics,
    )


def is_unhealthy(connector: ManagedConnector) -> bool:
    """Return whether a Connector needs remediation.

    Unhealthy means Twingate reports a non-``ALIVE`` (DEAD_*) state, or the
    container's Docker health is ``unhealthy``. The control loop uses the same
    predicate to track each Connector's first-unhealthy time for the
    unhealthy-duration gate, so the two stay in lock-step.
    """
    state = connector.twingate_state
    if state is not None and state is not ConnectorState.ALIVE:
        return True
    return connector.docker_health == "unhealthy"


def _unhealthy_long_enough(
    connector: ManagedConnector,
    first_unhealthy: dict[str, datetime],
    threshold_seconds: int,
    now: datetime,
) -> bool:
    """Return whether a Connector has been unhealthy past the duration gate.

    ``unhealthy_threshold_seconds`` requires a Connector to be *continuously*
    unhealthy for at least that long before any remediation fires, so a brief
    blip is ignored. With the gate disabled (``threshold_seconds == 0``) every
    unhealthy Connector qualifies immediately. The first-unhealthy timestamps are
    maintained by the loop (reset on recovery); a Connector with no recorded
    timestamp has not yet been observed unhealthy long enough.

    Args:
        connector: The Connector under evaluation.
        first_unhealthy: Map of connector id → the time it first went unhealthy.
        threshold_seconds: The configured continuous-unhealth gate length.
        now: Current time.

    Returns:
        ``True`` if remediation may proceed this cycle.
    """
    if threshold_seconds <= 0:
        return True
    since = first_unhealthy.get(connector.connector_id)
    if since is None:
        return False
    return (now - since).total_seconds() >= threshold_seconds


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
    policy: Policy,
    connectors: list[ManagedConnector],
    restart_counts: dict[str, int],
    now: datetime,
    first_seen: dict[str, datetime] | None = None,
    first_unhealthy: dict[str, datetime] | None = None,
    pending_replace_ids: set[str] | None = None,
) -> list[HealthAction]:
    """Decide health remediation for the Connectors in the Remote Network.

    Args:
        policy: The autoscaling policy (for ``max_restarts``,
            ``startup_grace_seconds`` and ``unhealthy_threshold_seconds``).
        connectors: The Remote Network's Connectors.
        restart_counts: Map of connector id → restart count within the restart
            window (fetched from the state store by the loop).
        now: Current time.
        first_seen: Map of connector id → the time FC first observed it, used to
            apply the startup grace window. When ``None`` (or a Connector is
            absent from it), no grace is applied and the prior behavior holds.
        first_unhealthy: Map of connector id → the time it first went unhealthy
            (reset on recovery by the loop), used to apply the
            ``unhealthy_threshold_seconds`` continuous-unhealth gate.
        pending_replace_ids: Connector ids already mid-replace (a replacement is
            provisioned and FC is waiting for it to become healthy). These are
            skipped so a single unhealthy Connector is not replaced twice.

    Returns:
        One :class:`HealthAction` per unhealthy, non-cordoned
        Connector that is past its startup grace and unhealthy-duration gates and
        not already mid-replace; ``restart`` until ``max_restarts`` is reached in
        the window, then ``replace``. Cordoned Connectors are a manual operator
        hand-off and get no health action at all.
    """
    seen = first_seen or {}
    unhealthy_since = first_unhealthy or {}
    pending = pending_replace_ids or set()
    actions: list[HealthAction] = []
    for connector in connectors:
        if connector.cordoned:
            # Cordon is a manual operator hand-off: FC takes its hands off this
            # Connector entirely, so no restart/replace remediation either.
            continue
        if connector.connector_id in pending:
            # A replacement is already in flight for this Connector; the loop
            # tears it down once the replacement is healthy (Key Design Rule #4).
            continue
        if not is_unhealthy(connector):
            continue
        if _in_startup_grace(connector, seen, policy.startup_grace_seconds, now):
            # Freshly provisioned and not yet heartbeated — give it time to come
            # up rather than restarting a Connector that is starting fine.
            continue
        if not _unhealthy_long_enough(
            connector, unhealthy_since, policy.unhealthy_threshold_seconds, now
        ):
            # Not yet continuously unhealthy past the threshold — a brief blip.
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
