"""Pure policy functions: watermarks, windows, cooldowns, floors, scale-step.

No I/O and no models — every function takes primitives and returns a primitive,
so the whole scaling rule set is exhaustively unit-testable in isolation. The
decider composes these against the resolved per-RN policy and the live
aggregates/cooldown state.

Load semantics are asymmetric and conservative:

* **High** (scale-up trigger): *any* available signal at or above its high
  watermark. Reacting to a single hot dimension is correct — one saturated
  resource is enough reason to add capacity.
* **Low** (scale-down trigger): *every* available signal at or below its low
  watermark, with at least one signal present. Removing capacity demands
  evidence that nothing is busy; a missing signal never counts as "low".

CPU is already per-effective-core normalized (0-100) by the collectors;
throughput is bytes/sec and is converted to Mbps here for watermark comparison.
"""

from datetime import datetime

# 1 byte/sec = 8 bits/sec; 1 Mbit = 1_000_000 bits.
_BITS_PER_BYTE = 8
_BITS_PER_MEGABIT = 1_000_000


def bps_to_mbps(bps: float) -> float:
    """Convert bytes/sec to megabits/sec (the watermark unit).

    Args:
        bps: Throughput in bytes per second.

    Returns:
        The equivalent throughput in megabits per second.
    """
    return bps * _BITS_PER_BYTE / _BITS_PER_MEGABIT


def is_high_load(
    *,
    cpu_norm: float | None,
    throughput_bps: float | None,
    cpu_high_pct: float,
    throughput_high_mbps: float,
) -> bool:
    """Return whether any available signal is at/above its high watermark.

    Args:
        cpu_norm: Average per-effective-core CPU utilization, or ``None`` if no
            CPU signal this window.
        throughput_bps: Average per-connector tunnel throughput in bytes/sec,
            or ``None`` if absent.
        cpu_high_pct: CPU high watermark (normalized percent).
        throughput_high_mbps: Throughput high watermark in Mbps.

    Returns:
        ``True`` if CPU or throughput (whichever is present) crosses its high
        watermark.
    """
    if cpu_norm is not None and cpu_norm >= cpu_high_pct:
        return True
    return throughput_bps is not None and bps_to_mbps(throughput_bps) >= throughput_high_mbps


def count_over_high_watermark(
    *,
    cpu_by_connector: dict[str, float],
    throughput_by_connector_bps: dict[str, float],
    cpu_high_pct: float,
    throughput_high_mbps: float,
) -> tuple[int, float | None]:
    """Count how many connectors are individually over their high watermark.

    A connector is "over the high watermark" when its windowed CPU is at/above
    ``cpu_high_pct`` **or** its windowed throughput (converted to Mbps) is
    at/above ``throughput_high_mbps``. This is the per-connector view the
    sticky-connector ``scale_up_trigger`` modes (``any``/``quorum``) reduce over
    the fleet, in contrast to the fleet-average :func:`is_high_load`.

    Args:
        cpu_by_connector: Map of connector id → its windowed normalized CPU
            (per-effective-core percent). Connectors with no CPU signal are
            simply absent.
        throughput_by_connector_bps: Map of connector id → its windowed
            per-connector throughput in bytes/sec. Absent when no signal.
        cpu_high_pct: CPU high watermark (normalized percent).
        throughput_high_mbps: Throughput high watermark in Mbps.

    Returns:
        A ``(count, hot_connector_max_cpu)`` tuple where ``count`` is the number
        of distinct connectors (across the union of both maps) that are over the
        high watermark, and ``hot_connector_max_cpu`` is the maximum windowed
        CPU value across **all** connectors that have a CPU value, or ``None``
        when no connector reported CPU.
    """
    ids = set(cpu_by_connector) | set(throughput_by_connector_bps)
    count = 0
    for connector_id in ids:
        cpu = cpu_by_connector.get(connector_id)
        throughput_bps = throughput_by_connector_bps.get(connector_id)
        cpu_hot = cpu is not None and cpu >= cpu_high_pct
        throughput_hot = (
            throughput_bps is not None and bps_to_mbps(throughput_bps) >= throughput_high_mbps
        )
        if cpu_hot or throughput_hot:
            count += 1
    hot_connector_max_cpu = max(cpu_by_connector.values()) if cpu_by_connector else None
    return count, hot_connector_max_cpu


def is_low_load(
    *,
    cpu_norm: float | None,
    throughput_bps: float | None,
    cpu_low_pct: float,
    throughput_low_mbps: float,
) -> bool:
    """Return whether all available signals are at/below their low watermarks.

    Requires at least one signal to be present; a fully absent set of signals
    is never "low" (no evidence to justify removing capacity).

    Args:
        cpu_norm: Average per-effective-core CPU utilization, or ``None``.
        throughput_bps: Average per-connector throughput in bytes/sec, or
            ``None``.
        cpu_low_pct: CPU low watermark (normalized percent).
        throughput_low_mbps: Throughput low watermark in Mbps.

    Returns:
        ``True`` only if every present signal is low and at least one is
        present.
    """
    present = False
    if cpu_norm is not None:
        present = True
        if cpu_norm > cpu_low_pct:
            return False
    if throughput_bps is not None:
        present = True
        if bps_to_mbps(throughput_bps) > throughput_low_mbps:
            return False
    return present


def cooldown_remaining(
    last_action_ts: datetime | None, cooldown_seconds: int, now: datetime
) -> float:
    """Return seconds left on a cooldown, or ``0`` if elapsed/never triggered.

    Args:
        last_action_ts: When the last action in this direction occurred, or
            ``None`` if there has been none.
        cooldown_seconds: The configured cooldown length.
        now: The current time.

    Returns:
        Remaining cooldown seconds (``0.0`` when no cooldown applies).
    """
    if last_action_ts is None:
        return 0.0
    elapsed = (now - last_action_ts).total_seconds()
    return max(0.0, cooldown_seconds - elapsed)


def scale_up_count(*, current: int, max_connectors: int, scale_step: int) -> int:
    """Return how many Connectors to add, clamped to the ceiling.

    Args:
        current: Current Connector count in the Remote Network.
        max_connectors: Configured ceiling.
        scale_step: Desired step size.

    Returns:
        The number to add (``0`` when already at/above the ceiling).
    """
    if current >= max_connectors:
        return 0
    return min(scale_step, max_connectors - current)


def floor_fill_count(*, current: int, min_connectors: int, max_connectors: int) -> int:
    """Return how many Connectors to add to restore the redundancy floor.

    Implements active enforcement of Key Design Rule #2: a Remote Network below
    ``min_connectors`` (including an empty one with no seed Connectors) is filled
    up to the floor in a single step, independent of load. The result is clamped
    to ``max_connectors`` as a defensive guard, though config validation already
    guarantees ``min_connectors <= max_connectors``.

    Args:
        current: Current Connector count in the Remote Network.
        min_connectors: Hard redundancy floor (>= 2).
        max_connectors: Configured ceiling.

    Returns:
        The number to add to reach the floor (``0`` when already at/above it).
    """
    if current >= min_connectors:
        return 0
    return min(min_connectors, max_connectors) - current


def scale_down_count(*, current: int, min_connectors: int, scale_step: int) -> int:
    """Return how many Connectors to remove, never breaching the floor.

    Enforces the hard redundancy floor (Key Design Rule #2): the result can
    never drop the count below ``min_connectors``.

    Args:
        current: Current Connector count in the Remote Network.
        min_connectors: Hard floor (>= 2).
        scale_step: Desired step size.

    Returns:
        The number to remove (``0`` when already at/below the floor).
    """
    if current <= min_connectors:
        return 0
    return min(scale_step, current - min_connectors)
