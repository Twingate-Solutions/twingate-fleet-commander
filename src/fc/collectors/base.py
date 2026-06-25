"""The ``Collector`` protocol and the source-independent normalization contract.

Every collector returns a normalized :class:`~fc.models.ResourceSample`. The
normalization contract is the whole point of this layer: regardless of source,

* **CPU** is reported as per-effective-core utilization on a ``0..100`` scale
  (average core utilization), never the raw, unbounded single-core percent that
  Docker and the custom image emit; and
* **throughput** is reported as bytes/sec.

The pure helpers here implement that contract and are shared by the concrete
collectors so the math lives in exactly one tested place. They take primitives
(not models) and never perform I/O, so they are trivially unit-testable.
"""

from collections.abc import Mapping
from datetime import datetime
from typing import Protocol, runtime_checkable

from fc.models import CollectorSource, ManagedConnector, ResourceSample

# Docker reports CPU limits as nanocpus: 1e9 nanocpus == one core.
_NANO_CPUS_PER_CORE = 1_000_000_000.0


class CollectorError(Exception):
    """Raised when a collector cannot produce a sample for one Connector.

    The control loop catches this per Connector/per collector, logs a
    ``collect.error`` event, and proceeds — one failed collection never aborts
    a cycle. The message carries only diagnostic context, never secrets.
    """


@runtime_checkable
class Collector(Protocol):
    """A source of normalized resource samples for a single Connector.

    Implementations are stateful: throughput/CPU-delta collectors retain the
    previous reading per Connector between cycles. :meth:`collect` returns
    ``None`` when no sample can be produced this cycle (e.g. the first reading
    with no prior delta, or an absent endpoint) and raises
    :class:`CollectorError` only on an unexpected failure worth logging.
    """

    source: CollectorSource

    async def collect(self, connector: ManagedConnector) -> ResourceSample | None:
        """Produce one normalized sample for ``connector``, or ``None``."""
        ...


def _clamp_pct(value: float) -> float:
    """Clamp a utilization figure to the documented 0-100 scale.

    Transient cgroup accounting (or a ``cpu_pct`` reported against a larger core
    count than the container's effective cores) can yield a value slightly over
    100 or below 0; the public contract of both normalizers is a 0-100 scale.
    """
    return max(0.0, min(100.0, value))


def normalize_cpu_deltas(*, cpu_delta: float, system_delta: float) -> float | None:
    """Normalize cgroup CPU deltas to per-effective-core utilization (0-100).

    Docker's raw multi-core percent is ``(cpu_delta / system_delta) * online *
    100``; dividing by ``online`` to get average core utilization cancels the
    ``online`` term, leaving ``(cpu_delta / system_delta) * 100``. Because
    ``system_cpu_usage`` already sums across every core, this is correct for any
    core count.

    Args:
        cpu_delta: Change in the container's total CPU usage since the previous
            reading.
        system_delta: Change in total system CPU usage over the same window.

    Returns:
        Average per-core utilization (0-100), or ``None`` when ``system_delta``
        is non-positive (no measurable window, e.g. the first reading).
    """
    if system_delta <= 0:
        return None
    return _clamp_pct((cpu_delta / system_delta) * 100.0)


def normalize_raw_cpu_pct(*, cpu_pct: float, effective_cores: float) -> float | None:
    """Normalize a raw single-core percent to per-effective-core utilization.

    The custom image's ``cpu_pct`` is per single core and unbounded (e.g. ``150``
    means 1.5 cores). Dividing by the container's effective core count yields
    average per-core utilization on the same ``0..100`` scale as
    :func:`normalize_cpu_deltas`.

    Args:
        cpu_pct: Raw per-single-core percent (may exceed 100).
        effective_cores: The container's effective core count.

    Returns:
        Average per-core utilization, or ``None`` when ``effective_cores`` is
        non-positive.
    """
    if effective_cores <= 0:
        return None
    return _clamp_pct(cpu_pct / effective_cores)


def compute_mem_pct(*, usage: int, limit: int | None) -> float | None:
    """Compute memory utilization percent, honoring a nullable limit.

    Memory is advisory only (Key Design Rule #8): when the container has no
    memory limit there is no meaningful percent.

    Args:
        usage: Memory usage in bytes.
        limit: Memory limit in bytes, or ``None``/``0`` when unset.

    Returns:
        ``usage / limit * 100`` when a positive limit is set, else ``None``.
    """
    if not limit or limit <= 0:
        return None
    return usage / limit * 100.0


def throughput_from_totals(
    prev_total: int | None,
    prev_ts: datetime | None,
    curr_total: int,
    curr_ts: datetime,
) -> float | None:
    """Derive bytes/sec from two cumulative byte-total readings.

    Args:
        prev_total: The previous cumulative byte total, or ``None`` on the first
            reading.
        prev_ts: Timestamp of the previous reading, or ``None``.
        curr_total: The current cumulative byte total.
        curr_ts: Timestamp of the current reading.

    Returns:
        Throughput in bytes/sec, or ``None`` when there is no usable prior
        reading, the interval is non-positive, or the counter went backwards
        (a restart/reset, which cannot be trusted as a rate).
    """
    if prev_total is None or prev_ts is None:
        return None
    interval = (curr_ts - prev_ts).total_seconds()
    if interval <= 0:
        return None
    delta = curr_total - prev_total
    if delta < 0:
        return None
    return delta / interval


def effective_cores_from_inspect(inspect: Mapping[str, object], *, host_cpus: int) -> float:
    """Resolve a container's effective core count from a Docker inspect payload.

    Honors an explicit CPU limit when one is set (``NanoCpus`` takes precedence,
    then ``CpuQuota``/``CpuPeriod``); otherwise falls back to the host core
    count, since an unconstrained container can use every core.

    Args:
        inspect: The container inspect dict (as returned by ``container.show``).
        host_cpus: The host's online core count, used when no CPU limit is set.

    Returns:
        The effective core count as a float (always positive).
    """
    host_config = inspect.get("HostConfig")
    if isinstance(host_config, dict):
        nano_cpus = host_config.get("NanoCpus")
        if isinstance(nano_cpus, int) and nano_cpus > 0:
            return nano_cpus / _NANO_CPUS_PER_CORE
        quota = host_config.get("CpuQuota")
        period = host_config.get("CpuPeriod")
        if isinstance(quota, int) and isinstance(period, int) and quota > 0 and period > 0:
            return quota / period
    return float(host_cpus)
