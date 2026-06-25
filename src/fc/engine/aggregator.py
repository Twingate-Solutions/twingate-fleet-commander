"""Sliding-window aggregation over the resource-sample stream.

The aggregator buffers the recent :class:`~fc.models.ResourceSample` stream per
connector and, on demand, reduces a single signal (CPU or throughput) over its
own trailing window using its own aggregation mode. Fleet Commander manages one
Remote Network (Key Design Rule N1), and each scale metric carries its own
window and aggregation (Key Design Rule #3), so the decider asks for one reduced
value per metric per cycle.

Reduction rules mirror the policy's signal semantics:

* **CPU** pools every in-window sample that carries a normalized CPU value
  across all connectors, then reduces that pool by the metric's ``agg``. A
  ``None`` (e.g. a throughput-only sample) is skipped, never treated as zero
  load.
* **Throughput** is reduced per connector first (over the window, by ``agg``)
  and then averaged across connectors, so the result is per-connector tunnel
  throughput regardless of how many samples each connector contributed —
  matching the per-connector throughput watermark.

The ``agg`` mode is one of ``avg`` (arithmetic mean), ``min``, or ``pNN`` (the
NN-th percentile, linear interpolation), validated by the config layer.
"""

from collections.abc import Iterable
from datetime import datetime, timedelta
from math import ceil, floor
from typing import Literal

from fc.models import ResourceSample

#: The signals the aggregator can reduce. Mirrors the scale metrics in the policy.
MetricSignal = Literal["cpu", "throughput"]


def reduce_values(values: list[float], agg: str) -> float | None:
    """Reduce a list of samples to a single value by aggregation mode.

    Args:
        values: The in-window sample values (already filtered to the signal).
        agg: ``avg`` (mean), ``min``, or ``pNN`` (NN-th percentile, 0-100).

    Returns:
        The reduced value, or ``None`` when ``values`` is empty.
    """
    if not values:
        return None
    if agg == "avg":
        return sum(values) / len(values)
    if agg == "min":
        return min(values)
    # ``pNN`` — validated by the config layer, so the slice always parses.
    return _percentile(values, int(agg[1:]))


def _percentile(values: list[float], pct: int) -> float:
    """Return the ``pct``-th percentile via linear interpolation between ranks."""
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100.0) * (len(ordered) - 1)
    low = floor(rank)
    high = ceil(rank)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


class Aggregator:
    """Buffers resource samples per connector and reduces per-metric windows.

    Retains samples for ``retention_seconds`` (set to the longest metric window
    in play) so a single buffer serves every metric's window.
    """

    def __init__(self, *, retention_seconds: int) -> None:
        """Build the aggregator.

        Args:
            retention_seconds: How long to keep samples; should be at least the
                longest metric window so :meth:`prune` never drops data a window
                still needs.
        """
        self._retention_seconds = retention_seconds
        self._samples: dict[str, list[ResourceSample]] = {}

    def ingest(self, samples: Iterable[ResourceSample]) -> None:
        """Add samples to the per-connector buffers.

        Args:
            samples: Newly collected samples (any connector, any source).
        """
        for sample in samples:
            self._samples.setdefault(sample.connector_id, []).append(sample)

    def prune(self, *, now: datetime) -> None:
        """Drop samples older than the retention window.

        Args:
            now: The current time; samples with ``ts`` before
                ``now - retention_seconds`` are removed.
        """
        cutoff = now - timedelta(seconds=self._retention_seconds)
        for connector_id, samples in list(self._samples.items()):
            kept = [s for s in samples if s.ts >= cutoff]
            if kept:
                self._samples[connector_id] = kept
            else:
                del self._samples[connector_id]

    def reduce(
        self,
        connector_ids: Iterable[str],
        *,
        signal: MetricSignal,
        window_seconds: int,
        agg: str,
        now: datetime,
    ) -> float | None:
        """Reduce one signal over its own window for the given connectors.

        Args:
            connector_ids: The connectors that belong to the Remote Network.
            signal: ``"cpu"`` (per-effective-core normalized percent) or
                ``"throughput"`` (per-connector bytes/sec).
            window_seconds: Trailing window length ending at ``now``.
            agg: Aggregation mode (``avg``/``min``/``pNN``).
            now: The window's end (current time).

        Returns:
            The reduced value for the metric, or ``None`` when no sample in the
            window carried that signal.
        """
        cutoff = now - timedelta(seconds=window_seconds)
        ids = set(connector_ids)

        if signal == "cpu":
            cpu_values = [
                sample.cpu_pct_norm
                for connector_id in ids
                for sample in self._samples.get(connector_id, [])
                if sample.ts >= cutoff and sample.cpu_pct_norm is not None
            ]
            return reduce_values(cpu_values, agg)

        # Throughput: reduce each connector's series by ``agg`` over the window,
        # then average across connectors to get per-connector tunnel throughput.
        per_connector: list[float] = []
        for connector_id in ids:
            connector_values = [
                sample.throughput_bps
                for sample in self._samples.get(connector_id, [])
                if sample.ts >= cutoff and sample.throughput_bps is not None
            ]
            reduced = reduce_values(connector_values, agg)
            if reduced is not None:
                per_connector.append(reduced)
        return reduce_values(per_connector, "avg")

    def reduce_per_connector(
        self,
        connector_ids: Iterable[str],
        *,
        signal: MetricSignal,
        window_seconds: int,
        agg: str,
        now: datetime,
    ) -> dict[str, float]:
        """Reduce one signal over its own window, per connector (no averaging).

        Unlike :meth:`reduce` (which pools/averages into a single fleet value),
        this returns each connector's *own* windowed reduced value, so the
        decider can see the per-connector spread and apply the sticky-connector
        ``scale_up_trigger`` (``any``/``quorum``). The same per-connector
        reduction used for throughput in :meth:`reduce` is applied here to CPU
        as well.

        Args:
            connector_ids: The connectors that belong to the Remote Network.
            signal: ``"cpu"`` (per-effective-core normalized percent) or
                ``"throughput"`` (per-connector bytes/sec).
            window_seconds: Trailing window length ending at ``now``.
            agg: Aggregation mode (``avg``/``min``/``pNN``).
            now: The window's end (current time).

        Returns:
            A map of connector id → that connector's reduced windowed value.
            Connectors with no in-window sample carrying the signal are omitted.
        """
        cutoff = now - timedelta(seconds=window_seconds)
        result: dict[str, float] = {}
        for connector_id in set(connector_ids):
            values = [
                value
                for sample in self._samples.get(connector_id, [])
                if sample.ts >= cutoff
                for value in (sample.cpu_pct_norm if signal == "cpu" else sample.throughput_bps,)
                if value is not None
            ]
            reduced = reduce_values(values, agg)
            if reduced is not None:
                result[connector_id] = reduced
        return result
