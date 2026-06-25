"""Per-RN sliding windows over resource samples (short up + long down windows).

The aggregator buffers the recent :class:`~fc.models.ResourceSample` stream per
connector and, on demand, reduces an arbitrary time window for a set of
connectors into a :class:`WindowAggregate`. The decider asks for two windows per
Remote Network — the short scale-up window and the long scale-down window — so
the asymmetric, sustained-window triggers (Key Design Rule #3) are computed from
the same buffered history.

Reduction rules mirror the policy's signal semantics:

* **CPU** averages only the samples that actually carry a normalized CPU value;
  a ``None`` (e.g. a Prometheus throughput-only sample) is skipped, never
  treated as zero load.
* **Throughput** is averaged per connector first and then across connectors, so
  the result is per-connector tunnel throughput regardless of how many samples
  each connector contributed — matching the per-connector throughput watermark.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

from fc.models import ManagedConnector, RemoteNetworkView, ResourceSample


@dataclass(frozen=True)
class WindowAggregate:
    """The reduction of one time window for one set of connectors.

    ``avg_cpu_norm`` and ``avg_throughput_bps`` are ``None`` when no sample in
    the window carried that signal.
    """

    avg_cpu_norm: float | None
    avg_throughput_bps: float | None
    sample_count: int
    connectors_with_data: int


def _mean(values: list[float]) -> float | None:
    """Return the arithmetic mean, or ``None`` for an empty list."""
    return sum(values) / len(values) if values else None


class Aggregator:
    """Buffers resource samples per connector and reduces time windows.

    Retains samples for ``retention_seconds`` (set to the longest window in
    play) so a single buffer serves both the up- and down-windows.
    """

    def __init__(self, *, retention_seconds: int) -> None:
        """Build the aggregator.

        Args:
            retention_seconds: How long to keep samples; should be at least the
                longest decision window so :meth:`prune` never drops data a
                window still needs.
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

    def window_aggregate(
        self, connector_ids: Iterable[str], *, window_seconds: int, now: datetime
    ) -> WindowAggregate:
        """Reduce one window for the given connectors.

        Args:
            connector_ids: The connectors that belong to this Remote Network.
            window_seconds: Window length ending at ``now``.
            now: The window's end (current time).

        Returns:
            The :class:`WindowAggregate` for the window.
        """
        cutoff = now - timedelta(seconds=window_seconds)
        ids = set(connector_ids)

        cpu_values: list[float] = []
        per_connector_throughput: list[float] = []
        sample_count = 0

        for connector_id in ids:
            connector_throughput: list[float] = []
            for sample in self._samples.get(connector_id, []):
                if sample.ts < cutoff:
                    continue
                sample_count += 1
                if sample.cpu_pct_norm is not None:
                    cpu_values.append(sample.cpu_pct_norm)
                if sample.throughput_bps is not None:
                    connector_throughput.append(sample.throughput_bps)
            connector_mean = _mean(connector_throughput)
            if connector_mean is not None:
                per_connector_throughput.append(connector_mean)

        return WindowAggregate(
            avg_cpu_norm=_mean(cpu_values),
            avg_throughput_bps=_mean(per_connector_throughput),
            sample_count=sample_count,
            connectors_with_data=len(per_connector_throughput),
        )

    def build_view(
        self,
        *,
        rn_id: str,
        name: str,
        connectors: list[ManagedConnector],
        up_window_seconds: int,
        down_window_seconds: int,
        now: datetime,
    ) -> RemoteNetworkView:
        """Build a :class:`RemoteNetworkView` with both windows' aggregates.

        The aggregates dict carries only the present (non-``None``) numbers, so
        a consumer can use ``.get(...)`` to distinguish "no signal" from a real
        value. Keys: ``up_cpu_norm``, ``up_throughput_bps``, ``down_cpu_norm``,
        ``down_throughput_bps``, and ``connector_count``.

        Args:
            rn_id: Remote Network id.
            name: Remote Network name.
            connectors: The managed Connectors in this Remote Network.
            up_window_seconds: Short scale-up window.
            down_window_seconds: Long scale-down window.
            now: Current time.

        Returns:
            The assembled view.
        """
        ids = [c.connector_id for c in connectors]
        up = self.window_aggregate(ids, window_seconds=up_window_seconds, now=now)
        down = self.window_aggregate(ids, window_seconds=down_window_seconds, now=now)

        aggregates: dict[str, float] = {"connector_count": float(len(connectors))}
        if up.avg_cpu_norm is not None:
            aggregates["up_cpu_norm"] = up.avg_cpu_norm
        if up.avg_throughput_bps is not None:
            aggregates["up_throughput_bps"] = up.avg_throughput_bps
        if down.avg_cpu_norm is not None:
            aggregates["down_cpu_norm"] = down.avg_cpu_norm
        if down.avg_throughput_bps is not None:
            aggregates["down_throughput_bps"] = down.avg_throughput_bps

        return RemoteNetworkView(
            rn_id=rn_id, name=name, connectors=connectors, aggregates=aggregates
        )
