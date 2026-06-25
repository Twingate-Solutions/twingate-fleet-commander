"""Tests for the per-RN sliding-window aggregator (``fc.engine.aggregator``).

Covers windowing (recent samples included, stale samples excluded), the short
up-window vs long down-window split, CPU averaging that skips ``None`` (a
missing signal is not zero load), per-connector throughput averaging then
averaging across connectors, the empty case, and pruning of stale samples.
"""

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

from fc.engine.aggregator import Aggregator, WindowAggregate
from fc.models import CollectorSource, ResourceSample

NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)


def _sample(
    connector_id: str,
    *,
    age_seconds: int,
    cpu: float | None = None,
    throughput: float | None = None,
    source: CollectorSource = CollectorSource.DOCKER_STATS,
) -> ResourceSample:
    return ResourceSample(
        connector_id=connector_id,
        source=source,
        ts=NOW - timedelta(seconds=age_seconds),
        cpu_pct_norm=cpu,
        mem_bytes=None,
        mem_pct=None,
        throughput_bps=throughput,
    )


def _agg(samples: Iterable[ResourceSample]) -> Aggregator:
    aggregator = Aggregator(retention_seconds=2000)
    aggregator.ingest(list(samples))
    return aggregator


def test_window_aggregate_averages_cpu_within_window() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=10, cpu=60.0),
            _sample("c1", age_seconds=20, cpu=80.0),
        ]
    )
    result = agg.window_aggregate(["c1"], window_seconds=300, now=NOW)
    assert isinstance(result, WindowAggregate)
    assert result.avg_cpu_norm == 70.0
    assert result.sample_count == 2


def test_window_excludes_stale_samples() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=100, cpu=90.0),  # inside 300s, outside 60s
            _sample("c1", age_seconds=10, cpu=10.0),  # inside both
        ]
    )
    short = agg.window_aggregate(["c1"], window_seconds=60, now=NOW)
    long = agg.window_aggregate(["c1"], window_seconds=300, now=NOW)
    assert short.avg_cpu_norm == 10.0  # only the recent sample
    assert long.avg_cpu_norm == 50.0  # both


def test_cpu_average_skips_none_not_treated_as_zero() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=10, cpu=None, throughput=1000.0),  # prometheus-style
            _sample("c1", age_seconds=20, cpu=80.0),
        ]
    )
    result = agg.window_aggregate(["c1"], window_seconds=300, now=NOW)
    assert result.avg_cpu_norm == 80.0  # the None sample does not drag it to 40


def test_throughput_averaged_per_connector_then_across() -> None:
    # c1 mean throughput = (1000+3000)/2 = 2000; c2 = 4000. Across = 3000.
    agg = _agg(
        [
            _sample("c1", age_seconds=10, throughput=1000.0),
            _sample("c1", age_seconds=20, throughput=3000.0),
            _sample("c2", age_seconds=10, throughput=4000.0),
        ]
    )
    result = agg.window_aggregate(["c1", "c2"], window_seconds=300, now=NOW)
    assert result.avg_throughput_bps == 3000.0
    assert result.connectors_with_data == 2


def test_empty_window_is_all_none() -> None:
    agg = _agg([])
    result = agg.window_aggregate(["c1"], window_seconds=300, now=NOW)
    assert result.avg_cpu_norm is None
    assert result.avg_throughput_bps is None
    assert result.sample_count == 0
    assert result.connectors_with_data == 0


def test_prune_drops_stale_samples() -> None:
    agg = Aggregator(retention_seconds=300)
    agg.ingest([_sample("c1", age_seconds=1000, cpu=99.0)])
    agg.prune(now=NOW)
    result = agg.window_aggregate(["c1"], window_seconds=300, now=NOW)
    assert result.sample_count == 0


def test_window_only_counts_requested_connectors() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=10, cpu=50.0),
            _sample("other", age_seconds=10, cpu=100.0),
        ]
    )
    result = agg.window_aggregate(["c1"], window_seconds=300, now=NOW)
    assert result.avg_cpu_norm == 50.0  # 'other' excluded
