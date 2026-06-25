"""Tests for the per-metric sliding-window aggregator (``fc.engine.aggregator``).

Covers windowing (recent samples included, stale samples excluded), per-metric
windows, CPU pooling that skips ``None`` (a missing signal is not zero load),
per-connector throughput reduction then averaging across connectors, the empty
case, pruning, and the aggregation modes (``avg``/``min``/``pNN``).
"""

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

import pytest

from fc.engine.aggregator import Aggregator, reduce_values
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


# --- reduce_values helper --------------------------------------------------


def test_reduce_values_empty_is_none() -> None:
    assert reduce_values([], "avg") is None


def test_reduce_values_avg() -> None:
    assert reduce_values([10.0, 20.0, 30.0], "avg") == 20.0


def test_reduce_values_min() -> None:
    assert reduce_values([30.0, 10.0, 20.0], "min") == 10.0


def test_reduce_values_percentile() -> None:
    # p50 of 1..5 (linear interpolation) is the median, 3.0.
    assert reduce_values([1.0, 2.0, 3.0, 4.0, 5.0], "p50") == 3.0
    # p100 is the max; p0 the min.
    assert reduce_values([1.0, 2.0, 3.0, 4.0, 5.0], "p100") == 5.0
    assert reduce_values([1.0, 2.0, 3.0, 4.0, 5.0], "p0") == 1.0


def test_reduce_values_percentile_interpolates() -> None:
    # p90 of 0..10 (11 points) → rank 9.0 → exactly 9.0.
    values = [float(i) for i in range(11)]
    assert reduce_values(values, "p90") == 9.0
    # p95 → rank 9.5 → halfway between 9 and 10.
    assert reduce_values(values, "p95") == 9.5


def test_reduce_values_single_element_percentile() -> None:
    assert reduce_values([42.0], "p95") == 42.0


# --- cpu reduction ---------------------------------------------------------


def test_reduce_cpu_avg_within_window() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=10, cpu=60.0),
            _sample("c1", age_seconds=20, cpu=80.0),
        ]
    )
    assert agg.reduce(["c1"], signal="cpu", window_seconds=300, agg="avg", now=NOW) == 70.0


def test_reduce_cpu_min_within_window() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=10, cpu=60.0),
            _sample("c1", age_seconds=20, cpu=80.0),
        ]
    )
    assert agg.reduce(["c1"], signal="cpu", window_seconds=300, agg="min", now=NOW) == 60.0


def test_reduce_cpu_percentile_pools_across_connectors() -> None:
    # Pool all in-window cpu samples across connectors, then take p50.
    agg = _agg(
        [
            _sample("c1", age_seconds=10, cpu=10.0),
            _sample("c2", age_seconds=10, cpu=50.0),
            _sample("c3", age_seconds=10, cpu=90.0),
        ]
    )
    assert (
        agg.reduce(["c1", "c2", "c3"], signal="cpu", window_seconds=300, agg="p50", now=NOW) == 50.0
    )


def test_window_excludes_stale_samples() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=100, cpu=90.0),  # inside 300s, outside 60s
            _sample("c1", age_seconds=10, cpu=10.0),  # inside both
        ]
    )
    short = agg.reduce(["c1"], signal="cpu", window_seconds=60, agg="avg", now=NOW)
    long = agg.reduce(["c1"], signal="cpu", window_seconds=300, agg="avg", now=NOW)
    assert short == 10.0  # only the recent sample
    assert long == 50.0  # both


def test_cpu_skips_none_not_treated_as_zero() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=10, cpu=None, throughput=1000.0),  # throughput-only sample
            _sample("c1", age_seconds=20, cpu=80.0),
        ]
    )
    # The None sample does not drag the average to 40.
    assert agg.reduce(["c1"], signal="cpu", window_seconds=300, agg="avg", now=NOW) == 80.0


# --- throughput reduction --------------------------------------------------


def test_throughput_reduced_per_connector_then_across() -> None:
    # c1 mean throughput = (1000+3000)/2 = 2000; c2 = 4000. Across mean = 3000.
    agg = _agg(
        [
            _sample("c1", age_seconds=10, throughput=1000.0),
            _sample("c1", age_seconds=20, throughput=3000.0),
            _sample("c2", age_seconds=10, throughput=4000.0),
        ]
    )
    result = agg.reduce(["c1", "c2"], signal="throughput", window_seconds=300, agg="avg", now=NOW)
    assert result == 3000.0


def test_throughput_min_is_per_connector_min_then_mean() -> None:
    # c1 min = 1000; c2 single sample = 4000. Across mean = 2500.
    agg = _agg(
        [
            _sample("c1", age_seconds=10, throughput=1000.0),
            _sample("c1", age_seconds=20, throughput=3000.0),
            _sample("c2", age_seconds=10, throughput=4000.0),
        ]
    )
    result = agg.reduce(["c1", "c2"], signal="throughput", window_seconds=300, agg="min", now=NOW)
    assert result == 2500.0


# --- edge cases ------------------------------------------------------------


@pytest.mark.parametrize("signal", ["cpu", "throughput"])
def test_empty_window_is_none(signal: str) -> None:
    agg = _agg([])
    assert agg.reduce(["c1"], signal=signal, window_seconds=300, agg="avg", now=NOW) is None  # type: ignore[arg-type]


def test_prune_drops_stale_samples() -> None:
    agg = Aggregator(retention_seconds=300)
    agg.ingest([_sample("c1", age_seconds=1000, cpu=99.0)])
    agg.prune(now=NOW)
    assert agg.reduce(["c1"], signal="cpu", window_seconds=300, agg="avg", now=NOW) is None


def test_reduce_only_counts_requested_connectors() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=10, cpu=50.0),
            _sample("other", age_seconds=10, cpu=100.0),
        ]
    )
    # 'other' is excluded.
    assert agg.reduce(["c1"], signal="cpu", window_seconds=300, agg="avg", now=NOW) == 50.0


# --- reduce_per_connector (sticky-connector view) --------------------------


def test_reduce_per_connector_cpu_reduces_each_separately() -> None:
    # c1 mean = (60+80)/2 = 70; c2 single = 90. No averaging across connectors.
    agg = _agg(
        [
            _sample("c1", age_seconds=10, cpu=60.0),
            _sample("c1", age_seconds=20, cpu=80.0),
            _sample("c2", age_seconds=10, cpu=90.0),
        ]
    )
    result = agg.reduce_per_connector(
        ["c1", "c2"], signal="cpu", window_seconds=300, agg="avg", now=NOW
    )
    assert result == {"c1": 70.0, "c2": 90.0}


def test_reduce_per_connector_throughput_reduces_each_separately() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=10, throughput=1000.0),
            _sample("c1", age_seconds=20, throughput=3000.0),
            _sample("c2", age_seconds=10, throughput=4000.0),
        ]
    )
    result = agg.reduce_per_connector(
        ["c1", "c2"], signal="throughput", window_seconds=300, agg="avg", now=NOW
    )
    assert result == {"c1": 2000.0, "c2": 4000.0}


def test_reduce_per_connector_omits_connectors_without_in_window_samples() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=10, cpu=50.0),
            _sample("c2", age_seconds=1000, cpu=99.0),  # stale, outside window
        ]
    )
    result = agg.reduce_per_connector(
        ["c1", "c2", "c3"], signal="cpu", window_seconds=300, agg="avg", now=NOW
    )
    # c2 has only a stale sample; c3 has none at all → both omitted.
    assert result == {"c1": 50.0}


def test_reduce_per_connector_respects_window_cutoff_and_agg() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=100, cpu=90.0),  # inside 300s, outside 60s
            _sample("c1", age_seconds=10, cpu=10.0),  # inside both
        ]
    )
    short = agg.reduce_per_connector(["c1"], signal="cpu", window_seconds=60, agg="avg", now=NOW)
    long_min = agg.reduce_per_connector(
        ["c1"], signal="cpu", window_seconds=300, agg="min", now=NOW
    )
    assert short == {"c1": 10.0}  # only the recent sample
    assert long_min == {"c1": 10.0}  # min of both


def test_reduce_per_connector_cpu_skips_none() -> None:
    agg = _agg(
        [
            _sample("c1", age_seconds=10, cpu=None, throughput=1000.0),
            _sample("c1", age_seconds=20, cpu=80.0),
        ]
    )
    result = agg.reduce_per_connector(["c1"], signal="cpu", window_seconds=300, agg="avg", now=NOW)
    assert result == {"c1": 80.0}  # the None sample is skipped, not zero


def test_reduce_per_connector_empty_is_empty_dict() -> None:
    agg = _agg([])
    assert (
        agg.reduce_per_connector(["c1"], signal="cpu", window_seconds=300, agg="avg", now=NOW) == {}
    )
