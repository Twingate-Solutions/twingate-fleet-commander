"""Tests for the collector normalization contract (``fc.collectors.base``).

These cover the pure, source-independent helpers that every collector funnels
through: CPU normalization to per-effective-core 0-100 (from cgroup deltas and
from a raw single-core percent), memory percent with a nullable limit,
throughput from cumulative byte totals, and effective-core resolution from a
Docker inspect payload. The ``Collector`` protocol is also exercised for
structural conformance.
"""

from datetime import UTC, datetime, timedelta

from fc.collectors.base import (
    Collector,
    compute_mem_pct,
    effective_cores_from_inspect,
    normalize_cpu_deltas,
    normalize_raw_cpu_pct,
    throughput_from_totals,
)
from fc.models import CollectorSource, ManagedConnector, ResourceSample

# --- normalize_cpu_deltas (docker_stats path) ------------------------------


def test_normalize_cpu_deltas_single_core_full_utilization() -> None:
    # Container consumed one core's worth while the (single-core) system
    # advanced the same amount -> 100% average core utilization.
    assert normalize_cpu_deltas(cpu_delta=1_000.0, system_delta=1_000.0) == 100.0


def test_normalize_cpu_deltas_multi_core_normalizes_to_per_core() -> None:
    # Two full cores out of four advanced -> 50% average core utilization,
    # regardless of core count (online cancels out of the formula).
    assert normalize_cpu_deltas(cpu_delta=2_000.0, system_delta=4_000.0) == 50.0


def test_normalize_cpu_deltas_zero_system_delta_is_none() -> None:
    # No measurable system advance (e.g. first sample) -> undefined, not a crash.
    assert normalize_cpu_deltas(cpu_delta=100.0, system_delta=0.0) is None


def test_normalize_cpu_deltas_negative_system_delta_is_none() -> None:
    assert normalize_cpu_deltas(cpu_delta=100.0, system_delta=-5.0) is None


# --- normalize_raw_cpu_pct (stdout_metrics path) ---------------------------


def test_normalize_raw_cpu_pct_divides_by_effective_cores() -> None:
    # 150% of a single core across 2 effective cores -> 75% per-effective-core.
    assert normalize_raw_cpu_pct(cpu_pct=150.0, effective_cores=2.0) == 75.0


def test_normalize_raw_cpu_pct_single_core_passthrough() -> None:
    assert normalize_raw_cpu_pct(cpu_pct=80.0, effective_cores=1.0) == 80.0


def test_normalize_raw_cpu_pct_zero_cores_is_none() -> None:
    assert normalize_raw_cpu_pct(cpu_pct=80.0, effective_cores=0.0) is None


def test_normalize_cpu_deltas_clamps_above_100() -> None:
    # Transient cgroup accounting can push cpu_delta past system_delta; the
    # documented 0-100 contract clamps it.
    assert normalize_cpu_deltas(cpu_delta=1_200.0, system_delta=1_000.0) == 100.0


def test_normalize_raw_cpu_pct_clamps_above_100() -> None:
    # 250% of a single core across only 2 effective cores -> clamps to 100.
    assert normalize_raw_cpu_pct(cpu_pct=250.0, effective_cores=2.0) == 100.0


# --- compute_mem_pct -------------------------------------------------------


def test_compute_mem_pct_with_limit() -> None:
    assert compute_mem_pct(usage=512, limit=1024) == 50.0


def test_compute_mem_pct_no_limit_is_none() -> None:
    # No container memory limit set -> advisory-only, percent is undefined.
    assert compute_mem_pct(usage=512, limit=None) is None


def test_compute_mem_pct_zero_limit_is_none() -> None:
    assert compute_mem_pct(usage=512, limit=0) is None


# --- throughput_from_totals ------------------------------------------------


def test_throughput_from_totals_basic_rate() -> None:
    t0 = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(seconds=10)
    # 10_000 bytes over 10 seconds -> 1000 bytes/sec.
    assert throughput_from_totals(0, t0, 10_000, t1) == 1000.0


def test_throughput_from_totals_no_previous_is_none() -> None:
    t1 = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)
    assert throughput_from_totals(None, None, 10_000, t1) is None


def test_throughput_from_totals_counter_reset_is_none() -> None:
    t0 = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(seconds=10)
    # Current total below previous -> counter reset (restart); not trustworthy.
    assert throughput_from_totals(50_000, t0, 10, t1) is None


def test_throughput_from_totals_zero_interval_is_none() -> None:
    t0 = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)
    assert throughput_from_totals(0, t0, 10_000, t0) is None


# --- effective_cores_from_inspect ------------------------------------------


def test_effective_cores_from_nano_cpus() -> None:
    inspect = {"HostConfig": {"NanoCpus": 2_500_000_000}}
    assert effective_cores_from_inspect(inspect, host_cpus=8) == 2.5


def test_effective_cores_from_cpu_quota_period() -> None:
    inspect = {"HostConfig": {"NanoCpus": 0, "CpuQuota": 150_000, "CpuPeriod": 100_000}}
    assert effective_cores_from_inspect(inspect, host_cpus=8) == 1.5


def test_effective_cores_falls_back_to_host_cpus() -> None:
    inspect = {"HostConfig": {"NanoCpus": 0, "CpuQuota": 0, "CpuPeriod": 0}}
    assert effective_cores_from_inspect(inspect, host_cpus=4) == 4.0


def test_effective_cores_missing_hostconfig_falls_back() -> None:
    assert effective_cores_from_inspect({}, host_cpus=4) == 4.0


# --- Collector protocol ----------------------------------------------------


def test_collector_protocol_structural_conformance() -> None:
    class _Fake:
        source = CollectorSource.DOCKER_STATS

        async def collect(self, connector: ManagedConnector) -> ResourceSample | None:
            return None

    assert isinstance(_Fake(), Collector)
