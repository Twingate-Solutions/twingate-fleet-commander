"""Tests for the pure policy functions (``fc.engine.policy``).

These encode the scaling rules with no I/O: high/low watermark tests over the
available (non-``None``) signals, the bytes/sec→Mbps conversion, cooldown
remaining time, and floor/ceiling-bounded scale-step counts. They are
exhaustively exercised because every scaling safety rail ultimately rests on
them.
"""

from datetime import UTC, datetime, timedelta

from fc.engine import policy

NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)


# --- bps_to_mbps -----------------------------------------------------------


def test_bps_to_mbps_converts_bytes_to_megabits() -> None:
    # 10 MB/s = 80 Mbit/s.
    assert policy.bps_to_mbps(10_000_000) == 80.0


# --- is_high_load: any present signal over its high watermark --------------


def test_high_load_true_when_cpu_over_watermark() -> None:
    assert (
        policy.is_high_load(
            cpu_norm=80.0,
            throughput_bps=0.0,
            cpu_high_pct=75.0,
            throughput_high_mbps=80.0,
        )
        is True
    )


def test_high_load_true_when_throughput_over_watermark() -> None:
    # 12.5 MB/s = 100 Mbit/s > 80 Mbit/s, even with low CPU.
    assert (
        policy.is_high_load(
            cpu_norm=5.0,
            throughput_bps=12_500_000,
            cpu_high_pct=75.0,
            throughput_high_mbps=80.0,
        )
        is True
    )


def test_high_load_false_when_both_below() -> None:
    assert (
        policy.is_high_load(
            cpu_norm=10.0,
            throughput_bps=1_000_000,
            cpu_high_pct=75.0,
            throughput_high_mbps=80.0,
        )
        is False
    )


def test_high_load_false_when_no_signals() -> None:
    assert (
        policy.is_high_load(
            cpu_norm=None,
            throughput_bps=None,
            cpu_high_pct=75.0,
            throughput_high_mbps=80.0,
        )
        is False
    )


# --- is_low_load: all present signals under low watermark, >=1 present -----


def test_low_load_true_when_all_present_signals_low() -> None:
    assert (
        policy.is_low_load(
            cpu_norm=10.0,
            throughput_bps=625_000,  # 5 Mbit/s < 10
            cpu_low_pct=25.0,
            throughput_low_mbps=10.0,
        )
        is True
    )


def test_low_load_true_when_only_cpu_present_and_low() -> None:
    # Throughput collection disabled/absent -> judge on the present signal.
    assert (
        policy.is_low_load(
            cpu_norm=10.0,
            throughput_bps=None,
            cpu_low_pct=25.0,
            throughput_low_mbps=10.0,
        )
        is True
    )


def test_low_load_false_when_one_present_signal_not_low() -> None:
    # CPU low but throughput high -> not safe to scale down.
    assert (
        policy.is_low_load(
            cpu_norm=10.0,
            throughput_bps=12_500_000,  # 100 Mbit/s
            cpu_low_pct=25.0,
            throughput_low_mbps=10.0,
        )
        is False
    )


def test_low_load_false_when_no_signals() -> None:
    assert (
        policy.is_low_load(
            cpu_norm=None,
            throughput_bps=None,
            cpu_low_pct=25.0,
            throughput_low_mbps=10.0,
        )
        is False
    )


# --- cooldown_remaining ----------------------------------------------------


def test_cooldown_remaining_none_last_ts_is_zero() -> None:
    assert policy.cooldown_remaining(None, 600, NOW) == 0.0


def test_cooldown_remaining_partial() -> None:
    last = NOW - timedelta(seconds=100)
    assert policy.cooldown_remaining(last, 600, NOW) == 500.0


def test_cooldown_remaining_elapsed_is_zero() -> None:
    last = NOW - timedelta(seconds=700)
    assert policy.cooldown_remaining(last, 600, NOW) == 0.0


# --- scale_up_count / scale_down_count -------------------------------------


def test_scale_up_count_respects_ceiling() -> None:
    assert policy.scale_up_count(current=2, max_connectors=6, scale_step=1) == 1
    assert policy.scale_up_count(current=6, max_connectors=6, scale_step=1) == 0
    assert policy.scale_up_count(current=5, max_connectors=6, scale_step=3) == 1  # clamp to ceiling


def test_scale_down_count_respects_floor() -> None:
    assert policy.scale_down_count(current=4, min_connectors=2, scale_step=1) == 1
    assert policy.scale_down_count(current=2, min_connectors=2, scale_step=1) == 0  # at floor
    assert policy.scale_down_count(current=3, min_connectors=2, scale_step=3) == 1  # clamp to floor


# --- floor_fill_count ------------------------------------------------------


def test_floor_fill_count_fills_deficit_to_floor() -> None:
    assert policy.floor_fill_count(current=0, min_connectors=2, max_connectors=6) == 2
    assert policy.floor_fill_count(current=1, min_connectors=3, max_connectors=6) == 2


def test_floor_fill_count_zero_at_or_above_floor() -> None:
    assert policy.floor_fill_count(current=2, min_connectors=2, max_connectors=6) == 0
    assert policy.floor_fill_count(current=5, min_connectors=2, max_connectors=6) == 0
