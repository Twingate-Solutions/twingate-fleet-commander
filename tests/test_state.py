"""Tests for the SQLite state store (``fc.state``).

Covers idempotent schema init, per-RN up/down cooldown round-trips, action
history (newest-first ordering, limit, per-RN filter), restart counting within
a window (for restart-before-replace), and persistence across a simulated
manager restart (a fresh store on the same file sees prior data).
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from fc.models import ActionRecord, ScaleDirection
from fc.state import Cooldowns, StateStore

NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)


def _db_path(tmp_path: Path) -> str:
    return str(tmp_path / "fc.sqlite3")


async def _store(tmp_path: Path) -> StateStore:
    store = StateStore(_db_path(tmp_path))
    await store.init()
    return store


def _action(
    *,
    action: str,
    rn_id: str = "rn-1",
    outcome: Literal["success", "fail"] = "success",
    ts: datetime = NOW,
    count: int = 1,
) -> ActionRecord:
    return ActionRecord(
        ts=ts, rn_id=rn_id, action=action, count=count, reason="test", outcome=outcome
    )


async def test_init_is_idempotent(tmp_path: Path) -> None:
    store = StateStore(_db_path(tmp_path))
    await store.init()
    await store.init()  # second call must not raise
    assert await store.get_cooldowns("rn-1") == Cooldowns(last_up_ts=None, last_down_ts=None)


async def test_cooldown_round_trip_up_and_down(tmp_path: Path) -> None:
    store = await _store(tmp_path)
    up_ts = NOW
    down_ts = NOW - timedelta(seconds=300)

    await store.set_cooldown("rn-1", ScaleDirection.UP, up_ts)
    await store.set_cooldown("rn-1", ScaleDirection.DOWN, down_ts)

    cooldowns = await store.get_cooldowns("rn-1")
    assert cooldowns.last_up_ts == up_ts
    assert cooldowns.last_down_ts == down_ts


async def test_cooldown_naive_timestamp_coerced_to_utc_aware(tmp_path: Path) -> None:
    """A naive datetime stored as a cooldown is read back as UTC-aware, so the
    decider's ``now - last_action_ts`` (now is tz-aware) never raises a
    naive/aware subtraction TypeError."""
    store = await _store(tmp_path)
    naive = datetime(2026, 6, 24, 12, 0, 0)
    await store.set_cooldown("rn-1", ScaleDirection.UP, naive)

    cooldowns = await store.get_cooldowns("rn-1")
    assert cooldowns.last_up_ts is not None
    assert cooldowns.last_up_ts.tzinfo is not None
    # Subtracting from an aware "now" must not raise.
    delta = NOW - cooldowns.last_up_ts
    assert delta.total_seconds() == 0.0


async def test_setting_one_direction_preserves_the_other(tmp_path: Path) -> None:
    store = await _store(tmp_path)
    await store.set_cooldown("rn-1", ScaleDirection.UP, NOW)
    await store.set_cooldown("rn-1", ScaleDirection.DOWN, NOW + timedelta(seconds=5))
    await store.set_cooldown("rn-1", ScaleDirection.UP, NOW + timedelta(seconds=10))

    cooldowns = await store.get_cooldowns("rn-1")
    assert cooldowns.last_up_ts == NOW + timedelta(seconds=10)
    assert cooldowns.last_down_ts == NOW + timedelta(seconds=5)


async def test_unknown_rn_has_empty_cooldowns(tmp_path: Path) -> None:
    store = await _store(tmp_path)
    assert await store.get_cooldowns("never-seen") == Cooldowns(None, None)


async def test_record_and_list_actions_newest_first(tmp_path: Path) -> None:
    store = await _store(tmp_path)
    await store.record_action(_action(action="provision", ts=NOW - timedelta(seconds=30)))
    await store.record_action(_action(action="deprovision", ts=NOW))

    actions = await store.recent_actions(limit=10)
    assert [a.action for a in actions] == ["deprovision", "provision"]


async def test_recent_actions_respects_limit_and_rn_filter(tmp_path: Path) -> None:
    store = await _store(tmp_path)
    await store.record_action(_action(action="provision", rn_id="rn-1"))
    await store.record_action(_action(action="provision", rn_id="rn-2"))
    await store.record_action(_action(action="restart", rn_id="rn-1"))

    assert len(await store.recent_actions(limit=1)) == 1
    rn1 = await store.recent_actions(limit=10, rn_id="rn-1")
    assert {a.action for a in rn1} == {"provision", "restart"}
    assert all(a.rn_id == "rn-1" for a in rn1)


async def test_count_recent_restarts_within_window(tmp_path: Path) -> None:
    store = await _store(tmp_path)
    since = NOW - timedelta(seconds=600)
    # Two restarts inside the window, one before it, one for another connector.
    await store.record_action(
        _action(action="restart", ts=NOW - timedelta(seconds=60)), connector_id="c1"
    )
    await store.record_action(
        _action(action="restart", ts=NOW - timedelta(seconds=120)), connector_id="c1"
    )
    await store.record_action(
        _action(action="restart", ts=NOW - timedelta(seconds=900)), connector_id="c1"
    )
    await store.record_action(
        _action(action="restart", ts=NOW - timedelta(seconds=30)), connector_id="c2"
    )

    assert await store.count_recent_restarts("c1", since=since) == 2


async def test_state_persists_across_restart(tmp_path: Path) -> None:
    path = _db_path(tmp_path)
    store = StateStore(path)
    await store.init()
    await store.set_cooldown("rn-1", ScaleDirection.UP, NOW)
    await store.record_action(_action(action="provision"))

    # Simulate a manager restart: a brand-new store on the same file.
    reopened = StateStore(path)
    await reopened.init()
    assert (await reopened.get_cooldowns("rn-1")).last_up_ts == NOW
    assert len(await reopened.recent_actions(limit=10)) == 1
