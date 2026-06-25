"""SQLite state: per-RN cooldown timers and the action-history log.

This is the only persistent state FC keeps; the Connector inventory is
rediscovered every cycle and never stored. Two things must survive a manager
restart (Key Design Rule #3): the per-RN up/down cooldown timestamps, so a
restart cannot reset a cooldown and cause thrashing, and the action history,
which both feeds the status UI and drives restart-before-replace.

All access goes through :func:`asyncio.to_thread` so the synchronous ``sqlite3``
calls never block the control loop. Each operation opens a short-lived
connection — connections are not shared across the threadpool's threads.
Timestamps are stored as ISO-8601 UTC strings.
"""

import asyncio
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from fc.models import ActionRecord, ScaleDirection

# Wait up to this long for a competing writer to release the lock before
# raising ``database is locked`` (multiple to_thread workers can write).
_BUSY_TIMEOUT_MS = 5000

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cooldowns (
    rn_id        TEXT PRIMARY KEY,
    last_up_ts   TEXT,
    last_down_ts TEXT
);
CREATE TABLE IF NOT EXISTS action_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT    NOT NULL,
    rn_id        TEXT    NOT NULL,
    connector_id TEXT,
    action       TEXT    NOT NULL,
    count        INTEGER NOT NULL,
    reason       TEXT    NOT NULL,
    outcome      TEXT    NOT NULL,
    actor        TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_action_history_ts ON action_history (ts);
CREATE INDEX IF NOT EXISTS idx_action_history_connector
    ON action_history (connector_id, action, ts);
CREATE TABLE IF NOT EXISTS cordons (
    connector_id TEXT PRIMARY KEY,
    ts           TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class Cooldowns:
    """The last scale-up and scale-down timestamps for one Remote Network."""

    last_up_ts: datetime | None
    last_down_ts: datetime | None


def _parse_ts(value: str | None) -> datetime | None:
    """Parse a stored ISO-8601 timestamp into a UTC-aware datetime, or ``None``.

    A timestamp stored without offset (a caller having passed a naive
    ``datetime``) is coerced to UTC so downstream arithmetic against the
    loop's timezone-aware ``now`` (see ``engine.policy.cooldown_remaining``)
    never raises ``can't subtract offset-naive and offset-aware datetimes``.
    """
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


class StateStore:
    """Async-friendly SQLite store for cooldowns and action history."""

    def __init__(self, path: str | Path) -> None:
        """Build the store.

        Args:
            path: Filesystem path to the SQLite database file.
        """
        self._path = str(path)

    def _connect(self) -> sqlite3.Connection:
        """Open a short-lived connection with WAL and a write-lock timeout.

        ``busy_timeout`` lets a connection wait for a competing writer instead
        of immediately raising ``database is locked`` — multiple ``to_thread``
        workers can issue writes (``set_cooldown``, ``record_action``) close
        together. ``synchronous=NORMAL`` is the safe, faster pairing with WAL.
        """
        conn = sqlite3.connect(self._path, timeout=_BUSY_TIMEOUT_MS / 1000)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        return conn

    async def init(self) -> None:
        """Create the schema if absent. Idempotent; safe to call every start."""

        def _run() -> None:
            with closing(self._connect()) as conn:
                conn.executescript(_SCHEMA)
                conn.commit()

        await asyncio.to_thread(_run)

    async def set_cooldown(self, rn_id: str, direction: ScaleDirection, ts: datetime) -> None:
        """Record the timestamp of a scale action in one direction.

        Args:
            rn_id: The Remote Network the action applied to.
            direction: :attr:`ScaleDirection.UP` or :attr:`ScaleDirection.DOWN`.
            ts: When the action occurred.
        """
        column = "last_up_ts" if direction is ScaleDirection.UP else "last_down_ts"
        iso = ts.isoformat()

        def _run() -> None:
            with closing(self._connect()) as conn:
                conn.execute(
                    f"INSERT INTO cooldowns (rn_id, {column}) VALUES (?, ?) "
                    f"ON CONFLICT(rn_id) DO UPDATE SET {column}=excluded.{column}",
                    (rn_id, iso),
                )
                conn.commit()

        await asyncio.to_thread(_run)

    async def get_cooldowns(self, rn_id: str) -> Cooldowns:
        """Return the cooldown timestamps for a Remote Network.

        Args:
            rn_id: The Remote Network id.

        Returns:
            The :class:`Cooldowns`; both fields are ``None`` if the RN has no
            recorded actions.
        """

        def _run() -> Cooldowns:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    "SELECT last_up_ts, last_down_ts FROM cooldowns WHERE rn_id=?",
                    (rn_id,),
                ).fetchone()
            if row is None:
                return Cooldowns(None, None)
            return Cooldowns(last_up_ts=_parse_ts(row[0]), last_down_ts=_parse_ts(row[1]))

        return await asyncio.to_thread(_run)

    async def record_action(self, record: ActionRecord, *, connector_id: str | None = None) -> None:
        """Append one action to the history log.

        Args:
            record: The action to persist.
            connector_id: Optional Connector the action targeted; used to count
                restarts for restart-before-replace. (The public
                :class:`ActionRecord` is RN-scoped; this column is internal
                bookkeeping.)
        """

        def _run() -> None:
            with closing(self._connect()) as conn:
                conn.execute(
                    "INSERT INTO action_history "
                    "(ts, rn_id, connector_id, action, count, reason, outcome, actor) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        record.ts.isoformat(),
                        record.rn_id,
                        connector_id,
                        record.action,
                        record.count,
                        record.reason,
                        record.outcome,
                        record.actor,
                    ),
                )
                conn.commit()

        await asyncio.to_thread(_run)

    async def recent_actions(
        self, *, limit: int = 50, rn_id: str | None = None
    ) -> list[ActionRecord]:
        """Return recent actions, newest first.

        Args:
            limit: Maximum number of rows to return.
            rn_id: When given, restrict to that Remote Network.

        Returns:
            The matching :class:`ActionRecord`s, most recent first.
        """

        def _run() -> list[ActionRecord]:
            query = "SELECT ts, rn_id, action, count, reason, outcome, actor FROM action_history"
            params: list[object] = []
            if rn_id is not None:
                query += " WHERE rn_id=?"
                params.append(rn_id)
            query += " ORDER BY id DESC LIMIT ?"
            params.append(limit)
            with closing(self._connect()) as conn:
                rows = conn.execute(query, params).fetchall()
            return [
                ActionRecord(
                    ts=_parse_ts(row[0]) or datetime.now(UTC),
                    rn_id=row[1],
                    action=row[2],
                    count=row[3],
                    reason=row[4],
                    outcome=row[5],
                    actor=row[6],
                )
                for row in rows
            ]

        return await asyncio.to_thread(_run)

    async def set_cordon(self, connector_id: str, cordoned: bool, *, ts: datetime) -> None:
        """Cordon or un-cordon a Connector (manual-override state).

        A cordoned Connector is excluded from autoscaling: the loop marks it on
        discovery and never picks it as a scale-down victim. Cordon state is
        persisted so it survives a manager restart.

        Args:
            connector_id: The Connector to (un)cordon.
            cordoned: ``True`` to cordon, ``False`` to lift the cordon.
            ts: When the change was made.
        """

        def _run() -> None:
            with closing(self._connect()) as conn:
                if cordoned:
                    conn.execute(
                        "INSERT INTO cordons (connector_id, ts) VALUES (?, ?) "
                        "ON CONFLICT(connector_id) DO UPDATE SET ts=excluded.ts",
                        (connector_id, ts.isoformat()),
                    )
                else:
                    conn.execute("DELETE FROM cordons WHERE connector_id=?", (connector_id,))
                conn.commit()

        await asyncio.to_thread(_run)

    async def list_cordoned(self) -> set[str]:
        """Return the set of currently-cordoned Connector ids."""

        def _run() -> set[str]:
            with closing(self._connect()) as conn:
                rows = conn.execute("SELECT connector_id FROM cordons").fetchall()
            return {row[0] for row in rows}

        return await asyncio.to_thread(_run)

    async def count_recent_restarts(self, connector_id: str, *, since: datetime) -> int:
        """Count restart actions for a Connector since a cutoff.

        Drives restart-before-replace (Key Design Rule #4): once the count in
        the restart window reaches ``max_restarts``, the decider escalates to a
        replace.

        Args:
            connector_id: The Connector to count restarts for.
            since: Only restarts at/after this time are counted.

        Returns:
            The number of restart actions in the window.
        """
        cutoff = since.isoformat()

        def _run() -> int:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM action_history "
                    "WHERE connector_id=? AND action='restart' AND ts>=?",
                    (connector_id, cutoff),
                ).fetchone()
            return int(row[0]) if row else 0

        return await asyncio.to_thread(_run)
