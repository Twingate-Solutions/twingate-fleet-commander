"""Shared status surface for the read-only UI and the override endpoints.

The control loop owns the live fleet picture but the FastAPI app must read it
without reaching into the loop's internals. This module is the seam between
them, mirroring how ``state.py`` is the seam for persistence:

* :class:`StatusState` holds the most recent :class:`FleetSnapshot` the loop
  publishes at the end of each cycle; the status API reads it.
* :class:`EventBuffer` is a bounded ring of the most recent structured log
  events, filled by a ``structlog`` processor, so the UI can show a "recent
  events" tail without a log backend.
* :class:`FleetOperator` is the narrow protocol the guarded override endpoints
  call — implemented by the control loop — so the API never imports the loop
  concretely and there is no import cycle.

The snapshot DTOs deliberately carry no secret material: only ids, names,
states, and numeric samples.
"""

from collections import deque
from collections.abc import MutableMapping
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel

from fc.models import ScaleDirection


class ConnectorStatus(BaseModel):
    """One Connector's glanceable status for the UI/API."""

    connector_id: str
    name: str
    twingate_state: str | None
    docker_health: str | None
    janus_locked: bool
    cordoned: bool
    cpu_pct_norm: float | None
    throughput_bps: float | None
    mem_bytes: int | None


class RemoteNetworkStatus(BaseModel):
    """A Remote Network's connector count against its configured bounds."""

    rn_id: str
    name: str
    count: int
    min_connectors: int
    max_connectors: int
    connectors: list[ConnectorStatus]


class FleetSnapshot(BaseModel):
    """The whole fleet as of one control-loop cycle."""

    cycle_id: str
    ts: datetime
    remote_networks: list[RemoteNetworkStatus]


class StatusState:
    """Holds the latest published :class:`FleetSnapshot` for the API to read.

    Thread-/task-safe for this access pattern: a single writer (the loop) swaps
    a reference and many readers (request handlers) read it; reference
    assignment is atomic in CPython, so no lock is needed.
    """

    def __init__(self) -> None:
        """Start with no snapshot (the API reports "no data yet")."""
        self._snapshot: FleetSnapshot | None = None

    def publish(self, snapshot: FleetSnapshot) -> None:
        """Replace the current snapshot with a freshly built one."""
        self._snapshot = snapshot

    def get(self) -> FleetSnapshot | None:
        """Return the latest snapshot, or ``None`` before the first cycle."""
        return self._snapshot


class EventBuffer:
    """A bounded ring buffer of recent structured log events for the UI tail.

    Wired into the ``structlog`` processor chain *after* secret redaction, so
    every buffered event is already scrubbed. Stores shallow copies so later
    mutation of an event dict cannot corrupt the buffer.
    """

    def __init__(self, maxlen: int = 200) -> None:
        """Build the buffer.

        Args:
            maxlen: Maximum number of events retained (oldest dropped first).
        """
        self._events: deque[dict[str, Any]] = deque(maxlen=maxlen)

    def processor(
        self, _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
    ) -> MutableMapping[str, Any]:
        """structlog processor: copy the event into the ring and pass it on."""
        self._events.append(dict(event_dict))
        return event_dict

    def tail(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return up to ``limit`` most recent events, newest first."""
        items = list(self._events)
        return items[-limit:][::-1]


@runtime_checkable
class FleetOperator(Protocol):
    """The narrow control surface the guarded override endpoints drive.

    Implemented by :class:`~fc.loop.ControlLoop`. Kept minimal so the API
    layer depends only on this protocol, never on the loop concretely.
    """

    async def manual_scale(self, rn_id: str, direction: ScaleDirection) -> bool:
        """Scale a Remote Network by one Connector; return whether it acted.

        Honors the floor (scale-down) and ceiling (scale-up); returns ``False``
        when the bound would be breached.
        """
        ...

    async def manual_cordon(self, connector_id: str, cordoned: bool) -> bool:
        """Mark/unmark a Connector as cordoned (excluded from autoscaling).

        Returns ``False`` if a cordon was refused because the Connector is not
        in the current fleet; ``True`` otherwise.
        """
        ...
