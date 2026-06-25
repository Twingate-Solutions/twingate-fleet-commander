"""The async control loop: discover -> collect -> decide -> act, plus heartbeat.

One cycle, repeated every ``poll_interval_seconds``, ties the whole system
together. It generates a ``cycle_id`` (bound onto every log line so a cycle's
signals → decision → action correlate), then:

#. **discover** — list managed containers (the authoritative inventory) and
   enrich them with Twingate liveness state by joining on connector id / name;
#. **collect** — run every enabled collector per Connector, isolating failures
   so one bad Connector never aborts the cycle;
#. **decide** — for the single managed Remote Network (Key Design Rule N1), ask
   the pure decider for a :class:`~fc.models.ScaleDecision` and any
   :class:`~fc.models.HealthAction`s;
#. **act** — execute the three-step provision / drain-before-delete deprovision
   / restart-before-replace sequences, recording each in state and metrics.

The cycle is wrapped so any unhandled error becomes a ``loop.cycle.error`` line
and the loop survives; a clean cycle ends with the ``loop.cycle.complete``
heartbeat and a refreshed ``fc_last_successful_cycle_timestamp_seconds`` so a
stuck manager is alertable by staleness.

Drain/replace *ordering* lives here, not in the actuator or decider (see
``documentation/ARCHITECTURE.md`` → Control-loop phases): the decider only labels intent and
``deprovision`` only stops/removes; the loop sequences ``connectorDelete`` →
``drain_grace_seconds`` → ``deprovision`` for scale-down and provision-new-then-
delete-old for a replace.
"""

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import structlog

from fc.actuator.base import Actuator, ActuatorError
from fc.collectors.base import Collector, CollectorError
from fc.config import Policy
from fc.engine.aggregator import Aggregator
from fc.engine.decider import decide_health, decide_scale, is_unhealthy
from fc.models import (
    ActionRecord,
    ConnectorState,
    HealthAction,
    ManagedConnector,
    ResourceSample,
    ScaleDecision,
    ScaleDirection,
)
from fc.observability import events
from fc.observability.metrics import Metrics, classify_health_reason
from fc.state import Cooldowns, StateStore
from fc.status import ConnectorStatus, FleetSnapshot, RemoteNetworkStatus, StatusState
from fc.twingate.client import TwingateApiError, TwingateClient

logger = structlog.get_logger(__name__)


def _default_name_factory(rn_id: str) -> str:
    """Generate a unique container/Connector name for a new Connector in an RN."""
    return f"fc-{uuid4().hex[:12]}"


@dataclass
class _PendingReplace:
    """A net-new replacement awaiting health before the old Connector is torn down.

    Tracks the in-flight wait-for-healthy replace (Key Design Rule #4): the
    replacement has been provisioned; the unhealthy ``old`` Connector is drained
    and deleted only once ``new_connector_id`` reports ALIVE/healthy on a later
    cycle. ``started_at`` bounds the wait via ``replace_health_timeout_seconds``.
    ``reason`` is the decider's remediation reason, carried so the eventual
    ``action.replace`` line can report why the replace was started.
    """

    new_connector_id: str
    started_at: datetime
    reason: str
    actor: str = "auto"


@dataclass
class CycleResult:
    """Outcome of one control-loop cycle, returned for inspection and tests.

    ``ok`` is ``False`` when the cycle aborted on an unhandled error (a
    ``loop.cycle.error`` was logged); the partial fields reflect whatever was
    completed before the abort.
    """

    cycle_id: str
    ok: bool = True
    rn_count: int = 0
    sample_count: int = 0
    decisions: list[ScaleDecision] = field(default_factory=list)
    health_actions: list[HealthAction] = field(default_factory=list)
    duration_ms: float = 0.0


class ControlLoop:
    """Runs the discover→collect→decide→act cycle and exposes it for tests.

    The loop performs all I/O; the decider it calls is pure. Every external
    dependency is injected so a full cycle can run against mocks. The clock,
    name factory, id factory, and sleep are injectable too, making a cycle
    deterministic under test.
    """

    def __init__(
        self,
        *,
        policy: Policy,
        twingate: TwingateClient,
        actuator: Actuator,
        collectors: list[Collector],
        aggregator: Aggregator,
        state: StateStore,
        metrics: Metrics,
        clock: Callable[[], datetime] | None = None,
        name_factory: Callable[[str], str] = _default_name_factory,
        id_factory: Callable[[], str] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        status: StatusState | None = None,
    ) -> None:
        """Build the control loop.

        Args:
            policy: The validated autoscaling policy for the single managed
                Remote Network.
            twingate: The Twingate Admin API client.
            actuator: The compute actuator (Docker, by default).
            collectors: The enabled collectors, run in order per Connector.
            aggregator: The sliding-window aggregator.
            state: The SQLite cooldown / action-history store.
            metrics: The manager self-metrics.
            clock: Returns "now"; defaults to UTC wall clock. Injectable for
                deterministic tests.
            name_factory: Maps the RN id to a fresh Connector name.
            id_factory: Returns a cycle id; defaults to a random uuid hex.
            sleep: Async sleep used for the drain grace; injectable so tests
                need not wait real seconds.
            status: Optional shared status surface; when set, the loop publishes
                a :class:`~fc.status.FleetSnapshot` at the end of each clean
                cycle for the read-only UI/API to read.
        """
        self._policy = policy
        self._twingate = twingate
        self._actuator = actuator
        self._collectors = collectors
        self._aggregator = aggregator
        self._state = state
        self._metrics = metrics
        self._clock = clock or (lambda: datetime.now(UTC))
        self._name_factory = name_factory
        self._id_factory = id_factory or (lambda: uuid4().hex)
        self._sleep = sleep
        self._status = status
        # Serializes actuation so the autoscaler cycle and a concurrent manual
        # override never act on the same RN at once (the loop and uvicorn run
        # together under one ``asyncio.gather``). Held across a whole cycle and
        # across each manual override; readiness/metrics endpoints don't take it.
        self._action_lock = asyncio.Lock()
        # Connector id → the time FC first observed it this run. Drives the
        # decider's startup grace window so a just-provisioned Connector is not
        # restarted before its first heartbeat. In-memory by design: after a
        # manager restart it simply re-grace once, which is harmless because
        # already-healthy Connectors report ALIVE and are not remediated anyway.
        self._first_seen: dict[str, datetime] = {}
        # Connector id → the time it first went (and has stayed) unhealthy.
        # Drives the decider's unhealthy-duration gate so a brief blip never
        # triggers remediation; the entry is cleared the moment the Connector
        # recovers. In-memory like ``_first_seen``.
        self._first_unhealthy: dict[str, datetime] = {}
        # Old connector id → its in-flight wait-for-healthy replacement. Set when
        # a replace begins (replacement provisioned) and cleared once the old
        # Connector is torn down after the replacement is healthy (Key Design
        # Rule #4). Cycle-spanning; in-memory (a restart forgets in-flight
        # replaces, which self-heals as the still-unhealthy old Connector is
        # re-evaluated next cycle).
        self._pending_replaces: dict[str, _PendingReplace] = {}

    # -- public entry points -------------------------------------------------

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        """Run cycles on the poll interval until ``stop_event`` is set.

        Each iteration runs one cycle (which never raises — errors are caught
        and logged inside), then waits ``poll_interval_seconds`` or until the
        stop event fires, whichever comes first, so shutdown is prompt.

        Args:
            stop_event: Set by the entrypoint's signal handler to request a
                graceful stop.
        """
        interval = self._policy.poll_interval_seconds
        while not stop_event.is_set():
            await self.run_cycle()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=interval)

    async def run_cycle(self) -> CycleResult:
        """Run exactly one discover→collect→decide→act cycle.

        Returns:
            A :class:`CycleResult` describing what happened. Never raises: an
            unhandled error is logged as ``loop.cycle.error`` and reflected as
            ``ok=False``.
        """
        cycle_id = self._id_factory()
        log = logger.bind(cycle_id=cycle_id)
        now = self._clock()
        started = time.monotonic()
        self._metrics.loop_iterations.inc()
        log.info(events.LOOP_CYCLE_START)

        result = CycleResult(cycle_id=cycle_id)
        try:
            async with self._action_lock:
                fleet = await self._discover(log)
                self._update_first_seen(fleet, now)
                self._update_first_unhealthy(fleet, now)
                samples = await self._collect(fleet, log)
                self._aggregator.ingest(samples)
                self._aggregator.prune(now=now)
                result.sample_count = len(samples)
                # The last sample seen per connector this cycle — threaded into
                # the health-action logs so a restart/replace line carries the
                # triggering signal alongside the decider's reason.
                latest_by_connector: dict[str, ResourceSample] = {}
                for sample in samples:
                    latest_by_connector[sample.connector_id] = sample

                # FC manages exactly one Remote Network (Key Design Rule N1).
                # Its Connectors are always evaluated — even when none are
                # discovered yet — so floor-fill can provision a baseline fleet
                # from empty (the "start only the manager and it self-provisions"
                # path).
                rn_id = self._policy.remote_network_id
                connectors = [c for c in fleet if c.rn_id == rn_id]
                result.rn_count = 1
                # Isolate a failure deciding/acting on the Remote Network (e.g. a
                # sqlite hiccup reading cooldowns) so the heartbeat, gauges, and
                # snapshot below still complete this cycle.
                try:
                    decision = await self._decide_scale(connectors, now, log)
                    result.decisions.append(decision)
                    await self._act_scale(decision, connectors, now, log)

                    # Advance any in-flight wait-for-healthy replaces first, so a
                    # completed replacement's old Connector is torn down and the
                    # set of mid-replace ids excludes them from new decisions.
                    await self._process_pending_replaces(connectors, now, log)
                    health = await self._decide_and_log_health(connectors, now, log)
                    result.health_actions.extend(health)
                    await self._act_health(health, connectors, latest_by_connector, now, log)
                except Exception as exc:
                    log.error(
                        events.LOOP_RN_ERROR,
                        rn_id=rn_id,
                        error=type(exc).__name__,
                        detail=str(exc),
                    )

                self._metrics.set_fleet_gauges(fleet)
                self._metrics.last_successful_cycle.set(now.timestamp())
                self._publish_snapshot(cycle_id, now, connectors, samples)
            result.duration_ms = (time.monotonic() - started) * 1000.0
            log.info(
                events.LOOP_CYCLE_COMPLETE,
                duration_ms=round(result.duration_ms, 1),
                rn_count=result.rn_count,
            )
        except Exception as exc:
            result.ok = False
            result.duration_ms = (time.monotonic() - started) * 1000.0
            log.error(events.LOOP_CYCLE_ERROR, error=type(exc).__name__, detail=str(exc))
        return result

    # -- ① discover ----------------------------------------------------------

    async def _discover(self, log: structlog.BoundLogger) -> list[ManagedConnector]:
        """List managed containers and enrich them with Twingate liveness.

        The managed-container list is the authoritative inventory; the Twingate
        connector list supplies ``twingate_state`` / ``last_heartbeat_at`` by
        joining on the connector-id label (falling back to a name match for
        seed containers FC did not create). Logical-only Twingate Connectors
        with no container are not counted — they are reconciled on a later
        cycle.

        Raises:
            ActuatorError | TwingateApiError: Propagated to abort the
                cycle (logged as ``loop.cycle.error``); the relevant error
                counter is incremented first.
        """
        log.debug(events.DISCOVER_START)
        try:
            containers = await self._actuator.list_managed()
        except ActuatorError:
            self._metrics.docker_api_errors.inc()
            raise
        try:
            tg_connectors = await self._twingate.list_connectors()
        except TwingateApiError:
            self._metrics.twingate_api_errors.inc()
            raise

        by_id = {c.connector_id: c for c in tg_connectors if c.connector_id}
        by_name = {c.name: c for c in tg_connectors if c.name}
        cordoned = await self._state.list_cordoned()

        fleet: list[ManagedConnector] = []
        for container in containers:
            tg = by_id.get(container.connector_id) if container.connector_id else None
            if tg is None:
                tg = by_name.get(container.name)
            update: dict[str, object] = {}
            if tg is not None:
                update = {
                    "connector_id": container.connector_id or tg.connector_id,
                    "rn_id": container.rn_id or tg.rn_id,
                    "twingate_state": tg.twingate_state,
                    "last_heartbeat_at": tg.last_heartbeat_at,
                }
            effective_id = str(update.get("connector_id", container.connector_id))
            if effective_id and effective_id in cordoned:
                update["cordoned"] = True
            fleet.append(container.model_copy(update=update) if update else container)

        counts: dict[str, int] = {}
        for connector in fleet:
            counts[connector.rn_id] = counts.get(connector.rn_id, 0) + 1
        log.info(events.DISCOVER_RESULT, fleet_size=len(fleet), per_rn=counts)
        log.debug(events.DISCOVER_COMPLETE, fleet_size=len(fleet))
        return fleet

    # -- ② collect -----------------------------------------------------------

    async def _collect(
        self, fleet: list[ManagedConnector], log: structlog.BoundLogger
    ) -> list[ResourceSample]:
        """Run every enabled collector for every Connector, isolating failures.

        A :class:`~fc.collectors.base.CollectorError` for one Connector/
        collector is logged as ``collect.error``, counted in
        ``fc_collect_errors_total``, and skipped; collection proceeds for the
        rest. A collector returning ``None`` (no sample this cycle) is simply
        omitted.
        """
        log.debug(events.COLLECT_START)
        samples: list[ResourceSample] = []
        for connector in fleet:
            for collector in self._collectors:
                try:
                    sample = await collector.collect(connector)
                except CollectorError as exc:
                    self._metrics.collect_errors.labels(collector=collector.source.value).inc()
                    log.warning(
                        events.COLLECT_ERROR,
                        connector_id=connector.connector_id,
                        source=collector.source.value,
                        error=str(exc),
                    )
                    continue
                if sample is not None:
                    samples.append(sample)
                    log.debug(
                        events.COLLECT_SAMPLE,
                        connector_id=connector.connector_id,
                        source=collector.source.value,
                    )
        log.debug(events.COLLECT_COMPLETE, sample_count=len(samples))
        return samples

    # -- ③ decide ------------------------------------------------------------

    def _update_first_seen(self, fleet: list[ManagedConnector], now: datetime) -> None:
        """Record first-seen times for the current fleet and forget departed ids.

        New Connectors get ``now`` as their first-seen time; ids no longer in
        the fleet are dropped so the map can't grow without bound and a recreated
        id starts a fresh grace window.
        """
        present = {c.connector_id for c in fleet}
        for connector_id in present:
            self._first_seen.setdefault(connector_id, now)
        for connector_id in self._first_seen.keys() - present:
            del self._first_seen[connector_id]

    def _update_first_unhealthy(self, fleet: list[ManagedConnector], now: datetime) -> None:
        """Track each Connector's continuous-unhealth start; reset on recovery.

        A Connector that is unhealthy this cycle keeps (or gets) its first-
        unhealthy timestamp; one that is healthy — or has departed the fleet —
        has its timestamp cleared so its unhealthy-duration timer restarts from
        scratch next time it goes unhealthy. This mirrors the decider's
        :func:`~fc.engine.decider.is_unhealthy` predicate exactly so the gate and
        the tracking never disagree.
        """
        present = {c.connector_id for c in fleet}
        for connector in fleet:
            if is_unhealthy(connector):
                self._first_unhealthy.setdefault(connector.connector_id, now)
            else:
                self._first_unhealthy.pop(connector.connector_id, None)
        for connector_id in self._first_unhealthy.keys() - present:
            del self._first_unhealthy[connector_id]

    async def _decide_scale(
        self,
        connectors: list[ManagedConnector],
        now: datetime,
        log: structlog.BoundLogger,
    ) -> ScaleDecision:
        """Reduce each scale metric over its own window and decide; then log."""
        log.debug(events.DECIDE_START)
        ids = [c.connector_id for c in connectors]
        rn_id = self._policy.remote_network_id
        metrics = self._policy.scale_metrics
        cpu_value = self._aggregator.reduce(
            ids,
            signal="cpu",
            window_seconds=metrics.cpu.window_seconds,
            agg=metrics.cpu.agg,
            now=now,
        )
        throughput_value = self._aggregator.reduce(
            ids,
            signal="throughput",
            window_seconds=metrics.throughput.window_seconds,
            agg=metrics.throughput.agg,
            now=now,
        )
        # Per-connector windowed values feed the sticky-connector scale-up
        # trigger (``any``/``quorum``); the fleet means above still drive the
        # ``mean`` mode and the scale-down path.
        cpu_by = self._aggregator.reduce_per_connector(
            ids,
            signal="cpu",
            window_seconds=metrics.cpu.window_seconds,
            agg=metrics.cpu.agg,
            now=now,
        )
        tput_by = self._aggregator.reduce_per_connector(
            ids,
            signal="throughput",
            window_seconds=metrics.throughput.window_seconds,
            agg=metrics.throughput.agg,
            now=now,
        )
        cooldowns = await self._state.get_cooldowns(rn_id)
        self._set_action_age_gauge(rn_id, cooldowns, now)
        decision = decide_scale(
            policy=self._policy,
            cpu_value=cpu_value,
            throughput_value=throughput_value,
            current_count=len(connectors),
            cooldowns=cooldowns,
            now=now,
            cpu_by_connector=cpu_by,
            throughput_by_connector_bps=tput_by,
        )
        self._log_decision(decision, log)
        log.debug(events.DECIDE_COMPLETE, direction=decision.direction.value)
        return decision

    def _set_action_age_gauge(self, rn_id: str, cooldowns: Cooldowns, now: datetime) -> None:
        """Set ``fc_seconds_since_last_action`` from the most recent cooldown ts.

        Uses the later of the last up/down action timestamps; if the RN has no
        recorded action yet, the gauge is left unset for that RN.
        """
        stamps = [ts for ts in (cooldowns.last_up_ts, cooldowns.last_down_ts) if ts is not None]
        if not stamps:
            return
        last = max(stamps)
        self._metrics.seconds_since_last_action.labels(rn=rn_id).set(
            max(0.0, (now - last).total_seconds())
        )

    @staticmethod
    def _log_decision(decision: ScaleDecision, log: structlog.BoundLogger) -> None:
        """Emit the right ``decide.*`` event for a scale decision."""
        if decision.direction is ScaleDirection.UP:
            log.info(
                events.DECIDE_SCALE_UP,
                rn_id=decision.rn_id,
                count=decision.count,
                reason=decision.reason,
                metrics=decision.metrics,
            )
        elif decision.direction is ScaleDirection.DOWN:
            log.info(
                events.DECIDE_SCALE_DOWN,
                rn_id=decision.rn_id,
                count=decision.count,
                reason=decision.reason,
                metrics=decision.metrics,
            )
        elif "cooldown_seconds_remaining" in decision.metrics:
            log.info(
                events.DECIDE_COOLDOWN_SKIP,
                rn_id=decision.rn_id,
                seconds_remaining=decision.metrics["cooldown_seconds_remaining"],
                reason=decision.reason,
                metrics=decision.metrics,
            )
        else:
            log.info(
                events.DECIDE_NO_ACTION,
                rn_id=decision.rn_id,
                reason=decision.reason,
                metrics=decision.metrics,
            )

    async def _decide_and_log_health(
        self,
        connectors: list[ManagedConnector],
        now: datetime,
        log: structlog.BoundLogger,
    ) -> list[HealthAction]:
        """Compute health actions for the RN, logging dead/unhealthy state."""
        log.debug(events.HEALTH_START)
        for connector in connectors:
            if connector.cordoned:
                # Cordon = operator hand-off; FC will not remediate it, so don't
                # emit a dead/unhealthy WARNING implying an action is coming.
                continue
            if connector.twingate_state is not None and connector.twingate_state.value.startswith(
                "DEAD"
            ):
                log.warning(
                    events.HEALTH_CONNECTOR_DEAD,
                    connector_id=connector.connector_id,
                    state=connector.twingate_state.value,
                )
            elif connector.docker_health == "unhealthy":
                log.warning(
                    events.HEALTH_UNHEALTHY,
                    connector_id=connector.connector_id,
                    failing_streak=connector.docker_failing_streak,
                )

        window_start = now - timedelta(seconds=self._policy.restart_window_seconds)
        restart_counts: dict[str, int] = {}
        for connector in connectors:
            if connector.cordoned:
                continue
            restart_counts[connector.connector_id] = await self._state.count_recent_restarts(
                connector.connector_id, since=window_start
            )
        actions = decide_health(
            policy=self._policy,
            connectors=connectors,
            restart_counts=restart_counts,
            now=now,
            first_seen=self._first_seen,
            first_unhealthy=self._first_unhealthy,
            pending_replace_ids=set(self._pending_replaces),
        )
        log.debug(events.HEALTH_COMPLETE, action_count=len(actions))
        return actions

    # -- ④ act ---------------------------------------------------------------

    async def _act_scale(
        self,
        decision: ScaleDecision,
        connectors: list[ManagedConnector],
        now: datetime,
        log: structlog.BoundLogger,
    ) -> None:
        """Execute a scale decision (no-op for NONE)."""
        rn_id = self._policy.remote_network_id
        if decision.direction is ScaleDirection.UP:
            provisioned = 0
            for _ in range(decision.count):
                if await self._provision_one(now, log) is not None:
                    provisioned += 1
            if provisioned:
                await self._state.set_cooldown(rn_id, ScaleDirection.UP, now)
        elif decision.direction is ScaleDirection.DOWN:
            victims = self._pick_victims(connectors, decision.count)
            removed = 0
            for victim in victims:
                if await self._deprovision_one(victim, now, log):
                    removed += 1
            if removed:
                await self._state.set_cooldown(rn_id, ScaleDirection.DOWN, now)

    @staticmethod
    def _pick_victims(connectors: list[ManagedConnector], count: int) -> list[ManagedConnector]:
        """Pick up to ``count`` scale-down victims, never a cordoned one.

        Cordoned Connectors are excluded. Those with a known logical connector id
        are preferred (so the logical Connector can be deleted to stop routing).
        The model carries no creation time and the Docker list order is not a
        reliable age signal, so within the id-known group the (stable) discovery
        order is used only as an arbitrary-but-deterministic tiebreak — not as a
        "newest"/"oldest" guarantee.
        """
        eligible = [c for c in connectors if not c.cordoned]
        eligible.sort(key=lambda c: c.connector_id == "")
        return eligible[:count]

    async def _provision_one(
        self,
        now: datetime,
        log: structlog.BoundLogger,
        *,
        actor: str = "auto",
    ) -> str | None:
        """Run the three-step provision; return the new connector id or ``None``.

        Steps (Key Design Rule #1): ``connectorCreate`` → ``connectorGenerateTokens``
        → ``actuator.provision``. A failure at any step is logged as
        ``action.provision.fail``, recorded, counted on the matching error
        metric, and yields ``None`` (the cycle continues). ``actor`` is
        ``"manual"`` when driven by an override endpoint, recorded on the audit
        row and the log line.
        """
        rn_id = self._policy.remote_network_id
        name = self._name_factory(rn_id)
        log.info(events.ACTION_PROVISION_START, rn_id=rn_id, name=name, actor=actor)
        try:
            connector = await self._twingate.create_connector(rn_id, name)
            tokens = await self._twingate.generate_tokens(connector.connector_id)
        except TwingateApiError as exc:
            self._metrics.twingate_api_errors.inc()
            log.error(events.ACTION_PROVISION_FAIL, rn_id=rn_id, error=str(exc), actor=actor)
            await self._record(rn_id, "provision", 1, "twingate error", "fail", now, actor=actor)
            return None
        try:
            await self._actuator.provision(rn_id, connector.connector_id, name, tokens)
        except ActuatorError as exc:
            self._metrics.docker_api_errors.inc()
            log.error(events.ACTION_PROVISION_FAIL, rn_id=rn_id, error=str(exc), actor=actor)
            await self._record(rn_id, "provision", 1, "docker error", "fail", now, actor=actor)
            return None

        log.info(
            events.ACTION_PROVISION_SUCCESS,
            rn_id=rn_id,
            connector_id=connector.connector_id,
            name=name,
            actor=actor,
        )
        self._metrics.scale_actions.labels(rn=rn_id, direction="up").inc()
        await self._record(
            rn_id,
            "provision",
            1,
            "scale up" if actor == "auto" else "manual scale up",
            "success",
            now,
            connector_id=connector.connector_id,
            actor=actor,
        )
        return connector.connector_id

    async def _deprovision_one(
        self,
        victim: ManagedConnector,
        now: datetime,
        log: structlog.BoundLogger,
        *,
        actor: str = "auto",
    ) -> bool:
        """Drain then remove one Connector; return whether it succeeded.

        Order (Key Design Rule #4): ``connectorDelete`` (controller stops
        routing) → wait ``drain_grace_seconds`` → ``actuator.deprovision``
        (stop + remove). A failure is logged as ``action.deprovision.fail`` and
        recorded. ``actor`` is ``"manual"`` when driven by an override endpoint.
        """
        rn_id = self._policy.remote_network_id
        log.info(
            events.ACTION_DEPROVISION_START,
            rn_id=rn_id,
            connector_id=victim.connector_id,
            drain_grace=self._policy.drain_grace_seconds,
            actor=actor,
        )
        try:
            if victim.connector_id:
                await self._twingate.delete_connector(victim.connector_id)
        except TwingateApiError as exc:
            self._metrics.twingate_api_errors.inc()
            log.error(
                events.ACTION_DEPROVISION_FAIL,
                rn_id=rn_id,
                connector_id=victim.connector_id,
                error=str(exc),
                actor=actor,
            )
            await self._record(
                rn_id,
                "deprovision",
                1,
                "twingate error",
                "fail",
                now,
                connector_id=victim.connector_id,
                actor=actor,
            )
            return False

        await self._sleep(self._policy.drain_grace_seconds)

        try:
            await self._actuator.deprovision(victim)
        except ActuatorError as exc:
            self._metrics.docker_api_errors.inc()
            log.error(
                events.ACTION_DEPROVISION_FAIL,
                rn_id=rn_id,
                connector_id=victim.connector_id,
                error=str(exc),
                actor=actor,
            )
            await self._record(
                rn_id,
                "deprovision",
                1,
                "docker error",
                "fail",
                now,
                connector_id=victim.connector_id,
                actor=actor,
            )
            return False

        log.info(
            events.ACTION_DEPROVISION_SUCCESS,
            rn_id=rn_id,
            connector_id=victim.connector_id,
            actor=actor,
        )
        self._metrics.scale_actions.labels(rn=rn_id, direction="down").inc()
        await self._record(
            rn_id,
            "deprovision",
            1,
            "scale down" if actor == "auto" else "manual scale down",
            "success",
            now,
            connector_id=victim.connector_id,
            actor=actor,
        )
        # Drop any cordon row for the removed Connector so a stale cordon can't
        # outlive it (and re-cordon a future Connector that reuses the id).
        if victim.connector_id:
            await self._state.set_cordon(victim.connector_id, False, ts=now)
        return True

    async def _act_health(
        self,
        health: list[HealthAction],
        connectors: list[ManagedConnector],
        latest_by_connector: dict[str, ResourceSample],
        now: datetime,
        log: structlog.BoundLogger,
    ) -> None:
        """Execute health actions: restart in place, or replace (new-then-old)."""
        by_id = {c.connector_id: c for c in connectors}
        for action in health:
            connector = by_id.get(action.connector_id)
            if connector is None:
                continue
            sample = latest_by_connector.get(connector.connector_id)
            self._metrics.health_actions.labels(
                kind=action.kind, reason_class=classify_health_reason(connector)
            ).inc()
            if action.kind == "restart":
                await self._restart_one(connector, action.reason, sample, now, log)
            else:
                await self._begin_replace(connector, action.reason, sample, now, log)

    @staticmethod
    def _sample_fields(sample: ResourceSample | None) -> dict[str, object] | None:
        """Return a bounded, secret-free dict of a sample's signal fields.

        Used to enrich health-action log lines with the triggering signal. The
        field set is fixed (no free text), so log cardinality stays bounded;
        ``None`` is returned when there was no sample for the Connector this
        cycle. Samples never carry secrets.
        """
        if sample is None:
            return None
        return {
            "cpu_pct_norm": sample.cpu_pct_norm,
            "throughput_bps": sample.throughput_bps,
            "mem_bytes": sample.mem_bytes,
            "source": sample.source.value,
        }

    async def _restart_one(
        self,
        connector: ManagedConnector,
        reason: str,
        sample: ResourceSample | None,
        now: datetime,
        log: structlog.BoundLogger,
    ) -> None:
        """Restart a Connector in place and record it."""
        rn_id = self._policy.remote_network_id
        window_start = now - timedelta(seconds=self._policy.restart_window_seconds)
        prior = await self._state.count_recent_restarts(connector.connector_id, since=window_start)
        log.info(
            events.ACTION_RESTART,
            rn_id=rn_id,
            connector_id=connector.connector_id,
            restart_count=prior + 1,
            reason=reason,
            state=connector.twingate_state.value if connector.twingate_state is not None else None,
            sample=self._sample_fields(sample),
        )
        try:
            await self._actuator.restart(connector)
        except ActuatorError as exc:
            self._metrics.docker_api_errors.inc()
            await self._record(
                rn_id,
                "restart",
                1,
                reason,
                "fail",
                now,
                connector_id=connector.connector_id,
            )
            log.error(
                events.DOCKER_API_ERROR,
                op="restart",
                connector_id=connector.connector_id,
                error=str(exc),
            )
            return
        self._metrics.restarts.labels(rn=rn_id).inc()
        await self._record(
            rn_id,
            "restart",
            1,
            reason,
            "success",
            now,
            connector_id=connector.connector_id,
        )

    async def _begin_replace(
        self,
        connector: ManagedConnector,
        reason: str,
        sample: ResourceSample | None,
        now: datetime,
        log: structlog.BoundLogger,
        *,
        actor: str = "auto",
    ) -> bool:
        """Begin a wait-for-healthy replace: provision the replacement, then wait.

        Provision the net-new replacement first so capacity never dips (Key
        Design Rule #4), then register a pending replace and return — the old,
        unhealthy Connector is **not** torn down here. A later cycle's
        :meth:`_process_pending_replaces` drains and deletes it only once the
        replacement reports ALIVE/healthy. If the replacement fails to provision,
        the old Connector is left in place for a later cycle to retry rather than
        leaving the RN short.

        ``actor`` is ``"manual"`` when driven by the override endpoint; it is
        carried on the pending replace so the eventual ``action.replace`` audit
        row records who initiated it. Returns whether the replacement was
        provisioned (and a pending replace registered).
        """
        rn_id = self._policy.remote_network_id
        new_id = await self._provision_one(now, log, actor=actor)
        if new_id is None:
            return False
        self._pending_replaces[connector.connector_id] = _PendingReplace(
            new_connector_id=new_id, started_at=now, reason=reason, actor=actor
        )
        log.info(
            events.HEALTH_REPLACE_PENDING,
            rn_id=rn_id,
            old_connector_id=connector.connector_id,
            new_connector_id=new_id,
            restart_window_seconds=self._policy.restart_window_seconds,
            reason=reason,
            state=connector.twingate_state.value if connector.twingate_state is not None else None,
            sample=self._sample_fields(sample),
            actor=actor,
        )
        return True

    async def _process_pending_replaces(
        self,
        connectors: list[ManagedConnector],
        now: datetime,
        log: structlog.BoundLogger,
    ) -> None:
        """Advance in-flight replaces: tear down old Connectors once new is healthy.

        For each pending replace (Key Design Rule #4):

        * if the old Connector already left the fleet, the replace is done —
          forget it;
        * if the replacement is now ALIVE/healthy, drain + delete the old
          Connector (the completion of the replace), counting it only when the
          old one is actually removed;
        * if the replacement is not yet healthy but the wait has exceeded
          ``replace_health_timeout_seconds``, this replace attempt has failed:
          log an alertable ``health.replace_timeout`` **once**, tear down the
          failed *replacement* (which never became ALIVE and so never carried
          traffic — Key Design Rule #4 forbids touching the OLD, traffic-serving
          Connector here) via the drain-before-delete path, and release the
          pending slot. The old Connector is left running and returns to normal
          health evaluation next cycle, where it re-escalates restart→replace —
          a fail-forward retry, each attempt independently alertable;
        * otherwise keep waiting.
        """
        if not self._pending_replaces:
            return
        rn_id = self._policy.remote_network_id
        by_id = {c.connector_id: c for c in connectors}
        timeout = self._policy.replace_health_timeout_seconds
        for old_id in list(self._pending_replaces):
            pending = self._pending_replaces[old_id]
            old = by_id.get(old_id)
            if old is None:
                # The old Connector is already gone (e.g. removed out-of-band);
                # the replacement stands in for it. Nothing left to tear down.
                del self._pending_replaces[old_id]
                continue
            new = by_id.get(pending.new_connector_id)
            if new is not None and self._is_replacement_healthy(new):
                removed = await self._deprovision_one(old, now, log, actor=pending.actor)
                log.info(
                    events.ACTION_REPLACE,
                    rn_id=rn_id,
                    old_connector_id=old_id,
                    new_connector_id=pending.new_connector_id,
                    old_removed=removed,
                    reason=pending.reason,
                    actor=pending.actor,
                )
                if removed:
                    self._metrics.replacements.labels(rn=rn_id).inc()
                    del self._pending_replaces[old_id]
                await self._record(
                    rn_id,
                    "replace",
                    1,
                    (
                        "replace complete: replacement healthy, old drained "
                        f"(restart window {self._policy.restart_window_seconds}s)"
                        if removed
                        else "replace incomplete: replacement healthy but old not removed"
                    ),
                    "success" if removed else "fail",
                    now,
                    connector_id=old_id,
                    actor=pending.actor,
                )
                continue
            waited = (now - pending.started_at).total_seconds()
            if waited >= timeout:
                # This replace attempt failed: the replacement never became
                # healthy in time. Emit the alertable timeout once, then clean
                # up so we neither re-warn every cycle nor leave the failed
                # replacement orphaned nor wedge the old Connector out of all
                # future remediation. The OLD (traffic-serving) Connector is
                # NEVER torn down here (Key Design Rule #4); only the failed
                # replacement — which never carried traffic — is removed.
                log.warning(
                    events.HEALTH_REPLACE_TIMEOUT,
                    rn_id=rn_id,
                    old_connector_id=old_id,
                    new_connector_id=pending.new_connector_id,
                    waited_s=round(waited, 1),
                )
                new = by_id.get(pending.new_connector_id)
                if new is not None:
                    await self._deprovision_one(new, now, log)
                # Release the slot regardless: the old Connector returns to
                # normal health evaluation next cycle (fail-forward retry).
                del self._pending_replaces[old_id]

    @staticmethod
    def _is_replacement_healthy(connector: ManagedConnector) -> bool:
        """Return whether a replacement is healthy enough to retire the old one.

        Requires an affirmative ALIVE from Twingate (a freshly provisioned
        Connector reports ``DEAD_NO_HEARTBEAT`` until its first heartbeat) and a
        Docker health that is not ``unhealthy``, so the old Connector is never
        torn down before the replacement is genuinely carrying traffic.
        """
        return (
            connector.twingate_state is ConnectorState.ALIVE
            and connector.docker_health != "unhealthy"
        )

    # -- helpers -------------------------------------------------------------

    async def _record(
        self,
        rn_id: str,
        action: str,
        count: int,
        reason: str,
        outcome: str,
        now: datetime,
        *,
        connector_id: str | None = None,
        actor: str = "auto",
    ) -> None:
        """Append an action to the history log (best-effort; never raises)."""
        record = ActionRecord(
            ts=now,
            rn_id=rn_id,
            action=action,
            count=count,
            reason=reason,
            outcome="success" if outcome == "success" else "fail",
            actor="manual" if actor == "manual" else "auto",
        )
        await self._state.record_action(record, connector_id=connector_id)

    def _publish_snapshot(
        self,
        cycle_id: str,
        now: datetime,
        connectors: list[ManagedConnector],
        samples: list[ResourceSample],
    ) -> None:
        """Build and publish the fleet snapshot for the status UI (no-op if unset)."""
        if self._status is None:
            return
        latest: dict[str, ResourceSample] = {}
        for sample in samples:
            latest[sample.connector_id] = sample  # last write wins this cycle

        statuses: list[ConnectorStatus] = []
        for connector in connectors:
            latest_sample = latest.get(connector.connector_id)
            statuses.append(
                ConnectorStatus(
                    connector_id=connector.connector_id,
                    name=connector.name,
                    twingate_state=(
                        connector.twingate_state.value
                        if connector.twingate_state is not None
                        else None
                    ),
                    docker_health=connector.docker_health,
                    cordoned=connector.cordoned,
                    cpu_pct_norm=latest_sample.cpu_pct_norm if latest_sample else None,
                    throughput_bps=latest_sample.throughput_bps if latest_sample else None,
                    mem_bytes=latest_sample.mem_bytes if latest_sample else None,
                )
            )
        rn_status = RemoteNetworkStatus(
            rn_id=self._policy.remote_network_id,
            name=self._policy.remote_network_name or self._policy.remote_network_id,
            count=len(connectors),
            min_connectors=self._policy.min_connectors,
            max_connectors=self._policy.max_connectors,
            connectors=statuses,
        )
        self._status.publish(FleetSnapshot(cycle_id=cycle_id, ts=now, remote_network=rn_status))

    # -- manual overrides (Session 7) ----------------------------------------

    async def manual_scale(self, rn_id: str, direction: ScaleDirection) -> bool:
        """Manually scale a Remote Network by one Connector (override endpoint).

        Honors the ceiling on scale-up and the floor on scale-down, reuses the
        same provision / drain-before-delete paths as the autoscaler, and audits
        the action with ``actor=manual``.

        Args:
            rn_id: The Remote Network to scale.
            direction: :attr:`ScaleDirection.UP` or :attr:`ScaleDirection.DOWN`.

        Returns:
            ``True`` if a Connector was added/removed; ``False`` if the bound
            would be breached or the action failed.
        """
        now = self._clock()
        log = logger.bind(actor="manual", rn_id=rn_id)
        async with self._action_lock:
            fleet = await self._discover(log)
            connectors = [c for c in fleet if c.rn_id == rn_id]
            count = len(connectors)

            if direction is ScaleDirection.UP:
                if count >= self._policy.max_connectors:
                    log.info(events.DECIDE_NO_ACTION, reason="manual scale-up at ceiling")
                    return False
                new_id = await self._provision_one(now, log, actor="manual")
                if new_id is not None:
                    await self._state.set_cooldown(rn_id, ScaleDirection.UP, now)
                return new_id is not None

            if direction is ScaleDirection.DOWN:
                if count <= self._policy.min_connectors:
                    log.info(events.DECIDE_NO_ACTION, reason="manual scale-down at floor")
                    return False
                victims = self._pick_victims(connectors, 1)
                if not victims:
                    return False
                removed = await self._deprovision_one(victims[0], now, log, actor="manual")
                if removed:
                    await self._state.set_cooldown(rn_id, ScaleDirection.DOWN, now)
                return removed

        return False

    async def manual_cordon(self, connector_id: str, cordoned: bool) -> bool:
        """Cordon or un-cordon a Connector and audit it (override endpoint).

        Cordon state is persisted; the next cycle's discovery marks the
        Connector so the autoscaler excludes it from scale-down victim
        selection. The override is written to the durable action history with
        ``actor=manual`` (so it appears in the UI's action table and survives a
        restart), not only as an in-memory event.

        Cordoning a Connector not present in the current fleet is refused
        (returns ``False``) so a typo cannot persist a phantom cordon;
        *un*-cordoning is always allowed so a stale cordon can be cleared even
        after the Connector is gone.

        Args:
            connector_id: The Connector to (un)cordon.
            cordoned: ``True`` to cordon, ``False`` to lift it.

        Returns:
            ``True`` if the cordon state was changed, ``False`` if a cordon was
            refused because the Connector is not in the current fleet.
        """
        now = self._clock()
        log = logger.bind(actor="manual", connector_id=connector_id)
        async with self._action_lock:
            fleet = await self._discover(log)
            match = next((c for c in fleet if c.connector_id == connector_id), None)
            if cordoned and match is None:
                log.warning(events.ACTION_CORDON, cordoned=cordoned, reason="connector not found")
                return False

            await self._state.set_cordon(connector_id, cordoned, ts=now)
            action = "cordon" if cordoned else "uncordon"
            rn_id = match.rn_id if match is not None else "unknown"
            await self._record(
                rn_id,
                action,
                1,
                "manual cordon" if cordoned else "manual uncordon",
                "success",
                now,
                connector_id=connector_id,
                actor="manual",
            )
            log.info(events.ACTION_CORDON, cordoned=cordoned, actor="manual", rn_id=rn_id)
            return True

    async def manual_replace(self, connector_id: str) -> bool:
        """Replace one Connector via the cycle-spanning net-new path (override).

        Reuses the wait-for-healthy replace path (Key Design Rule #4): the
        net-new replacement is provisioned **now** and a pending replace is
        registered; the target Connector is **not** torn down here — a later
        autoscaler cycle's :meth:`_process_pending_replaces` drains and deletes
        it only once the replacement reports ALIVE/healthy. Because the
        replacement is added before the old one is removed, the fleet never dips
        below ``min_connectors`` (the floor is honored implicitly). The action is
        audited with ``actor="manual"``.

        Args:
            connector_id: The Connector to replace.

        Returns:
            ``True`` if a replacement was provisioned and the replace is now in
            flight; ``False`` if the Connector is not in the current fleet, a
            replace is already pending for it, or the replacement failed to
            provision.
        """
        now = self._clock()
        log = logger.bind(actor="manual", connector_id=connector_id)
        async with self._action_lock:
            fleet = await self._discover(log)
            match = next((c for c in fleet if c.connector_id == connector_id), None)
            if match is None:
                log.warning(events.ACTION_REPLACE, reason="connector not found", actor="manual")
                return False
            if connector_id in self._pending_replaces:
                log.info(events.ACTION_REPLACE, reason="replace already in flight", actor="manual")
                return False
            return await self._begin_replace(
                match, "manual replace", None, now, log, actor="manual"
            )
