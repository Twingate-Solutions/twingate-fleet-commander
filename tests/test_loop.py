"""End-to-end control-loop tests against mocked collaborators.

A full discover→collect→decide→act cycle is exercised against a fake Twingate
client, a fake actuator, fake collectors, a real SQLite state store, a real
aggregator, and a real metrics registry. The matrix mirrors the safety rails:
scale-up on sustained high load, scale-down draining before delete, no action
within watermarks, cooldown suppression, collector-error isolation, the per-
cycle heartbeat + metric updates, restart-before-replace health remediation,
and the loop surviving a discovery failure.
"""

import asyncio
from collections.abc import MutableMapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import structlog
from pydantic import SecretStr

from fc.actuator.docker_actuator import DockerActuatorError
from fc.collectors.base import CollectorError
from fc.config import Policy
from fc.engine.aggregator import Aggregator
from fc.loop import ControlLoop
from fc.models import (
    ActionRecord,
    CollectorSource,
    ConnectorState,
    ManagedConnector,
    ResourceSample,
    ScaleDirection,
)
from fc.observability.metrics import Metrics
from fc.state import Cooldowns, StateStore
from fc.status import StatusState
from fc.twingate.client import ConnectorTokens, RemoteNetwork, TwingateApiError, TwingateClient

NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)

_POLICY_DICT: dict[str, Any] = {
    "poll_interval_seconds": 30,
    "connector_image": "twingate/connector:1",
    "collectors": {"docker_stats": True, "stdout_metrics": False},
    "labels": {
        "managed": "twingate.fc.managed",
        "remote_network": "twingate.fc.rn",
        "connector_id": "twingate.fc.connector_id",
    },
    "remote_network_id": "rn-1",
    "remote_network_name": "rn-1",
    "min_connectors": 2,
    "max_connectors": 6,
    "scale_step": 1,
    "scale_metrics": {
        "cpu": {"high_pct": 75.0, "low_pct": 25.0, "window_seconds": 300, "agg": "avg"},
        "throughput": {"high_mbps": 80.0, "low_mbps": 10.0, "window_seconds": 1200, "agg": "avg"},
    },
    "scale_up_cooldown_seconds": 600,
    "scale_down_cooldown_seconds": 1800,
    "drain_grace_seconds": 120,
    "max_restarts": 3,
    "restart_window_seconds": 600,
    # Disabled here so the existing single-cycle health tests assert restart
    # behavior directly; the startup grace window is wired-tested separately
    # (test_startup_grace_defers_restart) and unit-tested in test_decider.
    "startup_grace_seconds": 0,
    # Disabled by default so single-cycle health tests act immediately; the
    # duration gate is wired-tested explicitly (test_unhealthy_threshold_*).
    "unhealthy_threshold_seconds": 0,
}


def _policy(**overrides: Any) -> Policy:
    return Policy.model_validate({**_POLICY_DICT, **overrides})


# --- fakes -----------------------------------------------------------------


class FakeTwingate(TwingateClient):
    """A TwingateClient whose network calls are recorded, not performed."""

    def __init__(self, tg_connectors: list[ManagedConnector]) -> None:
        super().__init__("net", SecretStr("key"))
        self._tg = tg_connectors
        self.created: list[str] = []
        self.deleted: list[str] = []
        self._seq = 0
        self.create_should_raise = False

    async def list_connectors(self) -> list[ManagedConnector]:
        return list(self._tg)

    async def create_connector(self, rn_id: str, name: str | None = None) -> ManagedConnector:
        if self.create_should_raise:
            raise TwingateApiError("create failed", op_name="CreateConnector")
        self._seq += 1
        new_id = f"new-{self._seq}"
        self.created.append(new_id)
        return ManagedConnector(connector_id=new_id, name=name or new_id, rn_id=rn_id)

    async def generate_tokens(self, connector_id: str) -> ConnectorTokens:
        return ConnectorTokens(access_token=SecretStr("a"), refresh_token=SecretStr("r"))

    async def delete_connector(self, connector_id: str) -> None:
        self.deleted.append(connector_id)

    async def list_remote_networks(self) -> list[RemoteNetwork]:
        return [RemoteNetwork(id="rn-1", name="rn-1")]


class FakeActuator:
    """Structural Actuator that records lifecycle calls in order."""

    def __init__(self, managed: list[ManagedConnector]) -> None:
        self._managed = managed
        self.calls: list[tuple[str, str]] = []
        self.list_should_raise = False
        self.deprovision_should_raise = False
        self._seq = 0

    async def provision(
        self,
        rn_id: str,
        connector_id: str,
        name: str,
        tokens: ConnectorTokens,
    ) -> str:
        self._seq += 1
        self.calls.append(("provision", connector_id))
        return f"ctr-{connector_id}"

    async def deprovision(self, connector: ManagedConnector) -> None:
        self.calls.append(("deprovision", connector.connector_id))
        if self.deprovision_should_raise:
            raise DockerActuatorError("stop failed", op="deprovision")

    async def restart(self, connector: ManagedConnector) -> None:
        self.calls.append(("restart", connector.connector_id))

    async def list_managed(self) -> list[ManagedConnector]:
        if self.list_should_raise:
            raise DockerActuatorError("socket gone", op="list_managed")
        return list(self._managed)


class FakeCollector:
    """A collector that returns a fixed sample, or raises, per call."""

    def __init__(
        self,
        source: CollectorSource,
        *,
        cpu: float | None = None,
        throughput: float | None = None,
        raises: bool = False,
    ) -> None:
        self.source = source
        self._cpu = cpu
        self._throughput = throughput
        self._raises = raises

    async def collect(self, connector: ManagedConnector) -> ResourceSample | None:
        if self._raises:
            raise CollectorError("boom")
        if connector.container_id is None:
            return None
        return ResourceSample(
            connector_id=connector.connector_id,
            source=self.source,
            ts=NOW,
            cpu_pct_norm=self._cpu,
            mem_bytes=None,
            mem_pct=None,
            throughput_bps=self._throughput,
        )


async def _noop_sleep(_seconds: float) -> None:
    return None


def _container(cid: str, *, health: str = "healthy") -> ManagedConnector:
    return ManagedConnector(
        connector_id=cid,
        name=cid,
        rn_id="rn-1",
        container_id=f"ctr-{cid}",
        docker_health=health,
    )


def _tg(cid: str, state: ConnectorState = ConnectorState.ALIVE) -> ManagedConnector:
    return ManagedConnector(connector_id=cid, name=cid, rn_id="rn-1", twingate_state=state)


async def _make_loop(
    tmp_path: Path,
    *,
    containers: list[ManagedConnector],
    tg_connectors: list[ManagedConnector],
    collectors: list[Any],
    policy: Policy | None = None,
    status: StatusState | None = None,
    clock: Any = None,
) -> tuple[ControlLoop, FakeTwingate, FakeActuator, Metrics, StateStore]:
    state = StateStore(tmp_path / "state.sqlite3")
    await state.init()
    twingate = FakeTwingate(tg_connectors)
    actuator = FakeActuator(containers)
    metrics = Metrics()
    loop = ControlLoop(
        policy=policy or _policy(),
        twingate=twingate,
        actuator=actuator,
        collectors=collectors,
        aggregator=Aggregator(retention_seconds=3600),
        state=state,
        metrics=metrics,
        clock=clock or (lambda: NOW),
        name_factory=lambda rn: f"{rn}-new",
        id_factory=lambda: "cycle-1",
        sleep=_noop_sleep,
        status=status,
    )
    return loop, twingate, actuator, metrics, state


def _events(captured: list[MutableMapping[str, Any]]) -> set[str]:
    return {str(entry["event"]) for entry in captured}


# --- tests -----------------------------------------------------------------


async def test_full_cycle_scale_up(tmp_path: Path) -> None:
    loop, twingate, actuator, metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=90.0)],
    )
    with structlog.testing.capture_logs() as cap:
        result = await loop.run_cycle()

    assert result.ok
    assert result.decisions[0].direction is ScaleDirection.UP
    assert twingate.created == ["new-1"]
    assert ("provision", "new-1") in actuator.calls
    assert (
        metrics.registry.get_sample_value(
            "fc_scale_actions_total", {"rn": "rn-1", "direction": "up"}
        )
        == 1.0
    )
    cooldowns = await state.get_cooldowns("rn-1")
    assert cooldowns.last_up_ts is not None
    assert "decide.scale_up" in _events(cap)


async def test_scale_up_any_mode_on_single_hot_connector(tmp_path: Path) -> None:
    # Per-connector hot spread: c1 pinned hot, the rest cool. Under the ``any``
    # trigger a single hot connector is enough to scale the fleet up, even though
    # the fleet mean would be well below the high watermark.
    hot = FakeCollector(CollectorSource.DOCKER_STATS, cpu=100.0)
    cool = FakeCollector(CollectorSource.DOCKER_STATS, cpu=10.0)

    class _PerConnectorCollector:
        """Routes a hot reading to c1 and a cool reading to everyone else."""

        source = CollectorSource.DOCKER_STATS

        async def collect(self, connector: ManagedConnector) -> ResourceSample | None:
            chosen = hot if connector.connector_id == "c1" else cool
            return await chosen.collect(connector)

    loop, twingate, actuator, _metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container(f"c{i}") for i in range(1, 5)],
        tg_connectors=[_tg(f"c{i}") for i in range(1, 5)],
        collectors=[_PerConnectorCollector()],
        policy=_policy(scale_up_trigger="any"),
    )
    result = await loop.run_cycle()

    assert result.decisions[0].direction is ScaleDirection.UP
    assert result.decisions[0].metrics["connectors_over_high_watermark"] == 1.0
    assert result.decisions[0].metrics["hot_connector_max"] == 100.0
    assert twingate.created == ["new-1"]
    assert ("provision", "new-1") in actuator.calls


async def test_scale_down_drains_before_delete(tmp_path: Path) -> None:
    loop, twingate, actuator, metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2"), _container("c3")],
        tg_connectors=[_tg("c1"), _tg("c2"), _tg("c3")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=5.0)],
    )
    result = await loop.run_cycle()

    assert result.decisions[0].direction is ScaleDirection.DOWN
    # Logical delete (stop routing) happens before the container is removed.
    victim = twingate.deleted[0]
    assert actuator.calls == [("deprovision", victim)]
    assert twingate.deleted == [victim]
    assert (
        metrics.registry.get_sample_value(
            "fc_scale_actions_total", {"rn": "rn-1", "direction": "down"}
        )
        == 1.0
    )
    cooldowns = await state.get_cooldowns("rn-1")
    assert cooldowns.last_down_ts is not None


async def test_no_action_within_watermarks(tmp_path: Path) -> None:
    loop, twingate, actuator, _metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
    )
    with structlog.testing.capture_logs() as cap:
        result = await loop.run_cycle()

    assert result.decisions[0].direction is ScaleDirection.NONE
    assert twingate.created == [] and twingate.deleted == []
    assert actuator.calls == []
    assert "decide.no_action" in _events(cap)
    # The idle decision still surfaces its signal metrics for diagnostics.
    no_action_event = next(e for e in cap if e["event"] == "decide.no_action")
    assert "metrics" in no_action_event
    assert "connectors_over_high_watermark" in no_action_event["metrics"]


async def test_cooldown_suppresses_scale_up(tmp_path: Path) -> None:
    loop, twingate, _actuator, _metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=90.0)],
    )
    await state.set_cooldown("rn-1", ScaleDirection.UP, NOW - timedelta(seconds=60))

    with structlog.testing.capture_logs() as cap:
        result = await loop.run_cycle()

    assert result.decisions[0].direction is ScaleDirection.NONE
    assert twingate.created == []
    assert "decide.cooldown_skip" in _events(cap)
    cooldown_event = next(e for e in cap if e["event"] == "decide.cooldown_skip")
    assert "metrics" in cooldown_event
    assert "cooldown_seconds_remaining" in cooldown_event["metrics"]


async def test_collector_error_isolated_cycle_completes(tmp_path: Path) -> None:
    loop, _twingate, _actuator, metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1")],
        tg_connectors=[_tg("c1")],
        collectors=[
            FakeCollector(CollectorSource.DOCKER_STATS, raises=True),
            FakeCollector(CollectorSource.STDOUT_METRICS, cpu=50.0),
        ],
    )
    with structlog.testing.capture_logs() as cap:
        result = await loop.run_cycle()

    assert result.ok
    assert result.sample_count == 1  # the surviving collector's sample
    assert (
        metrics.registry.get_sample_value("fc_collect_errors_total", {"collector": "docker_stats"})
        == 1.0
    )
    assert "collect.error" in _events(cap)


async def test_heartbeat_and_metrics_each_cycle(tmp_path: Path) -> None:
    loop, _twingate, _actuator, metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
    )
    with structlog.testing.capture_logs() as cap:
        result = await loop.run_cycle()

    heartbeat = [e for e in cap if e["event"] == "loop.cycle.complete"]
    assert len(heartbeat) == 1
    assert "duration_ms" in heartbeat[0] and heartbeat[0]["rn_count"] == 1
    assert heartbeat[0]["cycle_id"] == "cycle-1"
    assert metrics.registry.get_sample_value("fc_loop_iterations_total") == 1.0
    assert (
        metrics.registry.get_sample_value("fc_last_successful_cycle_timestamp_seconds")
        == NOW.timestamp()
    )
    assert result.ok


async def test_health_restart_dead_connector(tmp_path: Path) -> None:
    loop, _twingate, actuator, metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1", ConnectorState.DEAD_NO_HEARTBEAT), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
    )
    with structlog.testing.capture_logs() as cap:
        await loop.run_cycle()

    assert ("restart", "c1") in actuator.calls
    assert metrics.registry.get_sample_value("fc_restarts_total", {"rn": "rn-1"}) == 1.0
    events = _events(cap)
    assert "health.connector_dead" in events and "action.restart" in events
    restarts = await state.count_recent_restarts("c1", since=NOW - timedelta(seconds=600))
    assert restarts == 1
    # The restart line carries the decider's reason and the triggering sample.
    restart_event = next(e for e in cap if e["event"] == "action.restart")
    assert restart_event["reason"]  # non-empty reason string
    assert restart_event["state"] == "DEAD_NO_HEARTBEAT"
    assert restart_event["sample"] == {
        "cpu_pct_norm": 50.0,
        "throughput_bps": None,
        "mem_bytes": None,
        "source": "docker_stats",
    }
    # fc_health_actions_total increments with kind=restart, the dead reason class.
    assert (
        metrics.registry.get_sample_value(
            "fc_health_actions_total",
            {"kind": "restart", "reason_class": "dead_no_heartbeat"},
        )
        == 1.0
    )


async def test_health_restart_docker_unhealthy_reason_class(tmp_path: Path) -> None:
    # A connector that is ALIVE in Twingate but unhealthy per Docker is restarted
    # and classified as docker_unhealthy on the fc_health_actions counter.
    loop, _twingate, actuator, metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1", health="unhealthy"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
    )
    with structlog.testing.capture_logs() as cap:
        await loop.run_cycle()

    assert ("restart", "c1") in actuator.calls
    restart_event = next(e for e in cap if e["event"] == "action.restart")
    assert restart_event["reason"]
    assert restart_event["state"] == "ALIVE"
    assert (
        metrics.registry.get_sample_value(
            "fc_health_actions_total",
            {"kind": "restart", "reason_class": "docker_unhealthy"},
        )
        == 1.0
    )


async def test_startup_grace_defers_restart(tmp_path: Path) -> None:
    # A freshly-discovered, never-heartbeated DEAD_NO_HEARTBEAT connector is in
    # its startup grace window this cycle, so the loop must NOT restart it.
    graced = _policy(startup_grace_seconds=90)
    loop, _twingate, actuator, _metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1", ConnectorState.DEAD_NO_HEARTBEAT), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
        policy=graced,
    )
    with structlog.testing.capture_logs() as cap:
        await loop.run_cycle()

    assert all(call[0] != "restart" for call in actuator.calls)
    assert "action.restart" not in _events(cap)


async def _seed_restarts(state: StateStore, connector_id: str, n: int) -> None:
    """Record ``n`` prior restart actions so the next health decision replaces."""
    for _ in range(n):
        await state.record_action(
            ActionRecord(
                ts=NOW, rn_id="rn-1", action="restart", count=1, reason="r", outcome="success"
            ),
            connector_id=connector_id,
        )


async def test_health_replace_waits_for_healthy_then_drains_old(tmp_path: Path) -> None:
    # Wait-for-healthy, cycle-spanning replace (Key Design Rule #4): cycle 1
    # provisions the replacement but leaves the old connector running; only once
    # the replacement reports ALIVE/healthy (cycle 2) is the old one drained.
    loop, twingate, actuator, metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1", ConnectorState.DEAD_HEARTBEAT_TOO_OLD), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
    )
    await _seed_restarts(state, "c1", 3)

    # Cycle 1: replacement provisioned, old NOT torn down (awaiting health).
    with structlog.testing.capture_logs() as cap1:
        await loop.run_cycle()
    assert twingate.created == ["new-1"]
    assert ("provision", "new-1") in actuator.calls
    assert "c1" not in twingate.deleted
    assert ("deprovision", "c1") not in actuator.calls
    assert "health.replace_pending" in _events(cap1)
    assert metrics.registry.get_sample_value("fc_replacements_total", {"rn": "rn-1"}) is None
    # The replace initiation carries the decider's reason + triggering sample.
    pending_event = next(e for e in cap1 if e["event"] == "health.replace_pending")
    assert pending_event["reason"]
    assert pending_event["state"] == "DEAD_HEARTBEAT_TOO_OLD"
    assert pending_event["sample"] == {
        "cpu_pct_norm": 50.0,
        "throughput_bps": None,
        "mem_bytes": None,
        "source": "docker_stats",
    }
    # A replace is one health action: kind=replace, dead_heartbeat_too_old class.
    assert (
        metrics.registry.get_sample_value(
            "fc_health_actions_total",
            {"kind": "replace", "reason_class": "dead_heartbeat_too_old"},
        )
        == 1.0
    )

    # The replacement now appears in the fleet, ALIVE and healthy.
    actuator._managed.append(_container("new-1"))
    twingate._tg.append(_tg("new-1"))

    # Cycle 2: replacement healthy → drain + delete the old one; replace done.
    with structlog.testing.capture_logs() as cap2:
        await loop.run_cycle()
    assert "c1" in twingate.deleted
    assert ("deprovision", "c1") in actuator.calls
    assert metrics.registry.get_sample_value("fc_replacements_total", {"rn": "rn-1"}) == 1.0
    replace_event = next(e for e in cap2 if e["event"] == "action.replace")
    assert replace_event["old_removed"] is True
    # The completion line reuses the stored reason from when the replace began.
    assert replace_event["reason"]


async def test_replace_does_not_drain_old_until_replacement_healthy(tmp_path: Path) -> None:
    # If the replacement is present but not yet ALIVE, the old connector is left
    # in place across cycles — capacity is never dropped pre-emptively.
    loop, twingate, actuator, _metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1", ConnectorState.DEAD_HEARTBEAT_TOO_OLD), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
        policy=_policy(startup_grace_seconds=90),
    )
    await _seed_restarts(state, "c1", 3)
    await loop.run_cycle()  # provisions new-1, registers pending

    # Replacement appears but is still DEAD_NO_HEARTBEAT (not yet carrying load).
    actuator._managed.append(_container("new-1"))
    twingate._tg.append(_tg("new-1", ConnectorState.DEAD_NO_HEARTBEAT))

    await loop.run_cycle()
    assert "c1" not in twingate.deleted
    assert ("deprovision", "c1") not in actuator.calls


async def test_replace_timeout_tears_down_failed_replacement_and_frees_old(tmp_path: Path) -> None:
    # When the replacement never becomes healthy within
    # replace_health_timeout_seconds, the replace attempt has failed: FC must
    # (a) emit the alertable timeout, (b) tear down the failed *replacement*
    # (which never carried traffic), (c) clear the pending slot, and (d) leave
    # the OLD, traffic-serving connector running (Key Design Rule #4). The old
    # one then becomes eligible for remediation again on the next cycle.
    clock = {"t": NOW}
    loop, twingate, actuator, _metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1", ConnectorState.DEAD_HEARTBEAT_TOO_OLD), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
        policy=_policy(startup_grace_seconds=90, replace_health_timeout_seconds=300),
        clock=lambda: clock["t"],
    )
    await _seed_restarts(state, "c1", 3)
    await loop.run_cycle()  # provisions new-1 at NOW, registers pending

    # Replacement appears but stays unhealthy; the clock moves past the bound.
    actuator._managed.append(_container("new-1"))
    twingate._tg.append(_tg("new-1", ConnectorState.DEAD_NO_HEARTBEAT))
    clock["t"] = NOW + timedelta(seconds=301)

    with structlog.testing.capture_logs() as cap:
        await loop.run_cycle()
    # (a) alertable timeout emitted, (b) failed replacement torn down, (c) old
    # connector left running (never deprovisioned-as-old by the timeout path).
    assert "health.replace_timeout" in _events(cap)
    assert "new-1" in twingate.deleted
    assert ("deprovision", "new-1") in actuator.calls
    assert "c1" not in twingate.deleted
    assert ("deprovision", "c1") not in actuator.calls

    # (d) the pending slot for the failed replace is released, so the still-
    # unhealthy old connector is no longer excluded from remediation: the same
    # cycle's health pass re-escalates it (its restart count still exceeds
    # max_restarts), beginning a fresh replace — a fail-forward retry. The old
    # entry for new-1 is gone; a new pending replace (new-2) takes its place.
    assert "health.replace_pending" in _events(cap)
    assert "c1" in loop._pending_replaces
    assert loop._pending_replaces["c1"].new_connector_id == "new-2"
    assert twingate.created == ["new-1", "new-2"]
    # The old connector itself is still untouched (never the one torn down).
    assert "c1" not in twingate.deleted


async def test_unhealthy_threshold_gates_then_acts(tmp_path: Path) -> None:
    # A connector unhealthy for less than unhealthy_threshold_seconds is left
    # alone (brief blip); once it has been continuously unhealthy past the gate,
    # it is restarted.
    clock = {"t": NOW}
    loop, _twingate, actuator, _metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1", health="unhealthy"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
        policy=_policy(unhealthy_threshold_seconds=60),
        clock=lambda: clock["t"],
    )

    await loop.run_cycle()  # first unhealthy observation → within gate → no action
    assert all(call[0] != "restart" for call in actuator.calls)

    clock["t"] = NOW + timedelta(seconds=61)
    await loop.run_cycle()  # continuously unhealthy past the gate → restart
    assert ("restart", "c1") in actuator.calls


async def test_unhealthy_timer_resets_on_recovery(tmp_path: Path) -> None:
    # Recovery between cycles resets the continuous-unhealth timer, so a later
    # blip starts counting from scratch and does not immediately act.
    clock = {"t": NOW}
    loop, _twingate, actuator, _metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1", health="unhealthy"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
        policy=_policy(unhealthy_threshold_seconds=60),
        clock=lambda: clock["t"],
    )
    await loop.run_cycle()  # unhealthy at NOW (within gate)

    # Recovers: the loop's first-unhealthy timer for c1 clears.
    actuator._managed[0] = _container("c1", health="healthy")
    clock["t"] = NOW + timedelta(seconds=40)
    await loop.run_cycle()

    # Goes unhealthy again well after the original first-unhealthy time; because
    # the timer reset, it is still within the gate and must not act yet.
    actuator._managed[0] = _container("c1", health="unhealthy")
    clock["t"] = NOW + timedelta(seconds=80)
    await loop.run_cycle()
    assert all(call[0] != "restart" for call in actuator.calls)


async def test_remote_network_decide_error_isolated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The single Remote Network blows up while deciding (a non-typed sqlite-
    # style error). The cycle must survive: the error is logged as
    # loop.rn.error and the heartbeat + freshness timestamp still publish
    # (Rule: a decide/act failure never aborts the cycle).
    loop, twingate, actuator, metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=90.0)],
    )

    async def flaky_get_cooldowns(rn_id: str) -> Cooldowns:
        raise RuntimeError("sqlite locked")

    monkeypatch.setattr(loop._state, "get_cooldowns", flaky_get_cooldowns)

    with structlog.testing.capture_logs() as cap:
        result = await loop.run_cycle()

    assert result.ok  # the cycle completes despite the decide failure
    rn_errors = [e for e in cap if e["event"] == "loop.rn.error"]
    assert rn_errors and rn_errors[0]["rn_id"] == "rn-1"
    # The decide failed before any action, so nothing was provisioned.
    assert twingate.created == [] and actuator.calls == []
    # Heartbeat + freshness timestamp still published.
    assert any(e["event"] == "loop.cycle.complete" for e in cap)
    assert (
        metrics.registry.get_sample_value("fc_last_successful_cycle_timestamp_seconds")
        == NOW.timestamp()
    )


async def test_discovery_failure_aborts_cycle_but_survives(tmp_path: Path) -> None:
    loop, _twingate, actuator, metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1")],
        tg_connectors=[_tg("c1")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
    )
    actuator.list_should_raise = True

    with structlog.testing.capture_logs() as cap:
        result = await loop.run_cycle()

    assert result.ok is False
    assert "loop.cycle.error" in _events(cap)
    assert metrics.registry.get_sample_value("fc_docker_api_errors_total") == 1.0


async def test_provision_failure_records_no_cooldown(tmp_path: Path) -> None:
    loop, twingate, actuator, metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=90.0)],
    )
    twingate.create_should_raise = True

    with structlog.testing.capture_logs() as cap:
        result = await loop.run_cycle()

    assert result.ok  # the cycle survives a provision failure
    assert ("provision", "new-1") not in actuator.calls
    assert metrics.registry.get_sample_value("fc_twingate_api_errors_total") == 1.0
    cooldowns = await state.get_cooldowns("rn-1")
    assert cooldowns.last_up_ts is None  # no successful provision → no cooldown
    assert "action.provision.fail" in _events(cap)


async def test_run_forever_runs_then_stops(tmp_path: Path) -> None:
    loop, _twingate, _actuator, metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
    )
    stop = asyncio.Event()
    task = asyncio.create_task(loop.run_forever(stop))
    await asyncio.sleep(0)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert (metrics.registry.get_sample_value("fc_loop_iterations_total") or 0) >= 1.0


@pytest.mark.parametrize("state_label", ["DEAD_NO_HEARTBEAT", "DEAD_NO_RELAYS"])
async def test_seed_container_joined_by_name(tmp_path: Path, state_label: str) -> None:
    # A seed container has no connector_id label; it must still join to its
    # Twingate connector by name so its liveness state drives health.
    seed = ManagedConnector(
        connector_id="",
        name="office-seed",
        rn_id="rn-1",
        container_id="ctr-seed",
        docker_health="healthy",
    )
    loop, _twingate, actuator, _metrics, _state = await _make_loop(
        tmp_path,
        containers=[seed, _container("c2")],
        tg_connectors=[
            ManagedConnector(
                connector_id="logical-1",
                name="office-seed",
                rn_id="rn-1",
                twingate_state=ConnectorState(state_label),
            ),
            _tg("c2"),
        ],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
    )
    await loop.run_cycle()
    # The seed adopted the logical id from the name join and was restarted.
    assert ("restart", "logical-1") in actuator.calls


# --- Session 7: snapshot publishing, cordon, manual overrides ---------------


async def test_cycle_publishes_status_snapshot(tmp_path: Path) -> None:
    status = StatusState()
    loop, _twingate, _actuator, _metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0, throughput=2_000_000.0)],
        status=status,
    )
    await loop.run_cycle()

    snap = status.get()
    assert snap is not None
    assert snap.cycle_id == "cycle-1"
    rn = snap.remote_network
    assert rn.rn_id == "rn-1" and rn.count == 2
    assert rn.min_connectors == 2 and rn.max_connectors == 6
    c1 = next(c for c in rn.connectors if c.connector_id == "c1")
    assert c1.twingate_state == "ALIVE"
    assert c1.cpu_pct_norm == 50.0
    assert c1.throughput_bps == 2_000_000.0


async def test_cordoned_connector_not_scaled_down(tmp_path: Path) -> None:
    # Three low-load connectors would scale down by one, but the only sensible
    # victim is cordoned, so it must not be chosen.
    loop, twingate, actuator, _metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2"), _container("c3")],
        tg_connectors=[_tg("c1"), _tg("c2"), _tg("c3")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=5.0)],
    )
    # Cordon c1 and c2, leaving c3 the only eligible victim.
    await state.set_cordon("c1", True, ts=NOW)
    await state.set_cordon("c2", True, ts=NOW)

    await loop.run_cycle()

    assert twingate.deleted == ["c3"]
    assert ("deprovision", "c3") in actuator.calls
    assert "c1" not in twingate.deleted and "c2" not in twingate.deleted


async def test_manual_scale_up_honors_ceiling(tmp_path: Path) -> None:
    # At max_connectors (6) a manual scale-up must refuse.
    containers = [_container(f"c{i}") for i in range(6)]
    tg = [_tg(f"c{i}") for i in range(6)]
    loop, twingate, _actuator, _metrics, _state = await _make_loop(
        tmp_path, containers=containers, tg_connectors=tg, collectors=[]
    )
    acted = await loop.manual_scale("rn-1", ScaleDirection.UP)
    assert acted is False
    assert twingate.created == []


async def test_manual_scale_down_honors_floor(tmp_path: Path) -> None:
    # At the floor (2) a manual scale-down must refuse.
    loop, twingate, actuator, _metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[],
    )
    acted = await loop.manual_scale("rn-1", ScaleDirection.DOWN)
    assert acted is False
    assert twingate.deleted == []
    assert actuator.calls == []


async def test_manual_scale_up_provisions_and_audits_manual(tmp_path: Path) -> None:
    loop, twingate, actuator, _metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[],
    )
    acted = await loop.manual_scale("rn-1", ScaleDirection.UP)
    assert acted is True
    assert twingate.created == ["new-1"]
    assert ("provision", "new-1") in actuator.calls
    actions = await state.recent_actions(limit=5)
    assert actions[0].actor == "manual"


async def test_manual_cordon_persists_and_applies_next_cycle(tmp_path: Path) -> None:
    loop, _twingate, actuator, _metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2"), _container("c3")],
        tg_connectors=[_tg("c1"), _tg("c2"), _tg("c3")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=5.0)],
    )
    await loop.manual_cordon("c1", True)
    await loop.manual_cordon("c2", True)
    assert await state.list_cordoned() == {"c1", "c2"}

    await loop.run_cycle()
    # Only c3 is eligible as the scale-down victim.
    assert ("deprovision", "c3") in actuator.calls
    assert ("deprovision", "c1") not in actuator.calls


async def test_scale_down_with_scale_step_and_cordons_holds_floor(tmp_path: Path) -> None:
    # 4 connectors, min 2, scale_step 2, two cordoned. The decider sizes the
    # scale-down against the total (4-2=2), but cordoned connectors are never
    # victims: only the two uncordoned may be removed, so the running floor of
    # 2 is preserved. Regression for the cordon/scale_step floor interaction.
    loop, twingate, actuator, _metrics, state = await _make_loop(
        tmp_path,
        containers=[_container(f"c{i}") for i in range(1, 5)],
        tg_connectors=[_tg(f"c{i}") for i in range(1, 5)],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=5.0)],
        policy=_policy(scale_step=2),
    )
    await state.set_cordon("c1", True, ts=NOW)
    await state.set_cordon("c2", True, ts=NOW)

    await loop.run_cycle()

    removed = {cid for kind, cid in actuator.calls if kind == "deprovision"}
    assert removed == {"c3", "c4"}  # only the uncordoned are eligible
    assert "c1" not in twingate.deleted and "c2" not in twingate.deleted
    # 4 started, exactly 2 removed → 2 remain == floor; never below.
    assert 4 - len(removed) == 2


async def test_manual_cordon_audited_in_action_history(tmp_path: Path) -> None:
    # A cordon override must land in the durable action history with
    # actor=manual (so it shows in the UI's action table and survives restart).
    loop, _twingate, _actuator, _metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[],
    )
    acted = await loop.manual_cordon("c1", True)
    assert acted is True
    actions = await state.recent_actions(limit=5)
    assert actions[0].action == "cordon"
    assert actions[0].actor == "manual"
    assert actions[0].rn_id == "rn-1"


async def test_manual_cordon_unknown_connector_refused(tmp_path: Path) -> None:
    # Cordoning a Connector not in the fleet is refused (no phantom cordon, no
    # audit row); un-cordoning is still allowed so stale rows can be cleared.
    loop, _twingate, _actuator, _metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[],
    )
    acted = await loop.manual_cordon("ghost", True)
    assert acted is False
    assert await state.list_cordoned() == set()
    assert await state.recent_actions(limit=5) == []


async def test_manual_replace_provisions_new_and_waits_for_healthy(tmp_path: Path) -> None:
    # A manual replace is net-new and cycle-spanning (Q1b + Rule #4): it
    # provisions the replacement and registers a pending replace WITHOUT tearing
    # down the target. At the floor (2 connectors) this still holds — capacity is
    # never dropped, so the floor is honored implicitly.
    loop, twingate, actuator, _metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[],
    )
    acted = await loop.manual_replace("c1")
    assert acted is True
    # Replacement provisioned; target NOT torn down yet (wait-for-healthy).
    assert twingate.created == ["new-1"]
    assert ("provision", "new-1") in actuator.calls
    assert "c1" not in twingate.deleted
    assert ("deprovision", "c1") not in actuator.calls
    # A pending replace is registered and tagged as a manual action.
    assert "c1" in loop._pending_replaces
    assert loop._pending_replaces["c1"].new_connector_id == "new-1"
    assert loop._pending_replaces["c1"].actor == "manual"
    # The provision step is audited with actor=manual.
    actions = await state.recent_actions(limit=5)
    assert actions[0].actor == "manual"


async def test_manual_replace_completes_with_manual_actor_on_next_cycle(tmp_path: Path) -> None:
    # Once the replacement reports ALIVE/healthy on a later cycle, the old
    # connector is drained and the completion is audited as actor=manual.
    loop, twingate, actuator, metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[],
    )
    await loop.manual_replace("c1")

    # The replacement appears in the fleet, ALIVE and healthy.
    actuator._managed.append(_container("new-1"))
    twingate._tg.append(_tg("new-1"))

    with structlog.testing.capture_logs() as cap:
        await loop.run_cycle()
    assert "c1" in twingate.deleted
    assert ("deprovision", "c1") in actuator.calls
    assert metrics.registry.get_sample_value("fc_replacements_total", {"rn": "rn-1"}) == 1.0
    replace_event = next(e for e in cap if e["event"] == "action.replace")
    assert replace_event["actor"] == "manual"
    replace_actions = [a for a in await state.recent_actions(limit=10) if a.action == "replace"]
    assert replace_actions and replace_actions[0].actor == "manual"


async def test_manual_replace_unknown_connector_refused(tmp_path: Path) -> None:
    # Replacing a Connector not in the current fleet is refused — no provision,
    # no pending slot.
    loop, twingate, actuator, _metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[],
    )
    acted = await loop.manual_replace("ghost")
    assert acted is False
    assert twingate.created == []
    assert actuator.calls == []
    assert "ghost" not in loop._pending_replaces


async def test_manual_replace_already_in_flight_refused(tmp_path: Path) -> None:
    # A second manual replace for a connector already mid-replace is a no-op
    # (no duplicate replacement).
    loop, twingate, _actuator, _metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1"), _tg("c2")],
        collectors=[],
    )
    assert await loop.manual_replace("c1") is True
    assert await loop.manual_replace("c1") is False
    assert twingate.created == ["new-1"]  # only one replacement ever provisioned
