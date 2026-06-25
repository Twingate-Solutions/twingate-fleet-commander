"""End-to-end control-loop tests against mocked collaborators.

A full discover→collect→decide→act cycle is exercised against a fake Twingate
client, a fake actuator, fake collectors, a real SQLite state store, a real
aggregator, and a real metrics registry. The matrix mirrors the safety rails:
scale-up on sustained high load, scale-down draining before delete, no action
within watermarks, cooldown suppression, collector-error isolation, the per-
cycle heartbeat + metric updates, restart-before-replace health remediation,
janus-lock skipping, and the loop surviving a discovery failure.
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
    "metrics_port": 9999,
    "collectors": {"docker_stats": True, "stdout_metrics": False, "prometheus": True},
    "labels": {
        "managed": "twingate.fc.managed",
        "remote_network": "twingate.fc.rn",
        "connector_id": "twingate.fc.connector_id",
    },
    "janus_lock_label": "twingate.janus.upgrading",
    "defaults": {
        "min_connectors": 2,
        "max_connectors": 6,
        "scale_step": 1,
        "cpu_high_pct": 75.0,
        "cpu_low_pct": 25.0,
        "throughput_high_mbps": 80.0,
        "throughput_low_mbps": 10.0,
        "mem_ceiling_bytes": 0,
        "scale_up_window_seconds": 300,
        "scale_down_window_seconds": 1200,
        "scale_up_cooldown_seconds": 600,
        "scale_down_cooldown_seconds": 1800,
        "drain_grace_seconds": 120,
        "max_restarts": 3,
        "restart_window_seconds": 600,
        # Disabled here so the existing single-cycle health tests assert restart
        # behavior directly; the startup grace window is wired-tested separately
        # (test_startup_grace_defers_restart) and unit-tested in test_decider.
        "startup_grace_seconds": 0,
    },
    "remote_networks": [{"id": "rn-1", "name": "rn-1"}],
}


def _policy(**defaults_overrides: Any) -> Policy:
    data = {**_POLICY_DICT}
    if defaults_overrides:
        data["defaults"] = {**_POLICY_DICT["defaults"], **defaults_overrides}
    return Policy.model_validate(data)


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
        *,
        mem_limit_bytes: int | None = None,
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


def _container(cid: str, *, health: str = "healthy", janus: bool = False) -> ManagedConnector:
    return ManagedConnector(
        connector_id=cid,
        name=cid,
        rn_id="rn-1",
        container_id=f"ctr-{cid}",
        docker_health=health,
        janus_locked=janus,
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
        clock=lambda: NOW,
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


async def test_collector_error_isolated_cycle_completes(tmp_path: Path) -> None:
    loop, _twingate, _actuator, metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1")],
        tg_connectors=[_tg("c1")],
        collectors=[
            FakeCollector(CollectorSource.DOCKER_STATS, raises=True),
            FakeCollector(CollectorSource.PROMETHEUS, cpu=50.0),
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


async def test_startup_grace_defers_restart(tmp_path: Path) -> None:
    # A freshly-discovered, never-heartbeated DEAD_NO_HEARTBEAT connector is in
    # its startup grace window this cycle, so the loop must NOT restart it.
    graced = Policy.model_validate(
        {**_POLICY_DICT, "defaults": {**_POLICY_DICT["defaults"], "startup_grace_seconds": 90}}
    )
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


async def test_health_replace_after_max_restarts(tmp_path: Path) -> None:
    loop, twingate, actuator, metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1", ConnectorState.DEAD_HEARTBEAT_TOO_OLD), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
    )
    for _ in range(3):
        await state.record_action(
            ActionRecord(
                ts=NOW, rn_id="rn-1", action="restart", count=1, reason="r", outcome="success"
            ),
            connector_id="c1",
        )

    with structlog.testing.capture_logs() as cap:
        await loop.run_cycle()

    # Provision-new-then-delete-old: a new connector is created and the old
    # one's logical Connector is deleted and its container removed.
    assert twingate.created  # replacement provisioned
    assert "c1" in twingate.deleted
    assert ("deprovision", "c1") in actuator.calls
    assert metrics.registry.get_sample_value("fc_replacements_total", {"rn": "rn-1"}) == 1.0
    assert "action.replace" in _events(cap)


async def test_replace_partial_failure_not_counted_success(tmp_path: Path) -> None:
    # The replacement is provisioned but the OLD connector fails to drain/remove.
    # The replace is incomplete: the replacements metric must NOT increment and
    # the audit row must record a failure (Rule #4 — capacity is never silently
    # overstated). The still-unhealthy old connector is left for a later cycle.
    loop, twingate, actuator, metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2")],
        tg_connectors=[_tg("c1", ConnectorState.DEAD_HEARTBEAT_TOO_OLD), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
    )
    for _ in range(3):
        await state.record_action(
            ActionRecord(
                ts=NOW, rn_id="rn-1", action="restart", count=1, reason="r", outcome="success"
            ),
            connector_id="c1",
        )
    actuator.deprovision_should_raise = True

    with structlog.testing.capture_logs() as cap:
        result = await loop.run_cycle()

    assert result.ok  # the cycle survives the partial replace
    assert twingate.created  # the replacement was still provisioned
    # No completed replace ⇒ the metric is never observed for this RN.
    assert metrics.registry.get_sample_value("fc_replacements_total", {"rn": "rn-1"}) is None
    replace_event = next(e for e in cap if e["event"] == "action.replace")
    assert replace_event["old_removed"] is False
    recent = await state.recent_actions(limit=20)
    replace_rows = [r for r in recent if r.action == "replace"]
    assert replace_rows and replace_rows[0].outcome == "fail"


async def test_per_remote_network_error_isolated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # One Remote Network blows up while deciding (a non-typed sqlite-style
    # error); the other RN must still be acted on and the cycle must still
    # complete with its heartbeat and freshness timestamp (Rule: one bad RN
    # never aborts a cycle).
    def _rn2(cid: str) -> ManagedConnector:
        return ManagedConnector(
            connector_id=cid,
            name=cid,
            rn_id="rn-2",
            container_id=f"ctr-{cid}",
            docker_health="healthy",
        )

    def _tg2(cid: str) -> ManagedConnector:
        return ManagedConnector(
            connector_id=cid, name=cid, rn_id="rn-2", twingate_state=ConnectorState.ALIVE
        )

    loop, twingate, actuator, metrics, state = await _make_loop(
        tmp_path,
        containers=[_container("c1"), _container("c2"), _rn2("d1"), _rn2("d2")],
        tg_connectors=[_tg("c1"), _tg("c2"), _tg2("d1"), _tg2("d2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=90.0)],
    )

    real_get = state.get_cooldowns

    async def flaky_get_cooldowns(rn_id: str) -> Cooldowns:
        if rn_id == "rn-2":
            raise RuntimeError("sqlite locked")
        return await real_get(rn_id)

    monkeypatch.setattr(state, "get_cooldowns", flaky_get_cooldowns)

    with structlog.testing.capture_logs() as cap:
        result = await loop.run_cycle()

    assert result.ok  # the cycle completes despite rn-2 failing
    rn_errors = [e for e in cap if e["event"] == "loop.rn.error"]
    assert rn_errors and rn_errors[0]["rn_id"] == "rn-2"
    # rn-1 was still scaled up (the healthy RN is unaffected by rn-2's failure).
    assert twingate.created and any(call[0] == "provision" for call in actuator.calls)
    # Heartbeat + freshness timestamp still published.
    assert any(e["event"] == "loop.cycle.complete" for e in cap)
    assert (
        metrics.registry.get_sample_value("fc_last_successful_cycle_timestamp_seconds")
        == NOW.timestamp()
    )


async def test_janus_locked_connector_skipped(tmp_path: Path) -> None:
    loop, _twingate, actuator, _metrics, _state = await _make_loop(
        tmp_path,
        containers=[_container("c1", janus=True), _container("c2")],
        tg_connectors=[_tg("c1", ConnectorState.DEAD_NO_RELAYS), _tg("c2")],
        collectors=[FakeCollector(CollectorSource.DOCKER_STATS, cpu=50.0)],
    )
    with structlog.testing.capture_logs() as cap:
        await loop.run_cycle()

    assert ("restart", "c1") not in actuator.calls
    assert all(call[0] != "restart" for call in actuator.calls)
    assert "janus.lock_engaged" in _events(cap)


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
    rn = snap.remote_networks[0]
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
