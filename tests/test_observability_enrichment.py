"""Unit tests for the observability-enrichment additions.

Covers the bounded ``reason_class`` classifier, the new
``fc_health_actions_total`` metric, and the event-name catalog's integrity
(uniqueness + presence of the new phase start/complete constants).
"""

from fc.models import ConnectorState, ManagedConnector
from fc.observability import events
from fc.observability.metrics import Metrics, classify_health_reason


def _connector(
    *,
    twingate_state: ConnectorState | None = None,
    docker_health: str | None = None,
) -> ManagedConnector:
    return ManagedConnector(
        connector_id="c1",
        name="c1",
        rn_id="rn-1",
        twingate_state=twingate_state,
        docker_health=docker_health,
    )


# --- classifier ------------------------------------------------------------


def test_classify_dead_no_relays() -> None:
    assert classify_health_reason(_connector(twingate_state=ConnectorState.DEAD_NO_RELAYS)) == (
        "dead_no_relays"
    )


def test_classify_dead_no_heartbeat() -> None:
    assert classify_health_reason(_connector(twingate_state=ConnectorState.DEAD_NO_HEARTBEAT)) == (
        "dead_no_heartbeat"
    )


def test_classify_dead_heartbeat_too_old() -> None:
    state = ConnectorState.DEAD_HEARTBEAT_TOO_OLD
    assert classify_health_reason(_connector(twingate_state=state)) == "dead_heartbeat_too_old"


def test_classify_docker_unhealthy() -> None:
    assert classify_health_reason(_connector(docker_health="unhealthy")) == "docker_unhealthy"


def test_classify_unknown_fallback() -> None:
    # No DEAD_* state and Docker health that is not "unhealthy" → unknown.
    assert classify_health_reason(_connector(docker_health="starting")) == "unknown"
    assert classify_health_reason(_connector()) == "unknown"
    assert classify_health_reason(_connector(twingate_state=ConnectorState.ALIVE)) == "unknown"


def test_classify_twingate_dead_precedes_docker_health() -> None:
    # A DEAD_* twingate state wins even when Docker also reports unhealthy.
    connector = _connector(twingate_state=ConnectorState.DEAD_NO_RELAYS, docker_health="unhealthy")
    assert classify_health_reason(connector) == "dead_no_relays"


# --- metric ----------------------------------------------------------------


def test_health_actions_counter_labels() -> None:
    metrics = Metrics()
    metrics.health_actions.labels(kind="restart", reason_class="dead_no_relays").inc()
    metrics.health_actions.labels(kind="replace", reason_class="docker_unhealthy").inc()
    body, _ = metrics.render()
    text = body.decode()
    assert 'fc_health_actions_total{kind="restart",reason_class="dead_no_relays"} 1.0' in text
    assert 'fc_health_actions_total{kind="replace",reason_class="docker_unhealthy"} 1.0' in text


# --- event catalog ---------------------------------------------------------


def test_event_constants_are_unique() -> None:
    values = [v for k, v in vars(events).items() if k.isupper() and isinstance(v, str)]
    assert len(values) == len(set(values)), "duplicate event-name constants"


def test_new_phase_events_present() -> None:
    assert events.DISCOVER_START == "discover.start"
    assert events.DISCOVER_COMPLETE == "discover.complete"
    assert events.COLLECT_START == "collect.start"
    assert events.COLLECT_COMPLETE == "collect.complete"
    assert events.DECIDE_START == "decide.start"
    assert events.DECIDE_COMPLETE == "decide.complete"
    assert events.HEALTH_START == "health.start"
    assert events.HEALTH_COMPLETE == "health.complete"
