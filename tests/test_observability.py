"""Tests for the observability layer: events catalog, logging redaction, metrics.

Covers the secret-redaction backstop (by key name, by ``SecretStr``, by secret
env-assignment value, and nested), the event catalog being unique and non-empty,
and the self-metrics registry rendering the documented series names plus the
fleet-gauge rollup.
"""

import json

from pydantic import SecretStr

from fc.models import ConnectorState, ManagedConnector
from fc.observability import events
from fc.observability.logging import REDACTED, redact_secrets
from fc.observability.metrics import Metrics


def _redact(event_dict: dict[str, object]) -> dict[str, object]:
    return dict(redact_secrets(None, "info", event_dict))


def test_redacts_by_key_name() -> None:
    out = _redact({"api_key": "abc123", "twingate_api_key": "k", "event": "x"})
    assert out["api_key"] == REDACTED
    assert out["twingate_api_key"] == REDACTED
    assert out["event"] == "x"


def test_redacts_token_and_secret_keys() -> None:
    out = _redact({"access_token": "t", "refresh_token": "r", "override_secret": "s"})
    assert out["access_token"] == REDACTED
    assert out["refresh_token"] == REDACTED
    assert out["override_secret"] == REDACTED


def test_redacts_secretstr_value_under_innocuous_key() -> None:
    out = _redact({"detail": SecretStr("hunter2")})
    assert out["detail"] == REDACTED


def test_redacts_secret_env_assignment_value() -> None:
    out = _redact({"line": "TWINGATE_ACCESS_TOKEN=leaked"})
    assert out["line"] == REDACTED


def test_redacts_secret_env_assignment_mid_string() -> None:
    # The token assignment need not start the string (e.g. an exception message
    # or a rendered container Env list element).
    out = _redact({"detail": "failed to run with TWINGATE_API_KEY=abc123 set"})
    assert out["detail"] == REDACTED


def test_redacts_bearer_and_api_key_header_values() -> None:
    out = _redact(
        {
            "auth": "Authorization: Bearer eyJhbG.payload.sig",
            "hdr": "X-API-KEY: deadbeefcafef00d",
        }
    )
    assert out["auth"] == REDACTED
    assert out["hdr"] == REDACTED


def test_redacts_nested_mapping_and_list() -> None:
    out = _redact({"env": {"password": "p", "ok": "v"}, "items": [SecretStr("z"), "fine"]})
    assert out["env"] == {"password": REDACTED, "ok": "v"}
    assert out["items"] == [REDACTED, "fine"]


def test_redacts_structured_env_entry_token_shape() -> None:
    # The cloud actuators carry tokens as ``{"name": "TWINGATE_*_TOKEN",
    # "value"/"secureValue": <token>}`` — the secret hides under an innocuous key
    # that no key marker would catch, so the backstop keys off the ``name`` field.
    out = _redact(
        {
            "overrides": [
                {"name": "TWINGATE_ACCESS_TOKEN", "value": "ecs_leaked"},
                {"name": "TWINGATE_REFRESH_TOKEN", "secureValue": "aci_leaked"},
                {"name": "TWINGATE_NETWORK", "value": "acme"},
            ]
        }
    )
    overrides = out["overrides"]
    assert isinstance(overrides, list)
    assert overrides[0] == {"name": "TWINGATE_ACCESS_TOKEN", "value": REDACTED}
    assert overrides[1] == {"name": "TWINGATE_REFRESH_TOKEN", "secureValue": REDACTED}
    # A non-secret name leaves its value intact.
    assert overrides[2] == {"name": "TWINGATE_NETWORK", "value": "acme"}


def test_non_secret_values_pass_through() -> None:
    out = _redact({"rn_id": "rn-1", "count": 3, "cpu": 42.0})
    assert out == {"rn_id": "rn-1", "count": 3, "cpu": 42.0}


def test_event_catalog_constants_unique_and_dotted() -> None:
    names = [
        getattr(events, attr)
        for attr in dir(events)
        if attr.isupper() and isinstance(getattr(events, attr), str)
    ]
    assert names, "expected event constants"
    assert len(names) == len(set(names)), "event names must be unique"
    assert all("." in name for name in names)
    assert events.LOOP_CYCLE_COMPLETE == "loop.cycle.complete"


def test_metrics_render_exposes_documented_series() -> None:
    metrics = Metrics()
    metrics.loop_iterations.inc()
    metrics.last_successful_cycle.set(1000.0)
    body, content_type = metrics.render()
    text = body.decode()
    assert "text/plain" in content_type
    assert "fc_loop_iterations_total" in text
    assert "fc_last_successful_cycle_timestamp_seconds" in text


def test_metrics_isolated_per_instance() -> None:
    a = Metrics()
    b = Metrics()
    a.loop_iterations.inc()
    assert a.registry.get_sample_value("fc_loop_iterations_total") == 1.0
    assert b.registry.get_sample_value("fc_loop_iterations_total") == 0.0


def test_set_fleet_gauges_counts_by_rn_and_state() -> None:
    metrics = Metrics()
    fleet = [
        ManagedConnector(
            connector_id="c1", name="c1", rn_id="rn-1", twingate_state=ConnectorState.ALIVE
        ),
        ManagedConnector(
            connector_id="c2", name="c2", rn_id="rn-1", twingate_state=ConnectorState.ALIVE
        ),
        ManagedConnector(
            connector_id="c3",
            name="c3",
            rn_id="rn-1",
            twingate_state=ConnectorState.DEAD_NO_HEARTBEAT,
        ),
        ManagedConnector(connector_id="c4", name="c4", rn_id="rn-2", docker_health="healthy"),
    ]
    metrics.set_fleet_gauges(fleet)
    get = metrics.registry.get_sample_value
    assert get("fc_connectors", {"rn": "rn-1", "state": "ALIVE"}) == 2.0
    assert get("fc_connectors", {"rn": "rn-1", "state": "DEAD_NO_HEARTBEAT"}) == 1.0
    assert get("fc_connectors", {"rn": "rn-2", "state": "healthy"}) == 1.0


def test_set_fleet_gauges_clears_stale_entries() -> None:
    metrics = Metrics()
    metrics.set_fleet_gauges(
        [
            ManagedConnector(
                connector_id="c1", name="c1", rn_id="rn-1", twingate_state=ConnectorState.ALIVE
            )
        ]
    )
    metrics.set_fleet_gauges([])  # fleet emptied
    assert (
        metrics.registry.get_sample_value("fc_connectors", {"rn": "rn-1", "state": "ALIVE"}) is None
    )


def test_metrics_labels_never_carry_tokens() -> None:
    # A defensive check: rendering must not contain a token even if one were
    # accidentally used as a label value upstream (it is not, by construction).
    metrics = Metrics()
    metrics.scale_actions.labels(rn="rn-1", direction="up").inc()
    text, _ = metrics.render()
    payload = json.dumps(text.decode())
    assert "accessToken" not in payload and "refreshToken" not in payload
