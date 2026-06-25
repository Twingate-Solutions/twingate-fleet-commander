"""prometheus_client registry and the manager self-metrics.

The manager is observable from outside (Key Design Rule #7): a customer's
Prometheus scrapes ``/metrics`` to supervise FC the same way FC supervises the
fleet. :class:`Metrics` owns a private :class:`~prometheus_client.CollectorRegistry`
(never the global default) so importing this module twice — or constructing the
manager inside a test — can never raise ``Duplicated timeseries`` and each test
gets an isolated metric set.

The single most important series is ``fc_last_successful_cycle_timestamp_seconds``:
a staleness alert on it (``time() - value > N``) detects a silent or stuck
manager. No metric label ever carries a secret or a token (Key Design Rule #8 /
secret-handling rule); labels are only Remote Network ids, scale directions,
connector states, and collector names.
"""

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

from fc.models import ConnectorState, ManagedConnector

#: The bounded set of ``reason_class`` values used as a metric label. Never a
#: free-text string — the cardinality of ``fc_health_actions_total`` must stay
#: fixed so a customer's Prometheus does not suffer a label explosion.
HealthReasonClass = str

_DEAD_REASON_CLASSES: dict[ConnectorState, str] = {
    ConnectorState.DEAD_NO_RELAYS: "dead_no_relays",
    ConnectorState.DEAD_NO_HEARTBEAT: "dead_no_heartbeat",
    ConnectorState.DEAD_HEARTBEAT_TOO_OLD: "dead_heartbeat_too_old",
}


def classify_health_reason(connector: ManagedConnector) -> HealthReasonClass:
    """Classify a Connector's unhealth into a bounded ``reason_class`` label.

    The Twingate-reported ``DEAD_*`` state takes precedence over Docker health:
    a Connector that Twingate marks dead is classified by that state even if its
    Docker health is also ``unhealthy``. The mapping is total and never returns
    free text, so it is safe as a Prometheus label value.

    Args:
        connector: The Connector a health action is being taken on.

    Returns:
        One of ``dead_no_relays``, ``dead_no_heartbeat``,
        ``dead_heartbeat_too_old``, ``docker_unhealthy``, or the ``unknown``
        fallback.
    """
    if connector.twingate_state is not None and connector.twingate_state in _DEAD_REASON_CLASSES:
        return _DEAD_REASON_CLASSES[connector.twingate_state]
    if connector.docker_health == "unhealthy":
        return "docker_unhealthy"
    return "unknown"


class Metrics:
    """The manager's self-metrics, bound to a private registry.

    Construct once and share across the loop and the API. Counter names are
    given *without* the ``_total`` suffix — prometheus_client appends it — so
    the exposed series match the documented catalog exactly (e.g.
    ``fc_loop_iterations`` is exposed as ``fc_loop_iterations_total``).
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        """Build the metric set.

        Args:
            registry: Optional registry to bind to; a fresh private
                :class:`~prometheus_client.CollectorRegistry` is created when
                omitted (the normal case).
        """
        self.registry = registry or CollectorRegistry()

        self.loop_iterations = Counter(
            "fc_loop_iterations",
            "Total control-loop cycles attempted.",
            registry=self.registry,
        )
        self.last_successful_cycle = Gauge(
            "fc_last_successful_cycle_timestamp_seconds",
            "Unix timestamp of the last cleanly completed cycle (staleness detector).",
            registry=self.registry,
        )
        self.connectors = Gauge(
            "fc_connectors",
            "Number of managed Connectors, by Remote Network and state.",
            ["rn", "state"],
            registry=self.registry,
        )
        self.scale_actions = Counter(
            "fc_scale_actions",
            "Total scale actions, by Remote Network and direction.",
            ["rn", "direction"],
            registry=self.registry,
        )
        self.restarts = Counter(
            "fc_restarts",
            "Total Connector restarts, by Remote Network.",
            ["rn"],
            registry=self.registry,
        )
        self.replacements = Counter(
            "fc_replacements",
            "Total Connector replacements, by Remote Network.",
            ["rn"],
            registry=self.registry,
        )
        self.health_actions = Counter(
            "fc_health_actions",
            "Total health-remediation actions, by action kind and bounded reason class.",
            ["kind", "reason_class"],
            registry=self.registry,
        )
        self.seconds_since_last_action = Gauge(
            "fc_seconds_since_last_action",
            "Seconds since the last scale action in a Remote Network.",
            ["rn"],
            registry=self.registry,
        )
        self.twingate_api_errors = Counter(
            "fc_twingate_api_errors",
            "Total Twingate Admin API call failures.",
            registry=self.registry,
        )
        self.docker_api_errors = Counter(
            "fc_docker_api_errors",
            "Total Docker API call failures.",
            registry=self.registry,
        )
        self.collect_errors = Counter(
            "fc_collect_errors",
            "Total collector failures, by collector source.",
            ["collector"],
            registry=self.registry,
        )

    def set_fleet_gauges(self, connectors: list[ManagedConnector]) -> None:
        """Reset and repopulate the ``fc_connectors`` gauge from the fleet.

        The gauge is cleared first so a Remote Network/state combination that
        disappeared between cycles drops to absent rather than retaining a
        stale value. Twingate state takes precedence as the state label; a
        Connector with no reported Twingate state falls back to its Docker
        health, then to ``unknown``.

        Args:
            connectors: The Connectors discovered this cycle.
        """
        self.connectors.clear()
        counts: dict[tuple[str, str], int] = {}
        for connector in connectors:
            if connector.twingate_state is not None:
                state = connector.twingate_state.value
            elif connector.docker_health is not None:
                state = connector.docker_health
            else:
                state = "unknown"
            key = (connector.rn_id, state)
            counts[key] = counts.get(key, 0) + 1
        for (rn_id, state), count in counts.items():
            self.connectors.labels(rn=rn_id, state=state).set(count)

    def render(self) -> tuple[bytes, str]:
        """Render the current metrics as a Prometheus text exposition.

        Returns:
            A ``(body, content_type)`` pair suitable for an HTTP ``/metrics``
            response.
        """
        return generate_latest(self.registry), CONTENT_TYPE_LATEST
