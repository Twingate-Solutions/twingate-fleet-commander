"""Cloud collector: parse the custom image's ``[metrics]`` lines from Azure Monitor.

The ACI mirror of the CloudWatch collector. On ACI there is no Docker socket, so
FC reads the **same** ``[metrics]`` lines the custom connector image emits (now
on stderr — the container group's logs capture both stdout and stderr), surfaced
in a Log Analytics workspace (``ContainerInstanceLog_CL``). The line classification and sample construction
are shared with the other collectors via :mod:`fc.collectors.metrics_payload`;
this module only adds the Log Analytics query transport and degrades gracefully
when the workspace or table is unavailable.

The query runs over the project's existing :class:`httpx.AsyncClient` against the
Log Analytics query API; a bearer token is supplied by an injected async
credential callable (scope :data:`LOGS_SCOPE`), so the test suite can drive it
with an ``httpx.MockTransport`` and a stub token. CPU is normalized against the
prescribed ACI sizing (1 effective vCPU, Key Design Rule N2).
"""

from typing import Any

import httpx
import structlog

from fc.actuator.aci_actuator import TokenProvider
from fc.collectors.base import CollectorError
from fc.collectors.metrics_payload import (
    build_sample_from_payload,
    has_known_fields,
    parse_metrics_line,
)
from fc.models import CollectorSource, ManagedConnector, ResourceSample
from fc.platform import AciSettings

logger = structlog.get_logger(__name__)

#: Log Analytics query API base and OAuth scope.
_LOGS_BASE = "https://api.loganalytics.io/v1"
LOGS_SCOPE = "https://api.loganalytics.io/.default"

# Prescribed ACI sizing is 1 vCPU (Key Design Rule N2).
_EFFECTIVE_CORES = 1.0

# Bounded result set per connector per cycle.
_QUERY_LIMIT = 400


def _kql(container_group: str, limit: int) -> str:
    """Build the KQL selecting recent log messages for one container group.

    Filters ``ContainerInstanceLog_CL`` to the connector's container group, newest
    first, projecting just the message text. The group name is FC-generated
    (``fc-<hex>``) so it needs no escaping.
    """
    return (
        "ContainerInstanceLog_CL "
        f'| where ContainerGroup_s == "{container_group}" '
        "| order by TimeGenerated desc "
        f"| take {limit} "
        "| project Message"
    )


def _messages_from_tables(payload: object) -> list[str]:
    """Extract message strings from a Log Analytics query response.

    The response shape is ``{"tables": [{"columns": [...], "rows": [[...]]}]}``;
    the message is the single projected column. Rows arrive newest-first.
    """
    if not isinstance(payload, dict):
        return []
    tables = payload.get("tables")
    if not isinstance(tables, list) or not tables:
        return []
    table = tables[0]
    if not isinstance(table, dict):
        return []
    columns = table.get("columns")
    rows = table.get("rows")
    if not isinstance(rows, list):
        return []
    # Find the Message column index (defaults to 0 for the single-column project).
    message_idx = 0
    if isinstance(columns, list):
        for idx, column in enumerate(columns):
            if isinstance(column, dict) and column.get("name") == "Message":
                message_idx = idx
                break
    messages: list[str] = []
    for row in rows:
        if isinstance(row, list) and len(row) > message_idx:
            value = row[message_idx]
            if isinstance(value, str):
                messages.append(value)
    return messages


class AzureLogsCollector:
    """Collects connector samples from the ``[metrics]`` lines in Azure Monitor."""

    source = CollectorSource.AZURE_MONITOR

    def __init__(
        self,
        http: httpx.AsyncClient,
        token_provider: TokenProvider,
        *,
        settings: AciSettings,
        limit: int = _QUERY_LIMIT,
    ) -> None:
        """Build the collector.

        Args:
            http: The shared async HTTP client.
            token_provider: Async callable returning a bearer token for a scope;
                called with :data:`LOGS_SCOPE`.
            settings: The ACI settings supplying the Log Analytics workspace id.
            limit: Maximum log rows to pull per connector per cycle.
        """
        self._http = http
        self._token_provider = token_provider
        self._settings = settings
        self._limit = limit
        self._drift_warned = False

    async def collect(self, connector: ManagedConnector) -> ResourceSample | None:
        """Return the latest Azure-Monitor metrics sample, or ``None``.

        ``None`` is returned for a group-less Connector, when no workspace is
        configured, when the query yields no rows, or when no metrics line is
        present in the messages.

        Raises:
            CollectorError: When the Log Analytics query fails.
        """
        if connector.container_id is None:
            return None
        workspace = self._settings.log_analytics_workspace_id
        if not workspace:
            return None

        url = f"{_LOGS_BASE}/workspaces/{workspace}/query"
        try:
            token = await self._token_provider(LOGS_SCOPE)
            response = await self._http.post(
                url,
                headers={"Authorization": f"Bearer {token}"},
                json={"query": _kql(connector.container_id, self._limit)},
            )
        except httpx.HTTPError as exc:
            raise CollectorError(f"azure log query failed: {type(exc).__name__}") from exc
        if response.status_code == 404:
            # Workspace/table not provisioned yet — degrade gracefully.
            return None
        if response.status_code != 200:
            raise CollectorError(f"azure log query returned status {response.status_code}")

        try:
            messages = _messages_from_tables(response.json())
        except ValueError as exc:
            raise CollectorError("azure log query returned non-JSON body") from exc

        # Rows arrive newest-first; the first line that parses as metrics is the
        # most recent metrics payload.
        payload: dict[str, Any] | None = None
        for message in messages:
            parsed = parse_metrics_line(message)
            if parsed is not None:
                payload = parsed
                break
        if payload is None:
            return None
        self._warn_on_schema_drift(payload, connector)

        return build_sample_from_payload(
            payload,
            connector_id=connector.connector_id,
            effective_cores=_EFFECTIVE_CORES,
            source=CollectorSource.AZURE_MONITOR,
        )

    def _warn_on_schema_drift(self, payload: dict[str, Any], connector: ManagedConnector) -> None:
        """Warn once if a metrics line parsed but carries no known schema field."""
        if self._drift_warned:
            return
        if not has_known_fields(payload):
            self._drift_warned = True
            logger.warning(
                "azure_logs.schema_drift",
                connector_id=connector.connector_id,
                keys=sorted(str(k) for k in payload),
            )
