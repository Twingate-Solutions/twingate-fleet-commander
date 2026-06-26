"""Cloud collector: parse the custom image's ``[metrics]`` lines from CloudWatch.

On ECS there is no Docker socket to tail and the Prometheus collector is gone,
so FC reads the **same** ``[metrics]`` lines the custom connector image emits
(now on stderr) — but from CloudWatch Logs, where the task's ``awslogs`` driver
delivers both stdout and stderr. The line classification and sample construction are shared with the
Docker-log collector via :mod:`fc.collectors.metrics_payload`; this module only
adds the CloudWatch transport and degrades gracefully when logs are unavailable.

CPU is normalized against the prescribed ECS task sizing (1 effective vCPU, Key
Design Rule N2) rather than a Docker inspect, which is absent on ECS.

The aioboto3 session is injected and duck-typed, so no AWS SDK symbol is
imported at module load and the test suite needs no AWS SDK installed.
"""

from typing import Any

import structlog

from fc.collectors.base import CollectorError
from fc.collectors.metrics_payload import (
    build_sample_from_payload,
    has_known_fields,
    latest_metrics_payload,
)
from fc.models import CollectorSource, ManagedConnector, ResourceSample
from fc.platform import EcsSettings

logger = structlog.get_logger(__name__)

# Prescribed ECS task sizing is 1 vCPU (Key Design Rule N2), so CPU is
# normalized against one effective core.
_EFFECTIVE_CORES = 1.0

# Bounded tail per connector so a busy task's log volume never stalls a cycle.
_LOG_LIMIT = 400


def _is_not_found(exc: Exception) -> bool:
    """Return whether an exception looks like a CloudWatch not-found.

    A missing log group/stream (a task that has not logged yet) is not an error
    — the collector degrades to ``None`` — so it is distinguished from genuine
    failures without importing botocore's exception classes.
    """
    name = type(exc).__name__
    if "ResourceNotFound" in name or "NotFound" in name:
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = response.get("Error", {}).get("Code", "")
        return isinstance(code, str) and "ResourceNotFound" in code
    return False


def _event_messages(resp: object) -> list[str]:
    """Extract the message strings from a ``get_log_events`` response."""
    if not isinstance(resp, dict):
        return []
    messages: list[str] = []
    for event in resp.get("events") or []:
        if isinstance(event, dict):
            message = event.get("message")
            if isinstance(message, str):
                messages.append(message)
    return messages


class CloudWatchLogsCollector:
    """Collects connector samples from the ``[metrics]`` lines in CloudWatch Logs."""

    source = CollectorSource.CLOUDWATCH_LOGS

    def __init__(
        self,
        session: Any,
        *,
        settings: EcsSettings,
        limit: int = _LOG_LIMIT,
    ) -> None:
        """Build the collector.

        Args:
            session: An ``aioboto3.Session`` (duck-typed); ``session.client(...)``
                must return an async-context-manager ``logs`` client.
            settings: The ECS settings supplying the log group, stream prefix,
                container name, and region.
            limit: Maximum log events to pull per connector per cycle.
        """
        self._session = session
        self._settings = settings
        self._limit = limit
        self._drift_warned = False

    def _log_stream(self, task_arn: str) -> str:
        """Derive the awslogs stream name for a task ARN.

        The ``awslogs`` driver names streams ``<prefix>/<container>/<task-id>``,
        where the task id is the final segment of the task ARN.
        """
        task_id = task_arn.rsplit("/", 1)[-1]
        return f"{self._settings.log_stream_prefix}/{self._settings.container_name}/{task_id}"

    async def collect(self, connector: ManagedConnector) -> ResourceSample | None:
        """Return the latest CloudWatch-Logs metrics sample, or ``None``.

        ``None`` is returned for a task-less Connector, when no log group is
        configured, when the log stream does not exist yet, or when the tail
        contains no metrics line.

        Raises:
            CollectorError: When a CloudWatch read fails for a reason other than
                a missing group/stream.
        """
        if connector.container_id is None:
            return None
        log_group = self._settings.log_group
        if not log_group:
            return None

        stream = self._log_stream(connector.container_id)
        try:
            async with self._session.client("logs", region_name=self._settings.region) as logs:
                resp = await logs.get_log_events(
                    logGroupName=log_group,
                    logStreamName=stream,
                    limit=self._limit,
                    startFromHead=False,
                )
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise CollectorError(f"cloudwatch get_log_events failed: {type(exc).__name__}") from exc

        payload = latest_metrics_payload(_event_messages(resp))
        if payload is None:
            return None
        self._warn_on_schema_drift(payload, connector)

        return build_sample_from_payload(
            payload,
            connector_id=connector.connector_id,
            effective_cores=_EFFECTIVE_CORES,
            source=CollectorSource.CLOUDWATCH_LOGS,
        )

    def _warn_on_schema_drift(self, payload: dict[str, Any], connector: ManagedConnector) -> None:
        """Warn once if a metrics line parsed but carries no known schema field."""
        if self._drift_warned:
            return
        if not has_known_fields(payload):
            self._drift_warned = True
            logger.warning(
                "cloudwatch_logs.schema_drift",
                connector_id=connector.connector_id,
                keys=sorted(str(k) for k in payload),
            )
