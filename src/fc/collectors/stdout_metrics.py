"""Opt-in collector: parse the custom image's ``[metrics]`` JSON lines.

The custom connector image writes a metrics JSON line every 60s. **As of the
2026-06 image update it writes metrics to stderr** — ``ANALYTICS`` traffic lines
and ordinary service logs stay on stdout — deliberately separating the two
writers so a metrics line can never interleave into, and corrupt, a large
analytics line (the bug that previously broke the log-shipper's parsing). This
collector therefore tails the container's **stderr** over the Docker API — an
independent read that does not interfere with the log-shipper — classifies each
line, and maps the most recent metrics payload onto a normalized
:class:`~fc.models.ResourceSample`.

The collector keeps its historical ``stdout_metrics`` name and
``CollectorSource.STDOUT_METRICS`` source (config key + metric labels) for
stability, even though the underlying stream is now stderr.

It is opt-in and custom-image only: on the official image no metrics lines
appear and :meth:`StdoutMetricsCollector.collect` returns ``None``.

The line classification, schema mapping, and sample construction live in
:mod:`fc.collectors.metrics_payload` so the Docker-log and cloud-log collectors
share exactly one implementation; this module only adds the Docker-log transport
and the inspect-based effective-core resolution.
"""

import os
from typing import TYPE_CHECKING, Any

import structlog

from fc.collectors.base import (
    CollectorError,
    effective_cores_from_inspect,
)
from fc.collectors.metrics_payload import (
    build_sample_from_payload,
    has_known_fields,
    latest_metrics_payload,
)
from fc.collectors.metrics_payload import parse_metrics_line as parse_metrics_line
from fc.docker_inspect import InspectCache
from fc.models import CollectorSource, ManagedConnector, ResourceSample

if TYPE_CHECKING:
    import aiodocker

logger = structlog.get_logger(__name__)

# Bounded tail so a busy connector's log volume never stalls the cycle; the
# image emits one metrics line per minute, so a few hundred lines comfortably
# covers a typical poll interval.
_LOG_TAIL = 400


def _iter_lines(log_result: object) -> list[str]:
    """Flatten an aiodocker ``log`` result into individual non-empty lines."""
    if isinstance(log_result, str):
        chunks = [log_result]
    elif isinstance(log_result, list):
        chunks = [c for c in log_result if isinstance(c, str)]
    else:
        return []
    lines: list[str] = []
    for chunk in chunks:
        lines.extend(piece for piece in chunk.splitlines() if piece.strip())
    return lines


class StdoutMetricsCollector:
    """Collects samples from the custom image's stdout metrics lines.

    Stateless across cycles (each metrics line carries its own per-interval
    deltas), so no previous snapshot is retained.
    """

    source = CollectorSource.STDOUT_METRICS

    def __init__(
        self,
        docker: "aiodocker.Docker",
        *,
        host_cpus: int | None = None,
        inspect_cache: InspectCache | None = None,
    ) -> None:
        """Build the collector.

        Args:
            docker: The shared aiodocker client for log tails and inspects.
            host_cpus: The host's online core count, used to resolve effective
                cores when a container has no CPU limit. Defaults to the
                manager host's CPU count.
            inspect_cache: Per-cycle inspect cache shared with the actuator so a
                container's ``show()`` is read once per cycle. A private cache is
                built when none is supplied.
        """
        self._docker = docker
        self._host_cpus = host_cpus if host_cpus and host_cpus > 0 else (os.cpu_count() or 1)
        self._inspect = inspect_cache or InspectCache(docker)
        self._drift_warned = False

    async def collect(self, connector: ManagedConnector) -> ResourceSample | None:
        """Return the latest stdout-metrics sample for a Connector, or ``None``.

        ``None`` is returned for a logical-only Connector and when the log tail
        contains no metrics line (e.g. the official image, or none emitted yet
        this window).

        Args:
            connector: The Connector to sample.

        Returns:
            A normalized :class:`ResourceSample`, or ``None``.

        Raises:
            CollectorError: When the Docker log/inspect read fails.
        """
        if connector.container_id is None:
            return None

        try:
            container = await self._docker.containers.get(connector.container_id)
            # Metrics are on the container's STDERR (the image moved them there so
            # they can't interleave with stdout ANALYTICS lines). Read stderr only:
            # a clean, low-volume stream where the once-a-minute metrics line is
            # never crowded out of the tail by high-volume analytics traffic.
            raw_log = await container.log(stdout=False, stderr=True, tail=_LOG_TAIL)
        except Exception as exc:
            raise CollectorError(f"docker log read failed: {type(exc).__name__}") from exc

        payload = latest_metrics_payload(_iter_lines(raw_log))
        if payload is None:
            return None
        self._warn_on_schema_drift(payload, connector)

        try:
            inspect = await self._inspect.inspect(connector.container_id)
        except Exception as exc:
            raise CollectorError(f"docker inspect failed: {type(exc).__name__}") from exc
        effective_cores = effective_cores_from_inspect(inspect, host_cpus=self._host_cpus)

        return build_sample_from_payload(
            payload,
            connector_id=connector.connector_id,
            effective_cores=effective_cores,
            source=CollectorSource.STDOUT_METRICS,
        )

    def _warn_on_schema_drift(self, payload: dict[str, Any], connector: ManagedConnector) -> None:
        """Warn once if a metrics line parsed but carries no known schema field.

        A well-formed ``[metrics]`` line that yields none of the known fields
        means the upstream image's metrics schema drifted: every field would map
        to ``None`` and the collector would silently degrade. Surfacing it once
        makes the drift visible without flooding the logs.
        """
        if self._drift_warned:
            return
        if not has_known_fields(payload):
            self._drift_warned = True
            logger.warning(
                "stdout_metrics.schema_drift",
                connector_id=connector.connector_id,
                keys=sorted(str(k) for k in payload),
            )
