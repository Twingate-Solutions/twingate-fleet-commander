"""Per-cycle cache of container inspect results, shared across collaborators.

Two collaborators need a container's Docker inspect (``container.show()``) every
control-loop cycle: the :class:`~fc.actuator.docker_actuator.DockerActuator`
reads authoritative health from ``State.Health.Status``, and the opt-in
:class:`~fc.collectors.stdout_metrics.StdoutMetricsCollector` reads the same
inspect to resolve the container's effective core count. Routing both through one
:class:`InspectCache` collapses what would otherwise be two ``show()`` calls per
connector per cycle into one.

The cache is cleared at the start of each cycle's fleet discovery
(:meth:`~fc.actuator.docker_actuator.DockerActuator.list_managed`), so every
cycle reads fresh state and the cache never outlives the cycle that filled it.
There is exactly one inspect per container per cycle: ``list_managed`` populates
the cache while reading health, and the collector reads back the cached value.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import aiodocker


class InspectCache:
    """A within-cycle memoization of ``container.show()`` keyed by container id.

    Not safe for concurrent use across cycles: :meth:`clear` must be called once
    per cycle (the actuator does this in ``list_managed``) before the cache is
    refilled. Within a cycle, calls are serialized by the loop's action lock, so
    no internal locking is needed.
    """

    def __init__(self, docker: "aiodocker.Docker") -> None:
        """Build the cache.

        Args:
            docker: The shared aiodocker client used to fetch inspects.
        """
        self._docker = docker
        self._cache: dict[str, dict[str, Any]] = {}

    async def inspect(self, container_id: str) -> dict[str, Any]:
        """Return the container's inspect dict, fetching once and caching it.

        Args:
            container_id: The container to inspect.

        Returns:
            The full ``container.show()`` payload. A second call within the same
            cycle for the same id returns the cached value without a Docker call.

        Raises:
            Exception: Whatever aiodocker raises on a failed get/show; callers
                isolate this (the actuator logs and treats health as unknown).
        """
        cached = self._cache.get(container_id)
        if cached is not None:
            return cached
        container = await self._docker.containers.get(container_id)
        # Typed as Any so the defensive non-dict guard below is meaningful: a
        # misbehaving backend could return a non-mapping, which we coerce to {}.
        data: Any = await container.show()
        if isinstance(data, dict):
            self._cache[container_id] = data
            return data
        return {}

    def clear(self) -> None:
        """Drop all cached inspects; call once at the start of each cycle."""
        self._cache.clear()
