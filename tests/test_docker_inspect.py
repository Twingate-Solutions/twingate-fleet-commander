"""Tests for :class:`fc.docker_inspect.InspectCache`.

The cache memoizes ``container.show()`` within a cycle so the actuator (health)
and the stdout-metrics collector (effective cores) share a single inspect per
container, and :meth:`clear` forces a fresh read on the next cycle.
"""

from typing import Any

from fc.docker_inspect import InspectCache


class _Container:
    def __init__(self, container_id: str, inspect: Any) -> None:
        self.id = container_id
        self._inspect = inspect
        self.show_calls = 0

    async def show(self, **kwargs: Any) -> Any:
        self.show_calls += 1
        return self._inspect


class _Containers:
    def __init__(self) -> None:
        self._by_id: dict[str, _Container] = {}
        self.get_calls = 0

    def add(self, container_id: str, inspect: Any) -> _Container:
        container = _Container(container_id, inspect)
        self._by_id[container_id] = container
        return container

    async def get(self, container_id: str, **kwargs: Any) -> _Container:
        self.get_calls += 1
        return self._by_id[container_id]


class _Docker:
    def __init__(self) -> None:
        self.containers = _Containers()


async def test_inspect_memoizes_within_cycle() -> None:
    docker = _Docker()
    container = docker.containers.add("c1", {"State": {"Health": {"Status": "healthy"}}})
    cache = InspectCache(docker)  # type: ignore[arg-type]

    first = await cache.inspect("c1")
    second = await cache.inspect("c1")

    assert first == {"State": {"Health": {"Status": "healthy"}}}
    assert first is second
    # One show() and one get() despite two reads — the point of the cache.
    assert container.show_calls == 1
    assert docker.containers.get_calls == 1


async def test_clear_forces_refetch_next_cycle() -> None:
    docker = _Docker()
    container = docker.containers.add("c1", {"x": 1})
    cache = InspectCache(docker)  # type: ignore[arg-type]

    await cache.inspect("c1")
    cache.clear()
    await cache.inspect("c1")

    assert container.show_calls == 2


async def test_non_dict_inspect_yields_empty_and_is_not_cached() -> None:
    docker = _Docker()
    container = docker.containers.add("c1", ["unexpected"])
    cache = InspectCache(docker)  # type: ignore[arg-type]

    assert await cache.inspect("c1") == {}
    # A non-dict result is not cached, so a second read tries again.
    await cache.inspect("c1")
    assert container.show_calls == 2
