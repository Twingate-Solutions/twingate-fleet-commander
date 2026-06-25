"""Tests for :func:`fc.actuator.factory.build_platform`.

Verifies the actuator + collector set are selected by ``FC_PLATFORM`` and that
the cloud branches accept injected client factories so selection is exercisable
with no cloud SDK installed.
"""

from pathlib import Path
from typing import Any

import httpx
import pytest

from fc.actuator.aci_actuator import AciActuator
from fc.actuator.docker_actuator import DockerActuator
from fc.actuator.ecs_actuator import EcsActuator
from fc.actuator.factory import build_platform
from fc.collectors.azure_logs import AzureLogsCollector
from fc.collectors.cloudwatch_logs import CloudWatchLogsCollector
from fc.collectors.docker_stats import DockerStatsCollector
from fc.collectors.stdout_metrics import StdoutMetricsCollector
from fc.config import Policy, Settings, load_policy

POLICY_YAML = """
poll_interval_seconds: 30
connector_image: "ghcr.io/twingate-solutions/twingate-custom-connector-container:latest"
collectors:
  docker_stats: true
  stdout_metrics: true
labels:
  managed: "twingate.fc.managed"
  remote_network: "twingate.fc.rn"
  connector_id: "twingate.fc.connector_id"
remote_network_id: "rn-aws"
min_connectors: 2
max_connectors: 6
scale_step: 1
scale_metrics:
  cpu:
    high_pct: 75
    low_pct: 25
    window_seconds: 300
    agg: avg
  throughput:
    high_mbps: 80
    low_mbps: 10
    window_seconds: 1200
    agg: avg
scale_up_cooldown_seconds: 600
scale_down_cooldown_seconds: 1800
drain_grace_seconds: 120
max_restarts: 3
restart_window_seconds: 600
"""


@pytest.fixture
def policy(tmp_path: Path) -> Policy:
    path = tmp_path / "config.yaml"
    path.write_text(POLICY_YAML, encoding="utf-8")
    return load_policy(path)


def _settings(monkeypatch: pytest.MonkeyPatch, platform: str) -> Settings:
    monkeypatch.setenv("TWINGATE_NETWORK", "acme")
    monkeypatch.setenv("TWINGATE_API_KEY", "tgp_secretvalue1234567")
    monkeypatch.setenv("FC_PLATFORM", platform)
    return Settings(_env_file=None)  # type: ignore[call-arg]


async def _token_provider(scope: str) -> str:
    return "fake"


async def test_factory_selects_docker(policy: Policy, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DOCKER_HOST", "tcp://localhost:2375")
    settings = _settings(monkeypatch, "docker")
    async with httpx.AsyncClient() as http:
        platform = build_platform(settings, policy, http=http)
        try:
            assert isinstance(platform.actuator, DockerActuator)
            kinds = [type(c) for c in platform.collectors]
            assert DockerStatsCollector in kinds
            assert StdoutMetricsCollector in kinds
        finally:
            await platform.aclose()


async def test_factory_selects_ecs(policy: Policy, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FC_ECS__CLUSTER", "fc-cluster")
    monkeypatch.setenv("FC_ECS__SUBNETS", "subnet-a")
    monkeypatch.setenv("FC_ECS__LOG_GROUP", "/fc/connectors")
    monkeypatch.setenv("FC_ECS__REGION", "us-east-1")  # required when log_group is set
    settings = _settings(monkeypatch, "ecs")

    sessions: list[object] = []

    def _fake_session() -> Any:
        marker = object()
        sessions.append(marker)
        return marker

    async with httpx.AsyncClient() as http:
        platform = build_platform(settings, policy, http=http, ecs_session_factory=_fake_session)
        await platform.aclose()

    assert isinstance(platform.actuator, EcsActuator)
    assert [type(c) for c in platform.collectors] == [CloudWatchLogsCollector]
    assert len(sessions) == 1  # the injected factory was used (no real SDK)


async def test_factory_selects_aci(policy: Policy, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FC_ACI__SUBSCRIPTION_ID", "sub-123")
    monkeypatch.setenv("FC_ACI__RESOURCE_GROUP", "fc-rg")
    monkeypatch.setenv("FC_ACI__REGION", "eastus")
    settings = _settings(monkeypatch, "aci")

    async with httpx.AsyncClient() as http:
        platform = build_platform(
            settings,
            policy,
            http=http,
            aci_token_provider_factory=lambda s: _token_provider,
        )
        await platform.aclose()

    assert isinstance(platform.actuator, AciActuator)
    assert [type(c) for c in platform.collectors] == [AzureLogsCollector]


async def test_factory_ecs_omits_collector_when_stdout_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(POLICY_YAML.replace("stdout_metrics: true", "stdout_metrics: false"), "utf-8")
    policy = load_policy(path)
    monkeypatch.setenv("FC_ECS__CLUSTER", "c")
    monkeypatch.setenv("FC_ECS__SUBNETS", "subnet-a")
    settings = _settings(monkeypatch, "ecs")
    async with httpx.AsyncClient() as http:
        platform = build_platform(settings, policy, http=http, ecs_session_factory=lambda: object())
        await platform.aclose()
    # No log collector when stdout metrics are off (docker_stats is N/A on ECS).
    assert platform.collectors == []
