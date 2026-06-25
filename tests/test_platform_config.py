"""Tests for platform selection + the discriminated ECS/ACI settings models.

Covers the explicit ``FC_PLATFORM`` choice on :class:`fc.config.Settings`
(default ``docker``, no auto-detection, invalid value rejected) and the fail-fast
ECS/ACI settings models in :mod:`fc.platform` (required fields, comma-separated
list parsing, the Azure service-principal secret kept as ``SecretStr``).
"""

import pytest
from pydantic import SecretStr, ValidationError

from fc.config import Settings
from fc.platform import AciSettings, EcsSettings, load_aci_settings, load_ecs_settings


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set the always-required Settings secrets."""
    monkeypatch.setenv("TWINGATE_NETWORK", "acme")
    monkeypatch.setenv("TWINGATE_API_KEY", "tgp_supersecretvalue123")


# -- FC_PLATFORM on Settings ------------------------------------------------


def test_platform_defaults_to_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.delenv("FC_PLATFORM", raising=False)
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.fc_platform == "docker"


def test_platform_explicit_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("FC_PLATFORM", "ecs")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.fc_platform == "ecs"


def test_platform_invalid_value_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("FC_PLATFORM", "gcp")  # not a supported backend
    with pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore[call-arg]


# -- ECS settings -----------------------------------------------------------


def _set_ecs_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FC_ECS__CLUSTER", "fc-cluster")
    monkeypatch.setenv("FC_ECS__SUBNETS", "subnet-a,subnet-b")


def test_ecs_settings_load_and_parse_lists(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_ecs_required(monkeypatch)
    monkeypatch.setenv("FC_ECS__SECURITY_GROUPS", "sg-1, sg-2")
    monkeypatch.setenv("FC_ECS__REGION", "us-east-1")
    monkeypatch.setenv("FC_ECS__ASSIGN_PUBLIC_IP", "true")
    settings = load_ecs_settings()
    assert settings.cluster == "fc-cluster"
    # Comma-separated env strings are split into lists (whitespace trimmed).
    assert settings.subnets == ["subnet-a", "subnet-b"]
    assert settings.security_groups == ["sg-1", "sg-2"]
    assert settings.region == "us-east-1"
    assert settings.assign_public_ip is True
    # Prescribed defaults.
    assert settings.launch_type == "FARGATE"
    assert settings.container_name == "connector"


def test_ecs_settings_missing_required_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FC_ECS__CLUSTER", raising=False)
    monkeypatch.delenv("FC_ECS__SUBNETS", raising=False)
    with pytest.raises(ValidationError):
        load_ecs_settings()


def test_ecs_settings_accepts_json_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FC_ECS__CLUSTER", "c")
    monkeypatch.setenv("FC_ECS__SUBNETS", '["subnet-x"]')
    monkeypatch.delenv("FC_ECS__LOG_GROUP", raising=False)
    settings = load_ecs_settings()
    assert settings.subnets == ["subnet-x"]


def test_ecs_settings_log_group_requires_region(monkeypatch: pytest.MonkeyPatch) -> None:
    # The awslogs driver cannot infer the region, so a log_group without a region
    # must fail fast rather than write an empty awslogs-region.
    _set_ecs_required(monkeypatch)
    monkeypatch.setenv("FC_ECS__LOG_GROUP", "/fc/connectors")
    monkeypatch.delenv("FC_ECS__REGION", raising=False)
    with pytest.raises(ValidationError):
        load_ecs_settings()


# -- ACI settings -----------------------------------------------------------


def _set_aci_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FC_ACI__SUBSCRIPTION_ID", "sub-123")
    monkeypatch.setenv("FC_ACI__RESOURCE_GROUP", "fc-rg")
    monkeypatch.setenv("FC_ACI__REGION", "eastus")


def test_aci_settings_load(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_aci_required(monkeypatch)
    monkeypatch.setenv("FC_ACI__SUBNET_ID", "/subscriptions/sub/.../subnets/s")
    settings = load_aci_settings()
    assert settings.subscription_id == "sub-123"
    assert settings.resource_group == "fc-rg"
    assert settings.region == "eastus"
    assert settings.subnet_id == "/subscriptions/sub/.../subnets/s"
    assert settings.client_secret is None


def test_aci_settings_secret_is_secretstr(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_aci_required(monkeypatch)
    monkeypatch.setenv("FC_ACI__TENANT_ID", "tenant")
    monkeypatch.setenv("FC_ACI__CLIENT_ID", "client")
    monkeypatch.setenv("FC_ACI__CLIENT_SECRET", "sp_secret_value")
    settings = load_aci_settings()
    assert isinstance(settings.client_secret, SecretStr)
    # The secret is redacted from repr/str but readable explicitly.
    assert "sp_secret_value" not in repr(settings)
    assert settings.client_secret.get_secret_value() == "sp_secret_value"


def test_aci_settings_missing_required_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FC_ACI__SUBSCRIPTION_ID", raising=False)
    monkeypatch.delenv("FC_ACI__RESOURCE_GROUP", raising=False)
    monkeypatch.delenv("FC_ACI__REGION", raising=False)
    with pytest.raises(ValidationError):
        load_aci_settings()


def test_ecs_and_aci_settings_are_distinct_models() -> None:
    # The discriminated union carries two distinct shapes (different field sets).
    assert EcsSettings.model_fields.keys() != AciSettings.model_fields.keys()
