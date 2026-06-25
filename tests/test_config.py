"""Unit tests for :mod:`fc.config` — Settings, Policy tree, and the loader."""

from pathlib import Path

import pytest

from fc.config import (
    ConfigError,
    Policy,
    ResolvedRemoteNetwork,
    Settings,
    load_policy,
)

GOOD_YAML = """
poll_interval_seconds: 30
connector_image: "twingate/connector:1"
metrics_port: 9999

collectors:
  docker_stats: true
  stdout_metrics: false
  prometheus: true

labels:
  managed: "twingate.fc.managed"
  remote_network: "twingate.fc.rn"
  connector_id: "twingate.fc.connector_id"
janus_lock_label: "twingate.janus.upgrading"

defaults:
  min_connectors: 2
  max_connectors: 6
  scale_step: 1
  cpu_high_pct: 75
  cpu_low_pct: 25
  throughput_high_mbps: 80
  throughput_low_mbps: 10
  mem_ceiling_bytes: 0
  scale_up_window_seconds: 300
  scale_down_window_seconds: 1200
  scale_up_cooldown_seconds: 600
  scale_down_cooldown_seconds: 1800
  drain_grace_seconds: 120
  max_restarts: 3
  restart_window_seconds: 600

remote_networks:
  - id: "rn-aws"
    name: "aws-prod"
    min_connectors: 3
    max_connectors: 10
  - id: "rn-office"
    name: "office"
"""


def _write(tmp_path: Path, content: str) -> Path:
    """Write ``content`` to a temp YAML file and return its path."""
    path = tmp_path / "config.yaml"
    path.write_text(content, encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# Policy loading + resolution
# --------------------------------------------------------------------------


def test_load_good_policy(tmp_path: Path) -> None:
    """A well-formed YAML loads into a Policy with the expected shape."""
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert isinstance(policy, Policy)
    assert policy.poll_interval_seconds == 30
    assert policy.connector_image == "twingate/connector:1"
    assert policy.metrics_port == 9999
    assert policy.collectors.prometheus is True
    assert policy.collectors.stdout_metrics is False
    assert policy.defaults.min_connectors == 2
    assert len(policy.remote_networks) == 2


def test_load_repo_example_config() -> None:
    """The shipped config.example.yaml is itself a valid policy."""
    repo_root = Path(__file__).resolve().parents[1]
    example = repo_root / "config" / "config.example.yaml"
    policy = load_policy(example)
    assert isinstance(policy, Policy)
    assert policy.defaults.min_connectors >= 2


def test_resolve_override_takes_precedence_and_inherits(tmp_path: Path) -> None:
    """aws-prod overrides min/max but inherits cpu_high_pct from defaults."""
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    resolved = policy.resolve_remote_network("rn-aws")
    assert isinstance(resolved, ResolvedRemoteNetwork)
    assert resolved.id == "rn-aws"
    assert resolved.name == "aws-prod"
    # Overridden:
    assert resolved.min_connectors == 3
    assert resolved.max_connectors == 10
    # Inherited from defaults:
    assert resolved.cpu_high_pct == 75
    assert resolved.cpu_low_pct == 25
    assert resolved.scale_up_window_seconds == 300


def test_resolve_inherits_all_defaults(tmp_path: Path) -> None:
    """office has no tunable overrides and inherits every default."""
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    resolved = policy.resolve_remote_network("rn-office")
    assert resolved.id == "rn-office"
    assert resolved.name == "office"
    assert resolved.min_connectors == 2
    assert resolved.max_connectors == 6
    assert resolved.cpu_high_pct == 75


def test_resolve_unknown_rn_falls_back_to_defaults(tmp_path: Path) -> None:
    """An RN id not in the config gets pure defaults, id==name==rn_id."""
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    resolved = policy.resolve_remote_network("rn-unknown")
    assert resolved.id == "rn-unknown"
    assert resolved.name == "rn-unknown"
    assert resolved.min_connectors == policy.defaults.min_connectors
    assert resolved.max_connectors == policy.defaults.max_connectors


def test_resolved_networks_maps_configured(tmp_path: Path) -> None:
    """resolved_networks returns one entry per configured RN, keyed by id."""
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    resolved = policy.resolved_networks()
    assert set(resolved) == {"rn-aws", "rn-office"}
    assert resolved["rn-aws"].min_connectors == 3


# --------------------------------------------------------------------------
# Fail-fast validation
# --------------------------------------------------------------------------


def test_unknown_key_rejected(tmp_path: Path) -> None:
    """An unknown YAML key (extra=forbid) raises ConfigError."""
    bad = GOOD_YAML + "\nbogus_top_level: true\n"
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_min_connectors_below_floor_rejected_in_defaults(tmp_path: Path) -> None:
    """defaults.min_connectors: 1 is below the hard floor and is rejected."""
    bad = GOOD_YAML.replace("min_connectors: 2", "min_connectors: 1")
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_override_min_connectors_below_floor_rejected(tmp_path: Path) -> None:
    """An override setting min_connectors below 2 is rejected at load time."""
    bad = GOOD_YAML.replace("min_connectors: 3", "min_connectors: 1")
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_max_less_than_min_rejected(tmp_path: Path) -> None:
    """max_connectors < min_connectors violates the invariant."""
    bad = GOOD_YAML.replace("max_connectors: 6", "max_connectors: 2").replace(
        "min_connectors: 2", "min_connectors: 4"
    )
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_cpu_high_not_above_low_rejected(tmp_path: Path) -> None:
    """cpu_high_pct must be strictly greater than cpu_low_pct."""
    bad = GOOD_YAML.replace("cpu_high_pct: 75", "cpu_high_pct: 25")
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_max_restarts_zero_rejected(tmp_path: Path) -> None:
    """max_restarts must be >= 1 so an unhealthy Connector is always restarted
    at least once before a replace (Key Design Rule #4 'restart first')."""
    bad = GOOD_YAML.replace("max_restarts: 3", "max_restarts: 0")
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_override_jointly_violating_invariant_rejected_at_load(tmp_path: Path) -> None:
    """An override whose tunables are each individually valid but jointly
    breach an invariant after merging onto the defaults fails fast at load
    time (not lazily mid-cycle): override max_connectors (2) < defaults
    min_connectors (4)."""
    bad = GOOD_YAML.replace("min_connectors: 2", "min_connectors: 4").replace(
        "max_connectors: 6", "max_connectors: 8"
    )
    # rn-aws override: drop its own min/max overrides, set only max_connectors: 2.
    bad = bad.replace(
        '  - id: "rn-aws"\n    name: "aws-prod"\n    min_connectors: 3\n    max_connectors: 10',
        '  - id: "rn-aws"\n    name: "aws-prod"\n    max_connectors: 2',
    )
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_missing_file_rejected(tmp_path: Path) -> None:
    """A non-existent policy path raises ConfigError."""
    with pytest.raises(ConfigError):
        load_policy(tmp_path / "does-not-exist.yaml")


def test_malformed_yaml_rejected(tmp_path: Path) -> None:
    """Syntactically invalid YAML raises ConfigError."""
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, "poll_interval_seconds: [unbalanced\n"))


def test_non_mapping_yaml_rejected(tmp_path: Path) -> None:
    """A YAML document that is not a mapping raises ConfigError."""
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, "- just\n- a\n- list\n"))


# --------------------------------------------------------------------------
# Settings (secrets)
# --------------------------------------------------------------------------


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set the minimal required env vars for Settings."""
    monkeypatch.setenv("TWINGATE_NETWORK", "acme")
    monkeypatch.setenv("TWINGATE_API_KEY", "tgp_supersecretvalue123")


def test_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Settings populates defaults for optional fields."""
    _set_required_env(monkeypatch)
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.twingate_network == "acme"
    assert settings.fc_config_path == "/app/config/config.yaml"
    assert settings.fc_state_path == "/app/state/fc.sqlite3"
    assert settings.fc_log_level == "info"
    assert settings.docker_host == "unix:///var/run/docker.sock"
    assert settings.fc_override_enabled is False
    assert settings.fc_override_secret is None


def test_api_key_is_secret_and_not_rendered(monkeypatch: pytest.MonkeyPatch) -> None:
    """The raw API key never appears in repr/str of Settings."""
    _set_required_env(monkeypatch)
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.twingate_api_key.get_secret_value() == "tgp_supersecretvalue123"
    assert "tgp_supersecretvalue123" not in repr(settings)
    assert "tgp_supersecretvalue123" not in str(settings)


def test_log_level_normalized(monkeypatch: pytest.MonkeyPatch) -> None:
    """A valid uppercase log level is normalized to lowercase."""
    _set_required_env(monkeypatch)
    monkeypatch.setenv("FC_LOG_LEVEL", "WARNING")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.fc_log_level == "warning"


def test_invalid_log_level_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """An invalid FC_LOG_LEVEL is rejected at construction."""
    _set_required_env(monkeypatch)
    monkeypatch.setenv("FC_LOG_LEVEL", "verbose")
    with pytest.raises(ValueError, match="fc_log_level"):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_override_enabled_without_secret_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enabling manual overrides without a secret is rejected at construction."""
    _set_required_env(monkeypatch)
    monkeypatch.setenv("FC_OVERRIDE_ENABLED", "true")
    with pytest.raises(ValueError, match="fc_override_secret"):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_override_enabled_with_short_secret_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """A too-short override secret is rejected when overrides are enabled."""
    _set_required_env(monkeypatch)
    monkeypatch.setenv("FC_OVERRIDE_ENABLED", "true")
    monkeypatch.setenv("FC_OVERRIDE_SECRET", "short")
    with pytest.raises(ValueError, match="fc_override_secret"):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_override_enabled_with_strong_secret_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    """A sufficiently long override secret is accepted and stays secret."""
    _set_required_env(monkeypatch)
    monkeypatch.setenv("FC_OVERRIDE_ENABLED", "true")
    monkeypatch.setenv("FC_OVERRIDE_SECRET", "this-is-a-long-enough-secret")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.fc_override_enabled is True
    assert settings.fc_override_secret is not None
    assert "this-is-a-long-enough-secret" not in repr(settings)


def test_override_disabled_allows_absent_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """With overrides disabled (default), no secret is required."""
    _set_required_env(monkeypatch)
    monkeypatch.setenv("FC_OVERRIDE_ENABLED", "false")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.fc_override_enabled is False
    assert settings.fc_override_secret is None
