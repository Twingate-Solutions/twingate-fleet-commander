"""Unit tests for :mod:`fc.config` — Settings, the flat Policy, and the loader."""

from pathlib import Path

import pytest

from fc.config import (
    ConfigError,
    Policy,
    Settings,
    load_policy,
)

GOOD_YAML = """
poll_interval_seconds: 30
connector_image: "twingate/connector:1"

collectors:
  docker_stats: true
  stdout_metrics: false

labels:
  managed: "twingate.fc.managed"
  remote_network: "twingate.fc.rn"
  connector_id: "twingate.fc.connector_id"

janus:
  enabled: true
  interval_seconds: 86400

remote_network_id: "rn-aws"
remote_network_name: "aws-prod"

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


def _write(tmp_path: Path, content: str) -> Path:
    """Write ``content`` to a temp YAML file and return its path."""
    path = tmp_path / "config.yaml"
    path.write_text(content, encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# Policy loading (flat, single-RN)
# --------------------------------------------------------------------------


def test_load_good_policy(tmp_path: Path) -> None:
    """A well-formed flat YAML loads into a Policy with the expected shape."""
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert isinstance(policy, Policy)
    assert policy.poll_interval_seconds == 30
    assert policy.connector_image == "twingate/connector:1"
    assert policy.collectors.docker_stats is True
    assert policy.collectors.stdout_metrics is False
    assert policy.remote_network_id == "rn-aws"
    assert policy.remote_network_name == "aws-prod"
    assert policy.min_connectors == 2
    assert policy.max_connectors == 6
    assert policy.scale_metrics.cpu.high_pct == 75
    assert policy.scale_metrics.cpu.window_seconds == 300
    assert policy.scale_metrics.throughput.window_seconds == 1200
    assert policy.scale_metrics.throughput.agg == "avg"


def test_remote_network_name_optional(tmp_path: Path) -> None:
    """remote_network_name may be omitted (defaults to None)."""
    yaml_no_name = GOOD_YAML.replace('remote_network_name: "aws-prod"\n', "")
    policy = load_policy(_write(tmp_path, yaml_no_name))
    assert policy.remote_network_name is None


def test_load_repo_example_config() -> None:
    """The shipped config.example.yaml is itself a valid policy."""
    repo_root = Path(__file__).resolve().parents[1]
    example = repo_root / "config" / "config.example.yaml"
    policy = load_policy(example)
    assert isinstance(policy, Policy)
    assert policy.min_connectors >= 2


# --------------------------------------------------------------------------
# Environment overrides (env → YAML → default precedence)
# --------------------------------------------------------------------------


def test_env_overrides_top_level_field(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An FC_POLICY__ env var overrides the YAML value for a top-level knob."""
    monkeypatch.setenv("FC_POLICY__MIN_CONNECTORS", "4")
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert policy.min_connectors == 4  # env wins
    assert policy.max_connectors == 6  # yaml retained


def test_env_overrides_nested_metric_field(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A nested per-metric knob is overridable; sibling YAML keys are retained."""
    monkeypatch.setenv("FC_POLICY__SCALE_METRICS__CPU__HIGH_PCT", "90")
    monkeypatch.setenv("FC_POLICY__SCALE_METRICS__THROUGHPUT__WINDOW_SECONDS", "1800")
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert policy.scale_metrics.cpu.high_pct == 90  # env wins
    assert policy.scale_metrics.cpu.low_pct == 25  # yaml retained (deep merge)
    assert policy.scale_metrics.cpu.window_seconds == 300  # yaml retained
    assert policy.scale_metrics.throughput.window_seconds == 1800  # env wins


def test_no_env_uses_yaml_then_default(tmp_path: Path) -> None:
    """With no env override, YAML wins and unspecified knobs use field defaults."""
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert policy.min_connectors == 2  # from YAML
    assert policy.startup_grace_seconds == 90  # field default (absent from YAML)


def test_connector_nofile_defaults(tmp_path: Path) -> None:
    """connector_nofile defaults to 131072 when absent from YAML."""
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert policy.connector_nofile == 131072


def test_connector_nofile_from_yaml(tmp_path: Path) -> None:
    """connector_nofile is read from YAML when present."""
    policy = load_policy(_write(tmp_path, GOOD_YAML + "connector_nofile: 262144\n"))
    assert policy.connector_nofile == 262144


def test_connector_nofile_below_min_rejected(tmp_path: Path) -> None:
    """connector_nofile below the 1024 floor is rejected."""
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, GOOD_YAML + "connector_nofile: 512\n"))


def test_connector_nofile_above_max_rejected(tmp_path: Path) -> None:
    """connector_nofile above 1048576 (the usual fs.nr_open) is rejected."""
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, GOOD_YAML + "connector_nofile: 2097152\n"))


def test_connector_tuning_defaults(tmp_path: Path) -> None:
    """Port range and log rotation default when absent from YAML."""
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert policy.connector_ephemeral_port_range == "10240 65535"
    assert policy.connector_log_max_size == "20m"
    assert policy.connector_log_max_file == 5


def test_connector_tuning_from_yaml(tmp_path: Path) -> None:
    """Port range and log rotation are read from YAML when present."""
    yaml = GOOD_YAML + (
        'connector_ephemeral_port_range: "20000 60000"\n'
        'connector_log_max_size: "50m"\n'
        "connector_log_max_file: 3\n"
    )
    policy = load_policy(_write(tmp_path, yaml))
    assert policy.connector_ephemeral_port_range == "20000 60000"
    assert policy.connector_log_max_size == "50m"
    assert policy.connector_log_max_file == 3


def test_port_range_malformed_rejected(tmp_path: Path) -> None:
    """A port range that is not two integers is rejected."""
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, GOOD_YAML + 'connector_ephemeral_port_range: "hi"\n'))


def test_port_range_low_not_below_high_rejected(tmp_path: Path) -> None:
    """A port range with low >= high is rejected."""
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, GOOD_YAML + 'connector_ephemeral_port_range: "60000 20000"\n'))


def test_port_range_out_of_bounds_rejected(tmp_path: Path) -> None:
    """A port range exceeding 65535 is rejected."""
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, GOOD_YAML + 'connector_ephemeral_port_range: "1024 70000"\n'))


def test_log_max_size_malformed_rejected(tmp_path: Path) -> None:
    """A log size that is not a Docker size string is rejected."""
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, GOOD_YAML + 'connector_log_max_size: "big"\n'))


def test_scale_up_trigger_defaults(tmp_path: Path) -> None:
    """The sticky-connector knobs default to quorum / 0.5 when absent from YAML."""
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert policy.scale_up_trigger == "quorum"
    assert policy.quorum_fraction == 0.5


def test_scale_up_trigger_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """scale_up_trigger is overridable via an FC_POLICY__ env var."""
    monkeypatch.setenv("FC_POLICY__SCALE_UP_TRIGGER", "any")
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert policy.scale_up_trigger == "any"


def test_quorum_fraction_from_yaml(tmp_path: Path) -> None:
    """quorum_fraction is read from YAML when present."""
    good = GOOD_YAML + "\nscale_up_trigger: quorum\nquorum_fraction: 0.75\n"
    policy = load_policy(_write(tmp_path, good))
    assert policy.quorum_fraction == 0.75


def test_janus_block_parsed_from_yaml(tmp_path: Path) -> None:
    """The janus enrolment block is read from YAML."""
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert policy.janus.enabled is True
    assert policy.janus.interval_seconds == 86400


def test_janus_defaults_when_absent(tmp_path: Path) -> None:
    """With no janus block, it defaults to enabled / 86400s (the whole block is optional)."""
    no_janus = GOOD_YAML.replace("janus:\n  enabled: true\n  interval_seconds: 86400\n\n", "")
    assert "janus:" not in no_janus
    policy = load_policy(_write(tmp_path, no_janus))
    assert policy.janus.enabled is True
    assert policy.janus.interval_seconds == 86400


def test_janus_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The janus block is overridable via FC_POLICY__JANUS__ env vars."""
    monkeypatch.setenv("FC_POLICY__JANUS__ENABLED", "false")
    monkeypatch.setenv("FC_POLICY__JANUS__INTERVAL_SECONDS", "3600")
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert policy.janus.enabled is False
    assert policy.janus.interval_seconds == 3600


def test_invalid_scale_up_trigger_rejected(tmp_path: Path) -> None:
    """An unrecognized scale_up_trigger value is rejected at load time."""
    bad = GOOD_YAML + "\nscale_up_trigger: median\n"
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_quorum_fraction_zero_rejected(tmp_path: Path) -> None:
    """quorum_fraction must be > 0 (a fraction of zero connectors is meaningless)."""
    bad = GOOD_YAML + "\nquorum_fraction: 0\n"
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_quorum_fraction_above_one_rejected(tmp_path: Path) -> None:
    """quorum_fraction must be <= 1 (a fraction over 100% is meaningless)."""
    bad = GOOD_YAML + "\nquorum_fraction: 1.5\n"
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_health_path_defaults(tmp_path: Path) -> None:
    """The Session 12 health knobs have sensible defaults when absent from YAML."""
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert policy.unhealthy_threshold_seconds == 60
    assert policy.replace_health_timeout_seconds == 300


def test_health_path_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The new health knobs are overridable via FC_POLICY__ env vars."""
    monkeypatch.setenv("FC_POLICY__UNHEALTHY_THRESHOLD_SECONDS", "0")
    monkeypatch.setenv("FC_POLICY__REPLACE_HEALTH_TIMEOUT_SECONDS", "120")
    policy = load_policy(_write(tmp_path, GOOD_YAML))
    assert policy.unhealthy_threshold_seconds == 0
    assert policy.replace_health_timeout_seconds == 120


def test_replace_health_timeout_zero_rejected(tmp_path: Path) -> None:
    """replace_health_timeout_seconds must be >= 1 (a real bound on the wait)."""
    bad = GOOD_YAML + "\nreplace_health_timeout_seconds: 0\n"
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_env_override_still_validated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An env override that breaches an invariant fails fast like a bad YAML."""
    monkeypatch.setenv("FC_POLICY__MIN_CONNECTORS", "1")  # below the hard floor
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, GOOD_YAML))


# --------------------------------------------------------------------------
# Fail-fast validation
# --------------------------------------------------------------------------


def test_unknown_key_rejected(tmp_path: Path) -> None:
    """An unknown YAML key (extra=forbid) raises ConfigError."""
    bad = GOOD_YAML + "\nbogus_top_level: true\n"
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_min_connectors_below_floor_rejected(tmp_path: Path) -> None:
    """min_connectors: 1 is below the hard floor and is rejected."""
    bad = GOOD_YAML.replace("min_connectors: 2", "min_connectors: 1")
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
    """cpu high_pct must be strictly greater than low_pct."""
    bad = GOOD_YAML.replace("high_pct: 75", "high_pct: 25")
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_throughput_high_not_above_low_rejected(tmp_path: Path) -> None:
    """throughput high_mbps must be strictly greater than low_mbps."""
    bad = GOOD_YAML.replace("high_mbps: 80", "high_mbps: 5")
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_bad_agg_mode_rejected(tmp_path: Path) -> None:
    """An unrecognized aggregation mode is rejected at load time."""
    bad = GOOD_YAML.replace("agg: avg", "agg: median", 1)
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_percentile_agg_accepted(tmp_path: Path) -> None:
    """A pNN aggregation mode is accepted (throughput → p95, cpu stays avg)."""
    good = GOOD_YAML.replace(
        "    high_mbps: 80\n    low_mbps: 10\n    window_seconds: 1200\n    agg: avg",
        "    high_mbps: 80\n    low_mbps: 10\n    window_seconds: 1200\n    agg: p95",
    )
    policy = load_policy(_write(tmp_path, good))
    assert policy.scale_metrics.throughput.agg == "p95"


def test_out_of_range_percentile_rejected(tmp_path: Path) -> None:
    """A percentile above 100 is rejected."""
    bad = GOOD_YAML.replace(
        "    window_seconds: 300\n    agg: avg",
        "    window_seconds: 300\n    agg: p150",
    )
    with pytest.raises(ConfigError):
        load_policy(_write(tmp_path, bad))


def test_max_restarts_zero_rejected(tmp_path: Path) -> None:
    """max_restarts must be >= 1 so an unhealthy Connector is always restarted
    at least once before a replace (Key Design Rule #4 'restart first')."""
    bad = GOOD_YAML.replace("max_restarts: 3", "max_restarts: 0")
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
