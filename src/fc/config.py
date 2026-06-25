"""Configuration: env-based ``Settings`` (secrets) and the YAML ``Policy``.

Two distinct configuration surfaces:

* :class:`Settings` — process/secret configuration sourced from environment
  variables (and an optional ``.env`` file). Holds the Twingate API key as a
  :class:`~pydantic.SecretStr` so it never renders in logs, ``repr``, or
  exceptions.
* :class:`Policy` — the non-secret autoscaling policy. Fleet Commander manages
  exactly **one** Remote Network (Key Design Rule N1), so the policy is a single
  flat model carrying that Remote Network's id (and optional name) alongside the
  scaling tunables directly — there is no per-RN list and no defaults/override
  merge. The policy is loaded from YAML and then **overlaid with environment
  variables**: every knob can be set via an ``FC_POLICY__`` env var, with
  precedence ``env → YAML → field default`` (see :func:`load_policy`).

All policy models forbid unknown keys so a typo in YAML (or an unknown env key
under the prefix) fails fast at startup. The hard floor (``min_connectors >= 2``)
and the watermark/bound ordering invariants are validated on the flat model.
"""

import re
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

LogLevel = str

_VALID_LOG_LEVELS = frozenset({"debug", "info", "warning", "error"})

# Minimum length of the manual-override shared secret when overrides are enabled.
_MIN_OVERRIDE_SECRET_LEN = 16

# Environment-variable prefix for policy overrides, e.g.
# ``FC_POLICY__MIN_CONNECTORS`` or ``FC_POLICY__SCALE_METRICS__CPU__HIGH_PCT``.
_POLICY_ENV_PREFIX = "FC_POLICY__"

# Allowed window-aggregation modes: ``avg``, ``min``, or a percentile ``pNN``
# (0-100), e.g. ``p95``.
_AGG_PERCENTILE_RE = re.compile(r"^p(100|[0-9]{1,2})$")


class ConfigError(Exception):
    """Raised when configuration cannot be loaded or validated.

    Carries a human-readable message suitable for fail-fast startup logging.
    Never contains secret values.
    """


class Settings(BaseSettings):
    """Process and secret configuration sourced from the environment.

    The Twingate API key is a :class:`~pydantic.SecretStr` and is therefore
    redacted from ``repr``/``str``; it is only ever read explicitly via
    ``get_secret_value()`` when building the GraphQL auth header.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
    )

    twingate_network: str
    twingate_api_key: SecretStr
    fc_config_path: str = "/app/config/config.yaml"
    fc_state_path: str = "/app/state/fc.sqlite3"
    fc_log_level: LogLevel = "info"
    docker_host: str = "unix:///var/run/docker.sock"
    # The compute backend FC actuates. Chosen *explicitly* — never auto-detected,
    # because FC deletes compute and the backend must be a deliberate operator
    # choice. ``docker`` (the default) drives the local Docker socket; ``ecs`` and
    # ``aci`` drive the cloud actuators, configured by the matching
    # ``FC_ECS__*`` / ``FC_ACI__*`` settings (see :mod:`fc.platform`).
    fc_platform: Literal["docker", "ecs", "aci"] = "docker"
    fc_override_enabled: bool = False
    fc_override_secret: SecretStr | None = None

    @model_validator(mode="after")
    def _validate_log_level(self) -> Self:
        """Normalize and validate ``fc_log_level`` against the allowed set."""
        normalized = self.fc_log_level.lower()
        if normalized not in _VALID_LOG_LEVELS:
            allowed = ", ".join(sorted(_VALID_LOG_LEVELS))
            raise ValueError(f"fc_log_level must be one of: {allowed}; got {self.fc_log_level!r}")
        object.__setattr__(self, "fc_log_level", normalized)
        return self

    @model_validator(mode="after")
    def _validate_override_secret(self) -> Self:
        """Require a non-trivial shared secret whenever manual overrides are enabled.

        Guards against enabling the override endpoints behind an empty or
        absent secret. The secret itself is never rendered.
        """
        if self.fc_override_enabled:
            secret = self.fc_override_secret.get_secret_value() if self.fc_override_secret else ""
            if len(secret) < _MIN_OVERRIDE_SECRET_LEN:
                raise ValueError(
                    "fc_override_secret must be set and at least "
                    f"{_MIN_OVERRIDE_SECRET_LEN} characters when fc_override_enabled is true"
                )
        return self


class CollectorToggles(BaseModel):
    """Enable/disable flags for each metric collector."""

    model_config = ConfigDict(extra="forbid")

    docker_stats: bool
    stdout_metrics: bool


class Labels(BaseModel):
    """Docker label keys FC sets and reads to identify managed Connectors."""

    model_config = ConfigDict(extra="forbid")

    managed: str
    remote_network: str
    connector_id: str


class JanusConfig(BaseModel):
    """Janus auto-update enrolment for provisioned Connectors.

    janus (the connector version-updater sidecar) has **no lock mechanism** — it
    upgrades a container whenever a newer image is published. FC therefore does
    not coordinate with janus via a lock; it simply *enrols* the Connectors it
    provisions by stamping janus's auto-update labels on them, and tolerates the
    brief container recreate a janus upgrade causes via the startup-grace and
    ``unhealthy_threshold_seconds`` windows (Key Design Rule #5).

    When ``enabled``, every provisioned Connector is stamped with
    ``janus.autoupdate.enable=true`` and ``janus.autoupdate.interval=<interval_seconds>``.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    interval_seconds: int = Field(default=86400, ge=1)


def _validate_agg(value: str) -> str:
    """Validate a window-aggregation mode (``avg``/``min``/``pNN``)."""
    if value in ("avg", "min"):
        return value
    if _AGG_PERCENTILE_RE.match(value):
        return value
    raise ValueError(f"agg must be 'avg', 'min', or a percentile 'pNN' (0-100); got {value!r}")


class CpuScaleMetric(BaseModel):
    """CPU scale trigger: watermarks, window, and time-aggregation mode.

    ``high_pct``/``low_pct`` are per-effective-core normalized utilization
    (0-100). The signal is reduced over the trailing ``window_seconds`` using
    ``agg`` (``avg`` by default, or ``min``/``pNN``) before being compared to
    the watermarks. The same windowed value drives both the high (scale-up) and
    low (scale-down) tests (Key Design Rule #3).
    """

    model_config = ConfigDict(extra="forbid")

    high_pct: float = Field(ge=0, le=100)
    low_pct: float = Field(ge=0, le=100)
    window_seconds: int = Field(ge=1)
    agg: str = "avg"

    @model_validator(mode="after")
    def _validate(self) -> Self:
        """Enforce ``high_pct > low_pct`` and a valid aggregation mode."""
        _validate_agg(self.agg)
        if self.high_pct <= self.low_pct:
            raise ValueError(f"cpu high_pct ({self.high_pct}) must be > low_pct ({self.low_pct})")
        return self


class ThroughputScaleMetric(BaseModel):
    """Throughput scale trigger: watermarks (Mbps), window, and aggregation.

    ``high_mbps``/``low_mbps`` are per-connector tunnel throughput in megabits
    per second. The signal is reduced over the trailing ``window_seconds`` using
    ``agg`` before comparison; the same windowed value drives both directions.
    """

    model_config = ConfigDict(extra="forbid")

    high_mbps: float = Field(ge=0)
    low_mbps: float = Field(ge=0)
    window_seconds: int = Field(ge=1)
    agg: str = "avg"

    @model_validator(mode="after")
    def _validate(self) -> Self:
        """Enforce ``high_mbps > low_mbps`` and a valid aggregation mode."""
        _validate_agg(self.agg)
        if self.high_mbps <= self.low_mbps:
            raise ValueError(
                f"throughput high_mbps ({self.high_mbps}) must be > low_mbps ({self.low_mbps})"
            )
        return self


class ScaleMetrics(BaseModel):
    """The per-metric scale triggers: CPU and tunnel throughput.

    Each metric carries its own watermarks, sustained-window length, and
    time-aggregation mode, so CPU can react on a short window while throughput
    reacts on a longer one (Key Design Rule #3, expressed per-metric).
    """

    model_config = ConfigDict(extra="forbid")

    cpu: CpuScaleMetric
    throughput: ThroughputScaleMetric


def _policy_yaml_source(data: dict[str, Any]) -> type[PydanticBaseSettingsSource]:
    """Build a settings source that yields the parsed YAML mapping verbatim.

    The returned source supplies the YAML values at lower priority than the
    environment, so ``env → YAML → default`` precedence holds.
    """

    class _YamlSource(PydanticBaseSettingsSource):
        def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
            # Not used: __call__ returns the whole mapping at once.
            return None, field_name, False

        def __call__(self) -> dict[str, Any]:
            return data

    return _YamlSource


class Policy(BaseSettings):
    """The full non-secret autoscaling policy for the single managed RN.

    Loaded from YAML and overlaid with ``FC_POLICY__`` environment variables.
    The model owns the single Remote Network's identity (``remote_network_id``
    and optional ``remote_network_name``) and every scaling tunable directly;
    there is no per-RN list. Construct it via :func:`load_policy`, never the
    bare constructor, so the YAML-plus-env source wiring is applied.

    Scale-up combination across the fleet is governed by ``scale_up_trigger``
    and ``quorum_fraction`` (the "sticky-connector" controls). These decide how
    *per-connector* high-watermark crossings combine into a single fleet
    scale-up decision: ``"any"`` reacts to a single hot Connector, ``"mean"``
    tests the fleet-average windowed signal (which can dilute one hot
    Connector), and ``"quorum"`` (the default) requires at least
    ``ceil(quorum_fraction * current_count)`` Connectors to be over their high
    watermark before adding capacity. Scale-*down* is unaffected — it stays
    deliberately conservative (every present signal at/below its low watermark).
    """

    model_config = SettingsConfigDict(
        env_prefix=_POLICY_ENV_PREFIX,
        env_nested_delimiter="__",
        extra="forbid",
        case_sensitive=False,
    )

    # -- the single managed Remote Network ----------------------------------
    remote_network_id: str
    remote_network_name: str | None = None

    # -- scaling bounds and step --------------------------------------------
    min_connectors: int = Field(ge=2)
    max_connectors: int = Field(ge=2)
    scale_step: int = Field(ge=1)

    # -- per-metric scale triggers ------------------------------------------
    scale_metrics: ScaleMetrics

    # -- scale-up combination across the fleet (the sticky-connector knobs) --
    # How per-connector high-watermark crossings combine into one fleet scale-up
    # decision. ``"any"`` adds capacity if a single Connector is hot; ``"mean"``
    # tests the fleet-average windowed signal (one hot Connector can be diluted
    # by quiet ones); ``"quorum"`` (default) requires a configurable fraction of
    # Connectors to be hot. Scale-down is unaffected.
    scale_up_trigger: Literal["any", "mean", "quorum"] = "quorum"
    # Fraction of Connectors that must be over the high watermark under quorum
    # mode; the integer threshold is ``max(1, ceil(quorum_fraction * count))``.
    quorum_fraction: float = Field(default=0.5, gt=0, le=1)

    # -- cooldowns, drain, health -------------------------------------------
    scale_up_cooldown_seconds: int = Field(ge=0)
    scale_down_cooldown_seconds: int = Field(ge=0)
    drain_grace_seconds: int = Field(ge=0)
    max_restarts: int = Field(ge=1)
    restart_window_seconds: int = Field(ge=1)
    # Grace window after FC first sees a Connector before a never-heartbeated
    # DEAD_NO_HEARTBEAT is treated as dead, so a freshly provisioned Connector
    # is not restarted before its first heartbeat registers. 0 disables grace.
    startup_grace_seconds: int = Field(default=90, ge=0)
    # A Connector must be *continuously* unhealthy for at least this long before
    # any health remediation (restart/replace) fires, so a brief blip never
    # triggers an action. The timer resets the moment the Connector recovers.
    # 0 disables the gate (act on the first unhealthy observation).
    unhealthy_threshold_seconds: int = Field(default=60, ge=0)
    # Bound on the cycle-spanning wait-for-healthy replace (Key Design Rule #4):
    # after the replacement is provisioned, FC waits up to this long for it to
    # report ALIVE/healthy before tearing down the unhealthy Connector. If the
    # replacement never becomes healthy within the bound, FC logs/alerts and
    # leaves the old Connector in place rather than dropping capacity.
    replace_health_timeout_seconds: int = Field(default=300, ge=1)

    # -- process / fleet-wide settings --------------------------------------
    poll_interval_seconds: int = Field(ge=1)
    connector_image: str
    collectors: CollectorToggles
    labels: Labels
    janus: JanusConfig = Field(default_factory=JanusConfig)

    @model_validator(mode="after")
    def _validate_invariants(self) -> Self:
        """Enforce floor, bound ordering, and (via the metric models) watermarks."""
        if self.max_connectors < self.min_connectors:
            raise ValueError(
                f"max_connectors ({self.max_connectors}) must be >= "
                f"min_connectors ({self.min_connectors})"
            )
        return self

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Order sources so env overrides YAML overrides defaults.

        ``init_settings`` carries the parsed YAML mapping (passed to the
        constructor by :func:`load_policy`); placing ``env_settings`` ahead of
        it makes ``FC_POLICY__*`` env vars win over the YAML, while YAML still
        wins over the model field defaults.
        """
        return (env_settings, dotenv_settings, init_settings)


def load_policy(path: str | Path) -> Policy:
    """Load and validate the YAML policy, overlaid with ``FC_POLICY__`` env vars.

    Precedence for every knob is ``env → YAML → field default``: a value set in
    an ``FC_POLICY__`` environment variable wins over the YAML file, which wins
    over the model's built-in default.

    Args:
        path: Filesystem path to the YAML policy file.

    Returns:
        The validated :class:`Policy`.

    Raises:
        ConfigError: On a missing file, a YAML parse error, a non-mapping
            document, or a Pydantic validation failure. The message is safe to
            log (YAML carries no secrets).
    """
    file_path = Path(path)
    try:
        raw = file_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"policy file not found: {file_path}") from exc
    except OSError as exc:
        raise ConfigError(f"could not read policy file {file_path}: {exc}") from exc

    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ConfigError(f"malformed YAML in policy file {file_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError(
            f"policy file {file_path} must contain a top-level mapping, got {type(data).__name__}"
        )

    try:
        return _build_policy(data)
    except ValidationError as exc:
        raise ConfigError(f"invalid policy in {file_path}: {exc}") from exc


def _build_policy(data: dict[str, Any]) -> Policy:
    """Construct a :class:`Policy` from a YAML mapping plus env overlay.

    Wires the YAML mapping in as a low-priority settings source so the
    ``FC_POLICY__`` environment overlay (and the field defaults) compose with it
    per :meth:`Policy.settings_customise_sources`.
    """
    yaml_source = _policy_yaml_source(data)

    class _PolicyWithYaml(Policy):
        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (env_settings, dotenv_settings, yaml_source(settings_cls))

    policy = _PolicyWithYaml()  # type: ignore[call-arg]  # values come from YAML + env sources
    # Re-validate as the public type so the returned object is a plain ``Policy``
    # (the subclass exists only to inject the YAML source).
    return Policy.model_validate(policy.model_dump())
