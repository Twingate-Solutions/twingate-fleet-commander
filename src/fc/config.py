"""Configuration: env-based ``Settings`` (secrets) and the YAML ``Policy`` tree.

Two distinct configuration surfaces:

* :class:`Settings` — process/secret configuration sourced from environment
  variables (and an optional ``.env`` file). Holds the Twingate API key as a
  :class:`~pydantic.SecretStr` so it never renders in logs, ``repr``, or
  exceptions.
* :class:`Policy` — the non-secret autoscaling policy loaded from YAML. The
  ``defaults`` block governs every Remote Network unless a per-RN override in
  ``remote_networks`` narrows it. :meth:`Policy.resolve_remote_network`
  merges an override onto the defaults to produce a fully-concrete
  :class:`ResolvedRemoteNetwork`.

All policy models forbid unknown keys so a typo in YAML fails fast at startup.
The hard floor (``min_connectors >= 2``) is enforced on both the defaults and
the resolved per-RN value, so an override can never breach redundancy.
"""

from pathlib import Path
from typing import Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = str

_VALID_LOG_LEVELS = frozenset({"debug", "info", "warning", "error"})

# Minimum length of the manual-override shared secret when overrides are enabled.
_MIN_OVERRIDE_SECRET_LEN = 16


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
    prometheus: bool


class Labels(BaseModel):
    """Docker label keys FC sets and reads to identify managed Connectors."""

    model_config = ConfigDict(extra="forbid")

    managed: str
    remote_network: str
    connector_id: str


class RemoteNetworkDefaults(BaseModel):
    """The ``defaults`` block: concrete tunables applied to every Remote Network.

    Every field is required and validated. ``min_connectors`` enforces the
    hard redundancy floor of 2. The model validator enforces the ordering
    invariants between the high/low watermarks and the connector bounds.
    """

    model_config = ConfigDict(extra="forbid")

    min_connectors: int = Field(ge=2)
    max_connectors: int = Field(ge=2)
    scale_step: int = Field(ge=1)
    cpu_high_pct: float = Field(ge=0, le=100)
    cpu_low_pct: float = Field(ge=0, le=100)
    throughput_high_mbps: float = Field(ge=0)
    throughput_low_mbps: float = Field(ge=0)
    mem_ceiling_bytes: int = Field(ge=0)
    scale_up_window_seconds: int = Field(ge=1)
    scale_down_window_seconds: int = Field(ge=1)
    scale_up_cooldown_seconds: int = Field(ge=0)
    scale_down_cooldown_seconds: int = Field(ge=0)
    drain_grace_seconds: int = Field(ge=0)
    max_restarts: int = Field(ge=1)
    restart_window_seconds: int = Field(ge=1)
    # Grace window after FC first sees a Connector before a never-heartbeated
    # DEAD_NO_HEARTBEAT is treated as dead, so a freshly provisioned Connector
    # is not restarted before its first heartbeat registers. 0 disables grace.
    startup_grace_seconds: int = Field(default=90, ge=0)

    @model_validator(mode="after")
    def _validate_invariants(self) -> Self:
        """Enforce floor, bound ordering, and watermark ordering invariants."""
        if self.max_connectors < self.min_connectors:
            raise ValueError(
                f"max_connectors ({self.max_connectors}) must be >= "
                f"min_connectors ({self.min_connectors})"
            )
        if self.cpu_high_pct <= self.cpu_low_pct:
            raise ValueError(
                f"cpu_high_pct ({self.cpu_high_pct}) must be > cpu_low_pct ({self.cpu_low_pct})"
            )
        if self.throughput_high_mbps <= self.throughput_low_mbps:
            raise ValueError(
                f"throughput_high_mbps ({self.throughput_high_mbps}) must be > "
                f"throughput_low_mbps ({self.throughput_low_mbps})"
            )
        return self


class RemoteNetworkOverride(BaseModel):
    """A per-RN override: ``id`` and ``name`` plus optional tunable overrides.

    Any tunable left as ``None`` is inherited from
    :class:`RemoteNetworkDefaults` at resolution time. Invariants are not
    checked here (an override is partial); they are enforced on the merged
    :class:`ResolvedRemoteNetwork`.
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    min_connectors: int | None = Field(default=None, ge=2)
    max_connectors: int | None = Field(default=None, ge=2)
    scale_step: int | None = Field(default=None, ge=1)
    cpu_high_pct: float | None = Field(default=None, ge=0, le=100)
    cpu_low_pct: float | None = Field(default=None, ge=0, le=100)
    throughput_high_mbps: float | None = Field(default=None, ge=0)
    throughput_low_mbps: float | None = Field(default=None, ge=0)
    mem_ceiling_bytes: int | None = Field(default=None, ge=0)
    scale_up_window_seconds: int | None = Field(default=None, ge=1)
    scale_down_window_seconds: int | None = Field(default=None, ge=1)
    scale_up_cooldown_seconds: int | None = Field(default=None, ge=0)
    scale_down_cooldown_seconds: int | None = Field(default=None, ge=0)
    drain_grace_seconds: int | None = Field(default=None, ge=0)
    max_restarts: int | None = Field(default=None, ge=1)
    restart_window_seconds: int | None = Field(default=None, ge=1)
    startup_grace_seconds: int | None = Field(default=None, ge=0)


# Tunable field names shared between defaults and overrides (everything on
# RemoteNetworkDefaults). Used to merge an override onto the defaults.
_TUNABLE_FIELDS: tuple[str, ...] = tuple(RemoteNetworkDefaults.model_fields.keys())


class ResolvedRemoteNetwork(RemoteNetworkDefaults):
    """A fully-concrete per-RN policy: defaults merged with any override.

    Inherits every tunable (and its validation, including the
    ``min_connectors >= 2`` floor and the ordering invariants) from
    :class:`RemoteNetworkDefaults`, and adds the RN identity (``id``,
    ``name``). Produced by :meth:`Policy.resolve_remote_network`.
    """

    id: str
    name: str


class Policy(BaseModel):
    """The full non-secret autoscaling policy loaded from YAML."""

    model_config = ConfigDict(extra="forbid")

    poll_interval_seconds: int = Field(ge=1)
    connector_image: str
    metrics_port: int = Field(ge=1, le=65535)
    collectors: CollectorToggles
    labels: Labels
    janus_lock_label: str
    defaults: RemoteNetworkDefaults
    remote_networks: list[RemoteNetworkOverride] = Field(default_factory=list)

    def resolve_remote_network(self, rn_id: str) -> ResolvedRemoteNetwork:
        """Return the fully-resolved policy for ``rn_id``.

        If ``rn_id`` matches a configured override, its non-``None`` tunables
        take precedence over :attr:`defaults`. If ``rn_id`` is not configured
        (e.g. an auto-discovered Remote Network), the pure defaults are
        applied and ``rn_id`` is used as both ``id`` and ``name``.

        Raises:
            ConfigError: If the merged result violates an invariant (e.g. an
                override sets ``min_connectors`` below the floor of 2).
        """
        override = next((rn for rn in self.remote_networks if rn.id == rn_id), None)
        merged: dict[str, object] = {
            field: getattr(self.defaults, field) for field in _TUNABLE_FIELDS
        }

        if override is None:
            merged["id"] = rn_id
            merged["name"] = rn_id
        else:
            for field in _TUNABLE_FIELDS:
                value = getattr(override, field)
                if value is not None:
                    merged[field] = value
            merged["id"] = override.id
            merged["name"] = override.name

        try:
            return ResolvedRemoteNetwork.model_validate(merged)
        except ValidationError as exc:
            raise ConfigError(
                f"invalid resolved policy for remote network {rn_id!r}: {exc}"
            ) from exc

    def resolved_networks(self) -> dict[str, ResolvedRemoteNetwork]:
        """Return resolved policies for every explicitly-configured RN, keyed by id."""
        return {rn.id: self.resolve_remote_network(rn.id) for rn in self.remote_networks}


def load_policy(path: str | Path) -> Policy:
    """Load and validate the YAML policy file into a :class:`Policy`.

    Args:
        path: Filesystem path to the YAML policy file.

    Returns:
        The validated :class:`Policy`.

    Raises:
        ConfigError: On a missing file, a YAML parse error, or a Pydantic
            validation failure. The message is safe to log (YAML carries no
            secrets).
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
        policy = Policy.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(f"invalid policy in {file_path}: {exc}") from exc

    # Eagerly resolve every configured override so an override whose tunables are
    # individually valid but jointly violate an invariant after merging onto the
    # defaults (e.g. an override max_connectors below the defaults' min_connectors)
    # fails fast here at startup rather than mid-cycle. Raises ConfigError.
    policy.resolved_networks()
    return policy
