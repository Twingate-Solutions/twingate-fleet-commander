"""Cloud platform settings, discriminated by ``FC_PLATFORM``.

Fleet Commander actuates exactly one compute backend, chosen *explicitly* by the
``FC_PLATFORM`` environment variable on :class:`~fc.config.Settings` (never
auto-detected — FC deletes compute, so the backend must be a deliberate operator
choice). The local-Docker default needs no extra settings; each cloud backend
has its own fail-fast settings model sourced from the environment:

* ``ecs`` → :class:`EcsSettings` (``FC_ECS__*``)
* ``aci`` → :class:`AciSettings` (``FC_ACI__*``)

Placement (cluster, subnets, region, resource group, ...) lives in these env
vars; **credentials do not**. AWS identity comes from the standard boto chain
(instance/task role or ``AWS_*``); Azure identity from the managed-identity /
service-principal chain. The only bespoke secret field is the optional Azure
service-principal ``client_secret`` (a :class:`~pydantic.SecretStr`), used when
an explicit service principal is configured instead of a managed identity.

Constructing a settings model with a required field unset raises a
:class:`~pydantic.ValidationError`, so a misconfigured cloud backend fails fast
at startup exactly like the YAML :class:`~fc.config.Policy`.
"""

import json
from typing import Annotated, Any, Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

#: The compute backends FC can actuate (mirrors ``Settings.fc_platform``).
Platform = Literal["docker", "ecs", "aci"]


def _split_csv(value: Any) -> Any:
    """Parse a list field's env value as either JSON or comma-separated.

    The list fields are annotated :class:`~pydantic_settings.NoDecode`, so the
    raw env string reaches this before-validator untouched. A JSON array
    (``["a","b"]``) is decoded as-is; anything else is treated as the more
    natural ``a,b`` comma-separated form. A non-string value (already a list)
    passes through unchanged.
    """
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except ValueError:
                parsed = None
            if isinstance(parsed, list):
                return parsed
        return [item.strip() for item in text.split(",") if item.strip()]
    return value


class EcsSettings(BaseSettings):
    """AWS ECS placement settings (``FC_ECS__*``).

    Credentials are **not** here — they come from the standard boto3 credential
    chain (ECS task role, instance profile, or ``AWS_*`` env). Only non-secret
    placement and the prescribed task shape live in these env vars.
    """

    model_config = SettingsConfigDict(
        env_prefix="FC_ECS__",
        extra="ignore",
        case_sensitive=False,
    )

    #: ECS cluster name (or ARN) FC runs connector tasks in.
    cluster: str
    #: awsvpc subnets the connector tasks attach to (``FC_ECS__SUBNETS`` —
    #: JSON list or comma-separated).
    subnets: Annotated[list[str], NoDecode]
    #: Security groups for the awsvpc ENI (optional; comma-separated allowed).
    security_groups: Annotated[list[str], NoDecode] = Field(default_factory=list)
    #: Task role ARN assumed by the connector container (optional).
    task_role_arn: str | None = None
    #: Execution role ARN for image pull + log writes (optional but usually
    #: required on Fargate).
    execution_role_arn: str | None = None
    #: AWS region; falls back to the boto default chain when unset.
    region: str | None = None
    #: Whether to assign a public IP to the task ENI (public-subnet deployments).
    assign_public_ip: bool = False
    #: ECS launch type for the connector tasks.
    launch_type: Literal["FARGATE", "EC2"] = "FARGATE"
    #: Task-definition family FC registers/uses for the connector.
    task_family: str = "fc-connector"
    #: Container name inside the task definition (used for env overrides + the
    #: CloudWatch log stream path).
    container_name: str = "connector"
    #: CloudWatch Logs group the connector's ``awslogs`` driver writes to and the
    #: log-based collector reads from. When unset, log-based collection is off.
    log_group: str | None = None
    #: ``awslogs-stream-prefix`` for the connector container; also the first
    #: segment of the log stream name the collector derives.
    log_stream_prefix: str = "fc"

    @field_validator("subnets", "security_groups", mode="before")
    @classmethod
    def _split_lists(cls, value: Any) -> Any:
        """Allow comma-separated env strings for the list fields."""
        return _split_csv(value)

    @model_validator(mode="after")
    def _require_region_with_log_group(self) -> "EcsSettings":
        """Fail fast when log-based collection is on but no region is set.

        The ``awslogs`` driver cannot infer the region the way the boto client
        chain can, so a ``log_group`` without an explicit ``region`` would write
        an empty ``awslogs-region`` and the task would fail to start.
        """
        if self.log_group and not self.region:
            raise ValueError("FC_ECS__REGION is required when FC_ECS__LOG_GROUP is set")
        return self


class AciSettings(BaseSettings):
    """Azure Container Instances placement settings (``FC_ACI__*``).

    Identity is taken from the managed-identity / Azure CLI / environment
    credential chain by default. A service principal can be configured
    explicitly via ``tenant_id`` + ``client_id`` + ``client_secret`` (the only
    bespoke secret field).
    """

    model_config = SettingsConfigDict(
        env_prefix="FC_ACI__",
        extra="ignore",
        case_sensitive=False,
    )

    #: Azure subscription id the container groups live in.
    subscription_id: str
    #: Resource group FC creates/deletes connector container groups in.
    resource_group: str
    #: Azure region (location) for the container groups.
    region: str
    #: VNet subnet resource id for VNet injection (optional).
    subnet_id: str | None = None
    #: Service-principal tenant id (optional; managed identity used when unset).
    tenant_id: str | None = None
    #: Service-principal client id (optional).
    client_id: str | None = None
    #: Service-principal client secret (optional; the only bespoke secret).
    client_secret: SecretStr | None = None
    #: Log Analytics workspace id the connector logs flow to; when unset,
    #: log-based collection is off.
    log_analytics_workspace_id: str | None = None
    #: Container name inside each container group (display/observability only).
    container_name: str = "connector"


#: The non-Docker platform settings union, discriminated by ``FC_PLATFORM``.
PlatformSettings = EcsSettings | AciSettings


def load_ecs_settings() -> EcsSettings:
    """Load and validate :class:`EcsSettings` from the environment (fail-fast)."""
    return EcsSettings()  # type: ignore[call-arg]  # fields come from env


def load_aci_settings() -> AciSettings:
    """Load and validate :class:`AciSettings` from the environment (fail-fast)."""
    return AciSettings()  # type: ignore[call-arg]  # fields come from env
