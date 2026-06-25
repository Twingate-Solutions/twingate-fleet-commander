"""Platform factory: build the actuator + collectors for ``FC_PLATFORM``.

This is the single place the explicit ``FC_PLATFORM`` choice is turned into a
concrete compute backend (Key Design Rule #9): each branch constructs the
matching :class:`~fc.actuator.base.Actuator`, its collector set, a readiness
probe, and a cleanup coroutine, and returns them as a :class:`Platform` bundle
the entrypoint wires into the loop and the API.

Cloud SDK imports are **lazy** — performed only inside the branch that needs
them — so the default Docker path (and the test suite) never require the optional
``aioboto3`` / ``azure-identity`` extras to be installed. The cloud branches also
accept injected client factories so a test can exercise selection without any
SDK present.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aiodocker
import httpx

from fc.actuator.aci_actuator import AciActuator, TokenProvider
from fc.actuator.base import Actuator
from fc.actuator.docker_actuator import DockerActuator
from fc.actuator.ecs_actuator import EcsActuator
from fc.collectors.azure_logs import AzureLogsCollector
from fc.collectors.base import Collector
from fc.collectors.cloudwatch_logs import CloudWatchLogsCollector
from fc.collectors.docker_stats import DockerStatsCollector
from fc.collectors.stdout_metrics import StdoutMetricsCollector
from fc.config import Policy, Settings
from fc.docker_inspect import InspectCache
from fc.platform import AciSettings, EcsSettings, load_aci_settings, load_ecs_settings

#: Factory that yields an ``aioboto3.Session`` (injected so tests need no SDK).
EcsSessionFactory = Callable[[], Any]
#: Factory that yields a :data:`~fc.actuator.aci_actuator.TokenProvider` for ACI.
AciTokenProviderFactory = Callable[[AciSettings], TokenProvider]


@dataclass
class Platform:
    """Everything the entrypoint needs for the selected compute backend.

    Attributes:
        actuator: The compute actuator the loop drives.
        collectors: The enabled collectors, run in order per Connector.
        compute_probe: An awaitable that succeeds iff the backend is reachable
            (wired into ``/readyz``).
        aclose: Cleanup coroutine for any long-lived backend resources.
    """

    actuator: Actuator
    collectors: list[Collector]
    compute_probe: Callable[[], Awaitable[object]]
    aclose: Callable[[], Awaitable[None]]


async def _noop_close() -> None:
    """Cleanup for backends that hold no long-lived client (cloud per-call)."""


def build_platform(
    settings: Settings,
    policy: Policy,
    *,
    http: httpx.AsyncClient,
    ecs_session_factory: EcsSessionFactory | None = None,
    aci_token_provider_factory: AciTokenProviderFactory | None = None,
) -> Platform:
    """Build the :class:`Platform` bundle for ``settings.fc_platform``.

    Args:
        settings: Process/secret settings (carries ``fc_platform`` + identity).
        policy: The validated autoscaling policy (image, labels, collector
            toggles, janus).
        http: The shared HTTP client (used by the ACI REST actuator/collector).
        ecs_session_factory: Optional override for the aioboto3 session builder
            (tests inject a fake; production uses the default lazy import).
        aci_token_provider_factory: Optional override for the Azure credential
            builder (tests inject a stub token provider).

    Returns:
        The platform bundle for the selected backend.
    """
    match settings.fc_platform:
        case "docker":
            return _build_docker(settings, policy)
        case "ecs":
            return _build_ecs(settings, policy, ecs_session_factory)
        case "aci":
            return _build_aci(settings, policy, http, aci_token_provider_factory)


def _build_docker(settings: Settings, policy: Policy) -> Platform:
    """Local-Docker backend: the default. Shares one inspect cache."""
    docker = aiodocker.Docker(url=settings.docker_host)
    inspect_cache = InspectCache(docker)
    actuator = DockerActuator(
        docker,
        network=settings.twingate_network,
        image=policy.connector_image,
        labels=policy.labels,
        janus_enabled=policy.janus.enabled,
        janus_interval_seconds=policy.janus.interval_seconds,
        inspect_cache=inspect_cache,
    )
    collectors: list[Collector] = []
    if policy.collectors.docker_stats:
        collectors.append(DockerStatsCollector(docker))
    if policy.collectors.stdout_metrics:
        collectors.append(StdoutMetricsCollector(docker, inspect_cache=inspect_cache))
    return Platform(
        actuator=actuator,
        collectors=collectors,
        compute_probe=docker.version,
        aclose=docker.close,
    )


def _default_aioboto3_session() -> Any:
    """Build a real ``aioboto3.Session`` (lazy import of the optional extra)."""
    import aioboto3

    return aioboto3.Session()


def _build_ecs(
    settings: Settings,
    policy: Policy,
    session_factory: EcsSessionFactory | None,
) -> Platform:
    """AWS ECS backend: RunTask actuator + CloudWatch-Logs collector."""
    ecs_settings: EcsSettings = load_ecs_settings()
    session = (session_factory or _default_aioboto3_session)()
    actuator = EcsActuator(
        session,
        settings=ecs_settings,
        network=settings.twingate_network,
        image=policy.connector_image,
        labels=policy.labels,
    )
    collectors: list[Collector] = []
    if policy.collectors.stdout_metrics:
        collectors.append(CloudWatchLogsCollector(session, settings=ecs_settings))
    return Platform(
        actuator=actuator,
        collectors=collectors,
        compute_probe=actuator.list_managed,
        aclose=_noop_close,
    )


def _default_azure_token_provider(
    settings: AciSettings,
) -> tuple[TokenProvider, Callable[[], Awaitable[None]]]:
    """Build an Azure bearer-token provider + its cleanup (lazy SDK import).

    Uses an explicit service principal when ``tenant_id`` + ``client_id`` +
    ``client_secret`` are configured, otherwise the default credential chain
    (managed identity / environment / Azure CLI). Returns the token callable
    paired with an ``aclose`` coroutine that closes the underlying async
    credential (which holds an HTTP client and cached tokens) at shutdown.
    """
    from azure.identity.aio import ClientSecretCredential, DefaultAzureCredential

    credential: Any
    if settings.tenant_id and settings.client_id and settings.client_secret:
        credential = ClientSecretCredential(
            settings.tenant_id,
            settings.client_id,
            settings.client_secret.get_secret_value(),
        )
    else:
        credential = DefaultAzureCredential()

    async def _get_token(scope: str) -> str:
        token = await credential.get_token(scope)
        return str(token.token)

    async def _aclose() -> None:
        await credential.close()

    return _get_token, _aclose


def _build_aci(
    settings: Settings,
    policy: Policy,
    http: httpx.AsyncClient,
    token_provider_factory: AciTokenProviderFactory | None,
) -> Platform:
    """Azure ACI backend: REST actuator + Azure-Monitor collector."""
    aci_settings: AciSettings = load_aci_settings()
    if token_provider_factory is not None:
        token_provider = token_provider_factory(aci_settings)
        aclose: Callable[[], Awaitable[None]] = _noop_close
    else:
        token_provider, aclose = _default_azure_token_provider(aci_settings)
    actuator = AciActuator(
        http,
        token_provider,
        settings=aci_settings,
        network=settings.twingate_network,
        image=policy.connector_image,
        labels=policy.labels,
    )
    collectors: list[Collector] = []
    if policy.collectors.stdout_metrics:
        collectors.append(AzureLogsCollector(http, token_provider, settings=aci_settings))
    return Platform(
        actuator=actuator,
        collectors=collectors,
        compute_probe=actuator.list_managed,
        aclose=aclose,
    )
