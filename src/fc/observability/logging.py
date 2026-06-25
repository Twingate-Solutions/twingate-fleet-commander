"""structlog configuration: JSON to stdout, standard fields, secret redaction.

One JSON event per line to stdout is FC's universal observability path — any
stdout collector in the environment ingests it with no extra wiring (see
``documentation/OBSERVABILITY.md``). :func:`configure_logging` installs
the processor chain once at startup; every ``structlog`` logger then emits lines
carrying the standard fields ``ts``, ``level``, and ``event`` (plus whatever
``cycle_id`` / ``rn_id`` / ``connector_id`` the caller binds).

The last line of defence before rendering is :func:`redact_secrets`, a processor
that scrubs secret-shaped material — both by key name (anything that looks like
a token/key/password field) and by value (a :class:`~pydantic.SecretStr`, or a
string carrying a recognised secret pattern: a ``TWINGATE_*`` token env
assignment or an ``Authorization: Bearer`` / ``X-API-KEY`` header, matched
anywhere in the string). It is a *backstop*, not a guarantee: the codebase keeps
secrets out of logs by construction, and this catches the recognised shapes if a
future call passes one in. It cannot catch an arbitrary high-entropy token under
an innocuous key, nor secrets rendered into exception text by
``format_exc_info`` (which runs after this processor) — keep relying on
``SecretStr`` and not logging raw token values.
"""

import logging
import re
import sys
from collections.abc import Callable, Mapping, MutableMapping
from typing import Any

import structlog
from pydantic import SecretStr

#: A structlog processor: ``(logger, method_name, event_dict) -> event_dict``.
Processor = Callable[[Any, str, MutableMapping[str, Any]], MutableMapping[str, Any]]

#: The replacement rendered in place of any redacted value.
REDACTED: str = "[REDACTED]"

#: Substrings (case-insensitive) that mark a log-event key as secret-bearing.
#: A key containing any of these has its value replaced with :data:`REDACTED`.
_SECRET_KEY_MARKERS: tuple[str, ...] = (
    "api_key",
    "apikey",
    "api-key",
    "token",
    "secret",
    "password",
    "passwd",
    "authorization",
)

#: Secret-bearing patterns whose presence *anywhere* in a string value means the
#: whole value must be redacted (not just the prefix): a ``TWINGATE_*`` token env
#: assignment (e.g. a leaked container ``Env`` entry, which need not sit at the
#: start of the string), or an ``Authorization: Bearer`` / ``X-API-KEY`` header
#: value. Matched case-insensitively and redacted wholesale rather than masked.
_SECRET_VALUE_PATTERN = re.compile(
    r"TWINGATE_(?:API_KEY|ACCESS_TOKEN|REFRESH_TOKEN)\s*=\S"
    r"|authorization\s*[:=]\s*bearer\s+\S"
    r"|bearer\s+[A-Za-z0-9._\-]{8,}"
    r"|x-api-key\s*[:=]\s*\S",
    re.IGNORECASE,
)


def _key_is_secret(key: str) -> bool:
    """Return whether an event-dict key names a secret-bearing field."""
    lowered = key.lower()
    return any(marker in lowered for marker in _SECRET_KEY_MARKERS)


def _redact_value(value: Any) -> Any:
    """Recursively redact a single value, descending into mappings and lists.

    A :class:`~pydantic.SecretStr` is always redacted; a string containing a
    recognised secret pattern (anywhere in the string) is redacted wholesale;
    mappings and sequences are walked so a secret nested inside a structure is
    still caught.
    """
    if isinstance(value, SecretStr):
        return REDACTED
    if isinstance(value, str):
        if _SECRET_VALUE_PATTERN.search(value):
            return REDACTED
        return value
    if isinstance(value, Mapping):
        return {
            k: (REDACTED if _key_is_secret(str(k)) else _redact_value(v)) for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return type(value)(_redact_value(v) for v in value)
    return value


def redact_secrets(
    _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """structlog processor that scrubs secret-shaped values from a log event.

    Redaction is applied both by key name (``api_key``, ``*token*``,
    ``*secret*``, ``password``, ``authorization``) and by value
    (:class:`~pydantic.SecretStr` instances and strings carrying a recognised
    secret pattern — a ``TWINGATE_*`` token env assignment or a bearer/
    ``X-API-KEY`` header value, matched anywhere in the string), descending into
    nested mappings and sequences.

    Args:
        _logger: The wrapped logger (unused).
        _method: The log method name (unused).
        event_dict: The accumulated event fields; mutated in place.

    Returns:
        The same ``event_dict`` with any secret-shaped values replaced by
        :data:`REDACTED`.
    """
    for key in list(event_dict.keys()):
        if _key_is_secret(str(key)):
            event_dict[key] = REDACTED
        else:
            event_dict[key] = _redact_value(event_dict[key])
    return event_dict


def configure_logging(
    level: str = "info", *, extra_processors: list[Processor] | None = None
) -> None:
    """Configure structlog to emit JSON lines to stdout with standard fields.

    Idempotent enough to call once at process start. The processor chain adds
    the log level and an ISO-8601 ``ts`` field, applies :func:`redact_secrets`
    as the secret backstop, runs any ``extra_processors`` (which therefore see
    already-redacted events), and renders JSON. The standard-library root logger
    is pointed at stdout at the same threshold so any non-structlog library log
    is still captured on the universal stdout path.

    Args:
        level: Minimum level name (``debug``/``info``/``warning``/``error``);
            case-insensitive.
        extra_processors: Optional processors inserted after redaction and
            before JSON rendering (e.g. the status UI's recent-events buffer).
            They run after redaction so anything they capture is already scrubbed.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
        force=True,
    )

    processors: list[Processor | Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", key="ts", utc=True),
        redact_secrets,
        *(extra_processors or []),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
