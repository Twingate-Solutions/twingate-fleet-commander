# Contributing to Fleet Commander

Thanks for your interest in improving FC. This is an example/reference project
(see the disclaimer at the top of the [README](README.md)) — it is provided
as-is, but contributions that improve correctness, clarity, or safety are
welcome.

## Ground rules

- **Be safe by default.** FC drives the Docker socket and the Twingate Admin
  API. Never weaken the safety rails (per-Remote-Network floor, drain-before-delete,
  asymmetric scaling windows, restart-before-replace, yielding to janus). See
  the design rules in [documentation/ARCHITECTURE.md](documentation/ARCHITECTURE.md).
- **Never log or persist secrets.** The Twingate API key and per-Connector
  tokens are write-only into the GraphQL header and Docker env. No secret may
  appear in a log line, exception, metric label, the status UI, or the state DB.
- **Keep the actuator an interface.** Docker-specific calls live behind the
  `Actuator` protocol so a multi-host backend can be swapped in later.

## Development setup

Requires **Python 3.12+** and Docker.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
playwright install chromium        # for the status-UI E2E tests
```

## Quality gates (run all four before opening a PR)

CI runs exactly these; they must pass locally first.

```bash
ruff check                 # lint
ruff format --check        # formatting
mypy                       # strict type checking
pytest                     # full suite (unit + status-UI E2E)
```

- Line length is 100 (`ruff format` owns wrapping).
- `mypy` runs in **strict** mode with `warn_unreachable` — type every signature.
- Tests use `pytest` + `pytest-asyncio` (auto mode); HTTP is mocked with `respx`
  and Docker/Twingate with in-memory fakes. No test touches a live socket or API.

## Coding conventions

- Modern type hints (`str | None`, `list[X]`), `match` where it reads well.
- Async throughout (`async def`, `httpx.AsyncClient`, `aiodocker`). Never block
  the event loop — wrap any sync/blocking call (SQLite, file I/O) in
  `asyncio.to_thread`.
- Pydantic v2 for all models, config, and the YAML policy schema; validate at
  startup and fail fast on bad config.
- `structlog` for all logging — JSON to stdout, never `print`. Every log line
  carries the standard fields (`ts`, `level`, `event`, `cycle_id`, and where
  relevant `rn_id` / `connector_id`). New event names go in
  `src/fc/observability/events.py` **and** the catalog in
  [documentation/OBSERVABILITY.md](documentation/OBSERVABILITY.md).
- `httpx` is the only HTTP client; `aiodocker` the only Docker path.
- Docstrings on every public class, method, and function.
- Typed exceptions (`TwingateApiError`, `DockerActuatorError`, `CollectorError`);
  the control loop isolates per-Connector and per-Remote-Network failures so one
  bad Connector or RN never aborts a cycle.

## Pull requests

1. Branch from `main`.
2. Make the change with tests — bug fixes get a regression test; new behavior
   gets unit tests covering the safety-rail matrix.
3. Run the four quality gates above until clean.
4. Open the PR with a clear description of the behavior change and why. Note any
   new config keys, env vars, events, or metrics so the reference docs stay in
   sync.

## Reporting issues

Open a GitHub issue with the FC version, your `config.yaml` (secrets redacted),
the relevant structured log lines (`event` + `cycle_id`), and what you expected
versus what happened.
