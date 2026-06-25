"""The event-name catalog as constants (used as the ``event`` log field).

Every structured log line FC emits uses one of these constants as its ``event``
field, so the event vocabulary lives in exactly one place and the documented
catalog in ``documentation/OBSERVABILITY.md`` can be diffed against it.
Grouping mirrors the control-loop phases: cycle lifecycle, discovery,
collection, decision, action, health, and the two external-dependency error
channels.

These are plain ``str`` constants (not an enum) so they drop straight into a
``structlog`` call — ``log.info(LOOP_CYCLE_START, ...)`` — and compare equal to
the raw strings asserted in tests and matched by downstream log queries.
"""

from typing import Final

# --- Cycle lifecycle -------------------------------------------------------
#: A control-loop cycle has begun (carries ``cycle_id``).
LOOP_CYCLE_START: Final = "loop.cycle.start"
#: A cycle completed cleanly — the **heartbeat** line. Its absence means a
#: silent/stuck manager (``cycle_id``, ``duration_ms``, ``rn_count``).
LOOP_CYCLE_COMPLETE: Final = "loop.cycle.complete"
#: An unhandled error aborted a cycle; the loop logs and continues (``error``).
LOOP_CYCLE_ERROR: Final = "loop.cycle.error"
#: An unhandled error while deciding/acting on one Remote Network; that RN is
#: skipped and the cycle continues with the rest (``rn_id``, ``error``). One bad
#: Remote Network never aborts the whole cycle.
LOOP_RN_ERROR: Final = "loop.rn.error"

# --- Discovery -------------------------------------------------------------
#: Fleet discovery finished; carries per-RN container/logical counts.
DISCOVER_RESULT: Final = "discover.result"

# --- Collection ------------------------------------------------------------
#: (debug) A single resource sample was taken (``connector_id``, ``source``).
COLLECT_SAMPLE: Final = "collect.sample"
#: A collector failed for one Connector; isolated and skipped (``source``).
COLLECT_ERROR: Final = "collect.error"

# --- Decision --------------------------------------------------------------
#: A scale-up was decided for a Remote Network (``count``, ``reason``).
DECIDE_SCALE_UP: Final = "decide.scale_up"
#: A scale-down was decided for a Remote Network (``count``, ``reason``).
DECIDE_SCALE_DOWN: Final = "decide.scale_down"
#: Steady state — no scaling action this cycle for the RN.
DECIDE_NO_ACTION: Final = "decide.no_action"
#: A scaling action was suppressed by an active cooldown (``direction``,
#: ``seconds_remaining``).
DECIDE_COOLDOWN_SKIP: Final = "decide.cooldown_skip"

# --- Actions ---------------------------------------------------------------
#: Provisioning a Connector began (``rn_id``, ``name``).
ACTION_PROVISION_START: Final = "action.provision.start"
#: A Connector was provisioned successfully (``rn_id``, ``connector_id``).
ACTION_PROVISION_SUCCESS: Final = "action.provision.success"
#: Provisioning failed at some step (``rn_id``, ``error``).
ACTION_PROVISION_FAIL: Final = "action.provision.fail"
#: Draining + removing a Connector began (``connector_id``, ``drain_grace``).
ACTION_DEPROVISION_START: Final = "action.deprovision.start"
#: A Connector was drained and removed successfully (``connector_id``).
ACTION_DEPROVISION_SUCCESS: Final = "action.deprovision.success"
#: Deprovisioning failed (``connector_id``, ``error``).
ACTION_DEPROVISION_FAIL: Final = "action.deprovision.fail"
#: A Connector was restarted in place (``connector_id``, ``restart_count``).
ACTION_RESTART: Final = "action.restart"
#: A Connector was replaced after repeated restart failures
#: (``old_connector_id``, ``new_connector_id``).
ACTION_REPLACE: Final = "action.replace"
#: A Connector was cordoned/un-cordoned via a manual override
#: (``connector_id``, ``cordoned``, ``actor=manual``).
ACTION_CORDON: Final = "action.cordon"

# --- Health ----------------------------------------------------------------
#: Twingate reports a Connector in a ``DEAD_*`` state (``connector_id``,
#: ``state``).
HEALTH_CONNECTOR_DEAD: Final = "health.connector_dead"
#: A Connector's Docker health is ``unhealthy`` (``connector_id``).
HEALTH_UNHEALTHY: Final = "health.unhealthy"

# --- Janus -----------------------------------------------------------------
#: A Connector was skipped because the janus upgrade lock is engaged
#: (``connector_id``).
JANUS_LOCK_ENGAGED: Final = "janus.lock_engaged"

# --- Config & external-dependency errors -----------------------------------
#: The policy configuration was (re)loaded (``path``).
CONFIG_RELOAD: Final = "config.reload"
#: A Twingate GraphQL call failed (``operation``, ``error``).
TWINGATE_API_ERROR: Final = "twingate_api.error"
#: A Docker API call failed (``op``, ``error``).
DOCKER_API_ERROR: Final = "docker_api.error"
