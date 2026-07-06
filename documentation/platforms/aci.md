# Fleet Commander on Azure Container Instances (ACI)

Fleet Commander (FC) can actuate a fleet of Twingate Connectors as **Azure
Container Instances** instead of local Docker containers. The decision engine,
policy, Twingate bookkeeping (`connectorCreate` → `connectorGenerateTokens` →
`connectorDelete`), and observability are **identical** to the Docker backend —
only the compute actuator and the metrics collector change.

Select this backend explicitly with `FC_PLATFORM=aci` (FC never auto-detects its
platform — it deletes compute, so the backend must be a deliberate choice).

---

## The model: one container group per Connector (1:1)

ACI's cleanest fit for FC's per-connector single-use token model is **one
container group per logical Connector** (Key Design Rule #1). The group holds the
connector container with its token, and the **group name is the Connector's
backend identity**. The actuator talks to the Azure Resource Manager REST API
over the project's existing `httpx` client (no Azure management SDK dependency);
a bearer token is supplied by the Azure credential chain.

| Actuator op | ARM REST call | Notes |
|---|---|---|
| `provision` | `PUT .../containerGroups/{name}` | Tokens injected as Azure **`secureValue`** env (write-only; never read back). Prescribed **1 vCPU / 2 GB** sizing (N2) + FC tags. |
| `restart` | `POST .../containerGroups/{name}/restart` | Restarts the **same** group/definition in place — same token, never two active (amended Rule #1). |
| `deprovision` | `DELETE .../containerGroups/{name}` | The control loop performs `connectorDelete` + the drain wait around this. A `404` (already gone) is tolerated. |
| `list_managed` | `GET .../containerGroups` | Filtered by the FC managed tag; maps the instance-view `state` into `docker_health` (`Running`→healthy, `Failed`→unhealthy). |

The Twingate side is unchanged from Docker.

### Identity scheme: tags

Like the ECS backend, ACI reuses the FC identity keys as resource **tags**
(`twingate.fc.managed=true`, `twingate.fc.rn`, `twingate.fc.connector_id`, and a
derived `twingate.fc.name`). `list_managed` filters the resource group's
container groups by the managed tag.

---

## Configuration

Placement is non-secret and set via `FC_ACI__*` env vars. Identity defaults to
the **managed identity / Azure credential chain**; configure an explicit service
principal only if you cannot use a managed identity.

| Env var | Required | Meaning |
|---|---|---|
| `FC_ACI__SUBSCRIPTION_ID` | ✅ | Azure subscription id |
| `FC_ACI__RESOURCE_GROUP` | ✅ | Resource group for the connector container groups |
| `FC_ACI__REGION` | ✅ | Azure region/location |
| `FC_ACI__SUBNET_ID` | | VNet subnet resource id (VNet injection) |
| `FC_ACI__CONTAINER_NAME` | | Container name inside each group (default `connector`) |
| `FC_ACI__LOG_ANALYTICS_WORKSPACE_ID` | | Log Analytics workspace id; **enables** log-based collection |
| `FC_ACI__TENANT_ID` | | Service-principal tenant id (optional) |
| `FC_ACI__CLIENT_ID` | | Service-principal client id (optional) |
| `FC_ACI__CLIENT_SECRET` | | Service-principal secret (optional; the only bespoke secret, kept as `SecretStr`) |

Install the optional Azure extra: `pip install -e '.[aci]'` (adds
`azure-identity` for the credential chain; the ARM/Log-Analytics calls use the
existing `httpx` dependency).

---

## Metrics collection: Azure Monitor / Log Analytics

There is no Docker socket on ACI and the Prometheus collector has been removed,
so FC reads the **same `[metrics]` stdout lines** the custom connector image
emits — from a **Log Analytics workspace** (`ContainerInstanceLog_CL`). Set
`FC_ACI__LOG_ANALYTICS_WORKSPACE_ID` and enable container-group diagnostics to
that workspace.

The collector runs a KQL query filtered to the connector's container group
(newest first), reuses the shared `[metrics]` parser, and normalizes CPU against
the prescribed **1 effective vCPU**. A missing workspace/table (`404`) degrades to
"no sample", not an error.

> Use the **custom** connector image for rich tunnel-throughput metrics; the
> official image emits no stdout metrics and, with no Docker socket on ACI, has no
> metrics source.

---

## Azure roles FC needs

FC needs a **managed identity** (or service principal) with permission to
create/restart/delete container groups in the target resource group and to query
the Log Analytics workspace. The **recommended least-privilege** grant is this
custom role — its action set is exactly the ARM calls the actuator and collector
make, scoped to the one resource group:

```json
{
  "Name": "Fleet Commander ACI",
  "Actions": [
    "Microsoft.ContainerInstance/containerGroups/read",
    "Microsoft.ContainerInstance/containerGroups/write",
    "Microsoft.ContainerInstance/containerGroups/delete",
    "Microsoft.ContainerInstance/containerGroups/restart/action",
    "Microsoft.OperationalInsights/workspaces/query/read"
  ],
  "AssignableScopes": ["/subscriptions/<sub>/resourceGroups/<rg>"]
}
```

The built-in roles **Contributor** (resource group) + **Log Analytics Reader**
(workspace) also work, but both are **broader than FC needs** — Contributor can
create/delete *any* resource type in the group, and Log Analytics Reader grants
more than the single `query/read` the collector uses. Prefer the custom role for
a component whose job is creating and **deleting** compute; fall back to the
built-ins only for a quick start.

> **Dedicate the resource group to FC.** Azure RBAC cannot condition the
> container-group write/delete actions on a resource tag, so FC can act on **any**
> container group in the assigned resource group. Put only FC-managed connector
> groups in that group so the blast radius is limited to the fleet.

With a managed identity, no secret is stored anywhere — leave the
`FC_ACI__TENANT_ID`/`CLIENT_ID`/`CLIENT_SECRET` trio unset and the default
credential chain picks up the identity.

---

## Running FC itself

Run FC as its own **single** container group (ACI) with a **system-assigned
managed identity**, or on a small VM with Docker:

1. Deploy the FC image as a container group; assign a managed identity and grant
   it the roles above.
2. Provide the env from `.env.example` (`FC_PLATFORM=aci`, the `FC_ACI__*`
   placement, the Twingate secrets via Key Vault references or secure env).
3. Mount/include the policy `config.yaml` and point `FC_CONFIG_PATH` at it.
4. Expose port 8080 for `/healthz`, `/readyz`, `/metrics`, and the status UI
   (kept private or behind a gateway — see the hardening notes in the README).
5. `/readyz` probes the ACI backend (via `list_managed`) and the Twingate API.

Persist the state SQLite file (`FC_STATE_PATH`) on an Azure Files mount so
cooldowns + action history survive a restart.

> **Note on error metrics.** Cloud-backend lifecycle failures are counted under
> the existing `fc_docker_api_errors_total` metric (the actuator backend error
> channel); the name is retained for dashboard compatibility.
