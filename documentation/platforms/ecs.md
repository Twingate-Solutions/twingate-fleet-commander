# Fleet Commander on AWS ECS

Fleet Commander (FC) can actuate a fleet of Twingate Connectors as **ECS tasks**
instead of local Docker containers. The decision engine, policy, Twingate
bookkeeping (`connectorCreate` → `connectorGenerateTokens` → `connectorDelete`),
and observability are **identical** to the Docker backend — only the compute
actuator and the metrics collector change.

Select this backend explicitly with `FC_PLATFORM=ecs` (FC never auto-detects its
platform — it deletes compute, so the backend must be a deliberate choice).

---

## The model: one ECS task per Connector (1:1)

FC uses `RunTask` to launch **one standalone ECS task per logical Connector** —
**not** an ECS Service with a `desiredCount`. This is required by Key Design
Rule #1: each Connector's access/refresh tokens are single-use, so they must map
1:1 to a single running task, and a Service recycling a task definition across
replacement tasks would violate that.

| Actuator op | ECS call(s) | Notes |
|---|---|---|
| `provision` | `RegisterTaskDefinition` (once, cached) → `RunTask` | Tokens travel as a **container-override environment** so the task definition never embeds a token. Prescribed **1 vCPU / 2 GB** sizing (N2; `cpu=1024`, `memory=2048`) + FC tags. |
| `restart` | `DescribeTasks` → `StopTask` → **wait for `STOPPED`** → `RunTask` | Reads the running task's override env (the **same token**), stops the old task, **polls `DescribeTasks` until it reaches `STOPPED`** (bounded by a timeout), then launches a replacement with that token. Because `StopTask` is asynchronous, the wait is what guarantees the token is never active in two tasks at once (amended Rule #1). If the old task does not stop within the timeout, the restart fails rather than risk a duplicate. |
| `deprovision` | `StopTask` | The control loop performs `connectorDelete` + the drain wait around this. |
| `list_managed` | `ListTasks` (narrowed by `startedBy=fc`) → `DescribeTasks` (`include=TAGS`) | Filtered by the FC managed tag; maps `healthStatus` into `docker_health`. |

The Twingate side is unchanged from Docker: FC still creates the logical
Connector and mints tokens via the GraphQL Admin API before `RunTask`, and
deletes it after `StopTask`.

### Identity scheme: tags, not labels

The Docker backend identifies its fleet with container **labels**; the ECS
backend uses the same keys as ECS **tags** (e.g. `twingate.fc.managed=true`,
`twingate.fc.rn`, `twingate.fc.connector_id`, and a derived `twingate.fc.name`).
`ListTasks` cannot filter by tag, so FC narrows by `startedBy=fc` first and then
filters the `DescribeTasks` result by the managed tag.

> **Sizing.** N2 prescribes 1 vCPU / 2 GB and FC registers the task with
> `cpu=1024`, `memory=2048`. `1024/2048` is a **valid AWS Fargate combination**
> (the 1-vCPU tier supports 2–8 GB) and is equally valid on the **EC2** launch
> type, so the default `FARGATE` launch type works out of the box with no memory
> adjustment. The Connector data path is single-threaded, so a 1-vCPU task lets a
> saturated Connector read ~100% normalized CPU; scale **horizontally** (more
> tasks) rather than up.

---

## Configuration

Placement is non-secret and set via `FC_ECS__*` env vars; **credentials are
not** — they come from the standard boto3 chain (ECS task role, EC2 instance
profile, or `AWS_*` env).

| Env var | Required | Meaning |
|---|---|---|
| `FC_ECS__CLUSTER` | ✅ | ECS cluster name or ARN |
| `FC_ECS__SUBNETS` | ✅ | awsvpc subnets (JSON list or comma-separated) |
| `FC_ECS__SECURITY_GROUPS` | | Security groups for the task ENI |
| `FC_ECS__REGION` | | AWS region (else the boto default chain) |
| `FC_ECS__ASSIGN_PUBLIC_IP` | | `true` for public-subnet tasks (default `false`) |
| `FC_ECS__LAUNCH_TYPE` | | `FARGATE` (default) or `EC2` |
| `FC_ECS__TASK_ROLE_ARN` | | Role the connector container assumes |
| `FC_ECS__EXECUTION_ROLE_ARN` | | Role for image pull + log writes (usually required on Fargate) |
| `FC_ECS__TASK_FAMILY` | | Task-def family (default `fc-connector`) |
| `FC_ECS__CONTAINER_NAME` | | Container name in the task def (default `connector`) |
| `FC_ECS__LOG_GROUP` | | CloudWatch Logs group; **enables** log-based collection |
| `FC_ECS__LOG_STREAM_PREFIX` | | `awslogs-stream-prefix` (default `fc`) |

Install the optional AWS extra: `pip install -e '.[ecs]'` (adds `aioboto3`).

---

## Metrics collection: CloudWatch Logs

There is no Docker socket on ECS and the Prometheus collector has been removed,
so FC reads the **same `[metrics]` stdout lines** the custom connector image
emits — from **CloudWatch Logs**. Set `FC_ECS__LOG_GROUP` and register the task
with the `awslogs` driver (FC does this automatically when a log group is set).

The collector derives each connector's log stream as
`<log_stream_prefix>/<container_name>/<task-id>` (the awslogs convention),
fetches the recent events, reuses the shared `[metrics]` parser, and normalizes
CPU against the prescribed **1 effective vCPU**. A missing group/stream (a task
that hasn't logged yet) degrades to "no sample", not an error.

> Use the **custom** connector image (`connector_image` in `config.yaml`) for
> rich tunnel-throughput metrics. The official image emits no stdout metrics and,
> with no Docker socket on ECS, has no metrics source — set
> `collectors.stdout_metrics: false` and expect CPU/throughput to be unavailable.

---

## IAM policy for FC's task role

FC itself runs as an ECS service (or any host with AWS credentials). Its **task
role** needs:

This is the **least-privilege** policy: the action set is exactly the five ECS
calls the actuator makes plus the log reads the collector makes, the
cluster-scopable actions are conditioned on FC's cluster, and `iam:PassRole` is
constrained to the two named connector roles **and** to ECS.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "FleetCommanderClusterScopedLifecycle",
      "Effect": "Allow",
      "Action": [
        "ecs:RunTask",
        "ecs:StopTask",
        "ecs:ListTasks",
        "ecs:DescribeTasks"
      ],
      "Resource": "*",
      "Condition": {
        "ArnEquals": {
          "ecs:cluster": "arn:aws:ecs:<region>:<acct>:cluster/<cluster>"
        }
      }
    },
    {
      "Sid": "FleetCommanderRegisterTaskDef",
      "Effect": "Allow",
      "Action": "ecs:RegisterTaskDefinition",
      "Resource": "*"
    },
    {
      "Sid": "FleetCommanderPassConnectorRoles",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "arn:aws:iam::<acct>:role/fc-connector-task",
        "arn:aws:iam::<acct>:role/fc-connector-exec"
      ],
      "Condition": {
        "StringEquals": { "iam:PassedToService": "ecs-tasks.amazonaws.com" }
      }
    },
    {
      "Sid": "FleetCommanderReadConnectorLogs",
      "Effect": "Allow",
      "Action": [
        "logs:GetLogEvents",
        "logs:DescribeLogStreams"
      ],
      "Resource": "arn:aws:logs:<region>:<acct>:log-group:/fc/connectors:*"
    }
  ]
}
```

Notes:

- The `ecs:cluster` **`Condition`** on `RunTask`/`StopTask`/`ListTasks`/
  `DescribeTasks` is the security boundary — without it the role can act on every
  cluster in the account. Keep it.
- `ecs:RegisterTaskDefinition` is an **account-level** action that cannot be
  scoped by cluster ARN, so its `Resource` is necessarily `"*"` (it is the one
  line that genuinely needs the wildcard).
- `iam:PassRole` is required because the task definition references the connector
  task/execution roles; the `iam:PassedToService` condition ensures those roles
  can only ever be handed to ECS, not assumed elsewhere.
- **`ecs:DescribeTasks` exposes the connector tokens.** ECS returns the RunTask
  container-override environment verbatim, and FC carries the access/refresh
  tokens there. Treat the cluster as a token-bearing boundary: grant
  `ecs:DescribeTasks`/`ecs:ListTasks` on it **only** to FC's task role, not
  broadly.

---

## Running FC itself on ECS

Run FC as a **single-task ECS service** (`desiredCount: 1` — FC is a singleton
control plane, 1:1 with one Remote Network per Rule N1):

1. Build/push the FC image; create a task definition with the env from
   `.env.example` (`FC_PLATFORM=ecs`, the `FC_ECS__*` placement, and the Twingate
   secrets via Secrets Manager / SSM).
2. Attach the IAM task role above.
3. Mount the policy `config.yaml` (a small EFS volume, or bake it into the image)
   and point `FC_CONFIG_PATH` at it.
4. Expose port 8080 for `/healthz`, `/readyz`, `/metrics`, and the status UI
   (behind a load balancer or kept private — see the hardening notes in the main
   README).
5. `/readyz` probes the ECS backend (via `list_managed`) and the Twingate API, so
   the service is only "ready" once both are reachable.

The state SQLite file (`FC_STATE_PATH`) holds cooldowns + action history; put it
on a persistent volume so they survive a task replacement.

> **Note on error metrics.** Cloud-backend lifecycle failures are counted under
> the existing `fc_docker_api_errors_total` metric (the actuator backend error
> channel); the name is retained for dashboard compatibility.
