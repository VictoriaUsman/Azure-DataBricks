# Databricks Workflow — Setup Guide

Schedule: **Every Wednesday and Friday at 8:00 AM EST**  
Cron:     `0 0 8 ? * WED,FRI *` (timezone: America/New_York)

---

## How to import into Databricks

### Option A — Databricks CLI (recommended)

```bash
# Install CLI
pip install databricks-cli

# Configure with your workspace URL and token
databricks configure --token
# Prompt: Databricks Host: https://<your-workspace>.azuredatabricks.net
# Prompt: Token: <your-personal-access-token>

# Create the job
databricks jobs create --json @jobs/pipeline_workflow.json

# Verify it was created
databricks jobs list
```

### Option B — Databricks UI

1. Open your Databricks workspace
2. Click **Workflows** in the left sidebar
3. Click **Create Job**
4. Click the kebab menu (⋮) → **Edit JSON**
5. Paste the contents of `pipeline_workflow.json`
6. Click **Save**

---

## Before importing — update these values

Open `pipeline_workflow.json` and replace:

| Placeholder | Replace with |
|---|---|
| `/retail-sales-platform/notebooks/...` | Your actual workspace notebook paths |
| `ian.tristan@romtech.com` | Your email address |
| `REPLACE_WITH_SLACK_WEBHOOK_ID` | Your Databricks webhook notification ID, or remove the `webhook_notifications` block entirely |
| `Standard_DS3_v2` | Your available node type (Community Edition: remove `job_clusters` block and use `existing_cluster_id` instead — see below) |

---

## Community Edition — use existing cluster

Community Edition does not support job clusters (spin-up on demand).  
Replace the `job_cluster_key` references with your running cluster ID:

```json
// Remove the "job_clusters": [...] block entirely

// On each task, replace:
"job_cluster_key": "pipeline_cluster"

// With:
"existing_cluster_id": "YOUR_CLUSTER_ID_HERE"
```

Find your cluster ID: **Compute** → click your cluster → copy the ID from the URL  
(`https://community.cloud.databricks.com/#/cluster/YOUR_CLUSTER_ID`)

---

## Dependency chain

```
00_setup
    │
    ▼
01_bronze_ingestion          timeout: 30 min   retries: 3 (every 5 min)
    │
    ▼
02_silver_transformation     timeout: 40 min   retries: 2 (every 2 min)
    │
    ▼
08_dbt_run                   timeout: 60 min   retries: 2 (every 2 min)
    │   ├─ dbt deps      (install dbt_utils)
    │   ├─ dbt snapshot  (SCD2 dim_customer)
    │   ├─ dbt run       (Gold → Warehouse → Analytics)
    │   └─ dbt test      (schema + data quality)
    ▼
06_monitoring                timeout:  5 min   retries: 1   run_if: ALL_DONE
```

Notebooks 03, 04, 05, 07 are **retained as reference implementations** but
are no longer wired into the workflow — dbt handles those layers end-to-end.

`06_monitoring` uses `run_if: ALL_DONE` — it runs even if an upstream task failed,  
so the health check and failure alert always fires regardless of where the pipeline broke.

---

## Parameters passed to every task

| Parameter | Value | Description |
|---|---|---|
| `batch_id` | `{{job.start_time.iso_date}}` | e.g. `2026-05-21` — used for logging and lineage |

Every notebook reads this via:
```python
BATCH_ID = dbutils.widgets.get("batch_id") if "batch_id" in [w.name for w in dbutils.widgets.getAll()] else "manual_run"
```
Running a notebook manually outside the job defaults to `"manual_run"`.

---

## Trigger a manual run

```bash
# Get the job ID
databricks jobs list

# Trigger now with today's date as batch_id
databricks jobs run-now --job-id <JOB_ID> \
  --notebook-params '{"batch_id": "2026-05-20"}'
```

Or in the UI: **Workflows** → select the job → **Run now**

---

## Notifications

- **On success:** email sent to the address in `email_notifications.on_success`
- **On failure:** email + Slack webhook (if configured)
- `06_monitoring` also dispatches its own Slack/email alerts via `utils/notifier.py`
  based on pipeline health checks (errors, rejection rate, data freshness)
