# Databricks notebook source
# MAGIC %md
# MAGIC # dbt Runner — Gold → Snapshot → Warehouse → Analytics
# MAGIC
# MAGIC **Purpose:** Install dbt-databricks, write `~/.dbt/profiles.yml` from
# MAGIC Databricks Secrets, then run:
# MAGIC
# MAGIC | Command | What it does |
# MAGIC |---|---|
# MAGIC | `dbt deps`     | Install packages (dbt_utils) |
# MAGIC | `dbt snapshot` | SCD Type 2 for dim_customer |
# MAGIC | `dbt run`      | All Gold, Warehouse, and Analytics models |
# MAGIC | `dbt test`     | Schema + data quality tests |
# MAGIC
# MAGIC **Replaces notebooks:** 03_gold_aggregation, 07_scd2_dim_customer,
# MAGIC 04_warehouse_star_schema, 05_analytics_queries
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - `dbt-databricks` installed on the cluster (recommended), or installed
# MAGIC   below via `%pip install` (adds ~3 min to first run — restart required).
# MAGIC - Databricks Secret Scope `retail_platform` with keys:
# MAGIC   `databricks_host`, `databricks_http_path`, `databricks_token`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dbt (skip if pre-installed on cluster init script)

# COMMAND ----------

# Comment this cell out if dbt-databricks is already on the cluster.
# Pre-installing via an init script is recommended for job clusters to
# avoid the mandatory kernel restart that %pip triggers.

# %pip install dbt-databricks dbt-core --quiet

# COMMAND ----------

import os, subprocess, textwrap, time

BATCH_ID    = (dbutils.widgets.get("batch_id")
               if "batch_id" in [w.name for w in dbutils.widgets.getAll()]
               else "manual_run")

# Absolute path to the dbt project inside the repo
DBT_PROJECT = "/Workspace/retail-sales-platform/dbt/retail_sales"

print("=" * 60)
print("DBT RUN — START")
print(f"batch_id : {BATCH_ID}")
print(f"project  : {DBT_PROJECT}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write profiles.yml from Databricks Secrets

# COMMAND ----------

def write_profiles():
    """
    Reads connection details from Databricks Secret Scope and writes
    ~/.dbt/profiles.yml.  Nothing sensitive is printed or persisted in
    notebook output.
    """
    scope = "retail_platform"
    host      = dbutils.secrets.get(scope, "databricks_host")
    http_path = dbutils.secrets.get(scope, "databricks_http_path")
    token     = dbutils.secrets.get(scope, "databricks_token")

    profile = textwrap.dedent(f"""
        retail_sales_platform:
          target: dev
          outputs:
            dev:
              type: databricks
              host: {host}
              http_path: {http_path}
              token: {token}
              schema: retail_platform
              threads: 4
              connect_retries: 3
              connect_timeout: 60
    """).strip()

    profiles_dir = os.path.expanduser("~/.dbt")
    os.makedirs(profiles_dir, exist_ok=True)
    with open(os.path.join(profiles_dir, "profiles.yml"), "w") as f:
        f.write(profile)
    print("  profiles.yml written from Databricks Secrets")


write_profiles()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run dbt

# COMMAND ----------

def run_dbt(cmd: list[str], step: str) -> None:
    """Run a dbt command, stream output, and raise on non-zero exit."""
    t0 = time.time()
    print(f"\n{'─'*50}")
    print(f"  {step}: dbt {' '.join(cmd)}")
    print(f"{'─'*50}")

    result = subprocess.run(
        ["dbt"] + cmd + [f"--project-dir={DBT_PROJECT}"],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)

    duration = int((time.time() - t0) * 1000)
    if result.returncode != 0:
        raise RuntimeError(
            f"dbt {cmd[0]} failed (exit {result.returncode}) in {duration}ms"
        )
    print(f"  ✓ {step} complete ({duration:,}ms)")


# ── 1. Install packages ───────────────────────────────────────────────────────
run_dbt(["deps"], "Install packages")

# ── 2. SCD Type 2 snapshot — dim_customer ────────────────────────────────────
run_dbt(["snapshot"], "SCD2 snapshot")

# ── 3. Run all models (Gold → Warehouse → Analytics) ─────────────────────────
run_dbt(["run"], "Build models")

# ── 4. Data quality tests ─────────────────────────────────────────────────────
run_dbt(["test"], "Data tests")

print("\n" + "=" * 60)
print("DBT RUN — COMPLETE")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick sanity checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dbt snapshot column overview
# MAGIC SELECT
# MAGIC     COUNT(*)                                AS total_versions,
# MAGIC     COUNT(DISTINCT customer_id)             AS unique_customers,
# MAGIC     SUM(CASE WHEN dbt_valid_to IS NULL THEN 1 ELSE 0 END) AS active_rows,
# MAGIC     SUM(CASE WHEN dbt_valid_to IS NOT NULL THEN 1 ELSE 0 END) AS history_rows
# MAGIC FROM retail_platform.dim_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify no orphan transactions in fact_sales
# MAGIC SELECT
# MAGIC     'orphan_customers' AS check_name,
# MAGIC     COUNT(*) AS orphan_count
# MAGIC FROM retail_platform.fact_sales f
# MAGIC LEFT JOIN retail_platform.dim_customer dc ON f.customer_key = dc.dbt_scd_id
# MAGIC WHERE dc.dbt_scd_id IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'orphan_products', COUNT(*)
# MAGIC FROM retail_platform.fact_sales f
# MAGIC LEFT JOIN retail_platform.dim_product dp ON f.product_key = dp.product_key
# MAGIC WHERE dp.product_key IS NULL
