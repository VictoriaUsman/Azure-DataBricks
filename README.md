# Retail Sales Analytics Platform

A production-grade data engineering platform demonstrating end-to-end Medallion
Architecture on Databricks with a full Azure stack mapping.

**Business case:** A retail company needs a scalable analytics platform to track
sales performance, customer segments, and product mix across 20 stores, 5,000
customers, and 50,000 transactions over two years.

---

## Architecture

```mermaid
flowchart LR
    SRC("CSV Landing Zone\n─────────────\nADF trigger\nin production")

    BRONZE("BRONZE\n─────────────\nRaw Delta\nAudit metadata\nbadRecordsPath")

    SILVER("SILVER\n─────────────\nType-safe\nDeduplicated\nValidated\nQuarantined")

    GOLD("GOLD\n─────────────\nAggregated\nRFM scoring\nStore / product\nperformance")

    WH("WAREHOUSE\n─────────────\nStar schema\nfact_sales\ndim_date/customer\nproduct/store")

    VIEWS("SQL VIEWS\n─────────────\nPower BI\nSynapse\n7 KPI views")

    SRC -->|ingest| BRONZE
    BRONZE -->|MERGE INTO| SILVER
    SILVER -->|aggregate| GOLD
    GOLD -->|model| WH
    WH -->|serve| VIEWS
```

```mermaid
flowchart LR
    subgraph Reliability
        RT("retry.py\nExp. backoff\n3 attempts\njitter")
        RL("rate_limiter.py\nToken bucket\nAPI throttling")
        NT("notifier.py\nMS Teams · Slack\nSMTP email")
        LG("logger.py\nDelta pipeline_logs\nStructured rows")
    end

    subgraph Quality
        VL("validation.py\nNull / dupe / range\nPass/fail scorecard")
        QT("Quarantine\n_quarantine/\nBad rows saved")
    end

    RT --> PIPE(Pipeline stages)
    RL --> PIPE
    PIPE --> LG
    PIPE --> VL
    VL --> QT
    LG --> NT
```

See [`architecture/architecture.md`](architecture/architecture.md) for the full
component diagram, Azure mapping, and optimization notes.

---

## Tech Stack Data Flow

End-to-end view of how data moves through the platform and which tools handle each layer.

```mermaid
flowchart TD
    CSV("CSV Landing Zone\nsales_transactions · customers\nproducts · stores")

    subgraph BRONZE ["① Bronze — Raw Ingestion"]
        B("PySpark + Delta Lake\nExplicit StructType schema\nbadRecordsPath · audit metadata\n_quarantine/ for rejected rows")
    end

    subgraph SILVER ["② Silver — Cleanse & Validate"]
        S("Delta MERGE INTO\nType-safe · deduplicated\nDataValidator scorecard\nQuarantine bad rows")
    end

    subgraph DBT ["③ dbt-databricks — Model & Test"]
        SN("snapshot: dim_customer\nSCD Type 2 · check strategy\ndbt_valid_from / dbt_valid_to")
        GD("Gold models\nRFM NTILE scoring\nDaily sales · store · product aggs")
        WH("Warehouse models\nfact_sales + dims\nStar schema · SCD2-aware join")
        AN("Analytics models\n7 KPI SQL views")
        GD --> WH
        SN --> WH
        WH --> AN
    end

    subgraph SERVE ["④ Serving"]
        PBI("Databricks SQL / Power BI\nExecutive KPIs · MoM growth\nCustomer tiers · Store performance")
    end

    subgraph OPS ["Operations"]
        WF("Databricks Workflows\nWed + Fri 8 AM EST\n5-task DAG with retries")
        MON("06_monitoring\nDelta pipeline_logs\nHealth · freshness checks")
        NT("notifier.py\nMS Teams · Slack · Email\nSecret Scope credentials")
        CD("Azure DevOps\nAzure Repos + Pipelines\nCI on PR · CD on merge to main")
        MON -->|alerts| NT
    end

    CSV -->|ingest| B
    B -->|MERGE INTO| S
    S -->|silver_customers| SN
    S -->|silver tables| GD
    AN -->|SQL views| PBI

    WF -.->|schedule| B
    WF -.->|schedule| S
    WF -.->|08_dbt_run notebook| SN
    CD -.->|deploy workflow JSON| WF
    B -.->|pipeline_logs| MON
    S -.->|pipeline_logs| MON
```

---

## Tech Stack

| Component           | This project                        | Azure Production        |
|---------------------|-------------------------------------|-------------------------|
| Storage             | DBFS                                | Azure Data Lake Gen2    |
| Orchestration       | Databricks Jobs                     | Azure Data Factory      |
| Processing          | PySpark / Databricks                | Databricks on Azure     |
| Table format        | Delta Lake                          | Delta Lake on ADLS      |
| Transformations     | dbt-databricks                      | dbt-databricks          |
| Warehouse           | Delta + SQL views                   | Azure Synapse Analytics |
| BI layer            | Databricks SQL                      | Power BI                |
| Monitoring          | Delta `pipeline_logs` table         | Azure Monitor           |
| Notifications       | MS Teams · Slack webhook · SMTP     | Azure Logic Apps        |
| Version control     | Git                                 | Azure DevOps            |
| CI/CD               | Azure DevOps Pipelines              | Azure DevOps Pipelines  |

---

## Quick Start

### Prerequisites
- Databricks Community Edition account
- Databricks Runtime 13.3 LTS or higher

### Notification setup (optional)

Alerts are sent to MS Teams, Slack, and/or email. Add credentials to a Databricks Secret Scope:

```bash
databricks secrets create-scope retail_platform

# MS Teams (recommended — get URL from: Channel → ... → Connectors → Incoming Webhook)
databricks secrets put --scope retail_platform --key teams_webhook_url

# Slack (optional)
databricks secrets put --scope retail_platform --key slack_webhook_url

# Email (optional)
databricks secrets put --scope retail_platform --key smtp_host
databricks secrets put --scope retail_platform --key smtp_user
databricks secrets put --scope retail_platform --key smtp_password
databricks secrets put --scope retail_platform --key email_from
```

If no secrets are configured, the pipeline runs normally — notifications are skipped with a warning.

### Run order

1. Import all notebooks into your Databricks workspace  
   (File → Import → select `.py` files)

2. Run in order:

   ```
   utils/data_generator          # generates 50k synthetic transactions
   notebooks/00_setup            # creates database + DBFS paths
   notebooks/01_bronze_ingestion
   notebooks/02_silver_transformation
   notebooks/08_dbt_run          # snapshot (SCD2) → Gold → Warehouse → Analytics → dbt test
   notebooks/06_monitoring       # health checks + alerts
   ```

   > **Note:** Notebooks 03–05 and 07 are retained as reference implementations showing
   > the manual PySpark approach. The workflow uses `08_dbt_run` which replaces them end-to-end.

3. Run tests:
   ```
   tests/test_transformations    # 10 unit tests, all should pass
   ```

---

## Key Features

### Medallion Architecture
- **Bronze**: Raw ingestion with explicit schemas, `badRecordsPath`, audit metadata
- **Silver**: Type-safe, deduplicated, validated with quarantine for bad rows
- **Gold**: Pre-aggregated business tables (daily sales, RFM, product performance)
- **Warehouse**: Star schema with SCD Type 2 `dim_customer` — historical customer attributes preserved at transaction time

### dbt Integration
- **dbt snapshot** manages `dim_customer` as SCD Type 2 using `check` strategy — tracks changes to `segment`, `city`, and `customer_tier`
- **Gold/Warehouse/Analytics** models replace the manual PySpark notebooks (03/04/05), bringing schema tests, `ref()` lineage, and `dbt docs`
- `fact_sales` joins to the snapshot using a date-range join so each transaction resolves the customer tier that was active when the sale occurred
- `dbt test` runs schema tests (not_null, unique, relationships) and data quality checks on every run
- `dbt-databricks` adapter uses `OPTIMIZE` + `ZORDER` post-hooks on large tables

### SCD Type 2 — dim_customer
- Customer attributes (`segment`, `city`, `customer_tier`) are tracked over time via dbt snapshot
- Each version row carries `dbt_valid_from` / `dbt_valid_to` (NULL = current active row)
- `fact_sales` resolves historically correct `customer_key` with a date-range join:  
  `transaction_date >= dbt_valid_from AND transaction_date < COALESCE(dbt_valid_to, '9999-12-31')`
- Late-arriving transactions still resolve to the correct historical snapshot row, as long as that version row has not been vacuumed

### Delta Lake Optimizations
- `OPTIMIZE + ZORDER BY` on all large tables (customer_id, product_id, date)
- Partition pruning by year/month on fact and transaction tables
- `MERGE INTO` for idempotent incremental loads — safe to re-run
- Change Data Feed enabled for incremental consumption

### Reliability
- **Retry with exponential backoff** (`utils/retry.py`): Delta writes retry up to 3 times with jitter to avoid thundering-herd on busy clusters; wired into Bronze ingestion
- **Rate limiting** (`utils/rate_limiter.py`): Token-bucket `RateLimiter` for external API calls during ingestion; `paginated_api_fetch()` helper wraps any paginated REST API
- Both utilities are independently tested and can be applied to any pipeline stage

### Notifications
- **MS Teams** (`utils/notifier.py`): MessageCard format via Incoming Webhook — color-coded by severity, shows stage, run ID, row counts, and duration; no Power Automate required
- **Slack**: Color-coded webhook messages (kept for backwards compatibility)
- **Email**: HTML-formatted SMTP alerts (works with Gmail, SendGrid, Office 365)
- Credentials loaded from Databricks Secret Scope — nothing hardcoded
- Delivery failures are caught and logged; they never crash the pipeline
- Monitoring notebook dispatches ERROR alerts on pipeline failures, WARN on high rejection rate or stale data, INFO on clean runs

### Data Quality
- Schema enforcement at Bronze (explicit StructType, no inference)
- Quarantine pattern at Silver (bad rows saved to `_quarantine/`, pipeline continues)
- `DataValidator` utility produces pass/fail scorecards with configurable thresholds
- `dbt test` enforces not_null, unique, and referential integrity on every model build
- 10 unit tests covering: deduplication, quarantine logic, RFM scoring, retry behaviour, rate limiter throttling

### Monitoring
- Every pipeline stage writes structured rows to `pipeline_logs` (Delta)
- `06_monitoring` queries: latest run status, error history, throughput trend, data freshness, Delta file health
- Three alert levels: ERROR (pipeline failure), WARN (data quality / staleness), INFO (healthy run)

### Analytics Layer
- 7 SQL views ready for Power BI connection
- KPIs: executive summary, MoM growth, customer tiers, product rankings, regional store performance, new vs. returning customers, historised revenue by tier
- Star schema: `fact_sales` + `dim_date`, `dim_customer` (SCD2), `dim_product`, `dim_store`

---

## Data Model

**Fact table:** `fact_sales` — 50,000 rows, grain: one transaction  
**Dimension tables:** `dim_date`, `dim_customer` (SCD2, 5,000 customers), `dim_product` (25), `dim_store` (20)

```mermaid
erDiagram
    fact_sales {
        string  transaction_id PK
        int     date_key       FK
        bigint  customer_key   FK
        bigint  product_key    FK
        bigint  store_key      FK
        int     quantity
        double  unit_price
        double  discount
        double  total_amount
        double  net_revenue
        string  status
        int     is_returned
    }

    dim_date {
        int    date_key    PK
        date   full_date
        int    year
        int    quarter
        int    month
        string month_name
        int    week_of_year
        int    day_of_week
        string day_name
        int    is_weekend
    }

    dim_customer {
        string customer_key    PK
        string customer_id
        string customer_name
        string email
        string city
        string segment
        string customer_tier
        double lifetime_value
        int    purchase_frequency
        ts     dbt_valid_from
        ts     dbt_valid_to
        int    dbt_is_deleted
    }

    dim_product {
        bigint product_key   PK
        string product_id
        string product_name
        string category
        string subcategory
        double unit_price
        double return_rate
        int    category_rank
    }

    dim_store {
        bigint store_key  PK
        string store_id
        string store_name
        string city
        string region
        string country
    }

    fact_sales }o--|| dim_date     : "date_key"
    fact_sales }o--|| dim_customer : "customer_key (SCD2 date-range join)"
    fact_sales }o--|| dim_product  : "product_key"
    fact_sales }o--|| dim_store    : "store_key"
```

**Source tables (synthetic):**
- `sales_transactions.csv` — 50,000 rows, 2023–2024
- `customers.csv` — 5,000 rows
- `products.csv` — 25 products across 5 categories
- `stores.csv` — 20 stores across 4 regions

---

## Optimization Results

| Table                  | Optimization Applied                      | Impact                           |
|------------------------|-------------------------------------------|----------------------------------|
| `silver_transactions`  | ZORDER BY (customer_id, product_id, date) | ~40% faster filtered reads       |
| `gold_sales_daily`     | ZORDER BY (store_id, category, date)      | Partition skipping on BI queries |
| `fact_sales`           | Partition by year/month + ZORDER          | Date-range queries skip partitions |
| All Silver/Gold tables | Auto-optimize + auto-compact              | Small file problem prevented     |
| Spark config           | AQE enabled, shuffle.partitions=8         | No empty shuffle files           |

---

## File Structure

```
retail-sales-platform/
├── README.md
├── architecture/
│   └── architecture.md
├── notebooks/
│   ├── 00_setup.py
│   ├── 01_bronze_ingestion.py        # retries on Delta write, success/failure notification
│   ├── 02_silver_transformation.py   # MERGE INTO, quarantine, validation scorecard
│   ├── 03_gold_aggregation.py        # reference: manual PySpark RFM + aggregations
│   ├── 04_warehouse_star_schema.py   # reference: manual star schema build
│   ├── 05_analytics_queries.py       # reference: manual SQL views
│   ├── 06_monitoring.py              # health checks, MS Teams/Slack/email alerts
│   ├── 07_scd2_dim_customer.py       # reference: manual SCD2 implementation
│   └── 08_dbt_run.py                 # dbt: snapshot → Gold → Warehouse → Analytics → test
├── dbt/
│   └── retail_sales/
│       ├── dbt_project.yml
│       ├── packages.yml              # dbt_utils
│       ├── profiles.yml.example      # template (real file excluded by .gitignore)
│       ├── snapshots/
│       │   └── dim_customer.sql      # SCD2 via dbt snapshot (check strategy)
│       └── models/
│           ├── sources.yml           # Silver table sources + freshness checks
│           ├── gold/
│           │   ├── gold_customer_summary.sql   # RFM NTILE scoring
│           │   ├── gold_sales_daily.sql
│           │   ├── gold_product_performance.sql
│           │   └── gold_store_performance.sql
│           ├── warehouse/
│           │   ├── fact_sales.sql    # SCD2-aware date-range join to dim_customer
│           │   ├── dim_date.sql
│           │   ├── dim_product.sql
│           │   └── dim_store.sql
│           └── analytics/
│               └── (7 KPI view models)
├── jobs/
│   ├── pipeline_workflow.json        # Databricks Workflows DAG definition
│   └── README.md                     # import guide + Community Edition setup
├── utils/
│   ├── data_generator.py             # 50k synthetic transactions
│   ├── logger.py                     # Delta-backed structured logger
│   ├── notifier.py                   # MS Teams + Slack webhook + SMTP email
│   ├── rate_limiter.py               # token-bucket rate limiter for API ingestion
│   ├── retry.py                      # exponential backoff decorator
│   └── validation.py                 # reusable data quality checks
├── sql/
│   └── create_views.sql              # Synapse-compatible SQL views
└── tests/
    └── test_transformations.py       # 10 unit tests (transforms + retry + rate limiter)
```
