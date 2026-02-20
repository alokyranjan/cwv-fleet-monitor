# CrUX BigQuery + Grafana: Technical Architecture Document

## Table of Contents

1. [Overview](#1-overview)
2. [Why Extract? Direct Query vs. Private Dataset](#2-why-extract-direct-query-vs-private-dataset)
3. [System Architecture](#3-system-architecture)
4. [Project Structure](#4-project-structure)
5. [GCP & BigQuery Setup](#5-gcp--bigquery-setup)
6. [Data Model](#6-data-model)
7. [Python Scripts — ETL Pipeline](#7-python-scripts--etl-pipeline)
8. [SQL Layer](#8-sql-layer)
9. [Docker & Grafana Infrastructure](#9-docker--grafana-infrastructure)
10. [Grafana Dashboards](#10-grafana-dashboards)
11. [Color Coding Convention](#11-color-coding-convention)
12. [CrUX Data Nuances](#12-crux-data-nuances)
13. [BigQuery Cost Analysis](#13-bigquery-cost-analysis)
14. [Operations Guide](#14-operations-guide)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. Overview

### Purpose

DealerOn is an automotive website provider with 7,000+ dealership sites (custom domains) across the US. This project provides fleet-wide visibility into Core Web Vitals (CWV) performance by:

1. Extracting CrUX (Chrome User Experience Report) data from Google's public BigQuery dataset
2. Filtering it to only DealerOn-managed origins (dealer websites)
3. Denormalizing dealer metadata (OEM brand, region, dealer group, etc.) into each row
4. Visualizing everything in Grafana dashboards running locally via Docker

### Scope (Phase 1)

- **Data granularity:** Monthly, origin-level (not per-page)
- **Data source:** `chrome-ux-report.materialized.device_summary` (Google's public CrUX dataset)
- **Visualization:** Local Docker Grafana instance with 3 provisioned dashboards
- **Automation:** Python scripts for setup, loading, extraction, backfill, and validation

### Core Web Vitals Thresholds

These are Google's official thresholds used throughout the project:

| Metric | Good | Needs Improvement | Poor | Unit |
|--------|------|-------------------|------|------|
| LCP (Largest Contentful Paint) | <= 2500 | <= 4000 | > 4000 | ms |
| INP (Interaction to Next Paint) | <= 200 | <= 500 | > 500 | ms |
| CLS (Cumulative Layout Shift) | <= 0.10 | <= 0.25 | > 0.25 | unitless |
| FCP (First Contentful Paint) | <= 1800 | <= 3000 | > 3000 | ms |
| TTFB (Time to First Byte) | <= 800 | <= 1800 | > 1800 | ms |

**CWV Pass/Fail:** Google defines a CWV "pass" as ALL 3 core metrics being good: LCP <= 2500 AND INP <= 200 AND CLS <= 0.10. Failing ANY single metric = fail.

---

## 2. Why Extract? Direct Query vs. Private Dataset

A core architectural decision in this project is **extracting** CrUX data into our own `cwv_monthly` table rather than querying Google's public CrUX table directly from Grafana. This section explains why.

### The Problem with Direct Queries

Google's public `chrome-ux-report.materialized.device_summary` table is massive. BigQuery scans the entire table partition for the target month before applying any filters — even our `INNER JOIN` against ~7,000 origins. Each partition scan is **~329 GB**.

If Grafana queried this table directly, **every single dashboard page load** would scan ~329 GB. With the 1 TB free tier, you'd exhaust it in **3 page loads**.

### Cost Comparison

| Scenario | Data scanned per page load | 50 loads/day × 30 days | Monthly cost |
|----------|---------------------------|------------------------|--------------|
| **Direct query** (public CrUX table) | ~329 GB | ~494 TB | ~$3,084 |
| **Extract first** (private `cwv_monthly` table) | ~15 MB | ~22 GB | $0 |

With extraction, we scan 329 GB **once per month** (well within the free tier), and all subsequent Grafana queries hit our small, optimized private table at ~15 MB per load — effectively free.

### Beyond Cost

| Benefit | Direct Query | Extraction Pattern |
|---------|-------------|-------------------|
| **Query speed** | Slow (329 GB scan per query) | Fast (15 MB scan, partitioned + clustered) |
| **Schema stability** | Vulnerable to upstream CrUX schema changes | Immune once extracted |
| **Denormalized metadata** | Every query needs a JOIN to `origins` | Metadata baked in at extraction time |
| **Historical accuracy** | Current metadata applied retroactively | Metadata preserved as-of extraction date |
| **Access control** | Dependent on public dataset availability | Own dataset, own IAM |
| **Free tier budget** | Consumed in 3 page loads | ~350 GB/month leaves ~650 GB for other work |

### Bottom Line

The extraction pattern converts an **O(n) cost model** (scales with dashboard usage) into an **O(1) cost model** (fixed monthly extraction regardless of viewers). The one-time backfill cost of ~$43 pays for itself after the equivalent of just 21 direct page loads.

---

## 3. System Architecture

```
+------------------+       +----------------------------+       +------------------+
|  Google CrUX     |       |  GCP BigQuery              |       |  Docker Grafana  |
|  Public Dataset  | ----> |  dealeron-crux project     | ----> |  localhost:3000  |
|  (monthly data)  |       |  dealeron_crux dataset     |       |  3 dashboards    |
+------------------+       +----------------------------+       +------------------+
        ^                          ^        |
        |                          |        |
   Python scripts            Python scripts |
   read from CrUX            write to BQ    |
                                            v
                               +-------------------+
                               |  Grafana BigQuery  |
                               |  Datasource Plugin |
                               |  (JWT auth via SA) |
                               +-------------------+
```

### Data Flow

1. **Origins CSV** -> `load_origins.py` -> BigQuery `origins` table
2. **CrUX public data** + **origins table** -> `extract_monthly.py` -> BigQuery `cwv_monthly` table
3. **cwv_monthly table** -> Grafana BigQuery datasource -> Dashboard panels (SQL queries)

### Authentication

- **Python scripts:** Use a GCP service account JSON key at `secrets/sa-key.json`
- **Grafana:** Uses the same service account via JWT authentication. The private key is extracted to `secrets/private-key.pem` and mounted into the container at `/etc/grafana/secrets/private-key.pem`

---

## 4. Project Structure

```
crux_bigquery/
├── config/
│   ├── settings.yaml              # Actual config (gitignored)
│   └── settings.example.yaml      # Template (committed)
├── scripts/
│   ├── common.py                  # Shared utilities
│   ├── setup_dataset.py           # Creates BQ dataset + tables (idempotent)
│   ├── load_origins.py            # Loads dealer origins CSV into BQ
│   ├── extract_monthly.py         # Extracts latest month from CrUX
│   ├── backfill.py                # Backfills historical months
│   ├── validate.py                # Data sanity checks
│   └── dry_run.py                 # BigQuery cost estimation
├── sql/
│   ├── create_origins.sql         # DDL reference
│   ├── create_cwv_monthly.sql     # DDL reference
│   └── extract_crux_monthly.sql   # Parameterized extraction query
├── data/
│   ├── dealeron_origins.csv       # Full dealer list (gitignored)
│   ├── origins_sample.csv         # Sample CSV (committed)
│   └── .gitkeep
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── bigquery.yaml      # BigQuery datasource config
│   │   └── dashboards/
│   │       └── dashboards.yaml    # Dashboard file provider config
│   └── dashboards/
│       ├── fleet_overview.json    # Dashboard 1: Fleet health
│       ├── worst_offenders.json   # Dashboard 2: Worst sites
│       └── site_drilldown.json    # Dashboard 3: Single-site deep dive
├── secrets/                       # gitignored
│   ├── sa-key.json                # GCP service account key
│   └── private-key.pem            # Extracted private key for Grafana
├── docker-compose.yaml
├── requirements.txt
├── .env.example
├── .env                           # gitignored
├── .gitignore
└── ARCHITECTURE.md                # This file
```

### What's Gitignored

- `secrets/` — service account key & private key
- `.env` — Grafana admin password, GCP project, SA email
- `config/settings.yaml` — project-specific BigQuery settings
- `data/*.csv` (except `origins_sample.csv`) — full dealer CSV can be large
- `grafana-data/` — Grafana's persistent volume data
- `__pycache__/` — Python bytecode

---

## 5. GCP & BigQuery Setup

### GCP Project

- **Project ID:** `dealeron-crux`
- **Service Account:** `your-sa@your-project.iam.gserviceaccount.com`
- **Location:** US
- **Required Roles:**
  - `roles/bigquery.dataEditor` on `dealeron_crux` dataset (for scripts to create tables and insert data)
  - `roles/bigquery.jobUser` on the project (to run queries)
  - Read access to `chrome-ux-report` public dataset (available to all authenticated GCP users)

### Configuration File: `config/settings.yaml`

```yaml
gcp:
  project_id: "dealeron-crux"
  service_account_key: "secrets/sa-key.json"
  location: "US"
bigquery:
  dataset_name: "dealeron_crux"
  origins_table: "origins"
  cwv_monthly_table: "cwv_monthly"
crux:
  source_table: "chrome-ux-report.materialized.device_summary"
  backfill_start: 202301
origins:
  csv_path: "data/dealeron_origins.csv"
```

### Environment Variables: `.env`

```
GF_SECURITY_ADMIN_PASSWORD=<your-password>
GCP_PROJECT_ID=dealeron-crux
SA_CLIENT_EMAIL=your-sa@your-project.iam.gserviceaccount.com
```

---

## 6. Data Model

### Table: `dealeron_crux.origins`

The dealer site registry. One row per dealer website origin.

| Column | Type | Mode | Description |
|--------|------|------|-------------|
| `origin` | STRING | REQUIRED | Full origin URL, e.g. `https://www.smithchevrolet.com` |
| `dealer_name` | STRING | NULLABLE | Display name |
| `dealer_group` | STRING | NULLABLE | Parent group (e.g., Hendrick Automotive) |
| `oem_brand` | STRING | NULLABLE | Comma-separated brands (e.g., "Chevrolet,GMC") |
| `region` | STRING | NULLABLE | Geographic region |
| `state` | STRING | NULLABLE | US state abbreviation |
| `platform_version` | STRING | NULLABLE | DealerOn platform version |
| `is_active` | BOOLEAN | NULLABLE | Active dealer flag |
| `tags` | STRING | NULLABLE | Comma-separated custom tags |
| `added_at` | TIMESTAMP | NULLABLE | Row creation time |
| `updated_at` | TIMESTAMP | NULLABLE | Last update time |

**Multi-brand handling:** The `oem_brand` field supports comma-separated values (e.g., "Chevrolet,GMC"). Dashboard queries use `UNNEST(SPLIT(oem_brand, ','))` to count multi-brand dealers under each brand individually.

### Table: `dealeron_crux.cwv_monthly`

Extracted CrUX data with denormalized dealer metadata. One row per origin + device + month combination.

**Why denormalized?** Grafana queries become single-table (no JOINs = faster, cheaper BigQuery costs), and historical accuracy is preserved — if a dealer changes OEM brands or groups later, the historical rows retain the metadata at the time of extraction.

**Partitioning & Clustering:**
- **Partitioned by:** `RANGE_BUCKET(yyyymm, GENERATE_ARRAY(202001, 203001, 1))` — integer range partitioning on month
- **Clustered by:** `origin, oem_brand, region` — optimizes queries that filter by these common dimensions

#### Column Groups

**Dimensions:**

| Column | Type | Description |
|--------|------|-------------|
| `yyyymm` | INT64 (REQUIRED) | Month as integer, e.g. `202601` |
| `date` | DATE | First day of the month |
| `origin` | STRING (REQUIRED) | Origin URL |
| `device` | STRING | `phone`, `desktop`, `tablet` |
| `rank` | INT64 | CrUX popularity rank bucket |

**Dealer Metadata (denormalized from origins):**

| Column | Type |
|--------|------|
| `dealer_name` | STRING |
| `dealer_group` | STRING |
| `oem_brand` | STRING |
| `region` | STRING |
| `state` | STRING |
| `platform_version` | STRING |

**P75 Values** — the 75th percentile user experience:

| Column | Type | Unit |
|--------|------|------|
| `p75_lcp` | FLOAT64 | milliseconds |
| `p75_fcp` | FLOAT64 | milliseconds |
| `p75_inp` | FLOAT64 | milliseconds |
| `p75_cls` | FLOAT64 | unitless (NOT multiplied by 100) |
| `p75_ttfb` | FLOAT64 | milliseconds |

**Distribution Values** — fraction of page loads in each bucket:

| Metric | Good | Needs Improvement | Poor |
|--------|------|-------------------|------|
| LCP | `fast_lcp` | `avg_lcp` | `slow_lcp` |
| FCP | `fast_fcp` | `avg_fcp` | `slow_fcp` |
| INP | `fast_inp` | `avg_inp` | `slow_inp` |
| CLS | `small_cls` | `medium_cls` | `large_cls` |
| TTFB | `fast_ttfb` | `avg_ttfb` | `slow_ttfb` |

> **IMPORTANT:** These distribution values are density-weighted — they are pre-multiplied by the device's traffic share. See [Section 12: CrUX Data Nuances](#12-crux-data-nuances) for details.

**Device Density:**

| Column | Type | Description |
|--------|------|-------------|
| `desktopDensity` | FLOAT64 | Fraction of traffic from desktop |
| `phoneDensity` | FLOAT64 | Fraction of traffic from phone |
| `tabletDensity` | FLOAT64 | Fraction of traffic from tablet |

**Navigation Types:**

| Column | Type | Description |
|--------|------|-------------|
| `nav_navigate` | FLOAT64 | Standard navigation |
| `nav_navigate_cache` | FLOAT64 | Navigation with cache |
| `nav_reload` | FLOAT64 | Page reload |
| `nav_restore` | FLOAT64 | Session restore |
| `nav_back_forward` | FLOAT64 | Back/forward navigation |
| `nav_back_forward_cache` | FLOAT64 | Back/forward with bfcache |
| `nav_prerender` | FLOAT64 | Prerendered navigation |

> **Note:** Navigation type data is sparse — only ~4% of origins have non-NULL values.

**Round-trip Time:**

| Column | Type |
|--------|------|
| `low_rtt` | FLOAT64 |
| `medium_rtt` | FLOAT64 |
| `high_rtt` | FLOAT64 |

**Meta:**

| Column | Type |
|--------|------|
| `extracted_at` | TIMESTAMP |

---

## 7. Python Scripts — ETL Pipeline

All scripts are in the `scripts/` directory and share common utilities from `common.py`.

### `scripts/common.py` — Shared Utilities

Provides 6 functions used by all scripts:

| Function | Purpose |
|----------|---------|
| `load_config(path)` | Loads `config/settings.yaml` (YAML -> dict). Exits with error if file not found. |
| `get_credentials(config)` | Builds `google.oauth2.service_account.Credentials` from SA key file. |
| `get_client(config)` | Creates an authenticated `bigquery.Client` with project/location. |
| `get_table_id(config, table_key)` | Returns fully-qualified table ID: `project.dataset.table`. |
| `read_sql(filename)` | Reads a `.sql` file from the `sql/` directory. |
| `format_sql(template, config, **kwargs)` | Substitutes `{project}`, `{dataset}`, and custom params into SQL. |

`PROJECT_ROOT` is computed as one level up from `scripts/`, so all path resolution is relative to the repo root.

### `scripts/setup_dataset.py` — Create BigQuery Resources

**Usage:** `python scripts/setup_dataset.py [--config path/to/settings.yaml]`

Creates 3 resources (all idempotent via `exists_ok=True`):

1. **Dataset** `dealeron_crux` in the US region
2. **Table** `origins` with the schema described in Section 6
3. **Table** `cwv_monthly` with range partitioning and clustering

### `scripts/load_origins.py` — Load Dealer Origins

**Usage:** `python scripts/load_origins.py [--csv path] [--append] [--config path]`

1. Reads the dealer CSV file (default: `data/dealeron_origins.csv`)
2. **Validates:**
   - `origin` column exists
   - All origins start with `https://`
   - Trailing slashes are removed
   - Duplicates are detected and removed (with warnings)
3. Adds `added_at` and `updated_at` timestamps
4. Loads into BigQuery:
   - **Default mode:** `WRITE_TRUNCATE` (full reload — replaces all data)
   - **`--append` mode:** `WRITE_APPEND` (adds new rows without deleting existing)
5. Reports total rows loaded and current table count

**CSV Format** (from `origins_sample.csv`):
```
origin,dealer_name,dealer_group,oem_brand,region,state,platform_version,is_active,tags
https://www.smithchevrolet.com,Smith Chevrolet,Smith Auto Group,Chevrolet,Southeast,GA,3.2,true,flagship
```

### `scripts/extract_monthly.py` — Extract Single Month

**Usage:** `python scripts/extract_monthly.py [--month YYYYMM] [--force] [--config path]`

1. **Month detection:** If `--month` is omitted, auto-detects the latest available month in the CrUX public dataset via `SELECT MAX(yyyymm) FROM chrome-ux-report.materialized.device_summary`
2. **Duplicate check:** Queries `cwv_monthly` for existing data for that month
   - If data exists and no `--force`: exits gracefully
   - If `--force`: deletes existing data first (`DELETE FROM cwv_monthly WHERE yyyymm = X`)
3. **Extraction:** Runs `sql/extract_crux_monthly.sql` — an `INSERT INTO ... SELECT` that joins CrUX public data with the `origins` table
4. **Summary:** Prints device breakdown (rows per device, avg P75 values)

### `scripts/backfill.py` — Backfill Historical Months

**Usage:** `python scripts/backfill.py [--start YYYYMM] [--end YYYYMM] [--force] [--config path]`

1. **Range:** From `--start` (default: `backfill_start` in config, i.e. 202301) to `--end` (default: latest CrUX month)
2. **Month generation:** `generate_months()` creates a list of YYYYMM integers, handling year rollover
3. **Per-month processing:** Calls the same `check_existing_data` / `delete_month_data` / `extract_month` functions from `extract_monthly.py`
4. **Progress reporting:** Skipped months, errors, and total rows added

### `scripts/validate.py` — Data Validation

**Usage:** `python scripts/validate.py [--config path]`

Runs 5 checks:

| Check | What it validates |
|-------|-------------------|
| **Origins table** | Has rows, reports total and active count |
| **CWV monthly coverage** | Lists all months with row counts and distinct origin counts |
| **NULL values** | No NULLs in `origin` or `yyyymm`. NULL `device` is expected (CrUX rank aggregates) |
| **Distribution sums** | Validates that `(fast + avg + slow) / deviceDensity ≈ 1.0` for each metric |
| **Origin coverage** | Reports how many active origins have CrUX data vs total active origins |

Exit code: 0 if all pass, 1 if any fail.

### `scripts/dry_run.py` — BigQuery Cost Estimation

**Usage:** `python scripts/dry_run.py [--months N] [--config path]`

Submits all extraction and dashboard queries to BigQuery in **dry-run mode** — BigQuery parses and validates the query, reports the bytes it would scan, but **never actually executes it**. No data is read, no cost is incurred.

Uses `QueryJobConfig(dry_run=True, use_query_cache=False)` to get the `total_bytes_processed` estimate.

**Output includes:**
1. **Extraction cost** — bytes scanned per month when reading from the public CrUX table
2. **Dashboard query costs** — every Grafana panel query across all 3 dashboards, grouped by dashboard with subtotals
3. **Cost summary** — single month, N-month backfill, ongoing monthly, and dashboard usage estimates at $6.25/TB

**Flags:**
- `--months N` — number of months for backfill estimate (default: 24)

### Python Dependencies (`requirements.txt`)

```
google-cloud-bigquery>=3.25.0
google-auth>=2.29.0
pyarrow>=15.0.0
pyyaml>=6.0.1
pandas>=2.2.0
```

---

## 8. SQL Layer

### `sql/extract_crux_monthly.sql` — The Core Extraction Query

This is the most critical SQL in the project. It's a parameterized `INSERT INTO ... SELECT` with 3 substitution variables:

- `{project}` — GCP project ID
- `{dataset}` — BigQuery dataset name
- `{target_yyyymm}` — Target month as integer

```sql
INSERT INTO `{project}.{dataset}.cwv_monthly`
SELECT
    crux.yyyymm, crux.date, crux.origin, crux.device, crux.rank,
    -- Denormalized from origins table at extraction time
    o.dealer_name, o.dealer_group, o.oem_brand, o.region, o.state, o.platform_version,
    -- P75 values
    crux.p75_lcp, crux.p75_fcp, crux.p75_inp, crux.p75_cls, crux.p75_ttfb,
    -- Distributions (density-weighted from CrUX)
    crux.fast_lcp, crux.avg_lcp, crux.slow_lcp,
    crux.fast_fcp, crux.avg_fcp, crux.slow_fcp,
    crux.fast_inp, crux.avg_inp, crux.slow_inp,
    crux.small_cls, crux.medium_cls, crux.large_cls,
    crux.fast_ttfb, crux.avg_ttfb, crux.slow_ttfb,
    -- Device density
    crux.desktopDensity, crux.phoneDensity, crux.tabletDensity,
    -- Navigation types
    crux.navigation_types_navigate, crux.navigation_types_navigate_cache,
    crux.navigation_types_reload, crux.navigation_types_restore,
    crux.navigation_types_back_forward, crux.navigation_types_back_forward_cache,
    crux.navigation_types_prerender,
    -- RTT
    crux.low_rtt, crux.medium_rtt, crux.high_rtt,
    -- Meta
    CURRENT_TIMESTAMP()
FROM `chrome-ux-report.materialized.device_summary` crux
INNER JOIN `{project}.{dataset}.origins` o ON crux.origin = o.origin
WHERE crux.yyyymm = {target_yyyymm} AND o.is_active = TRUE
```

**Key design decisions:**
- `INNER JOIN` ensures only DealerOn origins are extracted (not all 10M+ origins in CrUX)
- `o.is_active = TRUE` filters out deactivated dealers
- Dealer metadata is snapshotted at extraction time — if a dealer changes groups, historical rows retain the old metadata

### `sql/create_origins.sql` and `sql/create_cwv_monthly.sql`

These are DDL reference files documenting the table schemas. Actual table creation is done by `setup_dataset.py` using the BigQuery Python client (not SQL DDL), but these files serve as documentation.

---

## 9. Docker & Grafana Infrastructure

### `docker-compose.yaml`

```yaml
services:
  grafana:
    image: grafana/grafana:12.3.2
    container_name: dealeron-grafana
    dns:
      - 8.8.8.8
      - 8.8.4.4
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GF_SECURITY_ADMIN_PASSWORD:-admin}
      - GF_INSTALL_PLUGINS=grafana-bigquery-datasource
      - GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/fleet_overview.json
      - GCP_PROJECT_ID=${GCP_PROJECT_ID}
      - SA_CLIENT_EMAIL=${SA_CLIENT_EMAIL}
    volumes:
      - ./grafana/provisioning/datasources:/etc/grafana/provisioning/datasources:ro
      - ./grafana/provisioning/dashboards:/etc/grafana/provisioning/dashboards:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
      - ./secrets:/etc/grafana/secrets:ro
      - grafana-data:/var/lib/grafana
    restart: unless-stopped

volumes:
  grafana-data:
```

**Key configuration:**
- **DNS:** Explicitly set to Google DNS (8.8.8.8) to ensure the container can resolve BigQuery API endpoints
- **Plugin:** `grafana-bigquery-datasource` is installed at container startup
- **Home dashboard:** Fleet Overview is set as the default landing page
- **Volumes:** All config and dashboard files are mounted read-only (`:ro`). `grafana-data` is a persistent named volume for Grafana's internal SQLite database

### Grafana Datasource: `grafana/provisioning/datasources/bigquery.yaml`

```yaml
apiVersion: 1
datasources:
  - name: BigQuery
    type: grafana-bigquery-datasource
    uid: dealeron-bigquery
    access: proxy
    editable: true
    jsonData:
      authenticationType: jwt
      clientEmail: $SA_CLIENT_EMAIL
      defaultProject: $GCP_PROJECT_ID
      tokenUri: https://oauth2.googleapis.com/token
    secureJsonData:
      privateKey: $__file{/etc/grafana/secrets/private-key.pem}
```

- **UID:** `dealeron-bigquery` — this is the stable identifier referenced by ALL dashboard panel queries
- **Auth:** JWT using the service account's private key (extracted from the SA JSON into a separate PEM file)
- **`$SA_CLIENT_EMAIL` and `$GCP_PROJECT_ID`:** Resolved from environment variables passed to the container via `.env`

### Dashboard Provisioning: `grafana/provisioning/dashboards/dashboards.yaml`

```yaml
apiVersion: 1
providers:
  - name: CrUX Web Vitals
    orgId: 1
    folder: CrUX Web Vitals
    type: file
    disableDeletion: false
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards
      foldersFromFilesStructure: false
```

- Dashboards are loaded from `/var/lib/grafana/dashboards` (mounted from `./grafana/dashboards/`)
- `allowUiUpdates: true` allows editing dashboards in the UI during development
- Dashboards appear in a folder called "CrUX Web Vitals"

### Dashboard Update Workflow

**Important:** Grafana's file-based provisioning loads dashboards into its internal database on startup. Changes to JSON files on disk are NOT reliably picked up during runtime. To update dashboards after editing JSON files:

**Method 1: Restart Grafana**
```bash
docker compose restart grafana
```

**Method 2: Push via Grafana API (preferred — no downtime)**
```bash
# Build payload and push
python -c "
import json
with open('grafana/dashboards/fleet_overview.json', 'r') as f:
    dashboard = json.load(f)
dashboard['id'] = None
dashboard['version'] = 1
payload = {'dashboard': dashboard, 'overwrite': True, 'folderUid': ''}
with open('_push_payload.json', 'w') as f:
    json.dump(payload, f)
"
curl -s -X POST "http://localhost:3000/api/dashboards/db" \
  -H "Content-Type: application/json" \
  -u "admin:<your-password>" \
  -d @_push_payload.json
rm -f _push_payload.json
```

---

## 10. Grafana Dashboards

All 3 dashboards share these common settings:
- **Time range:** `now-2y` to `now`
- **Datasource UID:** `dealeron-bigquery`
- **Schema version:** 40
- **Template variable:** `device` (multi-select: phone, desktop, tablet — all selected by default)

### 10.1 Fleet Overview (`fleet_overview.json`)

**UID:** `fleet-overview`
**Purpose:** Bird's-eye view of fleet-wide CWV health

#### Layout (top to bottom):

**Row 1: CWV Pass Rates** (y:0)

| Panel | Type | Size | Query Logic |
|-------|------|------|-------------|
| LCP Pass Rate | stat | 5w | `COUNTIF(p75_lcp <= 2500) / COUNT(*)` for latest month |
| INP Pass Rate | stat | 5w | `COUNTIF(p75_inp <= 200) / COUNT(*)` |
| CLS Pass Rate | stat | 5w | `COUNTIF(p75_cls <= 0.10) / COUNT(*)` |
| FCP Pass Rate | stat | 5w | `COUNTIF(p75_fcp <= 1800) / COUNT(*)` |
| TTFB Pass Rate | stat | 4w | `COUNTIF(p75_ttfb <= 800) / COUNT(*)` |

Each uses threshold coloring: red (<50%), yellow (50-75%), green (>75%).

**Row 2: CWV Summary** (y:5)

| Panel | Type | Size | Query Logic |
|-------|------|------|-------------|
| CWV Pass / Fail | donut piechart | 8w | Groups by origin, AVGs p75 across devices, counts Pass (all 3 good) vs Fail (any bad). Green/red slices. |
| Pass Rate by Device | table | 16w | Per-device: site count, LCP/INP/CLS/CWV pass %, avg P75 values. Threshold-colored cells. |

**Row 3: Trends** (y:14)

| Panel | Type | Size | Query Logic |
|-------|------|------|-------------|
| CWV Pass Rate Trends | timeseries | 24w (full) | Monthly `COUNTIF` for LCP/INP/CLS good % over time. Uses metric colors (blue/purple/pink). |

**Row 4: Fleet P75 Trends** (y:23)

5 individual timeseries charts, each with its own y-axis and dashed threshold lines:

| Panel | Color | Thresholds |
|-------|-------|------------|
| Fleet Avg LCP | #5794F2 (blue) | 2500 / 4000 ms |
| Fleet Avg INP | #B877D9 (purple) | 200 / 500 ms |
| Fleet Avg CLS | #FF6EC7 (pink) | 0.10 / 0.25 |
| Fleet Avg FCP | #37BBCA (teal) | 1800 / 3000 ms |
| Fleet Avg TTFB | #8F8F8F (gray) | 800 / 1800 ms |

Each chart queries: `AVG(p75_xxx) GROUP BY yyyymm ORDER BY yyyymm`

**Row 5: Breakdown** (y:40)

| Panel | Type | Size | Query Logic |
|-------|------|------|-------------|
| Pass Rate by OEM Brand | horizontal barchart | 16w, 24h | Uses `UNNEST(SPLIT(oem_brand, ','))` for multi-brand dealers. `HAVING COUNT(*) >= 10` filters low-volume brands. No limit. |
| Coverage Summary | stat | 8w | `COUNT(DISTINCT origin)` with CrUX data vs total active origins |

---

### 10.2 Worst Offenders (`worst_offenders.json`)

**UID:** `worst-offenders`
**Purpose:** Identify the worst-performing sites for each metric

#### Layout:

**Row 1: CWV Distribution** (y:0)

3 gauge panels (LCP, INP, CLS) each showing good/needs_improvement/poor percentages.

**Distribution Query Pattern:**
```sql
SELECT
  ROUND(AVG(SAFE_DIVIDE(fast_lcp, fast_lcp + avg_lcp + slow_lcp)) * 100, 1) as good,
  ROUND(AVG(SAFE_DIVIDE(avg_lcp, fast_lcp + avg_lcp + slow_lcp)) * 100, 1) as needs_improvement,
  ROUND(AVG(SAFE_DIVIDE(slow_lcp, fast_lcp + avg_lcp + slow_lcp)) * 100, 1) as poor
```

The `SAFE_DIVIDE` normalization is critical — see [Section 12](#12-crux-data-nuances).

**Gauge threshold overrides (per field):**
- `good`: red(0) -> yellow(50) -> green(75) — high good% = green
- `needs_improvement`: green(0) -> yellow(15) -> red(30) — high NI% = red (bad)
- `poor`: green(0) -> yellow(10) -> red(25) — high poor% = red (bad)

**Row 2: Worst LCP Sites** (y:7)
**Row 3: Worst INP Sites** (y:20)
**Row 4: Worst CLS Sites** (y:33)

Each is a table panel showing the Top 25 worst sites for that metric, sorted by the primary P75 value descending.

**Table columns:** origin (with drilldown link), dealer_name, oem_brand, state, device, primary P75 metric, other 2 CWV P75 metrics, cwv_status (PASS/FAIL).

**Table features:**
- Threshold-colored cells (green/yellow/red) for all P75 columns
- `cwv_status` uses value mapping: PASS=green, FAIL=red
- Origin column links to Site Drilldown: `/d/site-drilldown/site-drilldown?var-origin=${__data.fields.origin}&from=now-2y&to=now`

---

### 10.3 Site Drilldown (`site_drilldown.json`)

**UID:** `site-drilldown`
**Purpose:** Deep dive into a single site's performance over time

**Additional template variable:** `origin` (textbox type — populated via URL parameter from drilldown links)

#### Layout:

**Row 1: Site Info** (y:0)

| Panel | Type | Size | Description |
|-------|------|------|-------------|
| Site Info | table | 12w | dealer_name, oem_brand, state, platform_version |
| LCP Status | stat | 4w | Latest P75 LCP with threshold color |
| INP Status | stat | 4w | Latest P75 INP with threshold color |
| CLS Status | stat | 4w | Latest P75 CLS with threshold color |

**Row 2: P75 Trends** (y:5)

5 individual timeseries charts for this specific origin:

| Panel | Color | Thresholds | Y Size |
|-------|-------|------------|--------|
| LCP Trend | #5794F2 (blue) | 2500/4000 | 8w |
| INP Trend | #B877D9 (purple) | 200/500 | 8w |
| CLS Trend | #5794F2 (blue) | 0.10/0.25 | 8w |
| FCP Trend | #37BBCA (teal) | 1800/3000 | 12w |
| TTFB Trend | #8F8F8F (gray) | 800/1800 | 12w |

All trend queries filter by `WHERE origin = '${origin}'` and group by `yyyymm`.

**Row 3: Distribution Over Time** (y:22)

3 stacked bar charts (LCP, INP, CLS) showing good/NI/poor percentages per month.

Uses `SAFE_DIVIDE` normalization with green/orange/red color overrides:
- good: `#0eb400` (green)
- needs_improvement: `#ffa400` (orange)
- poor: `#ff4e42` (red)

**Row 4: Device Breakdown** (y:31)

| Panel | Type | Size | Description |
|-------|------|------|-------------|
| P75 by Device | table | 24w | Rows: desktop, phone, tablet. Columns: P75 for all 5 metrics. Threshold-colored cells. |

Shows the **latest month only** (`WHERE yyyymm = MAX(yyyymm)`).

**Row 5: Navigation Types** (y:38)

| Panel | Type | Size | Description |
|-------|------|------|-------------|
| Navigation Breakdown | piechart | 12w | Pie chart of navigation types (Navigate, Cache, Reload, Back/Forward, BF Cache, Prerender). |

Displays `noValue: "No navigation data for this origin"` when data is NULL (~96% of origins).

---

## 11. Color Coding Convention

A strict color convention is enforced to avoid confusion:

### Threshold Colors (Green/Yellow/Red)

Used ONLY when the value directly represents good/needs-improvement/poor assessment:
- **Green:** Good / Pass
- **Yellow:** Needs Improvement
- **Red:** Poor / Fail

Applied to:
- Stat panels showing pass rates
- Table cells showing P75 values (colored against CWV thresholds)
- CWV Pass/Fail status (PASS=green, FAIL=red)
- Gauge arcs on distribution panels (with inverted thresholds for NI/poor)

### Metric Identity Colors

Used for time-series lines, bar chart series, and any context where the color identifies WHICH metric is shown (not whether it's good or bad):

| Metric | Color | Hex |
|--------|-------|-----|
| LCP | Blue | `#5794F2` |
| INP | Purple | `#B877D9` |
| CLS | Pink | `#FF6EC7` |
| FCP | Teal | `#37BBCA` |
| TTFB | Gray | `#8F8F8F` |

### Distribution Colors (Good/NI/Poor in Stacked Charts)

For stacked bar charts showing the good/NI/poor split:
| Category | Color | Hex |
|----------|-------|-----|
| Good | Green | `#0eb400` |
| Needs Improvement | Orange | `#ffa400` |
| Poor | Red | `#ff4e42` |

---

## 12. CrUX Data Nuances

### Density-Weighted Distributions

The CrUX `materialized.device_summary` table stores distribution values (e.g., `fast_lcp`, `avg_lcp`, `slow_lcp`) that are **pre-multiplied by device traffic density**. This means:

```
fast_lcp + avg_lcp + slow_lcp ≈ deviceDensity (NOT 1.0)
```

For a site where 70% of traffic is phone:
- Phone row: `fast_lcp + avg_lcp + slow_lcp ≈ 0.70`
- Desktop row: `fast_lcp + avg_lcp + slow_lcp ≈ 0.28`
- Tablet row: `fast_lcp + avg_lcp + slow_lcp ≈ 0.02`

To get true percentages (summing to 100%), you must normalize:

```sql
SAFE_DIVIDE(fast_lcp, fast_lcp + avg_lcp + slow_lcp)  -- true "good" fraction
```

This normalization is applied in all distribution-related queries (gauges, stacked bar charts).

The validation script (`validate.py`) verifies this by checking that `(fast + avg + slow) / deviceDensity ≈ 1.0`.

### Navigation Type Data Sparsity

Only ~4% of origins in the dataset have non-NULL navigation type data. This is a known characteristic of the CrUX dataset, not a data loading issue. The Site Drilldown navigation panel handles this with a `noValue` message.

### Monthly Data Timing

CrUX data is published monthly, typically around the 10th of the following month. The `yyyymm` field represents the data collection month (e.g., `202601` = January 2026 data). The `date` field is the first day of that month.

### Device-Level Rows

Each origin has up to 3 rows per month — one per device type (phone, desktop, tablet). Some origins may have fewer if there isn't enough traffic for a particular device type. The `rank` field indicates the CrUX popularity bucket.

---

## 13. BigQuery Cost Analysis

All cost estimates below are based on BigQuery on-demand pricing at **$6.25 per TB scanned** ([source](https://cloud.google.com/bigquery/pricing)). BigQuery also provides a **free tier of 1 TB of query scanning per month**. If your organization uses flat-rate/editions pricing (reserved slots), query costs are covered by the slot reservation and the per-TB charges below do not apply.

To regenerate these estimates at any time, run:
```bash
python scripts/dry_run.py          # default 24-month backfill
python scripts/dry_run.py --months 12  # custom backfill range
```

### Cost Area 1: CrUX Extraction (Python Scripts)

The extraction query (`extract_crux_monthly.sql`) reads from Google's public `chrome-ux-report.materialized.device_summary` table, which is very large. BigQuery scans the full table partition for the target month before applying the JOIN filter against our ~7,000 origins.

**Dry-run results (actual bytes reported by BigQuery):**

| Operation | Data Scanned | Cost |
|-----------|-------------|------|
| Single month extraction | 329 GB | ~$2.01 |
| **24-month backfill** | **329 GB x 24 = 7.9 TB** | **~$43** (first 1 TB free) |
| Ongoing monthly extraction | 329 GB/month | **$0** (within 1 TB free tier) |
| Re-extraction with `--force` | 329 GB (same as single month) | ~$2.01 (if free tier already used) |

> **Note:** The 329 GB scan is constant regardless of how many origins we have. It's driven by the size of Google's public CrUX table partition, not our data. Even if we had 100 origins instead of 7,000, the scan cost would be the same.

### Cost Area 2: Grafana Dashboard Queries

Every time a user opens a dashboard, each panel fires a separate SQL query against our `cwv_monthly` table. This table is small and well-optimized (partitioned by `yyyymm`, clustered by `origin, oem_brand, region`), so individual query scans are minimal.

**Dry-run results for representative dashboard queries:**

| Query | Data Scanned | Cost per query |
|-------|-------------|----------------|
| Pass Rate stat panel (latest month) | 0.23 MB | $0.000001 |
| CWV Pass Rate Trends (all months) | 0.38 MB | $0.000002 |
| Fleet Avg LCP Trend (all months) | 0.23 MB | $0.000001 |
| OEM Brand Breakdown (latest month) | 0.50 MB | $0.000003 |
| Distribution gauge (latest month) | 0.38 MB | $0.000002 |
| Worst LCP Top 25 table (latest month) | 1.19 MB | $0.000007 |
| Single-origin LCP Trend (all months) | 0.58 MB | $0.000003 |

**Estimated dashboard query costs by usage:**

| Usage Level | Daily Data Scanned | Monthly Cost |
|-------------|-------------------|--------------|
| 10 page loads/day | ~35 MB | ~$0.006 |
| 50 page loads/day | ~175 MB | ~$0.03 |
| 200 page loads/day | ~700 MB | ~$0.13 |

Dashboard query costs are negligible — even heavy usage stays well under $1/month, and would be fully covered by the 1 TB free tier.

### Cost Area 3: Storage

| Item | Estimated Size | Monthly Cost ($0.02/GB) |
|------|---------------|------------------------|
| `origins` table (~7,000 rows) | < 1 MB | ~$0.00 |
| `cwv_monthly` (24 months, ~500K rows) | ~50-100 MB | ~$0.002 |

Storage costs are effectively zero.

### Free Tier Impact

BigQuery provides **1 TB of free query scanning per month**. This significantly affects our costs:

- **Ongoing monthly:** A single extraction (329 GB) + all dashboard queries (~5-20 GB) = ~350 GB/month, well within the 1 TB free tier. **Effectively $0/month** under normal usage.
- **Backfill:** Running 24 months in a single billing month scans 7.9 TB. The first 1 TB is free, so the billable amount is 6.9 TB = **~$43** instead of ~$48.
- **Multiple extractions in one month:** If you re-extract with `--force` or run additional months, each adds 329 GB. Exceeding 1 TB in a month triggers per-TB charges on the overage.

### Total Cost Summary

| Component | One-Time (Backfill) | Ongoing Monthly |
|-----------|--------------------|-----------------|
| CrUX extraction from public dataset | ~$43 (24 months, first 1 TB free) | $0 (within free tier) |
| Grafana dashboard queries | — | $0 (within free tier) |
| BigQuery storage | — | ~$0.01 |
| **Total** | **~$43** | **~$0/month** |

> **Note:** The free tier is per billing account. If other teams or projects on the same billing account are also consuming the 1 TB free tier, the effective free allowance for this project may be less. In that case, the ongoing monthly cost reverts to ~$2.01/month for extraction + negligible dashboard costs.

### Cost Optimization Options

If costs need to be reduced:

- **BigQuery BI Engine:** Reserve a small amount of BI Engine capacity (~$0.0625/GB/hour) to cache the `cwv_monthly` table in memory. Eliminates all Grafana query scan costs.
- **Flat-rate pricing:** If the organization is already on BigQuery editions (slots), all query costs are absorbed by the reservation.
- **Grafana query caching:** Grafana Enterprise supports query caching. For open-source Grafana, setting a minimum refresh interval prevents excessive reloading.
- **Materialized views:** For expensive aggregations (OEM brand breakdown, fleet pass rates), scheduled materialized views can pre-compute results.
- **Reduce backfill range:** Instead of 24 months, backfill only 12 months (~$24 instead of ~$48).

---

## 14. Operations Guide

### Initial Setup (One-Time)

```bash
# 1. Clone the repo and install dependencies
pip install -r requirements.txt

# 2. Copy config templates
cp config/settings.example.yaml config/settings.yaml
cp .env.example .env

# 3. Edit config/settings.yaml with your GCP project details
# 4. Edit .env with Grafana password and SA email
# 5. Place service account key at secrets/sa-key.json
# 6. Extract private key for Grafana:
#    python -c "import json; f=open('secrets/sa-key.json'); d=json.load(f); open('secrets/private-key.pem','w').write(d['private_key'])"

# 7. Create BigQuery dataset and tables
python scripts/setup_dataset.py

# 8. Load dealer origins
python scripts/load_origins.py --csv data/dealeron_origins.csv

# 9. Backfill historical data (from Jan 2023 to latest)
python scripts/backfill.py

# 10. Validate data
python scripts/validate.py

# 11. Start Grafana
docker compose up -d

# 12. Open http://localhost:3000 (admin / your-password)
```

### Monthly Update Workflow

```bash
# Extract the latest available month
python scripts/extract_monthly.py

# Validate
python scripts/validate.py
```

The CrUX public dataset is updated monthly (~10th of the following month). Run `extract_monthly.py` without `--month` to auto-detect and extract the latest available data.

### Updating Origins

When new dealers are added or dealer metadata changes:

```bash
# Full reload (replaces all origins):
python scripts/load_origins.py --csv data/dealeron_origins.csv

# Append new dealers only:
python scripts/load_origins.py --csv data/new_dealers.csv --append
```

> **Note:** Updating origins only affects FUTURE extractions. Historical `cwv_monthly` rows retain the metadata that was present at extraction time. To update historical data, re-run the backfill with `--force`.

### Re-extracting Data

```bash
# Re-extract a specific month (deletes existing data first):
python scripts/extract_monthly.py --month 202601 --force

# Re-backfill everything:
python scripts/backfill.py --force
```

---

## 15. Troubleshooting

### Dashboard shows empty panels

1. **Check time range:** Must be set to "Last 2 years" or wider. Monthly data points won't show in "Last 6 hours".
2. **Check Grafana datasource:** Go to Connections > Data Sources > BigQuery > Test. If it fails, check the SA key and private PEM.
3. **Check data exists:**
   ```sql
   SELECT COUNT(*) FROM `dealeron-crux.dealeron_crux.cwv_monthly`
   ```

### Dashboard changes not reflecting after editing JSON

Grafana caches provisioned dashboards in its internal database. Use the API push method described in [Section 9](#9-docker--grafana-infrastructure) or restart the container.

### Browser caches old time range

Even after updating the dashboard JSON, the browser may cache the old time range. Use the timepicker to manually set "Last 2 years" or access the dashboard with explicit URL params:
```
http://localhost:3000/d/fleet-overview?from=now-2y&to=now
```

### Distribution percentages don't sum to 100%

If you see distributions like good=60%, NI=5%, poor=4% (total 69%), the query is missing `SAFE_DIVIDE` normalization. See [Section 12](#12-crux-data-nuances).

### Navigation pie chart is empty

Only ~4% of origins have navigation type data. Try an origin known to have data, or check:
```sql
SELECT origin FROM `dealeron-crux.dealeron_crux.cwv_monthly`
WHERE nav_navigate IS NOT NULL LIMIT 10
```

### BigQuery costs

See [Section 13: BigQuery Cost Analysis](#13-bigquery-cost-analysis) for detailed cost breakdowns. To monitor actual costs in production, query:
```sql
SELECT
  SUM(total_bytes_processed) / POW(1024, 4) as tb_scanned,
  SUM(total_bytes_processed) / POW(1024, 4) * 6.25 as estimated_cost_usd
FROM `region-us.INFORMATION_SCHEMA.JOBS`
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
```

### Grafana container can't connect to BigQuery

Check DNS resolution inside the container:
```bash
docker exec dealeron-grafana nslookup bigquery.googleapis.com
```
The `docker-compose.yaml` explicitly sets DNS to `8.8.8.8` to avoid corporate DNS issues.
