# E-Commerce Orders Pipeline — Azure Databricks
## 50 GB/day | Delta Lake | Bronze → Silver → Gold

---

## Files in this package

| File | Purpose | Schedule |
|---|---|---|
| `bronze_ingestion.py` | Ingest raw CSV from ADLS → Bronze Delta | Daily 7:00 AM |
| `silver_transform.py` | Clean, deduplicate, validate → Silver Delta | Daily (after Bronze) |
| `gold_aggregation.py` | Aggregate → 3 Gold Delta tables for Power BI | Daily (after Silver) |
| `maintenance.py` | VACUUM + table registration + health check | Weekly (Sunday) |
| `master_pipeline.py` | Runs all 3 steps with alerting (single-job mode) | Daily OR use Workflows |

---

## Dataset schema

Raw CSV: `orders.csv` (~50 GB/day, ~5M rows)

```
order_id, customer_id, product_id, category, city, state,
quantity, unit_price, amount, payment_mode, order_status,
order_date, delivery_date, ingestion_date
```

---

## Azure setup (one-time)

### 1. ADLS Gen2 containers
Create these containers in your Storage Account:
- `raw`    → source files dropped by ADF
- `bronze` → raw Delta
- `silver` → clean Delta
- `gold`   → aggregated Delta

### 2. App Registration (for ADLS mount)
```
Azure Portal → App Registrations → New Registration
  Name: databricks-adls-app
  Copy: client_id, tenant_id

→ Certificates & Secrets → New Client Secret
  Copy: client_secret value (disappears after leaving page)

→ Storage Account → IAM → Add role assignment
  Role: Storage Blob Data Contributor
  Member: databricks-adls-app
```

### 3. Mount ADLS in Databricks (run once in a notebook)
```python
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type":
      "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id":      "<client_id>",
  "fs.azure.account.oauth2.client.secret":  "<client_secret>",
  "fs.azure.account.oauth2.client.endpoint":
      "https://login.microsoftonline.com/<tenant_id>/oauth2/token"
}

for container in ["raw", "bronze", "silver", "gold"]:
    dbutils.fs.mount(
        source        = f"abfss://{container}@<storage_account>.dfs.core.windows.net/",
        mount_point   = f"/mnt/{container}",
        extra_configs = configs
    )
```

---

## Cluster recommendation (50 GB/day)

```
Type    : Job Cluster (auto-terminates after job)
Driver  : Standard_DS3_v2  (4 cores, 14 GB)
Workers : Standard_DS3_v2  × 4 (use Spot instances)
Runtime : Databricks LTS + Photon
```

---

## Databricks Workflow setup

Create a new Workflow with 3 tasks:

```
Task 1: bronze_ingestion.py
  Cluster: Job Cluster (4 × DS3_v2)
  Schedule: Daily 7:00 AM

Task 2: silver_transform.py
  Cluster: Job Cluster (4 × DS3_v2)
  Depends on: Task 1

Task 3: gold_aggregation.py
  Cluster: Job Cluster (4 × DS3_v2)
  Depends on: Task 2
```

Add a 4th task for maintenance (weekly):
```
Task 4: maintenance.py
  Schedule: Every Sunday 2:00 AM
```

---

## Gold tables for Power BI

After running `maintenance.py` once, connect Power BI:

```
Power BI → Get Data → Azure Databricks
Server  : adb-<workspace>.azuredatabricks.net
HTTP Path: /sql/1.0/warehouses/<id>

Tables:
  sales_db.gold_city_summary       ← map, city filters
  sales_db.gold_category_summary   ← category bar charts
  sales_db.gold_payment_summary    ← payment mode pie chart
```

---

## Z-ordering summary

| Layer  | ZORDER columns | Speeds up |
|---|---|---|
| Silver | city, product_id, category | Analyst queries by city/product |
| Gold city | city | Power BI city filter |
| Gold category | category | Category report filters |

Rule: Always ZORDER only today's partition:
```sql
OPTIMIZE delta.`/mnt/silver/sales/orders/`
WHERE ingestion_date = '2026-05-23'
ZORDER BY (city, product_id, category)
```

---

## Daily timeline

```
07:00 AM  → ADF drops 50 GB CSV into /mnt/raw/sales/orders/today/
07:05 AM  → Cluster starts, Task 1 begins
07:08 AM  → Bronze done (~3 min, 5M rows)
07:20 AM  → Silver done (~12 min, clean + ZORDER)
07:30 AM  → Gold done  (~10 min, 3 tables + ZORDER)
07:32 AM  → Cluster auto-terminates
07:35 AM  → Teams alert: Pipeline succeeded
08:00 AM  → Power BI dashboard refreshes ✓
```

---

## Cost estimate (Azure, approximate)

```
4× Standard_DS3_v2 Spot workers  →  ~₹3/hr each
1× Standard_DS3_v2 On-demand driver →  ~₹6/hr
Runtime: ~1.5 hrs/day

Daily  ≈ ₹24/day
Monthly ≈ ₹720/month (~$9 USD/month)
```
