# =============================================================================
# MAINTENANCE — VACUUM + Table Registration + Health Checks
# Pipeline : E-Commerce Orders
# Run      : Weekly (not daily) — schedule every Sunday
# =============================================================================
# What this script does:
#   1. Registers all Delta paths as SQL tables in Unity Catalog / Hive
#   2. Runs VACUUM on Bronze, Silver, Gold (retain 7 days of history)
#   3. Runs DESCRIBE HISTORY to log recent operations
#   4. Checks file counts and sizes per layer
# =============================================================================

from pyspark.sql import SparkSession
from datetime import date

spark = SparkSession.builder \
    .appName("Pipeline_Maintenance") \
    .getOrCreate()

today = str(date.today())

BRONZE_PATH = "/mnt/bronze/sales/orders/"
SILVER_PATH = "/mnt/silver/sales/orders/"
GOLD_CITY   = "/mnt/gold/city_daily_summary/"
GOLD_CAT    = "/mnt/gold/category_daily_summary/"
GOLD_PAY    = "/mnt/gold/payment_mode_summary/"

print(f"{'='*60}")
print(f"  MAINTENANCE | Date: {today}")
print(f"{'='*60}")


# ── Step 1: Register Delta tables in Hive Metastore ──────────────────────────
print("\n[Step 1] Registering Delta tables as SQL tables...")

spark.sql("CREATE DATABASE IF NOT EXISTS sales_db")

tables = {
    "sales_db.bronze_orders"           : BRONZE_PATH,
    "sales_db.silver_orders"           : SILVER_PATH,
    "sales_db.gold_city_summary"       : GOLD_CITY,
    "sales_db.gold_category_summary"   : GOLD_CAT,
    "sales_db.gold_payment_summary"    : GOLD_PAY,
}

for table_name, path in tables.items():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{path}'
    """)
    print(f"  ✓ {table_name}")

print("  ✓ All tables registered — queryable by name in notebooks and Power BI")


# ── Step 2: VACUUM all layers ─────────────────────────────────────────────────
# RETAIN 168 HOURS = 7 days of time travel history
# Delta keeps old file versions for time travel — VACUUM cleans them up
# Run weekly to control storage costs
print("\n[Step 2] Running VACUUM (retain 168 hours = 7 days)...")

paths_to_vacuum = [
    ("Bronze", BRONZE_PATH),
    ("Silver", SILVER_PATH),
    ("Gold - city",     GOLD_CITY),
    ("Gold - category", GOLD_CAT),
    ("Gold - payment",  GOLD_PAY),
]

for layer_name, path in paths_to_vacuum:
    print(f"  Vacuuming {layer_name}...")
    spark.sql(f"""
        VACUUM delta.`{path}` RETAIN 168 HOURS
    """)
    print(f"  ✓ {layer_name} VACUUM complete")


# ── Step 3: DESCRIBE HISTORY — log recent operations ─────────────────────────
print("\n[Step 3] Recent Silver table history (last 5 operations)...")
spark.sql(f"""
    DESCRIBE HISTORY delta.`{SILVER_PATH}` LIMIT 5
""").select(
    "version", "timestamp", "operation",
    "operationMetrics"
).show(5, truncate=False)


# ── Step 4: Table health check ────────────────────────────────────────────────
print("\n[Step 4] Table health check...")

for layer_name, path in paths_to_vacuum:
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`")
    row = detail.collect()[0]
    size_gb = round(row["sizeInBytes"] / (1024**3), 2) if row["sizeInBytes"] else 0
    num_files = row["numFiles"] if row["numFiles"] else 0
    print(f"  {layer_name:20s} | {num_files:6,} files | {size_gb:6.2f} GB")


print(f"\n{'='*60}")
print(f"  MAINTENANCE DONE | {today}")
print(f"  Next run: next Sunday")
print(f"{'='*60}\n")

# ── Power BI connection info ───────────────────────────────────────────────────
print("""
HOW TO CONNECT POWER BI TO GOLD TABLES:
  1. Power BI Desktop → Get Data → Azure Databricks
  2. Server : adb-<workspace-id>.azuredatabricks.net
  3. HTTP Path: /sql/1.0/warehouses/<warehouse-id>
  4. Tables  :
       sales_db.gold_city_summary       ← map visual, city filters
       sales_db.gold_category_summary   ← category bar charts
       sales_db.gold_payment_summary    ← payment mode pie chart

SAMPLE POWER BI QUERIES (fast due to ZORDER):
  SELECT * FROM sales_db.gold_city_summary
  WHERE city = 'JAIPUR'
  ORDER BY ingestion_date DESC

  SELECT * FROM sales_db.gold_category_summary
  WHERE category = 'ELECTRONICS'
  AND ingestion_date >= '2026-05-01'
""")
