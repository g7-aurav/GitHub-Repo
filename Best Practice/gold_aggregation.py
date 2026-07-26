# =============================================================================
# GOLD LAYER — Aggregations for Power BI / Reporting
# Pipeline : E-Commerce Orders (50 GB/day)
# Storage  : Azure Data Lake Storage Gen2 (ADLS)
# Format   : Delta Lake
# =============================================================================
# Gold Tables produced:
#   1. city_daily_summary     — sales metrics by city + date
#   2. category_daily_summary — sales metrics by category + date
#   3. payment_mode_summary   — transactions by payment mode + date
# =============================================================================
# Best practices applied:
#   - Silver df cached once, reused for all 3 aggregations
#   - CANCELLED orders excluded from revenue aggregations
#   - Looks back 3 days to correct late-arriving data
#   - MERGE into Gold (not append) — safe for re-runs
#   - ZORDER on Power BI filter columns
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, avg, max as _max,
    countDistinct, round as _round, lit
)
from delta.tables import DeltaTable
from datetime import date, timedelta

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Gold_Orders_Aggregation") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled",   "true") \
    .config("spark.sql.adaptive.enabled",                   "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled","true") \
    .config("spark.sql.shuffle.partitions",                 "200") \
    .getOrCreate()

# ── Config ────────────────────────────────────────────────────────────────────
today         = str(date.today())
three_days_ago = str(date.today() - timedelta(days=3))

SILVER_PATH = "/mnt/silver/sales/orders/"
GOLD_CITY   = "/mnt/gold/city_daily_summary/"
GOLD_CAT    = "/mnt/gold/category_daily_summary/"
GOLD_PAY    = "/mnt/gold/payment_mode_summary/"

print(f"{'='*60}")
print(f"  GOLD AGGREGATION | Date: {today}")
print(f"{'='*60}")


# ── Helper: MERGE into Gold table ─────────────────────────────────────────────
def merge_gold(df_new, gold_path, key_columns, zorder_col=None):
    """
    MERGE df_new into existing Gold Delta table.
    Creates fresh table on first run.
    Optionally runs OPTIMIZE + ZORDER after merge.
    """
    join_condition = " AND ".join(
        [f"target.{c} = source.{c}" for c in key_columns]
    )

    if DeltaTable.isDeltaTable(spark, gold_path):
        gold_table = DeltaTable.forPath(spark, gold_path)
        gold_table.alias("target").merge(
            df_new.alias("source"),
            join_condition
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        print(f"    ✓ MERGE complete → {gold_path}")
    else:
        df_new.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("ingestion_date") \
            .save(gold_path)
        print(f"    ✓ Initial write → {gold_path}")

    if zorder_col:
        spark.sql(f"""
            OPTIMIZE delta.`{gold_path}`
            WHERE ingestion_date = '{today}'
            ZORDER BY ({zorder_col})
        """)
        print(f"    ✓ ZORDER BY ({zorder_col}) complete")


# ── Step 1: Read Silver (last 3 days to handle late data) ────────────────────
# Reading 3 days ensures yesterday's numbers get corrected
# if late-arriving source data came in today
print(f"\n[Step 1] Reading Silver ({three_days_ago} to {today})...")
df_silver = spark.read \
    .format("delta") \
    .load(SILVER_PATH) \
    .filter(f"ingestion_date >= '{three_days_ago}'") \
    .cache()   # Cache once — reused for all 3 aggregations below

silver_count = df_silver.count()
print(f"  ✓ Silver rows loaded (3 days): {silver_count:,}")
print(f"  ✓ DataFrame cached for reuse")


# ── Step 2: Gold Table 1 — City daily summary ─────────────────────────────────
# Used by: Power BI map visual, city-level revenue dashboards
# ZORDER: city (primary filter in dashboards)
print(f"\n[Step 2] Building city_daily_summary...")

df_city = df_silver \
    .filter("order_status != 'CANCELLED'") \
    .groupBy("city", "state", "ingestion_date") \
    .agg(
        count("order_id")               .alias("total_orders"),
        _sum("amount")                  .alias("total_revenue"),
        _round(avg("amount"), 2)        .alias("avg_order_value"),
        _max("amount")                  .alias("max_order_value"),
        countDistinct("customer_id")    .alias("unique_customers"),
        _sum("quantity")                .alias("total_units_sold"),
        count(
            col("order_status") == lit("DELIVERED")
        )                               .alias("delivered_orders")
    )

# Sample output preview:
# ┌──────────┬───────────┬───────────────┬─────────────────┬──────────────────┐
# │ city     │ state     │ total_orders  │ total_revenue   │ unique_customers │
# ├──────────┼───────────┼───────────────┼─────────────────┼──────────────────┤
# │ JAIPUR   │ RAJASTHAN │ 120,000       │ 18,000,000      │ 45,000           │
# │ DELHI    │ DELHI     │ 200,000       │ 35,000,000      │ 80,000           │
# │ MUMBAI   │ MAHARASHT │ 180,000       │ 27,000,000      │ 70,000           │
# └──────────┴───────────┴───────────────┴─────────────────┴──────────────────┘

merge_gold(
    df_city,
    GOLD_CITY,
    key_columns=["city", "ingestion_date"],
    zorder_col="city"
)
print(f"  ✓ city_daily_summary done")


# ── Step 3: Gold Table 2 — Category daily summary ─────────────────────────────
# Used by: product category performance reports
# ZORDER: category (filtered in category-level reports)
print(f"\n[Step 3] Building category_daily_summary...")

df_category = df_silver \
    .filter("order_status != 'CANCELLED'") \
    .groupBy("category", "ingestion_date") \
    .agg(
        count("order_id")           .alias("total_orders"),
        _sum("amount")              .alias("total_revenue"),
        _round(avg("amount"), 2)    .alias("avg_order_value"),
        _sum("quantity")            .alias("total_units_sold"),
        countDistinct("product_id") .alias("unique_products"),
        countDistinct("customer_id").alias("unique_customers")
    )

# Sample output:
# ┌─────────────┬───────────────┬─────────────────┬──────────────────┐
# │ category    │ total_orders  │ total_revenue   │ total_units_sold │
# ├─────────────┼───────────────┼─────────────────┼──────────────────┤
# │ ELECTRONICS │ 50,000        │ 75,000,000      │ 60,000           │
# │ CLOTHING    │ 150,000       │ 15,000,000      │ 200,000          │
# │ GROCERY     │ 300,000       │ 9,000,000       │ 500,000          │
# └─────────────┴───────────────┴─────────────────┴──────────────────┘

merge_gold(
    df_category,
    GOLD_CAT,
    key_columns=["category", "ingestion_date"],
    zorder_col="category"
)
print(f"  ✓ category_daily_summary done")


# ── Step 4: Gold Table 3 — Payment mode summary ───────────────────────────────
# Used by: finance team, payment gateway analysis
# No ZORDER — small table, fast queries already
print(f"\n[Step 4] Building payment_mode_summary...")

df_payment = df_silver \
    .groupBy("payment_mode", "ingestion_date") \
    .agg(
        count("order_id")           .alias("total_transactions"),
        _sum("amount")              .alias("total_revenue"),
        _round(avg("amount"), 2)    .alias("avg_transaction_value"),
        countDistinct("customer_id").alias("unique_customers")
    )

# Sample output:
# ┌──────────────┬──────────────────────┬───────────────┐
# │ payment_mode │ total_transactions   │ total_revenue │
# ├──────────────┼──────────────────────┼───────────────┤
# │ UPI          │ 2,500,000            │ 37,500,000    │
# │ COD          │ 1,500,000            │ 22,500,000    │
# │ CARD         │ 800,000              │ 12,000,000    │
# │ NET_BANKING  │ 200,000              │ 3,000,000     │
# └──────────────┴──────────────────────┴───────────────┘

merge_gold(
    df_payment,
    GOLD_PAY,
    key_columns=["payment_mode", "ingestion_date"]
    # No ZORDER — small dimension table, already fast
)
print(f"  ✓ payment_mode_summary done")


# ── Step 5: Release cache ─────────────────────────────────────────────────────
df_silver.unpersist()
print(f"\n  ✓ Silver cache released")


print(f"\n{'='*60}")
print(f"  GOLD DONE | date={today}")
print(f"  Tables updated:")
print(f"    → city_daily_summary     → {GOLD_CITY}")
print(f"    → category_daily_summary → {GOLD_CAT}")
print(f"    → payment_mode_summary   → {GOLD_PAY}")
print(f"{'='*60}\n")
