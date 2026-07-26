# =============================================================================
# SILVER LAYER — Clean, Deduplicate, Validate, Transform
# Pipeline : E-Commerce Orders (50 GB/day)
# Storage  : Azure Data Lake Storage Gen2 (ADLS)
# Format   : Delta Lake
# =============================================================================
# What this script does:
#   1. Reads today's Bronze partition
#   2. Deduplicates on order_id (keeps latest by ingestion_timestamp)
#   3. Cleans & standardises all columns
#   4. Filters bad rows (nulls, negatives, zeros)
#   5. Runs data quality checks (halts if >5% critical nulls)
#   6. MERGEs into Silver Delta table (safe for re-runs & late data)
#   7. OPTIMIZE + ZORDER on today's partition only
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, trim, to_date, current_timestamp,
    lit, row_number, desc, when
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import date

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Silver_Orders_Transform") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled",   "true") \
    .config("spark.sql.adaptive.enabled",                   "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled","true") \
    .config("spark.sql.shuffle.partitions",                 "200") \
    .getOrCreate()

# ── Config ────────────────────────────────────────────────────────────────────
today       = str(date.today())            # e.g. "2026-05-23"
BRONZE_PATH = "/mnt/bronze/sales/orders/"
SILVER_PATH = "/mnt/silver/sales/orders/"

# Data quality thresholds
NULL_ORDER_ID_THRESHOLD = 0.05   # halt if >5% order_ids are null
NULL_CUSTOMER_THRESHOLD = 0.10   # warn  if >10% customer_ids are null

print(f"{'='*60}")
print(f"  SILVER TRANSFORM | Date: {today}")
print(f"{'='*60}")


# ── Step 1: Read today's Bronze partition ─────────────────────────────────────
print("\n[Step 1] Reading Bronze partition...")
df = spark.read \
    .format("delta") \
    .load(BRONZE_PATH) \
    .filter(f"ingestion_date = '{today}'")

total_raw = df.count()
print(f"  ✓ Rows from Bronze: {total_raw:,}")

if total_raw == 0:
    raise Exception(f"Bronze has 0 rows for {today} — check ingestion step")


# ── Step 2: Deduplicate on order_id ──────────────────────────────────────────
# Keep the latest record per order_id based on ingestion_timestamp
# Handles: source system re-sending same orders, CDC duplicates
print("\n[Step 2] Deduplicating on order_id...")
window_spec = Window.partitionBy("order_id").orderBy(desc("ingestion_timestamp"))

df = df \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter("row_num = 1") \
    .drop("row_num")

after_dedup = df.count()
duplicates_removed = total_raw - after_dedup
print(f"  ✓ After dedup   : {after_dedup:,} rows")
print(f"  ✓ Duplicates removed: {duplicates_removed:,}")


# ── Step 3: Clean and standardise columns ────────────────────────────────────
print("\n[Step 3] Cleaning and standardising columns...")

df = df \
    .withColumn("city",         upper(trim(col("city")))) \
    .withColumn("state",        upper(trim(col("state")))) \
    .withColumn("category",     upper(trim(col("category")))) \
    .withColumn("payment_mode", upper(trim(col("payment_mode")))) \
    .withColumn("order_status", upper(trim(col("order_status")))) \
    .withColumn("product_id",   trim(col("product_id"))) \
    .withColumn("customer_id",  trim(col("customer_id"))) \
    .withColumn("order_date",
        to_date(col("order_date"),    "yyyy-MM-dd")) \
    .withColumn("delivery_date",
        to_date(col("delivery_date"), "yyyy-MM-dd")) \
    .withColumn("amount",       col("amount").cast("double")) \
    .withColumn("unit_price",   col("unit_price").cast("double")) \
    .withColumn("quantity",     col("quantity").cast("integer")) \
    .withColumn("silver_load_timestamp", current_timestamp())

# Standardise payment_mode values
df = df.withColumn("payment_mode",
    when(col("payment_mode").isin("CARD","CREDIT CARD","DEBIT CARD"), "CARD")
    .when(col("payment_mode").isin("UPI","GPAY","PHONEPE","PAYTM"), "UPI")
    .when(col("payment_mode").isin("COD","CASH ON DELIVERY"), "COD")
    .when(col("payment_mode").isin("NET BANKING","NETBANKING"), "NET_BANKING")
    .otherwise(col("payment_mode"))
)

print("  ✓ Columns standardised: city, state, category, payment_mode, order_status")
print("  ✓ Dates parsed: order_date, delivery_date")
print("  ✓ payment_mode normalised: CARD / UPI / COD / NET_BANKING")


# ── Step 4: Filter bad rows ───────────────────────────────────────────────────
print("\n[Step 4] Filtering invalid rows...")

before_filter = df.count()

df_clean = df \
    .filter(col("order_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .filter(col("quantity") > 0) \
    .filter(col("unit_price") > 0)

after_filter = df_clean.count()
bad_rows = before_filter - after_filter
print(f"  ✓ Rows after filter : {after_filter:,}")
print(f"  ✓ Bad rows dropped  : {bad_rows:,}")


# ── Step 5: Data quality checks ───────────────────────────────────────────────
print("\n[Step 5] Running data quality checks...")

null_order_ids   = df_clean.filter(col("order_id").isNull()).count()
null_customers   = df_clean.filter(col("customer_id").isNull()).count()
negative_amounts = df_clean.filter(col("amount") < 0).count()
future_dates     = df_clean.filter(col("order_date") > lit(today)).count()

null_order_pct   = null_order_ids / after_dedup if after_dedup > 0 else 0
null_cust_pct    = null_customers  / after_dedup if after_dedup > 0 else 0

print(f"  Null order_ids   : {null_order_ids:,}  ({null_order_pct:.2%})")
print(f"  Null customer_ids: {null_customers:,}  ({null_cust_pct:.2%})")
print(f"  Negative amounts : {negative_amounts:,}")
print(f"  Future order dates: {future_dates:,}")

# Hard stop if critical quality fails
if null_order_pct > NULL_ORDER_ID_THRESHOLD:
    raise Exception(
        f"DQ FAILED: {null_order_pct:.2%} null order_ids "
        f"exceeds threshold {NULL_ORDER_ID_THRESHOLD:.0%} — halting pipeline"
    )

# Warning only for customer nulls
if null_cust_pct > NULL_CUSTOMER_THRESHOLD:
    print(f"  ⚠ WARNING: {null_cust_pct:.2%} null customer_ids "
          f"exceeds {NULL_CUSTOMER_THRESHOLD:.0%} warning threshold")

print("  ✓ Data quality checks passed")


# ── Step 6: MERGE into Silver ─────────────────────────────────────────────────
# MERGE handles:
#   - Re-sent files (same order_id updated) → UPDATE
#   - New orders → INSERT
#   - Safe to re-run multiple times
print(f"\n[Step 6] Merging into Silver Delta table...")
print(f"  Path: {SILVER_PATH}")

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    silver_table.alias("target").merge(
        df_clean.alias("source"),
        "target.order_id = source.order_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    print("  ✓ MERGE complete (upserted existing + inserted new rows)")
else:
    # First run — write fresh
    df_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("ingestion_date") \
        .save(SILVER_PATH)
    print("  ✓ Initial write complete (first run)")


# ── Step 7: OPTIMIZE + ZORDER on today's partition only ──────────────────────
# ZORDER BY city, product_id — columns analysts filter most
# Only today's partition → fast, doesn't re-process history
print(f"\n[Step 7] Running OPTIMIZE + ZORDER on partition {today}...")
spark.sql(f"""
    OPTIMIZE delta.`{SILVER_PATH}`
    WHERE ingestion_date = '{today}'
    ZORDER BY (city, product_id, category)
""")
print("  ✓ OPTIMIZE + ZORDER complete")
print("  ✓ ZORDER columns: city, product_id, category")
print("  ✓ Speeds up queries like:")
print("      WHERE city = 'JAIPUR' AND product_id = 'PROD-301'")
print("      WHERE category = 'ELECTRONICS' AND city = 'DELHI'")

print(f"\n{'='*60}")
print(f"  SILVER DONE | {after_filter:,} clean rows | date={today}")
print(f"  Duplicates removed : {duplicates_removed:,}")
print(f"  Bad rows dropped   : {bad_rows:,}")
print(f"{'='*60}\n")
