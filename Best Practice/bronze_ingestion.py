# =============================================================================
# BRONZE LAYER — Raw Ingestion
# Pipeline : E-Commerce Orders (50 GB/day)
# Storage  : Azure Data Lake Storage Gen2 (ADLS)
# Format   : Delta Lake
# =============================================================================
# Dataset Schema (orders.csv):
#   order_id, customer_id, product_id, category, city, state,
#   quantity, unit_price, amount, payment_mode, order_status,
#   order_date, delivery_date, ingestion_date
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from datetime import date

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Bronze_Orders_Ingestion") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled",   "true") \
    .config("spark.sql.adaptive.enabled",                   "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled","true") \
    .config("spark.sql.shuffle.partitions",                 "200") \
    .getOrCreate()

# ── Config ────────────────────────────────────────────────────────────────────
today           = str(date.today())            # e.g. "2026-05-23"
RAW_PATH        = f"/mnt/raw/sales/orders/{today}/"
BRONZE_PATH     = "/mnt/bronze/sales/orders/"
CHECKPOINT_PATH = "/mnt/checkpoints/bronze/sales/orders/"
SCHEMA_PATH     = CHECKPOINT_PATH + "schema/"

print(f"{'='*60}")
print(f"  BRONZE INGESTION | Date: {today}")
print(f"{'='*60}")


# ── Step 1: Validate file arrival ─────────────────────────────────────────────
print("\n[Step 1] Validating file arrival...")
try:
    files = dbutils.fs.ls(RAW_PATH)
    if len(files) == 0:
        raise Exception(f"No files found at {RAW_PATH}")
    print(f"  ✓ Found {len(files)} file(s) — proceeding")
    for f in files:
        print(f"    → {f.name}  ({round(f.size / (1024**3), 2)} GB)")
except Exception as e:
    print(f"  ✗ File check failed: {e}")
    raise


# ── Step 2: Read with Auto Loader (cloudFiles) ────────────────────────────────
# Auto Loader tracks every processed file via checkpoint.
# Re-running this job skips already-processed files automatically.
# trigger(availableNow=True) → behaves like batch, stops when all new files done.
print("\n[Step 2] Reading with Auto Loader (cloudFiles)...")
print(f"  Checkpoint : {CHECKPOINT_PATH}")
print(f"  Schema     : {SCHEMA_PATH}")

df_stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format",              "csv") \
    .option("cloudFiles.schemaLocation",      SCHEMA_PATH) \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.inferColumnTypes",    "true") \
    .option("header",                         "true") \
    .option("mode",                           "DROPMALFORMED") \
    .load(RAW_PATH)

print(f"  ✓ Auto Loader stream defined")


# ── Step 3: Add metadata columns ──────────────────────────────────────────────
print("\n[Step 3] Adding metadata columns...")
df_stream = df_stream \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file",         input_file_name()) \
    .withColumn("ingestion_date",      lit(today))
print("  ✓ Added: ingestion_timestamp, source_file, ingestion_date")


# ── Step 4: Repartition to avoid small files ──────────────────────────────────
# Target: ~3 GB per partition  |  50 GB / 16 = ~3 GB each
print("\n[Step 4] Repartitioning to 16 partitions (~3 GB each)...")
df_stream = df_stream.repartition(16)
print("  ✓ Repartitioned to 16")


# ── Step 5: Write to Bronze as Delta (via Auto Loader writeStream) ────────────
# Auto Loader writeStream with checkpointLocation ensures:
#   → Each file processed exactly once (no duplicates on re-run)
#   → New files detected and ingested automatically on next run
print(f"\n[Step 5] Writing to Bronze Delta table via Auto Loader...")
print(f"  Path: {BRONZE_PATH}")

query = df_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("mergeSchema",        "true") \
    .partitionBy("ingestion_date") \
    .trigger(availableNow=True) \
    .start(BRONZE_PATH)

query.awaitTermination()

# ── Extract row count from stream progress ────────────────────────────────────
last_progress = query.lastProgress
if last_progress:
    rows_written = last_progress.get("numOutputRows", "—")
    batch_id     = last_progress.get("batchId",       "—")
    print(f"  ✓ Write complete | Batch ID: {batch_id} | Rows written: {rows_written}")
else:
    print(f"  ✓ Write complete (no new files — all already checkpointed)")

print(f"\n{'='*60}")
print(f"  BRONZE DONE | ingestion_date={today}")
print(f"  Checkpoint saved → next run skips these files automatically")
print(f"{'='*60}\n")