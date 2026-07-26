# =============================================================================
# MASTER PIPELINE RUNNER
# Pipeline : E-Commerce Orders (50 GB/day) — Azure Databricks
# Schedule : Daily at 7:00 AM via Databricks Workflows
# =============================================================================
# Execution order:
#   Task 1 → bronze_ingestion.py
#   Task 2 → silver_transform.py   (depends on Task 1)
#   Task 3 → gold_aggregation.py   (depends on Task 2)
#
# This master script runs all 3 in sequence with:
#   - Full error handling + alerting (Teams webhook)
#   - Per-step timing
#   - Summary report at the end
#
# NOTE: In production, use Databricks Workflows UI to chain tasks
#       (each script as a separate task with dependencies).
#       This master runner is useful for local testing or single-job runs.
# =============================================================================

import time
import requests
from pyspark.sql import SparkSession
from datetime import date

spark = SparkSession.builder \
    .appName("Master_Pipeline_Runner") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled",   "true") \
    .config("spark.sql.adaptive.enabled",                   "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled","true") \
    .config("spark.sql.shuffle.partitions",                 "200") \
    .getOrCreate()

today = str(date.today())

# ── Alerting — Microsoft Teams Webhook ───────────────────────────────────────
# Replace with your actual Teams incoming webhook URL
TEAMS_WEBHOOK_URL = "https://outlook.office.com/webhook/YOUR-WEBHOOK-URL"

def send_teams_alert(title, message, color="0076D7"):
    """
    Send a notification card to Microsoft Teams channel.
    color: "0076D7" = blue (info), "D83B01" = red (error), "107C10" = green (success)
    """
    payload = {
        "@type"     : "MessageCard",
        "@context"  : "http://schema.org/extensions",
        "themeColor": color,
        "summary"   : title,
        "sections"  : [{
            "activityTitle"   : title,
            "activitySubtitle": f"Date: {today}",
            "text"            : message,
            "facts": [
                {"name": "Pipeline", "value": "E-Commerce Orders"},
                {"name": "Date",     "value": today},
                {"name": "Time",     "value": time.strftime("%H:%M:%S")},
            ]
        }]
    }
    try:
        requests.post(TEAMS_WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:
        print(f"  ⚠ Teams alert failed (non-critical): {e}")


# ── Pipeline steps definition ─────────────────────────────────────────────────
BRONZE_PATH = "/mnt/bronze/sales/orders/"
SILVER_PATH = "/mnt/silver/sales/orders/"
GOLD_CITY   = "/mnt/gold/city_daily_summary/"
GOLD_CAT    = "/mnt/gold/category_daily_summary/"
GOLD_PAY    = "/mnt/gold/payment_mode_summary/"


# ─────────────────────────────────────────────────────────────────────────────
# BRONZE INGESTION
# ─────────────────────────────────────────────────────────────────────────────
def run_bronze():
    from pyspark.sql.functions import current_timestamp, input_file_name, lit
    RAW_PATH = f"/mnt/raw/sales/orders/{today}/"

    files = dbutils.fs.ls(RAW_PATH)
    if len(files) == 0:
        raise Exception(f"No files at {RAW_PATH}")
    print(f"  Files found: {len(files)}")

    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(RAW_PATH) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file",         input_file_name()) \
        .withColumn("ingestion_date",      lit(today)) \
        .repartition(16)

    df_raw.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("ingestion_date") \
        .save(BRONZE_PATH)

    return df_raw.count()


# ─────────────────────────────────────────────────────────────────────────────
# SILVER TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────
def run_silver():
    from pyspark.sql.functions import (
        col, upper, trim, to_date, current_timestamp,
        row_number, desc, when
    )
    from pyspark.sql.window import Window
    from delta.tables import DeltaTable

    df = spark.read.format("delta").load(BRONZE_PATH) \
             .filter(f"ingestion_date = '{today}'")

    total_raw = df.count()

    # Deduplicate
    window_spec = Window.partitionBy("order_id").orderBy(desc("ingestion_timestamp"))
    df = df.withColumn("rn", row_number().over(window_spec)) \
           .filter("rn = 1").drop("rn")

    # Clean
    df = df \
        .withColumn("city",         upper(trim(col("city")))) \
        .withColumn("state",        upper(trim(col("state")))) \
        .withColumn("category",     upper(trim(col("category")))) \
        .withColumn("payment_mode", upper(trim(col("payment_mode")))) \
        .withColumn("order_status", upper(trim(col("order_status")))) \
        .withColumn("order_date",   to_date(col("order_date"),    "yyyy-MM-dd")) \
        .withColumn("delivery_date",to_date(col("delivery_date"), "yyyy-MM-dd")) \
        .withColumn("amount",       col("amount").cast("double")) \
        .withColumn("quantity",     col("quantity").cast("integer")) \
        .withColumn("payment_mode",
            when(col("payment_mode").isin("CARD","CREDIT CARD","DEBIT CARD"), "CARD")
            .when(col("payment_mode").isin("UPI","GPAY","PHONEPE","PAYTM"),   "UPI")
            .when(col("payment_mode").isin("COD","CASH ON DELIVERY"),         "COD")
            .when(col("payment_mode").isin("NET BANKING","NETBANKING"),        "NET_BANKING")
            .otherwise(col("payment_mode"))
        ) \
        .withColumn("silver_load_timestamp", current_timestamp())

    # Filter
    df_clean = df \
        .filter(col("order_id").isNotNull()) \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("amount") > 0) \
        .filter(col("quantity") > 0)

    clean_count = df_clean.count()

    # DQ check
    null_order_pct = df_clean.filter(col("order_id").isNull()).count() / max(total_raw, 1)
    if null_order_pct > 0.05:
        raise Exception(f"DQ FAILED: {null_order_pct:.2%} null order_ids")

    # Merge
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        silver_table.alias("t").merge(df_clean.alias("s"), "t.order_id = s.order_id") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df_clean.write.format("delta").mode("overwrite") \
            .option("overwriteSchema","true").partitionBy("ingestion_date").save(SILVER_PATH)

    # ZORDER
    spark.sql(f"""
        OPTIMIZE delta.`{SILVER_PATH}`
        WHERE ingestion_date = '{today}'
        ZORDER BY (city, product_id, category)
    """)

    return clean_count


# ─────────────────────────────────────────────────────────────────────────────
# GOLD AGGREGATION
# ─────────────────────────────────────────────────────────────────────────────
def run_gold():
    from pyspark.sql.functions import (
        col, sum as _sum, count, avg, max as _max,
        countDistinct, round as _round
    )
    from delta.tables import DeltaTable
    from datetime import timedelta

    three_days_ago = str(date.today() - timedelta(days=3))

    df_silver = spark.read.format("delta").load(SILVER_PATH) \
                     .filter(f"ingestion_date >= '{three_days_ago}'") \
                     .cache()

    def merge_gold(df_new, path, key_cols, zorder_col=None):
        join_cond = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])
        if DeltaTable.isDeltaTable(spark, path):
            DeltaTable.forPath(spark, path).alias("t").merge(df_new.alias("s"), join_cond) \
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            df_new.write.format("delta").mode("overwrite") \
                .option("overwriteSchema","true").partitionBy("ingestion_date").save(path)
        if zorder_col:
            spark.sql(f"OPTIMIZE delta.`{path}` WHERE ingestion_date='{today}' ZORDER BY ({zorder_col})")

    # City
    df_city = df_silver.filter("order_status != 'CANCELLED'") \
        .groupBy("city", "state", "ingestion_date").agg(
            count("order_id")            .alias("total_orders"),
            _sum("amount")               .alias("total_revenue"),
            _round(avg("amount"), 2)     .alias("avg_order_value"),
            _max("amount")               .alias("max_order_value"),
            countDistinct("customer_id") .alias("unique_customers"),
            _sum("quantity")             .alias("total_units_sold"),
        )
    merge_gold(df_city, GOLD_CITY, ["city","ingestion_date"], "city")

    # Category
    df_category = df_silver.filter("order_status != 'CANCELLED'") \
        .groupBy("category", "ingestion_date").agg(
            count("order_id")            .alias("total_orders"),
            _sum("amount")               .alias("total_revenue"),
            _round(avg("amount"), 2)     .alias("avg_order_value"),
            _sum("quantity")             .alias("total_units_sold"),
            countDistinct("product_id")  .alias("unique_products"),
            countDistinct("customer_id") .alias("unique_customers"),
        )
    merge_gold(df_category, GOLD_CAT, ["category","ingestion_date"], "category")

    # Payment mode
    df_payment = df_silver \
        .groupBy("payment_mode", "ingestion_date").agg(
            count("order_id")            .alias("total_transactions"),
            _sum("amount")               .alias("total_revenue"),
            _round(avg("amount"), 2)     .alias("avg_transaction_value"),
            countDistinct("customer_id") .alias("unique_customers"),
        )
    merge_gold(df_payment, GOLD_PAY, ["payment_mode","ingestion_date"])

    df_silver.unpersist()
    return 3   # 3 Gold tables updated


# ─────────────────────────────────────────────────────────────────────────────
# MASTER RUNNER — runs all steps with timing and alerting
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline():
    pipeline_start = time.time()
    results = {}

    send_teams_alert(
        "🚀 Pipeline Started",
        f"E-Commerce Orders pipeline started for {today}",
        color="0076D7"
    )

    steps = [
        ("Bronze Ingestion",  run_bronze),
        ("Silver Transform",  run_silver),
        ("Gold Aggregation",  run_gold),
    ]

    for step_name, step_fn in steps:
        print(f"\n{'─'*60}")
        print(f"  Starting: {step_name}")
        print(f"{'─'*60}")
        step_start = time.time()

        try:
            result = step_fn()
            elapsed = round(time.time() - step_start, 1)
            results[step_name] = {"status": "✓ success", "time": elapsed, "rows": result}
            print(f"\n  ✓ {step_name} completed in {elapsed}s")

        except Exception as e:
            elapsed = round(time.time() - step_start, 1)
            results[step_name] = {"status": "✗ failed", "time": elapsed, "error": str(e)}
            error_msg = f"Step '{step_name}' failed after {elapsed}s\nError: {str(e)}"
            print(f"\n  ✗ {step_name} FAILED: {e}")

            send_teams_alert(
                f"❌ Pipeline FAILED — {step_name}",
                error_msg,
                color="D83B01"
            )
            raise   # re-raise to stop pipeline

    total_elapsed = round(time.time() - pipeline_start, 1)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETE | {today} | Total time: {total_elapsed}s")
    print(f"{'='*60}")
    for step, r in results.items():
        print(f"  {r['status']}  {step:25s} | {r['time']}s | rows: {r.get('rows','—')}")
    print(f"{'='*60}\n")

    summary = "\n".join([
        f"{r['status']} {step} ({r['time']}s)"
        for step, r in results.items()
    ])
    send_teams_alert(
        f"✅ Pipeline Succeeded — {today}",
        f"All steps completed in {total_elapsed}s\n\n{summary}",
        color="107C10"
    )


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()
