from datetime import datetime, timezone
import json

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType, TimestampType


CONFIG = json.loads(
    spark.read.option("wholetext", True)
    .text("/lakehouse/default/Files/pipeline_config.json")
    .first()["value"]
)
SILVER_TABLE = CONFIG["tables"]["silver"]
DAILY_ROUTE_TABLE = CONFIG["tables"]["gold_daily_route_performance"]
HOURLY_VOLUME_TABLE = CONFIG["tables"]["gold_hourly_volume"]
QUALITY_LOG_TABLE = CONFIG["tables"]["quality_log"]
MAX_NULL_RATE = float(CONFIG["quality_thresholds"]["max_null_rate"])
MAX_FRESHNESS_MINUTES = float(CONFIG["quality_thresholds"]["max_gold_freshness_minutes"])
Z_SCORE_THRESHOLD = float(CONFIG["quality_thresholds"]["z_score_threshold"])
ON_TIME_THRESHOLD = int(CONFIG["on_time_delay_threshold_minutes"])
RUN_ID = datetime.now(timezone.utc).strftime("gold-%Y%m%dT%H%M%SZ")

QUALITY_LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("pipeline_stage", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_value", DoubleType(), True),
        StructField("threshold", DoubleType(), True),
        StructField("passed", BooleanType(), False),
        StructField("severity", StringType(), False),
        StructField("details", StringType(), True),
        StructField("checked_at", TimestampType(), False),
    ]
)


def write_quality_log(records):
    quality_rows = [
        (
            RUN_ID,
            "gold",
            record["name"],
            float(record["value"]),
            float(record["threshold"]),
            bool(record["passed"]),
            record.get("severity", "ERROR"),
            record.get("details", ""),
            datetime.now(timezone.utc).replace(tzinfo=None),
        )
        for record in records
    ]
    spark.createDataFrame(quality_rows, QUALITY_LOG_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(QUALITY_LOG_TABLE)


def fail_if_checks_failed(records):
    failed = [record["name"] for record in records if not record["passed"] and record.get("severity", "ERROR") == "ERROR"]
    if failed:
        raise AssertionError(f"Gold quality checks failed: {', '.join(failed)}")


silver_flights = spark.table(SILVER_TABLE)
daily_base = silver_flights.groupBy("flight_date", "origin", "dest").agg(
    F.count("*").alias("scheduled_flights"),
    F.sum(F.when(F.col("status") != "CANCELLED", F.lit(1)).otherwise(F.lit(0))).alias("operated_flights"),
    F.sum(F.when(F.col("status") == "CANCELLED", F.lit(1)).otherwise(F.lit(0))).alias("cancelled_flights"),
    F.sum("pax_count").alias("passenger_count"),
    F.avg(F.when(F.col("status") != "CANCELLED", F.col("delay_min"))).alias("avg_delay_min"),
    F.sum(
        F.when(
            (F.col("status") != "CANCELLED") & (F.col("delay_min") <= ON_TIME_THRESHOLD),
            F.lit(1),
        ).otherwise(F.lit(0))
    ).alias("on_time_flights"),
)

daily_metrics = (
    daily_base.withColumn("avg_delay_min", F.round(F.col("avg_delay_min"), 2))
    .withColumn("route", F.concat_ws(" → ", F.col("origin"), F.col("dest")))
    .withColumn(
        "on_time_pct",
        F.when(
            F.col("operated_flights") > 0,
            F.round(F.col("on_time_flights") / F.col("operated_flights"), 4),
        ).otherwise(F.lit(None).cast("double")),
    )
    .withColumn("cancellation_rate", F.round(F.col("cancelled_flights") / F.col("scheduled_flights"), 4))
)

route_history_window = Window.partitionBy("origin", "dest")
daily_route_performance = (
    daily_metrics.withColumn("route_avg_delay_min", F.avg("avg_delay_min").over(route_history_window))
    .withColumn("route_delay_stddev", F.stddev_samp("avg_delay_min").over(route_history_window))
    .withColumn(
        "delay_z_score",
        F.when(F.col("avg_delay_min").isNull(), F.lit(0.0))
        .when(
            F.col("route_delay_stddev") > 0,
            F.round(
                (F.col("avg_delay_min") - F.col("route_avg_delay_min")) / F.col("route_delay_stddev"),
                3,
            ),
        ).otherwise(F.lit(0.0)),
    )
    .withColumn(
        "delay_anomaly_flag",
        F.coalesce(F.abs(F.col("delay_z_score")) >= F.lit(Z_SCORE_THRESHOLD), F.lit(False)),
    )
    .drop("route_avg_delay_min", "route_delay_stddev")
    .withColumn("gold_loaded_at", F.current_timestamp())
)

hourly_volume = (
    silver_flights.withColumn("scheduled_dep_hour", F.hour("scheduled_dep_ts"))
    .groupBy("flight_date", "scheduled_dep_hour", "origin", "dest")
    .agg(
        F.count("*").alias("scheduled_flights"),
        F.sum("pax_count").alias("passenger_throughput"),
        F.sum(F.when(F.col("delay_flag"), F.lit(1)).otherwise(F.lit(0))).alias("delayed_flights"),
    )
    .withColumn("route", F.concat_ws(" → ", F.col("origin"), F.col("dest")))
    .withColumn("gold_loaded_at", F.current_timestamp())
)

daily_route_performance.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(DAILY_ROUTE_TABLE)
hourly_volume.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    HOURLY_VOLUME_TABLE
)

daily_row_count = daily_route_performance.count()
hourly_row_count = hourly_volume.count()
avg_delay_null_rate = (
    daily_route_performance.filter(F.col("avg_delay_min").isNull()).count() / daily_row_count
    if daily_row_count
    else 1.0
)
latest_silver_loaded_at = silver_flights.agg(F.max("silver_loaded_at").alias("latest")).first()["latest"]
freshness_minutes = (
    (datetime.now(timezone.utc).replace(tzinfo=None) - latest_silver_loaded_at).total_seconds() / 60
    if latest_silver_loaded_at
    else float("inf")
)
anomaly_count = daily_route_performance.filter(F.col("delay_anomaly_flag")).count()

quality_records = [
    {
        "name": "daily_route_row_count_positive",
        "value": float(daily_row_count),
        "threshold": 1.0,
        "passed": daily_row_count > 0,
    },
    {
        "name": "hourly_volume_row_count_positive",
        "value": float(hourly_row_count),
        "threshold": 1.0,
        "passed": hourly_row_count > 0,
    },
    {
        "name": "daily_avg_delay_null_rate",
        "value": avg_delay_null_rate,
        "threshold": MAX_NULL_RATE,
        "passed": avg_delay_null_rate < MAX_NULL_RATE,
    },
    {
        "name": "silver_to_gold_freshness_minutes",
        "value": freshness_minutes,
        "threshold": MAX_FRESHNESS_MINUTES,
        "passed": freshness_minutes < MAX_FRESHNESS_MINUTES,
    },
    {
        "name": "delay_anomaly_count",
        "value": float(anomaly_count),
        "threshold": Z_SCORE_THRESHOLD,
        "passed": True,
        "severity": "INFO",
        "details": "Anomalies use absolute route-level daily delay z-scores.",
    },
]

write_quality_log(quality_records)
fail_if_checks_failed(quality_records)

print(
    f"Gold aggregation completed: {daily_row_count:,} daily route rows and "
    f"{hourly_row_count:,} hourly volume rows written."
)
