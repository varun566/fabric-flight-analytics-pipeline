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
BRONZE_TABLE = CONFIG["tables"]["bronze"]
SILVER_TABLE = CONFIG["tables"]["silver"]
QUALITY_LOG_TABLE = CONFIG["tables"]["quality_log"]
MAX_NULL_RATE = float(CONFIG["quality_thresholds"]["max_null_rate"])
MIN_ROW_RATIO = float(CONFIG["quality_thresholds"]["min_silver_to_bronze_row_ratio"])
MAX_ROW_COUNT_DIFFERENCE_RATE = 1 - MIN_ROW_RATIO
RUN_ID = datetime.now(timezone.utc).strftime("silver-%Y%m%dT%H%M%SZ")

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
            "silver",
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
        raise AssertionError(f"Silver quality checks failed: {', '.join(failed)}")


bronze_flights = spark.table(BRONZE_TABLE)
bronze_row_count = bronze_flights.count()

typed_flights = bronze_flights.select(
    F.trim(F.col("flight_id")).alias("flight_id"),
    F.upper(F.trim(F.col("origin"))).alias("origin"),
    F.upper(F.trim(F.col("dest"))).alias("dest"),
    F.to_timestamp("scheduled_dep", "yyyy-MM-dd HH:mm:ss").alias("scheduled_dep_ts"),
    F.to_timestamp("actual_dep", "yyyy-MM-dd HH:mm:ss").alias("actual_dep_ts"),
    F.coalesce(F.col("delay_min").cast("integer"), F.lit(0)).alias("delay_min"),
    F.upper(F.trim(F.col("status"))).alias("status"),
    F.col("pax_count").cast("integer").alias("pax_count"),
    F.col("source_file"),
    F.col("bronze_loaded_at"),
)

deduplication_window = Window.partitionBy("flight_id").orderBy(
    F.col("actual_dep_ts").desc_nulls_last(),
    F.col("delay_min").desc_nulls_last(),
    F.col("pax_count").desc_nulls_last(),
)
deduplicated_flights = typed_flights.withColumn(
    "deduplication_rank", F.row_number().over(deduplication_window)
).filter(F.col("deduplication_rank") == 1).drop("deduplication_rank")

silver_flights = (
    deduplicated_flights.filter(
        F.col("flight_id").isNotNull()
        & (F.col("flight_id") != "")
        & F.col("origin").isNotNull()
        & (F.col("origin") != "")
        & F.col("dest").isNotNull()
        & (F.col("dest") != "")
    )
    .withColumn("flight_date", F.to_date("scheduled_dep_ts"))
    .withColumn("delay_flag", F.col("delay_min") > F.lit(15))
    .withColumn(
        "delay_bucket",
        F.when(F.col("status") == "CANCELLED", F.lit("Cancelled"))
        .when(F.col("delay_min") < 0, F.lit("Early"))
        .when(F.col("delay_min") <= 15, F.lit("On time (<=15m)"))
        .when(F.col("delay_min") <= 60, F.lit("Delayed (16-60m)"))
        .otherwise(F.lit("Delayed (60m+)")),
    )
    .withColumn("silver_loaded_at", F.current_timestamp())
)

silver_flights.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    SILVER_TABLE
)

silver_row_count = silver_flights.count()
silver_to_bronze_ratio = silver_row_count / bronze_row_count if bronze_row_count else 0.0
row_count_difference_rate = (
    abs(silver_row_count - bronze_row_count) / bronze_row_count if bronze_row_count else 1.0
)
required_columns = [
    "flight_id",
    "origin",
    "dest",
    "scheduled_dep_ts",
    "delay_min",
    "status",
    "pax_count",
]
quality_records = [
    {
        "name": "silver_to_bronze_row_difference_rate",
        "value": row_count_difference_rate,
        "threshold": MAX_ROW_COUNT_DIFFERENCE_RATE,
        "passed": row_count_difference_rate <= MAX_ROW_COUNT_DIFFERENCE_RATE,
    },
    {
        "name": "duplicates_removed",
        "value": float(bronze_row_count - deduplicated_flights.count()),
        "threshold": 0.0,
        "passed": True,
        "severity": "INFO",
    },
]

for column in required_columns:
    null_rate = silver_flights.filter(F.col(column).isNull()).count() / silver_row_count if silver_row_count else 1.0
    quality_records.append(
        {
            "name": f"{column}_null_rate",
            "value": null_rate,
            "threshold": MAX_NULL_RATE,
            "passed": null_rate < MAX_NULL_RATE,
        }
    )

write_quality_log(quality_records)
fail_if_checks_failed(quality_records)

print(
    f"Silver transformation completed: {silver_row_count:,} rows written to {SILVER_TABLE} "
    f"({silver_to_bronze_ratio:.2%} of Bronze)."
)
