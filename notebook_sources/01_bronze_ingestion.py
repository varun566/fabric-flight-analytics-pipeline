from datetime import datetime, timezone
import json

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType, TimestampType


CONFIG = json.loads(
    spark.read.option("wholetext", True)
    .text("/lakehouse/default/Files/pipeline_config.json")
    .first()["value"]
)
SOURCE_PATH = CONFIG["source_csv_path"]
BRONZE_TABLE = CONFIG["tables"]["bronze"]
QUALITY_LOG_TABLE = CONFIG["tables"]["quality_log"]
MAX_NULL_RATE = float(CONFIG["quality_thresholds"]["max_null_rate"])
RUN_ID = datetime.now(timezone.utc).strftime("bronze-%Y%m%dT%H%M%SZ")

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
            "bronze",
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
        raise AssertionError(f"Bronze quality checks failed: {', '.join(failed)}")


required_columns = [
    "flight_id",
    "origin",
    "dest",
    "scheduled_dep",
    "actual_dep",
    "delay_min",
    "status",
    "pax_count",
]
raw_source = spark.read.option("header", True).option("inferSchema", False).csv(SOURCE_PATH)
missing_columns = sorted(set(required_columns) - set(raw_source.columns))

quality_records = [
    {
        "name": "required_columns_present",
        "value": float(len(missing_columns)),
        "threshold": 0.0,
        "passed": not missing_columns,
        "details": "Missing columns: " + ", ".join(missing_columns) if missing_columns else "All required columns found.",
    }
]

if missing_columns:
    write_quality_log(quality_records)
    fail_if_checks_failed(quality_records)

bronze_flights = raw_source.select(
    *[F.col(column).cast("string").alias(column) for column in required_columns]
).withColumn("source_file", F.input_file_name()).withColumn("bronze_loaded_at", F.current_timestamp())

bronze_flights.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    BRONZE_TABLE
)

bronze_row_count = bronze_flights.count()
flight_id_null_rate = bronze_flights.filter(
    F.col("flight_id").isNull() | (F.trim(F.col("flight_id")) == "")
).count() / bronze_row_count
origin_null_rate = bronze_flights.filter(
    F.col("origin").isNull() | (F.trim(F.col("origin")) == "")
).count() / bronze_row_count
destination_null_rate = bronze_flights.filter(
    F.col("dest").isNull() | (F.trim(F.col("dest")) == "")
).count() / bronze_row_count

quality_records.extend(
    [
        {
            "name": "bronze_row_count_positive",
            "value": float(bronze_row_count),
            "threshold": 1.0,
            "passed": bronze_row_count > 0,
        },
        {
            "name": "flight_id_null_rate",
            "value": flight_id_null_rate,
            "threshold": MAX_NULL_RATE,
            "passed": flight_id_null_rate < MAX_NULL_RATE,
        },
        {
            "name": "origin_null_rate",
            "value": origin_null_rate,
            "threshold": MAX_NULL_RATE,
            "passed": origin_null_rate < MAX_NULL_RATE,
        },
        {
            "name": "destination_null_rate",
            "value": destination_null_rate,
            "threshold": MAX_NULL_RATE,
            "passed": destination_null_rate < MAX_NULL_RATE,
        },
    ]
)

write_quality_log(quality_records)
fail_if_checks_failed(quality_records)

print(f"Bronze ingestion completed: {bronze_row_count:,} rows written to {BRONZE_TABLE}.")
