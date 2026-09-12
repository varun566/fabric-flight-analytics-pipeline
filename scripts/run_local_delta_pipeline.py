#!/usr/bin/env python3
"""Run the Fabric-style medallion pipeline locally with Apache Spark and Delta Lake."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


ON_TIME_DELAY_THRESHOLD = 15
Z_SCORE_THRESHOLD = 3.0
MAX_NULL_RATE = 0.01
MAX_ROW_COUNT_DIFFERENCE_RATE = 0.05
REQUIRED_COLUMNS = [
    "flight_id",
    "origin",
    "dest",
    "scheduled_dep",
    "actual_dep",
    "delay_min",
    "status",
    "pax_count",
]


def local_path(root: Path, table_name: str) -> str:
    return str((root / "Tables" / table_name).resolve())


def add_check(
    checks: list[dict[str, object]],
    stage: str,
    name: str,
    value: float,
    threshold: float,
    passed: bool,
    severity: str = "ERROR",
) -> None:
    checks.append(
        {
            "run_id": datetime.now(timezone.utc).strftime("local-%Y%m%dT%H%M%SZ"),
            "pipeline_stage": stage,
            "check_name": name,
            "check_value": float(value),
            "threshold": float(threshold),
            "passed": bool(passed),
            "severity": severity,
            "checked_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }
    )


def assert_quality(checks: list[dict[str, object]]) -> None:
    failed = [
        str(check["check_name"])
        for check in checks
        if not bool(check["passed"]) and check["severity"] == "ERROR"
    ]
    if failed:
        raise AssertionError(f"Local Delta quality checks failed: {', '.join(failed)}")


def create_spark() -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("fabric-flight-analytics-local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def escape_sql_string(value: object) -> str:
    return str(value).replace("'", "''")


def create_quality_log(spark: SparkSession, checks: list[dict[str, object]]):
    rows = []
    for check in checks:
        rows.append(
            "SELECT "
            f"'{escape_sql_string(check['run_id'])}' AS run_id, "
            f"'{escape_sql_string(check['pipeline_stage'])}' AS pipeline_stage, "
            f"'{escape_sql_string(check['check_name'])}' AS check_name, "
            f"CAST({float(check['check_value'])} AS DOUBLE) AS check_value, "
            f"CAST({float(check['threshold'])} AS DOUBLE) AS threshold, "
            f"CAST({str(bool(check['passed'])).lower()} AS BOOLEAN) AS passed, "
            f"'{escape_sql_string(check['severity'])}' AS severity, "
            "current_timestamp() AS checked_at"
        )
    return spark.sql(" UNION ALL ".join(rows))


def run_pipeline(input_path: Path, lakehouse_root: Path) -> dict[str, int]:
    spark = create_spark()
    checks: list[dict[str, object]] = []
    try:
        bronze_path = local_path(lakehouse_root, "bronze_flights")
        silver_path = local_path(lakehouse_root, "silver_flights")
        daily_path = local_path(lakehouse_root, "gold_daily_route_performance")
        hourly_path = local_path(lakehouse_root, "gold_hourly_volume")
        quality_path = local_path(lakehouse_root, "quality_log")

        raw_source = spark.read.option("header", True).option("inferSchema", False).csv(str(input_path.resolve()))
        missing_columns = sorted(set(REQUIRED_COLUMNS) - set(raw_source.columns))
        add_check(checks, "bronze", "required_columns_present", len(missing_columns), 0, not missing_columns)
        if missing_columns:
            assert_quality(checks)

        bronze = raw_source.select(
            *[F.col(column).cast("string").alias(column) for column in REQUIRED_COLUMNS]
        ).withColumn("source_file", F.input_file_name()).withColumn("bronze_loaded_at", F.current_timestamp())
        bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(bronze_path)
        bronze_count = bronze.count()
        add_check(checks, "bronze", "bronze_row_count_positive", bronze_count, 1, bronze_count > 0)
        for column in ("flight_id", "origin", "dest"):
            null_rate = bronze.filter(
                F.col(column).isNull() | (F.trim(F.col(column)) == "")
            ).count() / bronze_count
            add_check(checks, "bronze", f"{column}_null_rate", null_rate, MAX_NULL_RATE, null_rate < MAX_NULL_RATE)

        typed = bronze.select(
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
        deduplicated = typed.withColumn(
            "deduplication_rank", F.row_number().over(deduplication_window)
        ).filter(F.col("deduplication_rank") == 1).drop("deduplication_rank")
        silver = (
            deduplicated.filter(
                F.col("flight_id").isNotNull()
                & (F.col("flight_id") != "")
                & F.col("origin").isNotNull()
                & (F.col("origin") != "")
                & F.col("dest").isNotNull()
                & (F.col("dest") != "")
            )
            .withColumn("flight_date", F.to_date("scheduled_dep_ts"))
            .withColumn("delay_flag", F.col("delay_min") > F.lit(ON_TIME_DELAY_THRESHOLD))
            .withColumn(
                "delay_bucket",
                F.when(F.col("status") == "CANCELLED", F.lit("Cancelled"))
                .when(F.col("delay_min") < 0, F.lit("Early"))
                .when(F.col("delay_min") <= ON_TIME_DELAY_THRESHOLD, F.lit("On time (<=15m)"))
                .when(F.col("delay_min") <= 60, F.lit("Delayed (16-60m)"))
                .otherwise(F.lit("Delayed (60m+)")),
            )
            .withColumn("silver_loaded_at", F.current_timestamp())
        )
        silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
        silver_count = silver.count()
        row_difference = abs(silver_count - bronze_count) / bronze_count
        add_check(
            checks,
            "silver",
            "silver_to_bronze_row_difference_rate",
            row_difference,
            MAX_ROW_COUNT_DIFFERENCE_RATE,
            row_difference <= MAX_ROW_COUNT_DIFFERENCE_RATE,
        )
        add_check(checks, "silver", "duplicates_removed", bronze_count - deduplicated.count(), 0, True, "INFO")
        for column in ("flight_id", "origin", "dest", "scheduled_dep_ts", "delay_min", "status", "pax_count"):
            null_rate = silver.filter(F.col(column).isNull()).count() / silver_count
            add_check(checks, "silver", f"{column}_null_rate", null_rate, MAX_NULL_RATE, null_rate < MAX_NULL_RATE)

        daily_base = silver.groupBy("flight_date", "origin", "dest").agg(
            F.count("*").alias("scheduled_flights"),
            F.sum(F.when(F.col("status") != "CANCELLED", F.lit(1)).otherwise(F.lit(0))).alias("operated_flights"),
            F.sum(F.when(F.col("status") == "CANCELLED", F.lit(1)).otherwise(F.lit(0))).alias("cancelled_flights"),
            F.sum("pax_count").alias("passenger_count"),
            F.avg(F.when(F.col("status") != "CANCELLED", F.col("delay_min"))).alias("avg_delay_min"),
            F.sum(F.when((F.col("status") != "CANCELLED") & (F.col("delay_min") <= ON_TIME_DELAY_THRESHOLD), F.lit(1)).otherwise(F.lit(0))).alias("on_time_flights"),
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
        daily_gold = (
            daily_metrics.withColumn("route_avg_delay_min", F.avg("avg_delay_min").over(route_history_window))
            .withColumn("route_delay_stddev", F.stddev_samp("avg_delay_min").over(route_history_window))
            .withColumn(
                "delay_z_score",
                F.when(F.col("avg_delay_min").isNull(), F.lit(0.0))
                .when(
                    F.col("route_delay_stddev") > 0,
                    F.round((F.col("avg_delay_min") - F.col("route_avg_delay_min")) / F.col("route_delay_stddev"), 3),
                )
                .otherwise(F.lit(0.0)),
            )
            .withColumn(
                "delay_anomaly_flag",
                F.coalesce(F.abs(F.col("delay_z_score")) >= F.lit(Z_SCORE_THRESHOLD), F.lit(False)),
            )
            .drop("route_avg_delay_min", "route_delay_stddev")
            .withColumn("gold_loaded_at", F.current_timestamp())
        )
        hourly_gold = (
            silver.withColumn("scheduled_dep_hour", F.hour("scheduled_dep_ts"))
            .groupBy("flight_date", "scheduled_dep_hour", "origin", "dest")
            .agg(
                F.count("*").alias("scheduled_flights"),
                F.sum("pax_count").alias("passenger_throughput"),
                F.sum(F.when(F.col("delay_flag"), F.lit(1)).otherwise(F.lit(0))).alias("delayed_flights"),
            )
            .withColumn("route", F.concat_ws(" → ", F.col("origin"), F.col("dest")))
            .withColumn("gold_loaded_at", F.current_timestamp())
        )
        daily_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(daily_path)
        hourly_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(hourly_path)
        daily_count = daily_gold.count()
        hourly_count = hourly_gold.count()
        avg_delay_null_rate = daily_gold.filter(F.col("avg_delay_min").isNull()).count() / daily_count
        anomaly_count = daily_gold.filter(F.col("delay_anomaly_flag")).count()
        add_check(checks, "gold", "daily_route_row_count_positive", daily_count, 1, daily_count > 0)
        add_check(checks, "gold", "hourly_volume_row_count_positive", hourly_count, 1, hourly_count > 0)
        add_check(checks, "gold", "daily_avg_delay_null_rate", avg_delay_null_rate, MAX_NULL_RATE, avg_delay_null_rate < MAX_NULL_RATE)
        add_check(checks, "gold", "silver_to_gold_freshness_minutes", 0, 60, True)
        add_check(checks, "gold", "delay_anomaly_count", anomaly_count, Z_SCORE_THRESHOLD, True, "INFO")

        quality_log = create_quality_log(spark, checks)
        quality_log.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(quality_path)
        assert_quality(checks)
        return {
            "bronze_rows": bronze_count,
            "silver_rows": silver_count,
            "gold_daily_rows": daily_count,
            "gold_hourly_rows": hourly_count,
            "anomalies": anomaly_count,
        }
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data/flight_operations.csv"))
    parser.add_argument("--lakehouse-root", type=Path, default=Path("work/local_delta_lakehouse"))
    arguments = parser.parse_args()
    result = run_pipeline(arguments.input, arguments.lakehouse_root)
    print(
        "Local Delta pipeline completed: "
        + ", ".join(f"{name}={value:,}" for name, value in result.items())
    )


if __name__ == "__main__":
    main()
