# Fabric Orchestration

Create a Fabric Data Pipeline named `pl_flight_analytics` after the initial notebook validation.

1. Add Notebook activities in this order: `01_bronze_ingestion`, `02_silver_transformation`, and `03_gold_aggregation`.
2. Attach each activity to `FlightAnalyticsLakehouse` and make each downstream activity depend on the prior activity succeeding.
3. Schedule the pipeline hourly only if a new CSV is uploaded or delivered to `Files/flight_ops`. The Gold freshness assertion intentionally fails when the latest Silver load is at least 60 minutes old.
4. In a production variant, replace the overwrite modes with an incremental watermarked append/merge pattern and retain the `quality_log` append history.

Failed data-quality assertions stop their notebook activity after recording its check results to `quality_log`. Use this table for an alerting query or a Power BI data-quality page.

