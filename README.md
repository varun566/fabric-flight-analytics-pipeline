# Fabric Flight Analytics Pipeline

An end-to-end Microsoft Fabric Lakehouse project that turns raw synthetic airline flight operations into reporting-ready Gold Delta tables. The pipeline uses a Bronze → Silver → Gold medallion architecture, PySpark quality gates, and a Direct Lake Power BI semantic model.

```text
CSV in Lakehouse Files
        │
        ▼
01 Bronze ingestion ──► bronze_flights (raw Delta/Parquet)
        │                       │ quality log
        ▼                       ▼
02 Silver transformation ─► silver_flights (clean, typed, deduplicated)
        │                       │ quality log
        ▼                       ▼
03 Gold aggregation ────► daily route performance + hourly volume
        │                       │ quality log
        ▼                       ▼
Power BI Direct Lake ◄────── Gold Delta tables + quality_log
```

## Repository layout

| Path | Purpose |
| --- | --- |
| `data/flight_operations.csv` | Generated synthetic source data with 24,600 rows. |
| `scripts/generate_flight_ops.py` | Deterministic CSV generator. |
| `notebook_sources/` | Version-controlled PySpark notebook source. |
| `notebooks/` | Generated `.ipynb` artifacts for Fabric import. |
| `config/pipeline_config.json` | Table names and quality thresholds. |
| `powerbi/` | Direct Lake report build specification and DAX measures. |
| `docs/orchestration.md` | Optional Fabric Data Pipeline run order and scheduling. |

## Local preparation

The committed data file is generated from code. Recreate it and its Fabric notebook artifacts with:

```bash
python3 scripts/generate_flight_ops.py
python3 scripts/export_notebooks.py
python3 -m unittest discover -s tests
```

The generator produces 24,000 primary flights plus 600 duplicate `flight_id` records. It introduces a minimal number of nulls in `origin`, `dest`, and `delay_min` so the Silver cleansing and data-quality controls can be demonstrated while retaining more than 95% of Bronze rows.

### Local visual preview

You can run a dependency-free local simulation of the same Bronze → Silver → Gold rules without a Fabric tenant:

```bash
python3 scripts/run_local_preview.py
open outputs/flight_analytics_preview.html
```

The generated dashboard shows pipeline output counts, route-delay comparison, on-time trends, z-score alerts, and quality-check results. Fabric remains the production execution path; the preview is provided for quick portfolio demonstrations.

### Interactive frontend dashboard

Build the polished, dependency-free frontend dashboard with route filtering, chart tooltips, anomaly search, and a data-quality view:

```bash
python3 scripts/build_frontend.py
open outputs/flight_analytics_dashboard.html
```

The frontend is self-contained, so it can also be served from the project root with `python3 -m http.server 8765` and opened at `/outputs/flight_analytics_dashboard.html`. Its embedded dashboard model is rebuilt from the same synthetic flight CSV used by the pipeline.

For GitHub Pages, build the site artifact into the repository's `docs/` directory:

```bash
python3 scripts/build_frontend.py --output docs/index.html
```

### Local PySpark + Delta Lake

For a no-account, no-card execution that creates real local Delta tables, install the compatible Spark and Delta packages and run:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-local.txt
.venv/bin/python scripts/run_local_delta_pipeline.py
```

The runner writes `bronze_flights`, `silver_flights`, two Gold tables, and `quality_log` as Delta tables below `work/local_delta_lakehouse/Tables/`. It mirrors the Fabric notebook logic using local Spark; the existing visual preview remains available after the run.

## Fabric workspace deployment

1. In a Fabric-capacity workspace, create a Lakehouse named `FlightAnalyticsLakehouse`.
2. In Lakehouse Explorer upload `data/flight_operations.csv` to `Files/flight_ops/flight_operations.csv`.
3. Upload `config/pipeline_config.json` to `Files/pipeline_config.json`.
4. Import the three `.ipynb` files from `notebooks/` into the workspace and attach `FlightAnalyticsLakehouse` as the default Lakehouse for each notebook.
5. Run the notebooks in numeric order. The expected managed Delta tables appear in the Lakehouse Tables area.
6. Review `quality_log` after every run. A failed error-severity check is logged before the notebook raises an assertion.

Small CSVs can be uploaded directly through Lakehouse Explorer, and raw files belong in the Lakehouse Files area. The notebooks use `/lakehouse/default/Files/...`, which requires the target Lakehouse to be attached as the notebook default. For another Lakehouse, replace the configured path with its ABFS path. Microsoft documents both file-upload and attached-Lakehouse path patterns in its [Lakehouse ingestion guidance](https://learn.microsoft.com/en-us/fabric/data-engineering/load-data-lakehouse) and [Lakehouse troubleshooting guide](https://learn.microsoft.com/en-us/fabric/data-engineering/troubleshoot-lakehouse).

## Data model

### Bronze: `bronze_flights`

The immutable-in-spirit landing table preserves all source business fields as strings and adds `source_file` and `bronze_loaded_at`. The managed Delta table uses Parquet data files underneath; no cleansing or type coercion occurs here.

| Column | Type in Bronze | Description |
| --- | --- | --- |
| `flight_id` | string | Synthetic flight key; duplicates intentionally exist. |
| `origin`, `dest` | string | IATA airport codes. |
| `scheduled_dep`, `actual_dep` | string | Source departure timestamps. |
| `delay_min` | string | Departure delay in minutes; may be blank. |
| `status` | string | `ON_TIME`, `DELAYED`, or `CANCELLED`. |
| `pax_count` | string | Passenger count. |
| `source_file`, `bronze_loaded_at` | string, timestamp | Ingestion audit fields. |

### Silver: `silver_flights`

`flight_id` is deduplicated deterministically, preferring the most complete departure record. Rows missing `flight_id`, `origin`, or `dest` are removed. `delay_min` is cast to integer and blanks become zero; timestamps and passenger counts are typed. It adds `flight_date`, `delay_flag` (`delay_min > 15`), `delay_bucket`, and `silver_loaded_at`.

### Gold: reporting tables

| Table | Grain | Metrics |
| --- | --- | --- |
| `gold_daily_route_performance` | flight date × origin × destination | scheduled, operated, cancelled, and on-time flights; passengers; average delay; on-time percent; cancellation rate; delay z-score and anomaly flag. |
| `gold_hourly_volume` | flight date × scheduled departure hour × origin × destination | scheduled flights, passenger throughput, and delayed flights. |
| `quality_log` | notebook run × check | measured value, threshold, pass/fail, severity, details, and check timestamp. |

`on_time_pct` is calculated on operated flights only and considers a flight on time when its delay is 15 minutes or less. `avg_delay_min` excludes cancelled flights. `delay_anomaly_flag` marks a date/route when its average delay is at least three route-level standard deviations from that route's historical average.

## Data quality controls

| Layer | Check | Failure rule |
| --- | --- | --- |
| Bronze | Required CSV columns; positive row count | Missing required column or zero rows. |
| Bronze | `flight_id`, origin, and destination null rates | Must each be below 1%. |
| Silver | Bronze/Silver row-count difference | Absolute difference must be at most 5% of Bronze rows. |
| Silver | Null rate for required typed fields | Each must be below 1%. |
| Gold | Both Gold tables populated; average delay completeness | Zero rows or `avg_delay_min` null rate at least 1%. |
| Gold | Freshness | Latest Silver load must be less than 60 minutes old. |
| Gold | Anomaly detection | Z-scores are calculated and anomaly totals are logged. |

Thresholds live in `config/pipeline_config.json`. Error-severity checks halt their current notebook; informational checks, such as duplicates removed and anomaly count, do not.

## Power BI Direct Lake

From `FlightAnalyticsLakehouse`, select **New semantic model**, include the two Gold tables, and name the model `Flight Operations Analytics`. This creates a Direct Lake model for the Lakehouse tables. Then use the measures and visual specification in `powerbi/README.md` to build the report. Current Microsoft guidance also supports creating Direct Lake models from a Lakehouse via **New semantic model** and editing them in the workspace; see [Direct Lake model development](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-develop) and [creating a semantic model](https://learn.microsoft.com/en-us/fabric/data-warehouse/create-semantic-model).

## Operational notes

- Run notebooks manually while developing, then use the dependencies and schedule in `docs/orchestration.md`.
- Use the Lakehouse table names in the configuration as a single source of truth. Changing one requires rerunning the corresponding notebook.
- The included dataset is synthetic and contains no personal data.
- Fabric workspace configuration, Lakehouse creation, notebook execution, semantic-model publishing, and GitHub repository creation require access to your Fabric tenant and GitHub account; the project files are ready for those actions.
