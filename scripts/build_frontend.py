#!/usr/bin/env python3
"""Build the interactive, dependency-free flight analytics dashboard."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import date
from pathlib import Path

from run_local_preview import transform


def iso_date(value: date) -> str:
    return value.isoformat()


def quality_type(check_name: str) -> str:
    normalized_name = check_name.lower()
    if "rate" in normalized_name or "difference" in normalized_name:
        return "percent"
    if "freshness" in normalized_name:
        return "minutes"
    return "count"


def build_dashboard_model(result: dict[str, object]) -> dict[str, object]:
    daily_rows = result["daily_rows"]
    route_accumulators: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "scheduled_flights": 0,
            "operated_flights": 0,
            "cancelled_flights": 0,
            "on_time_flights": 0,
            "passenger_count": 0,
            "delay_total": 0.0,
        }
    )
    route_trends: dict[str, list[dict[str, object]]] = defaultdict(list)
    anomalies = []

    for row in daily_rows:
        route = str(row["route"])
        accumulator = route_accumulators[route]
        for name in ("scheduled_flights", "operated_flights", "cancelled_flights", "on_time_flights", "passenger_count"):
            accumulator[name] += int(row[name])
        if row["avg_delay_min"] is not None:
            accumulator["delay_total"] += float(row["avg_delay_min"]) * int(row["operated_flights"])
        if row["on_time_pct"] is not None:
            route_trends[route].append({"date": iso_date(row["flight_date"]), "value": float(row["on_time_pct"])})
        if row["delay_anomaly_flag"]:
            anomalies.append(
                {
                    "date": iso_date(row["flight_date"]),
                    "route": route,
                    "avgDelay": float(row["avg_delay_min"] or 0),
                    "zScore": float(row["delay_z_score"]),
                    "cancellationRate": float(row["cancellation_rate"]),
                    "flightCount": int(row["scheduled_flights"]),
                }
            )

    route_metrics = []
    for route, values in route_accumulators.items():
        operated = values["operated_flights"]
        scheduled = values["scheduled_flights"]
        route_metrics.append(
            {
                "route": route,
                "scheduledFlights": int(scheduled),
                "operatedFlights": int(operated),
                "cancelledFlights": int(values["cancelled_flights"]),
                "onTimeFlights": int(values["on_time_flights"]),
                "passengerCount": int(values["passenger_count"]),
                "avgDelay": values["delay_total"] / operated if operated else 0,
                "onTimePct": values["on_time_flights"] / operated if operated else 0,
                "cancellationRate": values["cancelled_flights"] / scheduled if scheduled else 0,
            }
        )

    route_metrics.sort(key=lambda item: (-float(item["avgDelay"]), str(item["route"])))
    anomalies.sort(key=lambda item: abs(float(item["zScore"])), reverse=True)
    trend = [{"date": iso_date(flight_date), "value": float(value)} for flight_date, value in result["trend"]]
    first_date, last_date = trend[0]["date"], trend[-1]["date"]
    metrics = result["metrics"]
    all_scheduled = sum(int(row["scheduled_flights"]) for row in daily_rows)
    all_operated = sum(int(row["operated_flights"]) for row in daily_rows)
    all_cancelled = sum(int(row["cancelled_flights"]) for row in daily_rows)
    all_on_time = sum(int(row["on_time_flights"]) for row in daily_rows)

    quality = [
        {
            "layer": layer,
            "name": name.replace("Bronze ", "").replace("Silver ", "").replace("Gold ", ""),
            "actual": float(value),
            "threshold": float(threshold),
            "passed": bool(passed),
            "type": quality_type(name),
        }
        for layer, name, value, threshold, passed in result["quality"]
    ]

    return {
        "generatedAt": last_date,
        "coverage": {"start": first_date, "end": last_date, "days": len(trend)},
        "metrics": {
            "scheduledFlights": all_scheduled,
            "operatedFlights": all_operated,
            "cancelledFlights": all_cancelled,
            "onTimeFlights": all_on_time,
            "passengerCount": int(metrics["passenger_count"]),
            "avgDelay": float(metrics["avg_delay"]),
            "onTimePct": float(metrics["on_time_pct"]),
            "cancellationRate": float(metrics["cancellation_rate"]),
        },
        "pipeline": {
            "bronzeRows": int(result["bronze_count"]),
            "silverRows": int(result["silver_count"]),
            "goldDailyRows": len(daily_rows),
            "goldHourlyRows": int(result["hourly_rows"]),
        },
        "trend": trend,
        "routeMetrics": route_metrics,
        "routeTrends": {route: sorted(values, key=lambda item: str(item["date"])) for route, values in route_trends.items()},
        "anomalies": anomalies,
        "quality": quality,
    }


def read_input(input_path: Path) -> dict[str, object]:
    with input_path.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        return transform(list(reader), reader.fieldnames)


def build_html(project_root: Path, model: dict[str, object]) -> str:
    template = (project_root / "frontend" / "dashboard.html").read_text(encoding="utf-8")
    styles = (project_root / "frontend" / "styles.css").read_text(encoding="utf-8")
    app = (project_root / "frontend" / "app.js").read_text(encoding="utf-8")
    serialized_model = json.dumps(model, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    return (
        template.replace("/*__DASHBOARD_STYLES__*/", styles)
        .replace("/*__DASHBOARD_DATA__*/", serialized_model)
        .replace("/*__DASHBOARD_APP__*/", app)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data/flight_operations.csv"))
    parser.add_argument("--output", type=Path, default=Path("outputs/flight_analytics_dashboard.html"))
    arguments = parser.parse_args()
    project_root = Path(__file__).resolve().parents[1]
    model = build_dashboard_model(read_input(arguments.input))
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(build_html(project_root, model), encoding="utf-8")
    print(f"Interactive dashboard written to {arguments.output}")


if __name__ == "__main__":
    main()
