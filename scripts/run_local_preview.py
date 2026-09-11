#!/usr/bin/env python3
"""Run the Fabric pipeline logic locally and render a dependency-free dashboard preview."""

from __future__ import annotations

import argparse
import csv
import html
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
ON_TIME_DELAY_THRESHOLD = 15
Z_SCORE_THRESHOLD = 3.0
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


def parse_timestamp(value: str) -> datetime | None:
    return datetime.strptime(value, TIMESTAMP_FORMAT) if value else None


def parse_integer(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_row(row: dict[str, str]) -> dict[str, object]:
    return {
        "flight_id": row["flight_id"].strip(),
        "origin": row["origin"].strip().upper(),
        "dest": row["dest"].strip().upper(),
        "scheduled_dep_ts": parse_timestamp(row["scheduled_dep"].strip()),
        "actual_dep_ts": parse_timestamp(row["actual_dep"].strip()),
        "delay_min": parse_integer(row["delay_min"].strip()) or 0,
        "status": row["status"].strip().upper(),
        "pax_count": parse_integer(row["pax_count"].strip()),
    }


def deduplication_key(row: dict[str, object]) -> tuple[datetime, int, int]:
    return (
        row["actual_dep_ts"] or datetime.min,
        int(row["delay_min"]),
        int(row["pax_count"] or -1),
    )


def rate(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def format_number(value: float | int) -> str:
    return f"{value:,.1f}" if isinstance(value, float) else f"{value:,}"


def format_quality_value(name: str, value: float | int) -> str:
    if "rate" in name.lower() or "difference" in name.lower():
        return f"{float(value):.2%}"
    return format_number(value)


def transform(rows: list[dict[str, str]], fieldnames: list[str] | None) -> dict[str, object]:
    bronze_count = len(rows)
    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(fieldnames or []))
    bronze_quality = [
        ("Required columns", len(missing_columns), 0, not missing_columns),
        ("Bronze row count", bronze_count, 1, bronze_count > 0),
    ]
    for column in ("flight_id", "origin", "dest"):
        null_count = sum(not row[column].strip() for row in rows)
        bronze_quality.append((f"Bronze {column} null rate", rate(null_count, bronze_count), 0.01, rate(null_count, bronze_count) < 0.01))

    typed_rows = [normalize_row(row) for row in rows]
    latest_rows: dict[str, dict[str, object]] = {}
    for row in typed_rows:
        flight_id = str(row["flight_id"])
        if flight_id not in latest_rows or deduplication_key(row) > deduplication_key(latest_rows[flight_id]):
            latest_rows[flight_id] = row

    silver_rows = []
    for row in latest_rows.values():
        if not row["flight_id"] or not row["origin"] or not row["dest"]:
            continue
        row["flight_date"] = row["scheduled_dep_ts"].date() if row["scheduled_dep_ts"] else None
        row["delay_flag"] = int(row["delay_min"]) > ON_TIME_DELAY_THRESHOLD
        row["route"] = f"{row['origin']} → {row['dest']}"
        silver_rows.append(row)

    silver_count = len(silver_rows)
    silver_quality = [
        (
            "Bronze/Silver row-count difference",
            rate(abs(silver_count - bronze_count), bronze_count),
            0.05,
            rate(abs(silver_count - bronze_count), bronze_count) <= 0.05,
        ),
        ("Duplicate records removed", bronze_count - len(latest_rows), 0, True),
    ]
    for column in ("flight_id", "origin", "dest", "scheduled_dep_ts", "delay_min", "status", "pax_count"):
        null_count = sum(row[column] is None or row[column] == "" for row in silver_rows)
        silver_quality.append((f"Silver {column} null rate", rate(null_count, silver_count), 0.01, rate(null_count, silver_count) < 0.01))

    daily_accumulators: dict[tuple[object, str, str], dict[str, int]] = defaultdict(
        lambda: {
            "scheduled_flights": 0,
            "operated_flights": 0,
            "cancelled_flights": 0,
            "passenger_count": 0,
            "delay_total": 0,
            "on_time_flights": 0,
        }
    )
    hourly_volume: Counter[tuple[object, int, str, str]] = Counter()
    for row in silver_rows:
        key = (row["flight_date"], str(row["origin"]), str(row["dest"]))
        metrics = daily_accumulators[key]
        metrics["scheduled_flights"] += 1
        metrics["passenger_count"] += int(row["pax_count"] or 0)
        if row["status"] == "CANCELLED":
            metrics["cancelled_flights"] += 1
        else:
            metrics["operated_flights"] += 1
            metrics["delay_total"] += int(row["delay_min"])
            if int(row["delay_min"]) <= ON_TIME_DELAY_THRESHOLD:
                metrics["on_time_flights"] += 1
        scheduled = row["scheduled_dep_ts"]
        if scheduled:
            hourly_volume[(row["flight_date"], scheduled.hour, str(row["origin"]), str(row["dest"]))] += int(row["pax_count"] or 0)

    daily_rows = []
    for (flight_date, origin, destination), metrics in daily_accumulators.items():
        operated = metrics["operated_flights"]
        scheduled = metrics["scheduled_flights"]
        daily_rows.append(
            {
                "flight_date": flight_date,
                "origin": origin,
                "dest": destination,
                "route": f"{origin} → {destination}",
                **metrics,
                "avg_delay_min": metrics["delay_total"] / operated if operated else None,
                "on_time_pct": metrics["on_time_flights"] / operated if operated else None,
                "cancellation_rate": metrics["cancelled_flights"] / scheduled,
            }
        )

    route_delays: dict[str, list[float]] = defaultdict(list)
    for row in daily_rows:
        if row["avg_delay_min"] is not None:
            route_delays[str(row["route"])].append(float(row["avg_delay_min"]))
    for row in daily_rows:
        values = route_delays[str(row["route"])]
        if row["avg_delay_min"] is None or len(values) < 2:
            row["delay_z_score"] = 0.0
        else:
            deviation = statistics.stdev(values)
            row["delay_z_score"] = (float(row["avg_delay_min"]) - statistics.mean(values)) / deviation if deviation else 0.0
        row["delay_anomaly_flag"] = abs(float(row["delay_z_score"])) >= Z_SCORE_THRESHOLD

    route_summary: dict[str, dict[str, float]] = defaultdict(lambda: {"delay_total": 0.0, "operated": 0.0})
    trend_summary: dict[object, dict[str, int]] = defaultdict(lambda: {"on_time": 0, "operated": 0})
    for row in daily_rows:
        summary = route_summary[str(row["route"])]
        summary["delay_total"] += float(row["avg_delay_min"] or 0) * int(row["operated_flights"])
        summary["operated"] += int(row["operated_flights"])
        trend_summary[row["flight_date"]]["on_time"] += int(row["on_time_flights"])
        trend_summary[row["flight_date"]]["operated"] += int(row["operated_flights"])

    route_comparison = sorted(
        (
            (route_name, values["delay_total"] / values["operated"] if values["operated"] else 0.0)
            for route_name, values in route_summary.items()
        ),
        key=lambda item: item[1],
        reverse=True,
    )[:10]
    trend = sorted(
        (
            (flight_date, values["on_time"] / values["operated"] if values["operated"] else 0.0)
            for flight_date, values in trend_summary.items()
        ),
        key=lambda item: item[0],
    )
    anomalies = sorted(
        (row for row in daily_rows if row["delay_anomaly_flag"]),
        key=lambda row: abs(float(row["delay_z_score"])),
        reverse=True,
    )[:12]
    avg_delay = sum(float(row["delay_total"]) for row in daily_rows) / sum(int(row["operated_flights"]) for row in daily_rows)
    on_time_pct = sum(int(row["on_time_flights"]) for row in daily_rows) / sum(int(row["operated_flights"]) for row in daily_rows)
    cancellation_rate = sum(int(row["cancelled_flights"]) for row in daily_rows) / silver_count
    gold_quality = [
        ("Daily route row count", len(daily_rows), 1, bool(daily_rows)),
        ("Hourly volume row count", len(hourly_volume), 1, bool(hourly_volume)),
        ("Gold avg_delay_min null rate", rate(sum(row["avg_delay_min"] is None for row in daily_rows), len(daily_rows)), 0.01, rate(sum(row["avg_delay_min"] is None for row in daily_rows), len(daily_rows)) < 0.01),
        ("Silver-to-Gold freshness (minutes)", 0.0, 60.0, True),
        ("Delay anomalies detected", len(anomalies), Z_SCORE_THRESHOLD, True),
    ]

    return {
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "daily_rows": daily_rows,
        "hourly_rows": len(hourly_volume),
        "anomalies": anomalies,
        "route_comparison": route_comparison,
        "trend": trend,
        "metrics": {
            "avg_delay": avg_delay,
            "on_time_pct": on_time_pct,
            "cancellation_rate": cancellation_rate,
            "passenger_count": sum(int(row["passenger_count"]) for row in daily_rows),
        },
        "quality": [("Bronze", *record) for record in bronze_quality]
        + [("Silver", *record) for record in silver_quality]
        + [("Gold", *record) for record in gold_quality],
    }


def trend_svg(trend: list[tuple[object, float]]) -> str:
    width, height, padding = 920, 250, 36
    values = [value for _, value in trend]
    if not values:
        return ""
    coordinates = []
    for index, value in enumerate(values):
        x = padding + (width - 2 * padding) * index / max(len(values) - 1, 1)
        y = height - padding - (height - 2 * padding) * value
        coordinates.append(f"{x:.1f},{y:.1f}")
    first_date = trend[0][0].isoformat()
    last_date = trend[-1][0].isoformat()
    return f"""
    <svg viewBox=\"0 0 {width} {height}\" role=\"img\" aria-label=\"On-time performance trend\">
      <line x1=\"{padding}\" y1=\"{height - padding}\" x2=\"{width - padding}\" y2=\"{height - padding}\" class=\"axis\" />
      <line x1=\"{padding}\" y1=\"{padding}\" x2=\"{padding}\" y2=\"{height - padding}\" class=\"axis\" />
      <text x=\"4\" y=\"{padding + 5}\">100%</text><text x=\"10\" y=\"{height - padding}\">0%</text>
      <polyline points=\"{' '.join(coordinates)}\" class=\"trend\" />
      <text x=\"{padding}\" y=\"{height - 8}\">{first_date}</text>
      <text x=\"{width - 110}\" y=\"{height - 8}\">{last_date}</text>
    </svg>"""


def route_bars(routes: list[tuple[str, float]]) -> str:
    max_delay = max((delay for _, delay in routes), default=1.0)
    rows = []
    for route, delay in routes:
        width = 100 * delay / max_delay if max_delay else 0
        rows.append(
            f"<div class=\"bar-row\"><span>{html.escape(route)}</span><div class=\"bar-track\"><div class=\"bar\" style=\"width:{width:.1f}%\"></div></div><strong>{delay:.1f}m</strong></div>"
        )
    return "".join(rows)


def render_dashboard(result: dict[str, object], output_path: Path) -> None:
    metrics = result["metrics"]
    anomalies = result["anomalies"]
    quality = result["quality"]
    anomaly_rows = "".join(
        f"<tr><td>{row['flight_date'].isoformat()}</td><td>{html.escape(str(row['route']))}</td><td>{float(row['avg_delay_min']):.1f}</td><td>{float(row['delay_z_score']):.2f}</td><td>{float(row['cancellation_rate']):.1%}</td></tr>"
        for row in anomalies
    ) or "<tr><td colspan=\"5\">No route-day delay anomalies exceeded the z-score threshold.</td></tr>"
    quality_rows = "".join(
        f"<tr><td>{layer}</td><td>{html.escape(name)}</td><td>{format_quality_value(name, value)}</td><td>{format_quality_value(name, threshold)}</td><td><span class=\"{'pass' if passed else 'fail'}\">{'PASS' if passed else 'FAIL'}</span></td></tr>"
        for layer, name, value, threshold, passed in quality
    )
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        f"""<!doctype html>
<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>Flight Operations Analytics Preview</title>
<style>
:root {{ color-scheme: dark; font-family: Inter, ui-sans-serif, system-ui, sans-serif; background: #08111f; color: #e9f0f8; }}
body {{ margin: 0; background: linear-gradient(140deg, #08111f, #102a43 70%, #0f766e); min-height: 100vh; }}
main {{ max-width: 1180px; margin: auto; padding: 44px 24px 64px; }} h1 {{ margin: 0; font-size: clamp(2rem, 5vw, 3.2rem); }} h2 {{ margin-top: 0; font-size: 1.15rem; }} .eyebrow {{ color: #7dd3fc; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; }} .subtitle {{ color: #bdd1e5; margin: 10px 0 32px; }}
.cards {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; }} .card, .panel {{ background: rgba(9, 24, 42, .86); border: 1px solid rgba(125, 211, 252, .2); border-radius: 16px; box-shadow: 0 14px 32px rgba(0,0,0,.2); }} .card {{ padding: 20px; }} .label {{ color: #a7c0d8; font-size: .82rem; }} .value {{ font-size: 1.75rem; font-weight: 800; margin-top: 6px; }} .grid {{ display: grid; grid-template-columns: 1.1fr .9fr; gap: 18px; margin-top: 18px; }} .panel {{ padding: 20px; overflow: hidden; }}
svg {{ width: 100%; height: auto; }} .axis {{ stroke: #466784; stroke-width: 1; }} svg text {{ fill: #9db5ca; font-size: 12px; }} .trend {{ fill: none; stroke: #34d399; stroke-width: 3; stroke-linecap: round; stroke-linejoin: round; }} .bar-row {{ display: grid; grid-template-columns: 92px 1fr 45px; gap: 10px; align-items: center; margin: 11px 0; font-size: .85rem; }} .bar-track {{ height: 10px; background: #1f3b57; border-radius: 99px; overflow: hidden; }} .bar {{ height: 100%; background: linear-gradient(90deg, #38bdf8, #f59e0b); border-radius: inherit; }}
table {{ width: 100%; border-collapse: collapse; font-size: .85rem; }} th, td {{ padding: 10px 8px; text-align: left; border-bottom: 1px solid rgba(157,181,202,.16); }} th {{ color: #a7c0d8; }} .pass {{ color: #5eead4; font-weight: 800; }} .fail {{ color: #fda4af; font-weight: 800; }} footer {{ color: #9db5ca; font-size: .82rem; margin-top: 24px; }}
@media (max-width: 760px) {{ .cards, .grid {{ grid-template-columns: 1fr; }} }}
</style></head><body><main>
<p class=\"eyebrow\">Local execution preview</p><h1>Flight Operations Analytics</h1><p class=\"subtitle\">Bronze → Silver → Gold simulation using the committed synthetic flight dataset.</p>
<section class=\"cards\"><article class=\"card\"><div class=\"label\">On-time performance</div><div class=\"value\">{float(metrics['on_time_pct']):.1%}</div></article><article class=\"card\"><div class=\"label\">Average delay</div><div class=\"value\">{float(metrics['avg_delay']):.1f} min</div></article><article class=\"card\"><div class=\"label\">Cancellation rate</div><div class=\"value\">{float(metrics['cancellation_rate']):.1%}</div></article><article class=\"card\"><div class=\"label\">Scheduled passengers</div><div class=\"value\">{int(metrics['passenger_count']):,}</div></article></section>
<section class=\"grid\"><article class=\"panel\"><h2>On-time performance trend</h2>{trend_svg(result['trend'])}</article><article class=\"panel\"><h2>Routes with highest average delay</h2>{route_bars(result['route_comparison'])}</article></section>
<section class=\"grid\"><article class=\"panel\"><h2>Delay anomaly alerts</h2><table><thead><tr><th>Date</th><th>Route</th><th>Avg delay</th><th>Z-score</th><th>Cancellation</th></tr></thead><tbody>{anomaly_rows}</tbody></table></article><article class=\"panel\"><h2>Pipeline outputs</h2><table><tbody><tr><td>Bronze rows</td><td>{int(result['bronze_count']):,}</td></tr><tr><td>Silver rows</td><td>{int(result['silver_count']):,}</td></tr><tr><td>Gold daily route rows</td><td>{len(result['daily_rows']):,}</td></tr><tr><td>Gold hourly volume rows</td><td>{int(result['hourly_rows']):,}</td></tr><tr><td>Detected anomalies</td><td>{len(anomalies):,}</td></tr></tbody></table></article></section>
<section class=\"panel\" style=\"margin-top:18px\"><h2>Inline data-quality checks</h2><table><thead><tr><th>Layer</th><th>Check</th><th>Actual</th><th>Threshold</th><th>Status</th></tr></thead><tbody>{quality_rows}</tbody></table></section>
<footer>Generated locally at {generated_at}. Fabric execution remains the production path; this preview mirrors its transformations without requiring a Fabric tenant.</footer></main></body></html>""",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data/flight_operations.csv"))
    parser.add_argument("--output", type=Path, default=Path("outputs/flight_analytics_preview.html"))
    arguments = parser.parse_args()
    with arguments.input.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        result = transform(list(reader), reader.fieldnames)
    render_dashboard(result, arguments.output)
    print(
        f"Preview written to {arguments.output}: {result['bronze_count']:,} Bronze rows, "
        f"{result['silver_count']:,} Silver rows, {len(result['daily_rows']):,} Gold route-day rows."
    )


if __name__ == "__main__":
    main()
