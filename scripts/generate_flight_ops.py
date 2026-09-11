#!/usr/bin/env python3
"""Generate a deterministic, Fabric-ready synthetic flight-operations CSV."""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


AIRPORTS = [
    "ATL",
    "BOS",
    "DEN",
    "DFW",
    "JFK",
    "LAS",
    "LAX",
    "MIA",
    "ORD",
    "PHX",
    "SEA",
    "SFO",
]
FIELDNAMES = [
    "flight_id",
    "origin",
    "dest",
    "scheduled_dep",
    "actual_dep",
    "delay_min",
    "status",
    "pax_count",
]


def build_flight(flight_number: int, start_date: datetime, rng: random.Random) -> dict[str, str]:
    origin = rng.choice(AIRPORTS)
    destination = rng.choice([airport for airport in AIRPORTS if airport != origin])
    scheduled_departure = start_date + timedelta(
        days=rng.randrange(90), minutes=rng.randrange(24 * 60)
    )
    is_cancelled = rng.random() < 0.022
    delay = max(-10, round(rng.gauss(8, 22)))

    if is_cancelled:
        actual_departure = ""
        delay_value = ""
        status = "CANCELLED"
    else:
        actual_departure = scheduled_departure + timedelta(minutes=delay)
        delay_value = str(delay)
        status = "DELAYED" if delay > 15 else "ON_TIME"

    record = {
        "flight_id": f"FLT{flight_number:07d}",
        "origin": origin,
        "dest": destination,
        "scheduled_dep": scheduled_departure.strftime("%Y-%m-%d %H:%M:%S"),
        "actual_dep": actual_departure.strftime("%Y-%m-%d %H:%M:%S")
        if actual_departure
        else "",
        "delay_min": delay_value,
        "status": status,
        "pax_count": str(rng.randint(45, 230)),
    }

    if not is_cancelled and rng.random() < 0.004:
        record["delay_min"] = ""
    if rng.random() < 0.001:
        record["origin"] = ""
    if rng.random() < 0.001:
        record["dest"] = ""
    return record


def generate(output_path: Path, primary_rows: int, duplicate_rows: int, seed: int) -> int:
    rng = random.Random(seed)
    start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=90)
    records = [build_flight(index, start_date, rng) for index in range(1, primary_rows + 1)]

    duplicates = rng.sample(records, duplicate_rows)
    for record in duplicates:
        duplicate = record.copy()
        if duplicate["status"] != "CANCELLED" and duplicate["delay_min"]:
            duplicate_delay = int(duplicate["delay_min"]) + rng.choice([-5, 5, 10])
            scheduled = datetime.strptime(duplicate["scheduled_dep"], "%Y-%m-%d %H:%M:%S")
            duplicate["delay_min"] = str(duplicate_delay)
            duplicate["actual_dep"] = (scheduled + timedelta(minutes=duplicate_delay)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            duplicate["status"] = "DELAYED" if duplicate_delay > 15 else "ON_TIME"
        records.append(duplicate)

    rng.shuffle(records)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(records)
    return len(records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/flight_operations.csv"),
        help="CSV path to create (default: data/flight_operations.csv).",
    )
    parser.add_argument("--primary-rows", type=int, default=24_000)
    parser.add_argument("--duplicate-rows", type=int, default=600)
    parser.add_argument("--seed", type=int, default=42)
    arguments = parser.parse_args()

    if arguments.primary_rows < 10_000:
        parser.error("--primary-rows must be at least 10,000")
    if not 0 <= arguments.duplicate_rows < arguments.primary_rows:
        parser.error("--duplicate-rows must be non-negative and less than --primary-rows")

    total_rows = generate(
        arguments.output,
        arguments.primary_rows,
        arguments.duplicate_rows,
        arguments.seed,
    )
    print(f"Wrote {total_rows:,} records to {arguments.output}")


if __name__ == "__main__":
    main()
