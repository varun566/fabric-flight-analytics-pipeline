from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from scripts.generate_flight_ops import FIELDNAMES, generate


class FlightOperationsGeneratorTests(unittest.TestCase):
    def test_generates_required_schema_and_expected_row_count(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_path = Path(temporary_directory) / "flight_operations.csv"
            total_rows = generate(output_path, primary_rows=10_000, duplicate_rows=100, seed=7)

            with output_path.open(newline="", encoding="utf-8") as csv_file:
                reader = csv.DictReader(csv_file)
                rows = list(reader)

        self.assertEqual(total_rows, 10_100)
        self.assertEqual(reader.fieldnames, FIELDNAMES)
        self.assertEqual(len(rows), 10_100)
        self.assertTrue(all(row["flight_id"] for row in rows))


if __name__ == "__main__":
    unittest.main()

