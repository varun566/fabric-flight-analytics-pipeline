# Data Files

`flight_operations.csv` is a deterministic synthetic flight-operations dataset. It contains 24,000 primary rows and 600 repeat `flight_id` records, along with a very small number of intentional nulls for validating the Silver-layer rules.

Regenerate it with:

```bash
python3 scripts/generate_flight_ops.py
```

Upload both `flight_operations.csv` and `config/pipeline_config.json` to the Lakehouse `Files` area as described in the root README.

