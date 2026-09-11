#!/usr/bin/env python3
"""Create importable Jupyter notebooks from the version-controlled PySpark sources."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = ROOT / "notebook_sources"
NOTEBOOK_DIR = ROOT / "notebooks"


def notebook_payload(source_path: Path) -> dict:
    title = source_path.stem.replace("_", " ").title()
    source = source_path.read_text(encoding="utf-8").splitlines(keepends=True)
    return {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    f"# {title}\\n",
                    "Microsoft Fabric Lakehouse notebook. Attach the target Lakehouse before running.\\n",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": source,
            },
        ],
        "metadata": {
            "kernelspec": {"display_name": "PySpark", "language": "python", "name": "pyspark"},
            "language_info": {"name": "python", "version": "3.11"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main() -> None:
    NOTEBOOK_DIR.mkdir(exist_ok=True)
    for source_path in sorted(SOURCE_DIR.glob("*.py")):
        target_path = NOTEBOOK_DIR / f"{source_path.stem}.ipynb"
        target_path.write_text(json.dumps(notebook_payload(source_path), indent=2), encoding="utf-8")
        print(f"Wrote {target_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

