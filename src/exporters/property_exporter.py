from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

BUSINESS_FIELDS = [
    "source_domain",
    "url_final",
    "title",
    "price_text",
    "location_text",
    "surface_text",
    "rooms_text",
    "description_text",
    "confidence_score",
    "snapshot_path",
    "parser_key",
    "parse_status",
]


def to_business_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: record.get(key) for key in BUSINESS_FIELDS}


def write_jsonl(records: list[dict[str, Any]], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for row in records:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return output_path


def write_csv(records: list[dict[str, Any]], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=BUSINESS_FIELDS)
        writer.writeheader()
        for row in records:
            writer.writerow({key: row.get(key) for key in BUSINESS_FIELDS})
    return output_path
