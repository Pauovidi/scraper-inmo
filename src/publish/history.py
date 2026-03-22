from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from src.publish.dedupe import PORTAL_ORDER
from src.utils.paths import history_dir, published_dir

MASTER_FIELDS = [
    "portal",
    "source_domain",
    "listing_key",
    "external_id",
    "canonical_url",
    "dedupe_method",
    "url_final",
    "title",
    "price_text",
    "price_value",
    "location_text",
    "surface_sqm",
    "rooms_count",
    "first_seen_date",
    "last_seen_date",
    "seen_count",
    "workflow_status",
    "workflow_updated_at",
    "workflow_note",
    "parser_key",
    "parse_status",
]

PUBLISHED_FIELDS = [
    "portal",
    "source_domain",
    "listing_key",
    "workflow_status",
    "workflow_updated_at",
    "workflow_note",
    "title",
    "price_text",
    "price_value",
    "location_text",
    "surface_sqm",
    "rooms_count",
    "url_final",
    "first_seen_date",
    "last_seen_date",
    "seen_count",
    "parser_key",
    "parse_status",
]


def master_history_path(root_dir: Path | None = None) -> Path:
    base = root_dir if root_dir is not None else history_dir()
    base.mkdir(parents=True, exist_ok=True)
    return base / "listings_master.jsonl"


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _write_jsonl_rows(rows: list[dict[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path


def load_master_records(root_dir: Path | None = None) -> list[dict[str, Any]]:
    return _read_jsonl_rows(master_history_path(root_dir=root_dir))


def load_master_map(root_dir: Path | None = None) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("listing_key")): row
        for row in load_master_records(root_dir=root_dir)
        if row.get("listing_key")
    }


def write_master_records(records: list[dict[str, Any]], root_dir: Path | None = None) -> Path:
    sorted_rows = sorted(
        records,
        key=lambda row: (
            str(row.get("portal", "")),
            str(row.get("last_seen_date", "")),
            str(row.get("title", "")),
        ),
        reverse=True,
    )
    return _write_jsonl_rows(sorted_rows, master_history_path(root_dir=root_dir))


def published_day_dir(publish_date: str, root_dir: Path | None = None) -> Path:
    base = root_dir if root_dir is not None else published_dir()
    target = base / publish_date
    target.mkdir(parents=True, exist_ok=True)
    return target


def list_published_dates(root_dir: Path | None = None) -> list[str]:
    base = root_dir if root_dir is not None else published_dir()
    if not base.exists():
        return []
    return sorted([entry.name for entry in base.iterdir() if entry.is_dir()], reverse=True)


def load_published_summary(publish_date: str, root_dir: Path | None = None) -> dict[str, Any] | None:
    summary_path = published_day_dir(publish_date=publish_date, root_dir=root_dir) / "summary.json"
    if not summary_path.exists():
        return None
    try:
        return json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def write_published_csv(rows: list[dict[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PUBLISHED_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in PUBLISHED_FIELDS})
    return path


def write_daily_outputs(
    *,
    publish_date: str,
    rows_by_portal: dict[str, list[dict[str, Any]]],
    all_rows: list[dict[str, Any]] | None = None,
    root_dir: Path | None = None,
) -> dict[str, str]:
    out_dir = published_day_dir(publish_date=publish_date, root_dir=root_dir)
    output_paths: dict[str, str] = {}

    collected_rows: list[dict[str, Any]] = []
    for portal in PORTAL_ORDER:
        rows = rows_by_portal.get(portal, [])
        collected_rows.extend(rows)
        csv_path = out_dir / f"{portal}.csv"
        write_published_csv(rows, csv_path)
        output_paths[portal] = str(csv_path)

    all_csv_path = out_dir / "all.csv"
    write_published_csv(all_rows if all_rows is not None else collected_rows, all_csv_path)
    output_paths["all"] = str(all_csv_path)
    return output_paths
