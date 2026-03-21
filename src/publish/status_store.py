from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.paths import history_dir

WORKFLOW_STATUSES = {"pending", "processed", "discarded"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def status_store_path(root_dir: Path | None = None) -> Path:
    base = root_dir if root_dir is not None else history_dir()
    base.mkdir(parents=True, exist_ok=True)
    return base / "listing_status.jsonl"


def read_status_rows(root_dir: Path | None = None) -> list[dict[str, Any]]:
    path = status_store_path(root_dir=root_dir)
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


def read_status_map(root_dir: Path | None = None) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("listing_key")): row
        for row in read_status_rows(root_dir=root_dir)
        if row.get("listing_key")
    }


def write_status_rows(rows: list[dict[str, Any]], root_dir: Path | None = None) -> Path:
    path = status_store_path(root_dir=root_dir)
    sorted_rows = sorted(rows, key=lambda row: str(row.get("listing_key", "")))
    with path.open("w", encoding="utf-8") as handle:
        for row in sorted_rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path


def upsert_listing_status(
    *,
    listing_key: str,
    status: str,
    note: str | None = None,
    root_dir: Path | None = None,
    updated_at: str | None = None,
) -> dict[str, Any]:
    normalized_status = str(status).strip().lower()
    if normalized_status not in WORKFLOW_STATUSES:
        raise ValueError(f"Invalid workflow status: {status}")

    if not listing_key:
        raise ValueError("listing_key is required")

    status_map = read_status_map(root_dir=root_dir)
    existing = status_map.get(listing_key, {})
    record = {
        "listing_key": listing_key,
        "workflow_status": normalized_status,
        "workflow_updated_at": updated_at or _utc_now_iso(),
        "workflow_note": note if note is not None else existing.get("workflow_note"),
    }
    status_map[listing_key] = record
    write_status_rows(list(status_map.values()), root_dir=root_dir)
    return record
