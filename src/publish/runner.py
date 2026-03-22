from __future__ import annotations

import csv
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.pipeline import list_pipeline_runs, run_job_full
from src.publish.dedupe import PORTAL_ORDER, dedupe_records
from src.publish.history import (
    load_master_map,
    load_published_summary,
    master_history_path,
    published_day_dir,
    write_daily_outputs,
    write_master_records,
)
from src.publish.status_store import (
    WORKFLOW_STATUSES,
    read_status_map,
    status_store_path,
    upsert_listing_status,
    write_status_rows,
)
from src.utils.paths import history_dir, published_dir


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today_iso() -> str:
    return date.today().isoformat()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
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


def _read_export_rows(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    export_paths = manifest.get("export_paths", {}) or {}
    csv_path = Path(str(export_paths.get("csv", ""))) if export_paths.get("csv") else None
    jsonl_path = Path(str(export_paths.get("jsonl", ""))) if export_paths.get("jsonl") else None

    if csv_path and csv_path.exists():
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    if jsonl_path and jsonl_path.exists():
        return _read_jsonl(jsonl_path)

    raise FileNotFoundError("No export CSV/JSONL found for pipeline manifest")


def _pipeline_run_date(manifest: dict[str, Any]) -> str:
    timestamp = str(manifest.get("timestamp_utc_start") or "")
    return timestamp[:10] if len(timestamp) >= 10 else ""


def _latest_pipeline_manifest(job_name: str) -> tuple[dict[str, Any], Path] | None:
    rows = list_pipeline_runs(job_name=job_name)
    rows.sort(
        key=lambda row: str(row.get("timestamp_utc_end") or row.get("timestamp_utc_start") or ""),
        reverse=True,
    )

    for row in rows:
        manifest_path = row.get("manifest_path")
        if not manifest_path:
            continue
        path = Path(str(manifest_path))
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        try:
            _read_export_rows(payload)
        except FileNotFoundError:
            continue
        return payload, path
    return None


def _best_same_day_pipeline_manifest(job_name: str, publish_date: str) -> tuple[dict[str, Any], Path] | None:
    rows = list_pipeline_runs(job_name=job_name)
    candidates: list[tuple[int, str, dict[str, Any], Path]] = []

    for row in rows:
        manifest_path = row.get("manifest_path")
        if not manifest_path:
            continue
        path = Path(str(manifest_path))
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        if _pipeline_run_date(payload) != publish_date:
            continue
        try:
            export_rows = _read_export_rows(payload)
        except FileNotFoundError:
            continue
        timestamp = str(row.get("timestamp_utc_end") or row.get("timestamp_utc_start") or "")
        candidates.append((len(export_rows), timestamp, payload, path))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    _, _, payload, path = candidates[0]
    return payload, path


def _resolve_pipeline_manifest(job_name: str, publish_date: str) -> tuple[dict[str, Any], Path, bool]:
    same_day = _best_same_day_pipeline_manifest(job_name=job_name, publish_date=publish_date)
    if same_day is not None:
        return same_day[0], same_day[1], False

    latest = _latest_pipeline_manifest(job_name=job_name)
    if latest is not None and _pipeline_run_date(latest[0]) == publish_date:
        return latest[0], latest[1], False

    result = run_job_full(job_name=job_name)
    manifest_path = Path(result.manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return manifest, manifest_path, True


def _merge_workflow(
    *,
    listing_key: str,
    existing_row: dict[str, Any] | None,
    status_map: dict[str, dict[str, Any]],
    published_at: str,
) -> dict[str, Any]:
    status_record = status_map.get(listing_key)
    if status_record:
        return {
            "workflow_status": status_record.get("workflow_status", "pending"),
            "workflow_updated_at": status_record.get("workflow_updated_at", published_at),
            "workflow_note": status_record.get("workflow_note"),
        }

    if existing_row:
        record = {
            "listing_key": listing_key,
            "workflow_status": existing_row.get("workflow_status", "pending"),
            "workflow_updated_at": existing_row.get("workflow_updated_at", published_at),
            "workflow_note": existing_row.get("workflow_note"),
        }
        status_map[listing_key] = record
        return {
            "workflow_status": record["workflow_status"],
            "workflow_updated_at": record["workflow_updated_at"],
            "workflow_note": record.get("workflow_note"),
        }

    default_record = {
        "listing_key": listing_key,
        "workflow_status": "pending",
        "workflow_updated_at": published_at,
        "workflow_note": None,
    }
    status_map[listing_key] = default_record
    return {
        "workflow_status": "pending",
        "workflow_updated_at": published_at,
        "workflow_note": None,
    }


def publish_records(
    *,
    job_name: str,
    records: list[dict[str, Any]],
    publish_date: str | None = None,
    history_root_dir: Path | None = None,
    published_root_dir: Path | None = None,
    pipeline_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current_date = publish_date or _today_iso()
    published_at = _utc_now_iso()

    history_root = history_root_dir if history_root_dir is not None else history_dir()
    published_root = published_root_dir if published_root_dir is not None else published_dir()

    master_by_key = load_master_map(root_dir=history_root)
    status_map = read_status_map(root_dir=history_root)
    current_rows = dedupe_records(records)

    new_today_rows: list[dict[str, Any]] = []

    for current in current_rows:
        listing_key = str(current["listing_key"])
        existing = master_by_key.get(listing_key)
        workflow = _merge_workflow(
            listing_key=listing_key,
            existing_row=existing,
            status_map=status_map,
            published_at=published_at,
        )

        if existing:
            first_seen_date = str(existing.get("first_seen_date") or current_date)
            previous_last_seen = str(existing.get("last_seen_date") or "")
            if previous_last_seen == current_date:
                seen_count = int(existing.get("seen_count") or 1)
            else:
                seen_count = int(existing.get("seen_count") or 0) + 1
        else:
            first_seen_date = current_date
            seen_count = 1

        merged = {
            **existing,
            **current,
            **workflow,
            "first_seen_date": first_seen_date,
            "last_seen_date": current_date,
            "seen_count": seen_count,
        } if existing else {
            **current,
            **workflow,
            "first_seen_date": first_seen_date,
            "last_seen_date": current_date,
            "seen_count": seen_count,
        }

        master_by_key[listing_key] = merged
        if first_seen_date == current_date:
            new_today_rows.append(merged)

    write_status_rows(list(status_map.values()), root_dir=history_root)
    master_path = write_master_records(list(master_by_key.values()), root_dir=history_root)

    rows_by_portal: dict[str, list[dict[str, Any]]] = {portal: [] for portal in PORTAL_ORDER}
    for row in new_today_rows:
        portal = str(row.get("portal") or "")
        if portal in rows_by_portal:
            rows_by_portal[portal].append(row)

    output_paths = write_daily_outputs(
        publish_date=current_date,
        rows_by_portal=rows_by_portal,
        all_rows=new_today_rows,
        root_dir=published_root,
    )

    summary = {
        "job_name": job_name,
        "publish_date": current_date,
        "published_at": published_at,
        "source_pipeline_run_id": (pipeline_context or {}).get("pipeline_run_id"),
        "source_job_run_id": (pipeline_context or {}).get("job_run_id"),
        "source_manifest_path": (pipeline_context or {}).get("manifest_path"),
        "pipeline_executed": bool((pipeline_context or {}).get("pipeline_executed", False)),
        "input_records_count": len(records),
        "deduped_records_count": len(current_rows),
        "new_listings_count": len(new_today_rows),
        "history_total_count": len(master_by_key),
        "portal_counts": {portal: len(rows_by_portal[portal]) for portal in PORTAL_ORDER},
        "output_paths": output_paths,
        "history_paths": {
            "listings_master": str(master_path),
            "listing_status": str(status_store_path(root_dir=history_root)),
        },
    }

    summary_path = published_day_dir(publish_date=current_date, root_dir=published_root) / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    summary["summary_path"] = str(summary_path)
    return summary


def publish_daily(
    *,
    job_name: str,
    publish_date: str | None = None,
    history_root_dir: Path | None = None,
    published_root_dir: Path | None = None,
) -> dict[str, Any]:
    current_date = publish_date or _today_iso()
    manifest, manifest_path, pipeline_executed = _resolve_pipeline_manifest(job_name=job_name, publish_date=current_date)
    records = _read_export_rows(manifest)

    return publish_records(
        job_name=job_name,
        records=records,
        publish_date=current_date,
        history_root_dir=history_root_dir,
        published_root_dir=published_root_dir,
        pipeline_context={
            "pipeline_run_id": manifest.get("pipeline_run_id"),
            "job_run_id": manifest.get("job_run_id"),
            "manifest_path": str(manifest_path),
            "pipeline_executed": pipeline_executed,
        },
    )


def set_listing_status(
    *,
    listing_key: str,
    status: str,
    note: str | None = None,
    history_root_dir: Path | None = None,
) -> dict[str, Any]:
    normalized_status = str(status).strip().lower()
    if normalized_status not in WORKFLOW_STATUSES:
        raise ValueError(f"Invalid workflow status: {status}")

    history_root = history_root_dir if history_root_dir is not None else history_dir()
    status_record = upsert_listing_status(
        listing_key=listing_key,
        status=normalized_status,
        note=note,
        root_dir=history_root,
    )

    master_by_key = load_master_map(root_dir=history_root)
    if listing_key in master_by_key:
        master_by_key[listing_key]["workflow_status"] = status_record["workflow_status"]
        master_by_key[listing_key]["workflow_updated_at"] = status_record["workflow_updated_at"]
        master_by_key[listing_key]["workflow_note"] = status_record.get("workflow_note")
        write_master_records(list(master_by_key.values()), root_dir=history_root)

    return status_record


def load_client_view(publish_date: str | None = None, history_root_dir: Path | None = None, published_root_dir: Path | None = None) -> dict[str, Any]:
    current_date = publish_date or _today_iso()
    summary = load_published_summary(current_date, root_dir=published_root_dir)
    master_rows = list(load_master_map(root_dir=history_root_dir).values())
    return {
        "publish_date": current_date,
        "summary": summary,
        "history_count": len(master_rows),
        "history_path": str(master_history_path(root_dir=history_root_dir)),
        "status_path": str(status_store_path(root_dir=history_root_dir)),
    }
