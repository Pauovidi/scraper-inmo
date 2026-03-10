from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.exporters import to_business_record, write_csv, write_jsonl
from src.jobs.index import load_job_run_manifest
from src.parsers.index import append_parse_run_entry
from src.parsers.registry import parse_with_registry
from src.parsers.snapshot_bridge import SnapshotBridge
from src.utils.paths import discovered_dir, exports_dir, parsed_dir


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_snapshot(snapshot_path: str | Path) -> dict[str, Any]:
    bundle = SnapshotBridge.load(snapshot_path)
    record = parse_with_registry(bundle)
    return record.to_dict()


def parse_job_run(
    *,
    job_name: str,
    run_id: str,
    output_root_dir: Path | None = None,
    parse_runs_index_file: Path | None = None,
    job_runs_index_file: Path | None = None,
) -> dict[str, Any]:
    started = _utc_now_iso()
    manifest = load_job_run_manifest(job_name=job_name, run_id=run_id, index_file=job_runs_index_file)

    output_root = output_root_dir if output_root_dir is not None else parsed_dir() / "job_runs"
    out_dir = output_root / job_name / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    parsed_jsonl = out_dir / "parsed.jsonl"

    records: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for snapshot_path in manifest.get("snapshot_paths", []):
        try:
            record = parse_snapshot(snapshot_path)
            records.append(record)
        except Exception as exc:  # pragma: no cover
            errors.append({"snapshot_path": str(snapshot_path), "error": f"{type(exc).__name__}: {exc}"})

    write_jsonl(records, parsed_jsonl)

    ended = _utc_now_iso()
    summary = {
        "job_name": job_name,
        "run_id": run_id,
        "timestamp_utc_start": started,
        "timestamp_utc_end": ended,
        "total_snapshots": len(manifest.get("snapshot_paths", [])),
        "parsed_count": len(records),
        "error_count": len(errors),
        "parsed_output_path": str(parsed_jsonl),
        "errors": errors,
    }

    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    append_parse_run_entry(
        {
            "job_name": job_name,
            "run_id": run_id,
            "run_type": "job_run",
            "timestamp_utc_start": started,
            "timestamp_utc_end": ended,
            "parsed_count": len(records),
            "error_count": len(errors),
            "parsed_output_path": str(parsed_jsonl),
            "summary_path": str(summary_path),
        },
        index_file=parse_runs_index_file,
    )

    return summary


def _load_archived_snapshot_paths(job_name: str, run_id: str, discovery_root_dir: Path | None = None) -> list[str]:
    root = discovery_root_dir if discovery_root_dir is not None else discovered_dir() / "job_runs"
    archive_summary_path = root / job_name / run_id / "archive_summary.json"
    if not archive_summary_path.exists():
        raise FileNotFoundError(f"archive_summary.json not found: {archive_summary_path}")

    payload = json.loads(archive_summary_path.read_text(encoding="utf-8"))
    paths = payload.get("archived_snapshot_paths", [])
    if not isinstance(paths, list):
        return []
    return [str(p) for p in paths if p]


def parse_discovered(
    *,
    job_name: str,
    run_id: str,
    discovery_root_dir: Path | None = None,
    parsed_root_dir: Path | None = None,
    export_root_dir: Path | None = None,
    parse_runs_index_file: Path | None = None,
) -> dict[str, Any]:
    started = _utc_now_iso()

    snapshot_paths = _load_archived_snapshot_paths(job_name=job_name, run_id=run_id, discovery_root_dir=discovery_root_dir)

    parsed_base = parsed_root_dir if parsed_root_dir is not None else parsed_dir() / "discovered"
    parsed_out_dir = parsed_base / job_name / run_id
    parsed_out_dir.mkdir(parents=True, exist_ok=True)

    parsed_details_path = parsed_out_dir / "parsed_details.jsonl"
    parsed_summary_path = parsed_out_dir / "summary.json"

    export_base = export_root_dir if export_root_dir is not None else exports_dir()
    export_out_dir = export_base / job_name / run_id
    export_out_dir.mkdir(parents=True, exist_ok=True)

    export_jsonl_path = export_out_dir / "properties.jsonl"
    export_csv_path = export_out_dir / "properties.csv"

    records: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for snapshot_path in snapshot_paths:
        try:
            record = parse_snapshot(snapshot_path)
            records.append(record)
        except Exception as exc:  # pragma: no cover
            errors.append({"snapshot_path": str(snapshot_path), "error": f"{type(exc).__name__}: {exc}"})

    write_jsonl(records, parsed_details_path)

    business_records = [to_business_record(rec) for rec in records]
    write_jsonl(business_records, export_jsonl_path)
    write_csv(business_records, export_csv_path)

    detail_count = sum(1 for rec in records if rec.get("page_kind") == "detail")
    ended = _utc_now_iso()

    summary = {
        "job_name": job_name,
        "run_id": run_id,
        "timestamp_utc_start": started,
        "timestamp_utc_end": ended,
        "total_archived_snapshots": len(snapshot_paths),
        "parsed_count": len(records),
        "detail_count": detail_count,
        "error_count": len(errors),
        "parsed_details_path": str(parsed_details_path),
        "export_jsonl_path": str(export_jsonl_path),
        "export_csv_path": str(export_csv_path),
        "errors": errors,
    }
    parsed_summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    append_parse_run_entry(
        {
            "job_name": job_name,
            "run_id": run_id,
            "run_type": "discovered_details",
            "timestamp_utc_start": started,
            "timestamp_utc_end": ended,
            "parsed_count": len(records),
            "error_count": len(errors),
            "parsed_output_path": str(parsed_details_path),
            "summary_path": str(parsed_summary_path),
            "export_jsonl_path": str(export_jsonl_path),
            "export_csv_path": str(export_csv_path),
        },
        index_file=parse_runs_index_file,
    )

    return summary
