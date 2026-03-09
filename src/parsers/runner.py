from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.jobs.index import load_job_run_manifest
from src.parsers.index import append_parse_run_entry
from src.parsers.registry import parse_with_registry
from src.parsers.snapshot_bridge import SnapshotBridge
from src.utils.paths import parsed_dir


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

    with parsed_jsonl.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

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

    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    append_parse_run_entry(
        {
            "job_name": job_name,
            "run_id": run_id,
            "timestamp_utc_start": started,
            "timestamp_utc_end": ended,
            "parsed_count": len(records),
            "error_count": len(errors),
            "parsed_output_path": str(parsed_jsonl),
            "summary_path": str(out_dir / "summary.json"),
        },
        index_file=parse_runs_index_file,
    )

    return summary

