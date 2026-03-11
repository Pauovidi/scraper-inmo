from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.utils.paths import index_dir


def pipeline_runs_index_file() -> Path:
    return index_dir() / "pipeline_runs_index.jsonl"


def read_pipeline_run_entries(index_file: Path | None = None) -> list[dict[str, Any]]:
    target = index_file or pipeline_runs_index_file()
    if not target.exists():
        return []

    rows: list[dict[str, Any]] = []
    with target.open("r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def append_pipeline_run_entry(entry: dict[str, Any], index_file: Path | None = None) -> Path:
    target = index_file or pipeline_runs_index_file()
    target.parent.mkdir(parents=True, exist_ok=True)

    rows = read_pipeline_run_entries(index_file=target)
    pipeline_run_id = entry.get("pipeline_run_id")

    if pipeline_run_id:
        replaced = False
        updated: list[dict[str, Any]] = []
        for row in rows:
            if row.get("pipeline_run_id") == pipeline_run_id:
                updated.append(entry)
                replaced = True
            else:
                updated.append(row)
        if not replaced:
            updated.append(entry)
        rows = updated
    else:
        rows.append(entry)

    with target.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return target


def _dedupe_pipeline_runs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped_by_id: dict[str, dict[str, Any]] = {}
    passthrough: list[dict[str, Any]] = []

    for row in rows:
        pipeline_run_id = row.get("pipeline_run_id")
        if pipeline_run_id:
            # Keep the last-seen entry for a pipeline run id.
            deduped_by_id[str(pipeline_run_id)] = row
        else:
            passthrough.append(row)

    return [*passthrough, *deduped_by_id.values()]


def list_pipeline_runs(job_name: str | None = None, index_file: Path | None = None) -> list[dict[str, Any]]:
    rows = _dedupe_pipeline_runs(read_pipeline_run_entries(index_file=index_file))
    if job_name:
        rows = [row for row in rows if row.get("job_name") == job_name]
    return rows


def find_pipeline_run(job_name: str, pipeline_run_id: str, index_file: Path | None = None) -> dict[str, Any]:
    for row in _dedupe_pipeline_runs(read_pipeline_run_entries(index_file=index_file)):
        if row.get("job_name") == job_name and row.get("pipeline_run_id") == pipeline_run_id:
            return row
    raise KeyError(f"Pipeline run not found: job={job_name} pipeline_run_id={pipeline_run_id}")


def load_pipeline_run_manifest(job_name: str, pipeline_run_id: str, index_file: Path | None = None) -> dict[str, Any]:
    row = find_pipeline_run(job_name=job_name, pipeline_run_id=pipeline_run_id, index_file=index_file)
    manifest_path = Path(str(row.get("manifest_path", "")))
    if not manifest_path.exists():
        raise FileNotFoundError(f"Pipeline manifest not found: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))
