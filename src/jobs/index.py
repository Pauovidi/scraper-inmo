from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.utils.paths import index_dir


def job_runs_index_file() -> Path:
    return index_dir() / "job_runs_index.jsonl"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def append_job_run_index_entry(entry: dict[str, Any], index_file: Path | None = None) -> Path:
    target = index_file or job_runs_index_file()
    _ensure_parent(target)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return target


def read_job_run_entries(index_file: Path | None = None) -> list[dict[str, Any]]:
    target = index_file or job_runs_index_file()
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


def list_job_runs(job_name: str | None = None, index_file: Path | None = None) -> list[dict[str, Any]]:
    rows = read_job_run_entries(index_file=index_file)
    if job_name:
        return [row for row in rows if row.get("job_name") == job_name]
    return rows


def find_job_run(job_name: str, run_id: str, index_file: Path | None = None) -> dict[str, Any]:
    for row in read_job_run_entries(index_file=index_file):
        if row.get("job_name") == job_name and row.get("run_id") == run_id:
            return row
    raise KeyError(f"Job run not found: job={job_name} run_id={run_id}")


def load_job_run_manifest(job_name: str, run_id: str, index_file: Path | None = None) -> dict[str, Any]:
    row = find_job_run(job_name=job_name, run_id=run_id, index_file=index_file)
    manifest_path = Path(row["manifest_path"])
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))
