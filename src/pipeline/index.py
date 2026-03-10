from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.utils.paths import index_dir


def pipeline_runs_index_file() -> Path:
    return index_dir() / "pipeline_runs_index.jsonl"


def append_pipeline_run_entry(entry: dict[str, Any], index_file: Path | None = None) -> Path:
    target = index_file or pipeline_runs_index_file()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return target


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
