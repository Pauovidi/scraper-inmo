from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.utils.paths import index_dir


def parse_runs_index_file() -> Path:
    return index_dir() / "parse_runs_index.jsonl"


def append_parse_run_entry(entry: dict[str, Any], index_file: Path | None = None) -> Path:
    target = index_file or parse_runs_index_file()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return target
