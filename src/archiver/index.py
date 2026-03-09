from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.utils.paths import data_dir, repo_root


def index_dir() -> Path:
    return data_dir() / "index"


def snapshots_index_file() -> Path:
    return index_dir() / "snapshots_index.jsonl"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def append_snapshot_index_entry(entry: dict[str, Any], index_file: Path | None = None) -> Path:
    target = index_file or snapshots_index_file()
    _ensure_parent(target)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return target


def read_index_entries(index_file: Path | None = None) -> list[dict[str, Any]]:
    target = index_file or snapshots_index_file()
    if not target.exists():
        return []

    entries: list[dict[str, Any]] = []
    with target.open("r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def list_snapshots(
    *,
    domain: str | None = None,
    date: str | None = None,
    status: str | None = None,
    index_file: Path | None = None,
) -> list[dict[str, Any]]:
    entries = read_index_entries(index_file=index_file)
    filtered: list[dict[str, Any]] = []
    for entry in entries:
        if domain and entry.get("domain") != domain:
            continue
        if date and entry.get("date") != date:
            continue
        if status and entry.get("status") != status:
            continue
        filtered.append(entry)
    return filtered


def find_previous_same_url_day(
    *,
    url_original: str,
    date: str,
    index_file: Path | None = None,
) -> list[dict[str, Any]]:
    entries = read_index_entries(index_file=index_file)
    previous = [
        item for item in entries if item.get("url_original") == url_original and item.get("date") == date
    ]
    return previous


def resolve_meta_path(snapshot_path: str | Path) -> Path:
    path = Path(snapshot_path)
    if not path.is_absolute():
        path = repo_root() / path

    if path.is_dir():
        candidate = path / "meta.json"
    else:
        candidate = path if path.name == "meta.json" else path.parent / "meta.json"

    return candidate


def load_snapshot_meta(snapshot_path: str | Path) -> dict[str, Any]:
    meta_path = resolve_meta_path(snapshot_path)
    if not meta_path.exists():
        raise FileNotFoundError(f"meta.json not found at: {meta_path}")
    return json.loads(meta_path.read_text(encoding="utf-8"))
