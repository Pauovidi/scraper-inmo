from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def data_dir() -> Path:
    return repo_root() / "data"


def logs_dir() -> Path:
    return data_dir() / "logs"


def snapshots_dir() -> Path:
    return data_dir() / "snapshots"


def index_dir() -> Path:
    return data_dir() / "index"


def job_runs_dir() -> Path:
    return data_dir() / "job_runs"


def parsed_dir() -> Path:
    return data_dir() / "parsed"


def discovered_dir() -> Path:
    return data_dir() / "discovered"


def exports_dir() -> Path:
    return data_dir() / "exports"
