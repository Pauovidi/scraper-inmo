from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from src.archiver.service import archive_url
from src.config import resolve_job_plan
from src.jobs.index import append_job_run_index_entry
from src.utils.paths import job_runs_dir


@dataclass
class JobRunResult:
    job_name: str
    run_id: str
    manifest_path: Path
    total_urls: int
    ok_count: int
    partial_count: int
    error_count: int


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _make_run_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"


def run_job(
    job_name: str,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
    archive_fn=archive_url,
    archive_output_base_dir: Path | None = None,
    snapshot_index_file: Path | None = None,
    manifest_root_dir: Path | None = None,
    job_runs_index_file: Path | None = None,
) -> JobRunResult:
    started = _utc_now_iso()
    run_id = _make_run_id()

    plan = resolve_job_plan(job_name)
    job = plan["job"]
    url_items = plan["url_items"]

    base_dir = manifest_root_dir if manifest_root_dir is not None else job_runs_dir()
    run_dir = base_dir / job_name / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "manifest.json"

    ok_count = 0
    partial_count = 0
    error_count = 0
    snapshot_paths: list[str] = []
    errors_summary: list[dict[str, Any]] = []
    url_results: list[dict[str, Any]] = []

    for item in url_items:
        url = item["url"]
        source_domain = item["source_domain"]
        rate_limit = float(item.get("rate_limit_seconds", 0) or 0)
        timeout_seconds = int(item.get("timeout_seconds", 20) or 20)

        status = "error"
        try:
            result = archive_fn(
                url=url,
                timeout=timeout_seconds,
                output_base_dir=archive_output_base_dir,
                index_file=snapshot_index_file,
            )
            status = result.status
            snapshot_paths.append(str(result.output_dir))
            if status == "ok":
                ok_count += 1
            elif status == "partial":
                partial_count += 1
            else:
                error_count += 1
                errors_summary.append({"url": url, "source": source_domain, "error": "archive_status_error"})
        except Exception as exc:  # pragma: no cover
            error_count += 1
            errors_summary.append(
                {
                    "url": url,
                    "source": source_domain,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

        url_results.append(
            {
                "url": url,
                "source_domain": source_domain,
                "status": status,
                "timeout_seconds": timeout_seconds,
                "rate_limit_seconds": rate_limit,
            }
        )

        if rate_limit > 0:
            sleep_fn(rate_limit)

    ended = _utc_now_iso()
    manifest: dict[str, Any] = {
        "job_name": job_name,
        "run_id": run_id,
        "timestamp_utc_start": started,
        "timestamp_utc_end": ended,
        "sources_resolved": {
            "included": [src["domain"] for src in plan["included_sources"]],
            "excluded": plan["excluded_sources"],
        },
        "start_urls": [item["url"] for item in url_items],
        "duplicate_start_urls_skipped": plan["duplicate_start_urls_skipped"],
        "total_urls": len(url_items),
        "ok_count": ok_count,
        "partial_count": partial_count,
        "error_count": error_count,
        "snapshot_paths": snapshot_paths,
        "errors_summary": errors_summary,
        "url_results": url_results,
        "job_filters": job.get("filters", {}),
        "job_max_urls": job.get("max_urls"),
    }

    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    index_entry = {
        "job_name": job_name,
        "run_id": run_id,
        "timestamp_utc_start": started,
        "timestamp_utc_end": ended,
        "total_urls": len(url_items),
        "ok_count": ok_count,
        "partial_count": partial_count,
        "error_count": error_count,
        "manifest_path": str(manifest_path),
    }
    append_job_run_index_entry(index_entry, index_file=job_runs_index_file)

    return JobRunResult(
        job_name=job_name,
        run_id=run_id,
        manifest_path=manifest_path,
        total_urls=len(url_items),
        ok_count=ok_count,
        partial_count=partial_count,
        error_count=error_count,
    )

