from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from src.discovery.runner import archive_discovered, discover_job_run
from src.jobs.runner import run_job
from src.parsers.runner import parse_discovered
from src.pipeline.index import append_pipeline_run_entry
from src.utils.paths import pipeline_runs_dir


@dataclass
class PipelineRunResult:
    pipeline_run_id: str
    job_name: str
    status: str
    manifest_path: Path
    job_run_id: str | None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["manifest_path"] = str(self.manifest_path)
        return data


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _make_pipeline_run_id() -> str:
    base = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{base}_{uuid.uuid4().hex[:8]}"


def _pipeline_manifest_path(job_name: str, pipeline_run_id: str, root_dir: Path | None = None) -> Path:
    base = root_dir if root_dir is not None else pipeline_runs_dir()
    out_dir = base / job_name / pipeline_run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / "manifest.json"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _find_latest_manifest(job_name: str, root_dir: Path | None = None) -> Path | None:
    base = root_dir if root_dir is not None else pipeline_runs_dir()
    job_dir = base / job_name
    if not job_dir.exists():
        return None

    manifests = [p for p in job_dir.glob("*/manifest.json") if p.exists()]
    if not manifests:
        return None

    manifests.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return manifests[0]


def _is_discovery_done(manifest: dict[str, Any]) -> bool:
    summary_path = manifest.get("discovery_summary_path")
    discovered_path = manifest.get("discovered_output_path")
    return bool(summary_path and discovered_path and Path(summary_path).exists() and Path(discovered_path).exists())


def _is_archive_discovered_done(manifest: dict[str, Any]) -> bool:
    summary_path = manifest.get("archive_discovered_summary_path")
    if not summary_path or not Path(summary_path).exists():
        return False
    payload = _load_json(Path(summary_path))
    return isinstance(payload.get("archived_snapshot_paths"), list)


def _is_parse_discovered_done(manifest: dict[str, Any]) -> bool:
    summary_path = manifest.get("parse_discovered_summary_path")
    export_paths = manifest.get("export_paths", {})
    if not summary_path or not Path(summary_path).exists():
        return False
    csv_path = export_paths.get("csv")
    jsonl_path = export_paths.get("jsonl")
    return bool(csv_path and jsonl_path and Path(csv_path).exists() and Path(jsonl_path).exists())


def _append_error(errors: list[dict[str, Any]], step: str, exc: Exception) -> None:
    errors.append({"step": step, "error": f"{type(exc).__name__}: {exc}"})


def run_job_full(
    *,
    job_name: str,
    resume: bool = False,
    force_discovery: bool = False,
    force_archive_discovered: bool = False,
    force_parse: bool = False,
    run_job_fn=run_job,
    discover_fn=discover_job_run,
    archive_discovered_fn=archive_discovered,
    parse_discovered_fn=parse_discovered,
    pipeline_root_dir: Path | None = None,
    pipeline_index_file: Path | None = None,
    log_fn: Callable[[str], None] = print,
) -> PipelineRunResult:
    if resume:
        latest = _find_latest_manifest(job_name=job_name, root_dir=pipeline_root_dir)
        if latest is not None:
            manifest_path = latest
            manifest = _load_json(latest)
            log_fn(f"[pipeline] step=pipeline status=resumed pipeline_run_id={manifest.get('pipeline_run_id')}")
        else:
            pipeline_run_id = _make_pipeline_run_id()
            manifest_path = _pipeline_manifest_path(job_name=job_name, pipeline_run_id=pipeline_run_id, root_dir=pipeline_root_dir)
            manifest = {
                "pipeline_run_id": pipeline_run_id,
                "job_name": job_name,
                "timestamp_utc_start": _utc_now_iso(),
                "step_statuses": {},
                "errors_summary": [],
            }
            log_fn(f"[pipeline] step=pipeline status=started pipeline_run_id={pipeline_run_id}")
    else:
        pipeline_run_id = _make_pipeline_run_id()
        manifest_path = _pipeline_manifest_path(job_name=job_name, pipeline_run_id=pipeline_run_id, root_dir=pipeline_root_dir)
        manifest = {
            "pipeline_run_id": pipeline_run_id,
            "job_name": job_name,
            "timestamp_utc_start": _utc_now_iso(),
            "step_statuses": {},
            "errors_summary": [],
        }
        log_fn(f"[pipeline] step=pipeline status=started pipeline_run_id={pipeline_run_id}")

    pipeline_run_id = str(manifest["pipeline_run_id"])
    step_statuses: dict[str, str] = dict(manifest.get("step_statuses", {}))
    errors_summary: list[dict[str, Any]] = list(manifest.get("errors_summary", []))

    job_run_id = manifest.get("job_run_id")
    job_manifest_path = manifest.get("job_manifest_path")

    if resume and job_run_id and job_manifest_path and Path(job_manifest_path).exists():
        step_statuses["run-job"] = "skipped"
        log_fn("[pipeline] step=run-job status=skipped")
    else:
        try:
            log_fn("[pipeline] step=run-job status=started")
            result = run_job_fn(job_name=job_name)
            job_run_id = result.run_id
            job_manifest_path = str(result.manifest_path)
            manifest["job_run_id"] = job_run_id
            manifest["job_manifest_path"] = job_manifest_path
            step_statuses["run-job"] = "completed"
            log_fn(f"[pipeline] step=run-job status=completed run_id={job_run_id}")
        except Exception as exc:
            step_statuses["run-job"] = "failed"
            _append_error(errors_summary, "run-job", exc)
            log_fn(f"[pipeline] step=run-job status=failed error={type(exc).__name__}")

    discovery_summary_path = manifest.get("discovery_summary_path")
    discovered_output_path = manifest.get("discovered_output_path")
    discovery_run_id = manifest.get("discovery_run_id") or job_run_id

    if step_statuses.get("run-job") == "failed" or not job_run_id:
        step_statuses["discover-job-run"] = "skipped"
        log_fn("[pipeline] step=discover-job-run status=skipped reason=missing_job_run")
    elif resume and not force_discovery and _is_discovery_done(manifest):
        step_statuses["discover-job-run"] = "skipped"
        log_fn("[pipeline] step=discover-job-run status=skipped")
    else:
        try:
            log_fn("[pipeline] step=discover-job-run status=started")
            summary = discover_fn(job_name=job_name, run_id=str(job_run_id))
            discovery_run_id = str(job_run_id)
            discovery_summary_path = str(Path(summary["discovered_output_path"]).parent / "summary.json")
            discovered_output_path = str(summary["discovered_output_path"])
            manifest["discovery_run_id"] = discovery_run_id
            manifest["discovery_summary_path"] = discovery_summary_path
            manifest["discovered_output_path"] = discovered_output_path
            step_statuses["discover-job-run"] = "completed"
            log_fn("[pipeline] step=discover-job-run status=completed")
        except Exception as exc:
            step_statuses["discover-job-run"] = "failed"
            _append_error(errors_summary, "discover-job-run", exc)
            log_fn(f"[pipeline] step=discover-job-run status=failed error={type(exc).__name__}")

    archive_summary_path = manifest.get("archive_discovered_summary_path")
    if step_statuses.get("discover-job-run") == "failed":
        step_statuses["archive-discovered"] = "skipped"
        log_fn("[pipeline] step=archive-discovered status=skipped reason=discovery_failed")
    elif resume and not force_archive_discovered and _is_archive_discovered_done(manifest):
        step_statuses["archive-discovered"] = "skipped"
        log_fn("[pipeline] step=archive-discovered status=skipped")
    else:
        try:
            log_fn("[pipeline] step=archive-discovered status=started")
            summary = archive_discovered_fn(job_name=job_name, run_id=str(job_run_id))
            archive_summary_path = str(Path(discovered_output_path).parent / "archive_summary.json") if discovered_output_path else ""
            manifest["archive_discovered_summary_path"] = archive_summary_path
            manifest["archive_discovered_counts"] = {
                "ok_count": summary.get("ok_count", 0),
                "partial_count": summary.get("partial_count", 0),
                "error_count": summary.get("error_count", 0),
            }
            step_statuses["archive-discovered"] = "completed"
            log_fn("[pipeline] step=archive-discovered status=completed")
        except Exception as exc:
            step_statuses["archive-discovered"] = "failed"
            _append_error(errors_summary, "archive-discovered", exc)
            log_fn(f"[pipeline] step=archive-discovered status=failed error={type(exc).__name__}")

    parse_summary_path = manifest.get("parse_discovered_summary_path")
    if step_statuses.get("archive-discovered") == "failed":
        step_statuses["parse-discovered"] = "skipped"
        log_fn("[pipeline] step=parse-discovered status=skipped reason=archive_failed")
    elif resume and not force_parse and _is_parse_discovered_done(manifest):
        step_statuses["parse-discovered"] = "skipped"
        log_fn("[pipeline] step=parse-discovered status=skipped")
    else:
        try:
            log_fn("[pipeline] step=parse-discovered status=started")
            summary = parse_discovered_fn(job_name=job_name, run_id=str(job_run_id))
            parse_summary_path = str(Path(summary["parsed_details_path"]).parent / "summary.json")
            manifest["parse_discovered_summary_path"] = parse_summary_path
            manifest["export_paths"] = {
                "jsonl": str(summary.get("export_jsonl_path", "")),
                "csv": str(summary.get("export_csv_path", "")),
            }
            step_statuses["parse-discovered"] = "completed"
            log_fn("[pipeline] step=parse-discovered status=completed")
        except Exception as exc:
            step_statuses["parse-discovered"] = "failed"
            _append_error(errors_summary, "parse-discovered", exc)
            log_fn(f"[pipeline] step=parse-discovered status=failed error={type(exc).__name__}")

    counts = manifest.get("archive_discovered_counts", {})
    has_nonfatal_errors = bool((counts.get("error_count", 0) or 0) > 0)
    if any(value == "failed" for value in step_statuses.values()):
        status = "failed"
    elif has_nonfatal_errors:
        status = "partial"
    else:
        status = "completed"

    manifest["step_statuses"] = step_statuses
    manifest["errors_summary"] = errors_summary
    manifest["status"] = status
    manifest["timestamp_utc_end"] = _utc_now_iso()
    manifest["job_name"] = job_name
    manifest["pipeline_run_id"] = pipeline_run_id
    manifest["job_run_id"] = job_run_id
    manifest["discovery_run_id"] = discovery_run_id
    manifest["archive_discovered_summary_path"] = archive_summary_path
    manifest["parse_discovered_summary_path"] = parse_summary_path
    manifest["output_job_run_id"] = job_run_id
    manifest["output_paths"] = {
        "discovered_output_path": discovered_output_path,
        "archive_discovered_summary_path": archive_summary_path,
        "parse_discovered_summary_path": parse_summary_path,
        "export_jsonl_path": (manifest.get("export_paths") or {}).get("jsonl"),
        "export_csv_path": (manifest.get("export_paths") or {}).get("csv"),
    }

    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    append_pipeline_run_entry(
        {
            "pipeline_run_id": pipeline_run_id,
            "job_name": job_name,
            "timestamp_utc_start": manifest.get("timestamp_utc_start"),
            "timestamp_utc_end": manifest.get("timestamp_utc_end"),
            "status": status,
            "job_run_id": job_run_id,
            "discovery_run_id": discovery_run_id,
            "manifest_path": str(manifest_path),
        },
        index_file=pipeline_index_file,
    )

    return PipelineRunResult(
        pipeline_run_id=pipeline_run_id,
        job_name=job_name,
        status=status,
        manifest_path=manifest_path,
        job_run_id=str(job_run_id) if job_run_id is not None else None,
    )
