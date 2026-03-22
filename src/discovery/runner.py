from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.archiver import archive_url
from src.discovery.extractor import discover_candidate_urls
from src.discovery.index import append_discovery_run_entry
from src.discovery.models import DiscoveredUrl
from src.jobs.index import load_job_run_manifest
from src.parsers import parse_snapshot
from src.parsers.registry import resolve_parser_key_for_domain
from src.parsers.snapshot_bridge import SnapshotBridge
from src.utils.paths import discovered_dir


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _discovery_output_dir(job_name: str, run_id: str, output_root_dir: Path | None = None) -> Path:
    root = output_root_dir if output_root_dir is not None else discovered_dir() / "job_runs"
    return root / job_name / run_id


def discover_job_run(
    *,
    job_name: str,
    run_id: str,
    output_root_dir: Path | None = None,
    job_runs_index_file: Path | None = None,
    discovery_index_file: Path | None = None,
) -> dict[str, Any]:
    started = _utc_now_iso()
    manifest = load_job_run_manifest(job_name=job_name, run_id=run_id, index_file=job_runs_index_file)

    out_dir = _discovery_output_dir(job_name, run_id, output_root_dir=output_root_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    discovered_path = out_dir / "discovered_urls.jsonl"
    summary_path = out_dir / "summary.json"

    discovered: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for snapshot_path in manifest.get("snapshot_paths", []):
        try:
            bundle = SnapshotBridge.load(snapshot_path)
            parsed = parse_snapshot(snapshot_path)
            page_kind = parsed.get("page_kind", "unknown")
            if page_kind == "detail":
                continue

            domain = str(bundle.meta.get("domain", ""))
            parser_key = resolve_parser_key_for_domain(domain)
            allowed_domain = domain if domain and domain != "local-file" else None

            candidates = discover_candidate_urls(bundle, parser_key=parser_key, allowed_domain=allowed_domain)

            for url in candidates:
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                rec = DiscoveredUrl(
                    job_name=job_name,
                    run_id=run_id,
                    source_domain=domain,
                    parser_key=parser_key,
                    parent_snapshot_id=str(bundle.meta.get("snapshot_id", "")),
                    parent_run_id=str(bundle.meta.get("run_id", "")),
                    parent_snapshot_path=str(bundle.meta.get("snapshot_path", snapshot_path)),
                    page_kind=page_kind,
                    discovered_url=url,
                    discovered_at=_utc_now_iso(),
                )
                discovered.append(rec.to_dict())
        except Exception as exc:  # pragma: no cover
            errors.append({"snapshot_path": str(snapshot_path), "error": f"{type(exc).__name__}: {exc}"})

    with discovered_path.open("w", encoding="utf-8") as fh:
        for row in discovered:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    ended = _utc_now_iso()
    summary = {
        "job_name": job_name,
        "run_id": run_id,
        "timestamp_utc_start": started,
        "timestamp_utc_end": ended,
        "input_snapshots": len(manifest.get("snapshot_paths", [])),
        "discovered_urls_count": len(discovered),
        "errors_count": len(errors),
        "discovered_output_path": str(discovered_path),
        "errors": errors,
    }
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    append_discovery_run_entry(
        {
            "job_name": job_name,
            "run_id": run_id,
            "timestamp_utc_start": started,
            "timestamp_utc_end": ended,
            "discovered_urls_count": len(discovered),
            "errors_count": len(errors),
            "discovered_output_path": str(discovered_path),
            "summary_path": str(summary_path),
        },
        index_file=discovery_index_file,
    )

    return summary


def archive_discovered(
    *,
    job_name: str,
    run_id: str,
    output_root_dir: Path | None = None,
) -> dict[str, Any]:
    out_dir = _discovery_output_dir(job_name, run_id, output_root_dir=output_root_dir)
    discovered_path = out_dir / "discovered_urls.jsonl"
    if not discovered_path.exists():
        raise FileNotFoundError(f"discovered_urls.jsonl not found: {discovered_path}")

    lines = [line for line in discovered_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    urls = [json.loads(line).get("discovered_url") for line in lines]

    ok_count = 0
    partial_count = 0
    error_count = 0
    archived_snapshot_paths: list[str] = []
    archive_results: list[dict[str, Any]] = []

    for index, url in enumerate(urls):
        if not url:
            continue
        discovered_row = json.loads(lines[index])
        result_row = {
            "source_domain": discovered_row.get("source_domain"),
            "discovered_url": url,
            "status": "error",
            "snapshot_path": None,
        }
        try:
            listing_page_url = str(discovered_row.get("listing_page_url") or "").strip()
            request_headers = {"Referer": listing_page_url} if listing_page_url else None
            result = archive_url(
                url=url,
                timeout=20,
                request_headers=request_headers,
                session_warmup_url=listing_page_url or None,
            )
            archived_snapshot_paths.append(str(result.output_dir))
            result_row["status"] = result.status
            result_row["snapshot_path"] = str(result.output_dir)
            if listing_page_url:
                result_row["listing_page_url"] = listing_page_url
            if result.status == "ok":
                ok_count += 1
            elif result.status == "partial":
                partial_count += 1
            else:
                error_count += 1
        except Exception as exc:
            error_count += 1
            result_row["error"] = f"{type(exc).__name__}: {exc}"
        archive_results.append(result_row)

    summary = {
        "job_name": job_name,
        "run_id": run_id,
        "total_urls": len(urls),
        "ok_count": ok_count,
        "partial_count": partial_count,
        "error_count": error_count,
        "archived_snapshot_paths": archived_snapshot_paths,
        "results": archive_results,
    }
    (out_dir / "archive_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary
