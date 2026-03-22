from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.utils.listing_identity import portal_slug
from src.utils.paths import discovered_dir, harvest_dir, parsed_dir

TARGET_PORTALS = ("fotocasa", "idealista", "milanuncios", "pisos", "yaencontre")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _blank_portal_funnel(portal: str) -> dict[str, Any]:
    return {
        "portal": portal,
        "listing_pages_attempted": 0,
        "listing_pages_ok": 0,
        "listing_pages_error": 0,
        "cards_detected": 0,
        "candidates_emitted": 0,
        "candidates_deduped_out": 0,
        "candidates_rejected_by_rules": 0,
        "candidates_sent_to_detail": 0,
        "detail_archive_ok": 0,
        "detail_archive_error": 0,
        "parsed_detail_ok": 0,
        "parsed_detail_partial": 0,
        "parsed_detail_error": 0,
    }


def _portal_report_map(harvest_summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    portal_map = {portal: _blank_portal_funnel(portal) for portal in TARGET_PORTALS}
    for portal, source_summary in dict(harvest_summary.get("portal_summaries", {})).items():
        target = portal_map.setdefault(portal, _blank_portal_funnel(portal))
        for key in [
            "listing_pages_attempted",
            "listing_pages_ok",
            "listing_pages_error",
            "cards_detected",
            "candidates_emitted",
            "candidates_deduped_out",
            "candidates_rejected_by_rules",
            "candidates_sent_to_detail",
        ]:
            target[key] = int(source_summary.get(key, 0) or 0)
        target["source_domain"] = source_summary.get("source_domain")
        target["rejection_reasons"] = source_summary.get("rejection_reasons", {})
    return portal_map


def build_funnel_report(
    *,
    job_name: str,
    run_id: str,
    harvest_summary_path: str | Path,
    harvest_root_dir: Path | None = None,
    discovery_root_dir: Path | None = None,
    parsed_root_dir: Path | None = None,
) -> dict[str, Any]:
    harvest_summary = _read_json(Path(harvest_summary_path))
    portal_reports = _portal_report_map(harvest_summary)

    discovery_base = discovery_root_dir if discovery_root_dir is not None else discovered_dir() / "job_runs"
    archive_summary_path = discovery_base / job_name / run_id / "archive_summary.json"
    archive_results = []
    if archive_summary_path.exists():
        archive_results = list(_read_json(archive_summary_path).get("results", []))
        for row in archive_results:
            portal = portal_slug(row.get("source_domain"))
            report = portal_reports.setdefault(portal, _blank_portal_funnel(portal))
            status = str(row.get("status") or "")
            if status in {"ok", "partial"}:
                report["detail_archive_ok"] += 1
            else:
                report["detail_archive_error"] += 1

    snapshot_to_portal = {
        str(row.get("snapshot_path")): portal_slug(row.get("source_domain"))
        for row in archive_results
        if row.get("snapshot_path")
    }

    parsed_base = parsed_root_dir if parsed_root_dir is not None else parsed_dir() / "discovered"
    parsed_summary_path = parsed_base / job_name / run_id / "summary.json"
    parsed_details_path = parsed_base / job_name / run_id / "parsed_details.jsonl"
    if parsed_summary_path.exists():
        parsed_records = _read_jsonl(parsed_details_path)
        for record in parsed_records:
            if str(record.get("page_kind") or "") != "detail":
                continue
            portal = portal_slug(record.get("source_domain"))
            report = portal_reports.setdefault(portal, _blank_portal_funnel(portal))
            parse_status = str(record.get("parse_status") or "")
            if parse_status == "ok":
                report["parsed_detail_ok"] += 1
            elif parse_status == "partial":
                report["parsed_detail_partial"] += 1
            else:
                report["parsed_detail_error"] += 1

        parsed_summary = _read_json(parsed_summary_path)
        for error in parsed_summary.get("errors", []):
            portal = snapshot_to_portal.get(str(error.get("snapshot_path") or ""))
            if not portal:
                continue
            report = portal_reports.setdefault(portal, _blank_portal_funnel(portal))
            report["parsed_detail_error"] += 1

    totals = _blank_portal_funnel("all")
    for report in portal_reports.values():
        for key in [
            "listing_pages_attempted",
            "listing_pages_ok",
            "listing_pages_error",
            "cards_detected",
            "candidates_emitted",
            "candidates_deduped_out",
            "candidates_rejected_by_rules",
            "candidates_sent_to_detail",
            "detail_archive_ok",
            "detail_archive_error",
            "parsed_detail_ok",
            "parsed_detail_partial",
            "parsed_detail_error",
        ]:
            totals[key] += int(report.get(key, 0) or 0)

    report = {
        "job_name": job_name,
        "run_id": run_id,
        "harvest_summary_path": str(harvest_summary_path),
        "archive_summary_path": str(archive_summary_path) if archive_summary_path.exists() else None,
        "parsed_summary_path": str(parsed_summary_path) if parsed_summary_path.exists() else None,
        "portal_reports": {portal: portal_reports[portal] for portal in TARGET_PORTALS},
        "totals": totals,
    }

    output_base = harvest_root_dir if harvest_root_dir is not None else harvest_dir()
    harvest_date = str(harvest_summary.get("harvest_date") or "")
    output_dir = output_base / harvest_date if harvest_date else output_base
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"funnel_report_{job_name}_{run_id}.json"
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    report["output_path"] = str(output_path)
    return report
