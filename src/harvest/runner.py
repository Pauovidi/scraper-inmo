from __future__ import annotations

import json
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.config import resolve_job_harvest_plan
from src.discovery.models import DiscoveredUrl
from src.harvest.listing_fetcher import build_listing_page_plan, fetch_listing_pages
from src.harvest.listing_parser import dedupe_candidates, extract_listing_candidates
from src.harvest.models import ListingCandidate
from src.parsers.snapshot_bridge import SnapshotBridge
from src.publish.history import load_master_map
from src.utils.listing_identity import canonicalize_url
from src.utils.listing_identity import portal_slug
from src.utils.paths import discovered_dir, harvest_dir
from src.utils.time_utils import now_utc_iso


def _make_harvest_run_id() -> str:
    base = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{base}_{uuid.uuid4().hex[:8]}"


def _today_iso() -> str:
    return date.today().isoformat()


def _write_jsonl(rows: list[dict[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _is_candidate_relevant(candidate: ListingCandidate, history_map: dict[str, dict[str, Any]], today_iso: str) -> tuple[bool, str]:
    existing = history_map.get(candidate.listing_key)
    if existing is None:
        return True, "new_listing_key"

    if str(existing.get("last_seen_date") or "") != today_iso:
        return True, "seen_previous_day"

    return False, "already_seen_today"


def _candidate_to_discovered_row(candidate: ListingCandidate, *, run_id: str, selection_reason: str) -> dict[str, Any]:
    payload = DiscoveredUrl(
        job_name=candidate.job_name,
        run_id=run_id,
        source_domain=candidate.source_domain,
        parser_key=candidate.parser_key,
        parent_snapshot_id=candidate.listing_snapshot_id,
        parent_run_id=candidate.listing_snapshot_run_id,
        parent_snapshot_path=candidate.listing_snapshot_path,
        page_kind="listing",
        discovered_url=candidate.candidate_url,
        discovered_at=candidate.discovered_at,
        external_id=candidate.external_id,
        candidate_listing_key=candidate.listing_key,
        title_text=candidate.title_text,
        price_text=candidate.price_text,
        location_text=candidate.location_text,
        surface_text=candidate.surface_text,
        rooms_text=candidate.rooms_text,
        listing_page_url=candidate.listing_page_url,
        acquisition_type="listing_harvest",
        selection_reason=selection_reason,
    ).to_dict()
    return payload


def _merge_discovered_rows(existing_rows: list[dict[str, Any]], new_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    aliases: dict[str, str] = {}

    def identity_keys(row: dict[str, Any]) -> list[str]:
        keys = [
            str(row.get("external_id") or "").strip(),
            str(row.get("candidate_listing_key") or "").strip(),
            canonicalize_url(row.get("discovered_url")),
            str(row.get("discovered_url") or "").strip(),
        ]
        return [key for key in keys if key]

    for row in [*existing_rows, *new_rows]:
        keys = identity_keys(row)
        primary_key = next((aliases[key] for key in keys if key in aliases), keys[0] if keys else "")
        if not primary_key:
            continue

        current = merged.get(primary_key)
        if current is None:
            merged[primary_key] = row
        else:
            current_score = sum(1 for field in ["title_text", "price_text", "location_text", "surface_text", "rooms_text"] if current.get(field))
            row_score = sum(1 for field in ["title_text", "price_text", "location_text", "surface_text", "rooms_text"] if row.get(field))
            if row_score > current_score:
                merged[primary_key] = row

        for key in keys:
            aliases[key] = primary_key
    return list(merged.values())


def _persist_discovered_rows(
    *,
    job_name: str,
    run_id: str,
    new_rows: list[dict[str, Any]],
    discovery_root_dir: Path | None = None,
) -> dict[str, Any]:
    base = discovery_root_dir if discovery_root_dir is not None else discovered_dir() / "job_runs"
    out_dir = base / job_name / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    discovered_path = out_dir / "discovered_urls.jsonl"
    existing_rows = _read_jsonl(discovered_path)
    merged_rows = _merge_discovered_rows(existing_rows, new_rows)
    _write_jsonl(merged_rows, discovered_path)

    summary_path = out_dir / "summary.json"
    existing_summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
    existing_summary.update(
        {
            "job_name": job_name,
            "run_id": run_id,
            "timestamp_utc_end": now_utc_iso(),
            "discovered_urls_count": len(merged_rows),
            "discovered_output_path": str(discovered_path),
            "harvest_merged_count": len(new_rows),
        }
    )
    summary_path.write_text(json.dumps(existing_summary, indent=2, ensure_ascii=False), encoding="utf-8")

    return {
        "discovered_output_path": str(discovered_path),
        "summary_path": str(summary_path),
        "merged_total_count": len(merged_rows),
        "merged_new_count": len(new_rows),
    }


def harvest_listings(
    *,
    job_name: str,
    linked_run_id: str | None = None,
    merge_into_discovery: bool = False,
    harvest_root_dir: Path | None = None,
    discovery_root_dir: Path | None = None,
    history_root_dir: Path | None = None,
    snapshot_output_base_dir: Path | None = None,
    snapshot_index_file: Path | None = None,
    archive_fn=None,
    sleep_fn=None,
) -> dict[str, Any]:
    from src.archiver import archive_url

    archive_callable = archive_fn or archive_url
    effective_sleep = sleep_fn if sleep_fn is not None else __import__("time").sleep

    started = now_utc_iso()
    harvest_run_id = _make_harvest_run_id()
    harvest_date = _today_iso()
    history_map = load_master_map(root_dir=history_root_dir)
    resolved = resolve_job_harvest_plan(job_name)

    root = harvest_root_dir if harvest_root_dir is not None else harvest_dir()
    day_dir = root / harvest_date
    day_dir.mkdir(parents=True, exist_ok=True)

    portal_summaries: dict[str, dict[str, Any]] = {}
    all_selected_rows: list[dict[str, Any]] = []
    all_candidate_rows: list[dict[str, Any]] = []
    overall_errors: list[dict[str, Any]] = []

    for source in resolved["included_sources"]:
        source_domain = str(source.get("domain", ""))
        portal = portal_slug(source_domain)
        portal_dir = day_dir / portal
        listing_pages_dir = portal_dir / "listing_pages"
        listing_pages_dir.mkdir(parents=True, exist_ok=True)

        page_plans = build_listing_page_plan(source)
        page_fetch_rows = fetch_listing_pages(
            plans=page_plans,
            archive_fn=archive_callable,
            sleep_fn=effective_sleep,
            output_base_dir=snapshot_output_base_dir,
            snapshot_index_file=snapshot_index_file,
        )

        portal_candidates: list[ListingCandidate] = []
        page_rows: list[dict[str, Any]] = []

        for page_row in page_fetch_rows:
            page_rows.append(page_row)
            snapshot_path = str(page_row.get("snapshot_path") or "")
            status = str(page_row.get("status") or "")
            if status not in {"ok", "partial"} or not snapshot_path:
                overall_errors.append(
                    {
                        "source_domain": source_domain,
                        "listing_page_url": page_row.get("plan", {}).get("listing_page_url"),
                        "error": f"listing_snapshot_status_{status or 'unknown'}",
                    }
                )
                continue

            try:
                bundle = SnapshotBridge.load(snapshot_path)
                plan_payload = page_row.get("plan", {})
                extracted = extract_listing_candidates(
                    bundle,
                    job_name=job_name,
                    harvest_run_id=harvest_run_id,
                    source_domain=source_domain,
                    parser_key=str(source.get("parser_key", "generic")),
                    page_number=int(plan_payload.get("page_number", 1) or 1),
                )
                portal_candidates.extend(extracted)
            except Exception as exc:  # pragma: no cover
                overall_errors.append(
                    {
                        "source_domain": source_domain,
                        "listing_page_url": page_row.get("plan", {}).get("listing_page_url"),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        deduped_candidates = dedupe_candidates(portal_candidates)
        persisted_candidate_rows: list[dict[str, Any]] = []
        selected_rows: list[dict[str, Any]] = []
        skipped_known_today = 0

        for candidate in deduped_candidates:
            relevant, selection_reason = _is_candidate_relevant(candidate, history_map, harvest_date)
            row = candidate.to_dict()
            row["selected_for_detail"] = relevant
            row["selection_reason"] = selection_reason
            persisted_candidate_rows.append(row)
            all_candidate_rows.append(row)
            if relevant:
                selected_row = _candidate_to_discovered_row(candidate, run_id=linked_run_id or harvest_run_id, selection_reason=selection_reason)
                selected_rows.append(selected_row)
            else:
                skipped_known_today += 1

        candidates_path = portal_dir / "candidates.jsonl"
        listing_pages_manifest_path = listing_pages_dir / "manifest.jsonl"
        portal_summary_path = portal_dir / "summary.json"

        _write_jsonl(persisted_candidate_rows, candidates_path)
        _write_jsonl(page_rows, listing_pages_manifest_path)

        portal_summary = {
            "portal": portal,
            "source_domain": source_domain,
            "harvest_run_id": harvest_run_id,
            "listing_pages_requested": len(page_plans),
            "listing_pages_archived": sum(1 for row in page_rows if row.get("status") in {"ok", "partial"}),
            "candidates_extracted_count": len(portal_candidates),
            "candidates_unique_count": len(deduped_candidates),
            "candidates_passed_to_detail_count": len(selected_rows),
            "candidates_skipped_known_today_count": skipped_known_today,
            "candidates_path": str(candidates_path),
            "listing_pages_manifest_path": str(listing_pages_manifest_path),
        }
        portal_summary_path.write_text(json.dumps(portal_summary, indent=2, ensure_ascii=False), encoding="utf-8")
        portal_summaries[portal] = portal_summary
        all_selected_rows.extend(selected_rows)

    discovery_merge = None
    if linked_run_id and merge_into_discovery:
        discovery_merge = _persist_discovered_rows(
            job_name=job_name,
            run_id=linked_run_id,
            new_rows=all_selected_rows,
            discovery_root_dir=discovery_root_dir,
        )

    summary = {
        "job_name": job_name,
        "harvest_run_id": harvest_run_id,
        "linked_run_id": linked_run_id,
        "timestamp_utc_start": started,
        "timestamp_utc_end": now_utc_iso(),
        "harvest_date": harvest_date,
        "source_count": len(resolved["included_sources"]),
        "excluded_sources": resolved["excluded_sources"],
        "portal_summaries": portal_summaries,
        "candidate_count": len(all_candidate_rows),
        "selected_for_detail_count": len(all_selected_rows),
        "errors_count": len(overall_errors),
        "errors": overall_errors,
        "data_root": str(day_dir),
    }
    if discovery_merge is not None:
        summary["discovery_merge"] = discovery_merge

    (day_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary
