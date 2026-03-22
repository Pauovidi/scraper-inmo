from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.utils.paths import repo_root

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None


REQUIRED_SOURCE_FIELDS = {
    "domain",
    "enabled",
    "mode",
    "start_urls",
    "rate_limit_seconds",
    "login_allowed",
    "archiver_enabled",
    "parser_key",
    "notes",
}

REQUIRED_JOB_FIELDS = {
    "job_name",
    "sources",
    "filters",
    "max_urls",
    "notes",
}


def _config_root() -> Path:
    return repo_root() / "config"


def sources_dir() -> Path:
    return _config_root() / "sources"


def jobs_dir() -> Path:
    return _config_root() / "jobs"


def _load_yaml_file(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")

    if yaml is not None:
        data = yaml.safe_load(raw)
    else:
        # JSON is valid YAML 1.2; this keeps loader functional without PyYAML.
        data = json.loads(raw)

    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a mapping object: {path}")
    return data


def _validate_required_fields(data: dict[str, Any], required: set[str], path: Path) -> None:
    missing = sorted(required - set(data.keys()))
    if missing:
        raise ValueError(f"Missing required fields in {path.name}: {', '.join(missing)}")


def _validate_source(data: dict[str, Any], path: Path) -> None:
    _validate_required_fields(data, REQUIRED_SOURCE_FIELDS, path)

    if not isinstance(data["start_urls"], list) or not data["start_urls"]:
        raise ValueError(f"start_urls must be a non-empty list in {path.name}")

    if not isinstance(data["rate_limit_seconds"], (int, float)):
        raise ValueError(f"rate_limit_seconds must be numeric in {path.name}")

    if "timeout_seconds" in data and not isinstance(data["timeout_seconds"], (int, float)):
        raise ValueError(f"timeout_seconds must be numeric in {path.name}")

    if "listing_start_urls" in data and (
        not isinstance(data["listing_start_urls"], list) or not data["listing_start_urls"]
    ):
        raise ValueError(f"listing_start_urls must be a non-empty list in {path.name}")

    if "max_listing_pages" in data and not isinstance(data["max_listing_pages"], int):
        raise ValueError(f"max_listing_pages must be an integer in {path.name}")

    if "listing_page_start" in data and not isinstance(data["listing_page_start"], int):
        raise ValueError(f"listing_page_start must be an integer in {path.name}")

    if "harvest_enabled" in data and not isinstance(data["harvest_enabled"], bool):
        raise ValueError(f"harvest_enabled must be boolean in {path.name}")


def _apply_source_defaults(data: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(data)
    normalized.setdefault("timeout_seconds", 20)
    normalized.setdefault("harvest_enabled", False)
    normalized.setdefault("listing_start_urls", list(normalized.get("start_urls", [])))
    normalized.setdefault("max_listing_pages", 1)
    normalized.setdefault("listing_page_start", 1)
    normalized.setdefault("listing_first_page_uses_start_url", True)
    normalized.setdefault("listing_page_param", None)
    normalized.setdefault("listing_page_url_template", None)
    return normalized


def _validate_job(data: dict[str, Any], path: Path) -> None:
    _validate_required_fields(data, REQUIRED_JOB_FIELDS, path)

    if not isinstance(data["sources"], list) or not data["sources"]:
        raise ValueError(f"sources must be a non-empty list in {path.name}")


def load_sources() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted(sources_dir().glob("*.yaml")):
        data = _apply_source_defaults(_load_yaml_file(path))
        _validate_source(data, path)
        out.append(data)
    return out


def load_source_by_domain(domain: str) -> dict[str, Any]:
    for source in load_sources():
        if source.get("domain") == domain:
            return source
    raise KeyError(f"Source not found for domain: {domain}")


def load_jobs() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted(jobs_dir().glob("*.yaml")):
        data = _load_yaml_file(path)
        _validate_job(data, path)
        out.append(data)
    return out


def load_job_by_name(job_name: str) -> dict[str, Any]:
    for job in load_jobs():
        if job.get("job_name") == job_name:
            return job
    raise KeyError(f"Job not found: {job_name}")


def resolve_job_sources(job_name: str) -> dict[str, Any]:
    job = load_job_by_name(job_name)
    sources_by_domain = {src["domain"]: src for src in load_sources()}

    included_sources: list[dict[str, Any]] = []
    excluded_sources: list[dict[str, Any]] = []

    for domain in job["sources"]:
        src = sources_by_domain.get(domain)
        if not src:
            excluded_sources.append({"domain": domain, "reason": "missing_source"})
            continue

        if not src.get("enabled", False):
            excluded_sources.append({"domain": domain, "reason": "disabled"})
            continue

        if not src.get("archiver_enabled", False):
            excluded_sources.append({"domain": domain, "reason": "archiver_disabled"})
            continue

        included_sources.append(src)

    return {
        "job": job,
        "included_sources": included_sources,
        "excluded_sources": excluded_sources,
    }


def resolve_job_start_urls(job_name: str) -> list[str]:
    resolved = resolve_job_sources(job_name)

    start_urls: list[str] = []
    for src in resolved["included_sources"]:
        start_urls.extend(src.get("start_urls", []))

    seen: set[str] = set()
    deduped: list[str] = []
    for url in start_urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)

    max_urls = int(resolved["job"].get("max_urls", 0) or 0)
    if max_urls > 0:
        return deduped[:max_urls]
    return deduped


def resolve_job_plan(job_name: str) -> dict[str, Any]:
    resolved = resolve_job_sources(job_name)
    job = resolved["job"]

    url_items: list[dict[str, Any]] = []
    seen: set[str] = set()
    duplicate_count = 0

    for src in resolved["included_sources"]:
        domain = src["domain"]
        rate_limit = float(src.get("rate_limit_seconds", 0))
        for url in src.get("start_urls", []):
            if url in seen:
                duplicate_count += 1
                continue
            seen.add(url)
            url_items.append(
                {
                    "url": url,
                    "source_domain": domain,
                    "rate_limit_seconds": rate_limit,
                    "timeout_seconds": float(src.get("timeout_seconds", 20) or 20),
                }
            )

    max_urls = int(job.get("max_urls", 0) or 0)
    if max_urls > 0:
        url_items = url_items[:max_urls]

    return {
        "job": job,
        "included_sources": resolved["included_sources"],
        "excluded_sources": resolved["excluded_sources"],
        "url_items": url_items,
        "duplicate_start_urls_skipped": duplicate_count,
    }


def resolve_job_harvest_plan(job_name: str) -> dict[str, Any]:
    resolved = resolve_job_sources(job_name)
    job = resolved["job"]

    included_sources: list[dict[str, Any]] = []
    excluded_sources = list(resolved["excluded_sources"])

    for src in resolved["included_sources"]:
        if not src.get("harvest_enabled", False):
            excluded_sources.append({"domain": src["domain"], "reason": "harvest_disabled"})
            continue

        listing_start_urls = src.get("listing_start_urls") or src.get("start_urls") or []
        if not isinstance(listing_start_urls, list) or not listing_start_urls:
            excluded_sources.append({"domain": src["domain"], "reason": "missing_listing_start_urls"})
            continue

        normalized = dict(src)
        normalized["listing_start_urls"] = listing_start_urls
        included_sources.append(normalized)

    return {
        "job": job,
        "included_sources": included_sources,
        "excluded_sources": excluded_sources,
    }

