from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, unquote, urlparse
from urllib.request import Request, url2pathname, urlopen

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None

try:
    from markdownify import markdownify as html_to_markdown  # type: ignore
except Exception:  # pragma: no cover
    html_to_markdown = None

try:
    from slugify import slugify as external_slugify  # type: ignore
except Exception:  # pragma: no cover
    external_slugify = None

from src.archiver.index import append_snapshot_index_entry, find_previous_same_url_day
from src.utils.logging_utils import get_logger
from src.utils.paths import snapshots_dir

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


@dataclass
class ArchiveResult:
    snapshot_id: str
    run_id: str
    status: str
    output_dir: Path
    meta_path: Path


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _make_run_id() -> str:
    base = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{base}_{uuid.uuid4().hex[:8]}"


def _normalize_domain(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme == "file":
        return "local-file"

    domain = (parsed.hostname or parsed.netloc or "").lower().strip()
    if domain.startswith("www."):
        domain = domain[4:]
    domain = re.sub(r"[^a-z0-9.-]+", "-", domain)
    return domain or "unknown-domain"


def _stable_snapshot_id(url: str) -> str:
    return hashlib.sha256(url.strip().encode("utf-8")).hexdigest()[:16]


def _simple_slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def _slug_or_hash(url: str) -> str:
    parsed = urlparse(url)
    leaf = parsed.path.rstrip("/").split("/")[-1] if parsed.path else ""
    slug = external_slugify(leaf) if external_slugify else _simple_slugify(leaf)
    return slug or _stable_snapshot_id(url)


def _status_from_outputs(has_html: bool, has_md: bool) -> str:
    if has_html and has_md:
        return "ok"
    if has_html or has_md:
        return "partial"
    return "error"


def _sha256_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _read_file_url(file_url: str) -> tuple[str, str, str | None]:
    parsed = urlparse(file_url)
    local_path = Path(url2pathname(unquote(parsed.path.lstrip("/"))))
    if not local_path.exists():
        raise FileNotFoundError(f"Local file does not exist: {local_path}")
    html = local_path.read_text(encoding="utf-8", errors="replace")
    return file_url, html, "text/html"


def _fetch_html_requests(url: str, timeout: int) -> tuple[str, str, str | None]:
    response = requests.get(url, headers={"User-Agent": DEFAULT_USER_AGENT}, timeout=timeout)
    response.raise_for_status()
    response.encoding = response.encoding or response.apparent_encoding or "utf-8"
    return response.url, response.text, response.headers.get("Content-Type")


def _fetch_html_urllib(url: str, timeout: int) -> tuple[str, str, str | None]:
    request = Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
    with urlopen(request, timeout=timeout) as response:
        final_url = response.geturl()
        content_type = response.headers.get("Content-Type")
        raw = response.read()
        html = raw.decode("utf-8", errors="replace")
    return final_url, html, content_type


def _build_markdown_new_urls(final_url: str) -> list[str]:
    return [
        f"https://markdown.new/{final_url}",
        f"https://markdown.new/?url={quote_plus(final_url)}",
    ]


def _fetch_markdown_via_markdown_new(final_url: str, timeout: int) -> str:
    candidates = _build_markdown_new_urls(final_url)
    last_error: Exception | None = None

    for endpoint in candidates:
        try:
            if requests is not None:
                response = requests.get(endpoint, headers={"User-Agent": DEFAULT_USER_AGENT}, timeout=timeout)
                response.raise_for_status()
                text = response.text.strip()
            else:
                req = Request(endpoint, headers={"User-Agent": DEFAULT_USER_AGENT})
                with urlopen(req, timeout=timeout) as response:
                    text = response.read().decode("utf-8", errors="replace").strip()

            if len(text) > 30:
                return text
        except Exception as exc:  # pragma: no cover
            last_error = exc

    if last_error is not None:
        raise RuntimeError(f"markdown.new failed: {type(last_error).__name__}: {last_error}")
    raise RuntimeError("markdown.new returned empty content")


def _basic_html_to_text_markdown(html: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", "", html, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?</style>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"</(p|div|h1|h2|h3|h4|h5|h6|li|br)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _html_to_markdown(html: str) -> str:
    if BeautifulSoup is not None and html_to_markdown is not None:
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        cleaned = str(soup)
        return html_to_markdown(cleaned, heading_style="ATX", strip=["img"])

    return _basic_html_to_text_markdown(html)


def _resolve_dedup_info(
    *,
    previous_entries: list[dict[str, Any]],
    content_hash_preferred: str | None,
    html_hash: str | None,
    markdown_hash: str | None,
) -> dict[str, Any]:
    matched_snapshot_id: str | None = None
    matched_run_id: str | None = None
    match_reason = "none"

    for entry in reversed(previous_entries):
        prev_pref = entry.get("content_hash_preferred")
        if content_hash_preferred and prev_pref and prev_pref == content_hash_preferred:
            matched_snapshot_id = entry.get("snapshot_id")
            matched_run_id = entry.get("run_id")
            match_reason = "content_hash_match"
            break

        prev_md = entry.get("markdown_hash")
        prev_html = entry.get("html_hash")
        if markdown_hash and prev_md and markdown_hash == prev_md:
            matched_snapshot_id = entry.get("snapshot_id")
            matched_run_id = entry.get("run_id")
            match_reason = "markdown_hash_match"
            break
        if html_hash and prev_html and html_hash == prev_html:
            matched_snapshot_id = entry.get("snapshot_id")
            matched_run_id = entry.get("run_id")
            match_reason = "html_hash_match"
            break

    return {
        "same_url_same_day_previous_count": len(previous_entries),
        "is_duplicate_content": matched_run_id is not None,
        "matched_snapshot_id": matched_snapshot_id,
        "matched_run_id": matched_run_id,
        "match_reason": match_reason,
    }


def archive_url(
    url: str,
    timeout: int = 20,
    output_base_dir: Path | None = None,
    index_file: Path | None = None,
) -> ArchiveResult:
    logger = get_logger("archiver")
    start = time.perf_counter()
    timestamp = _utc_now_iso()
    run_date = timestamp[:10]
    run_id = _make_run_id()

    domain = _normalize_domain(url)
    snapshot_id = _stable_snapshot_id(url)
    slug_or_hash = _slug_or_hash(url)

    base_dir = output_base_dir if output_base_dir is not None else snapshots_dir()
    out_dir = base_dir / domain / run_date / slug_or_hash / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    html_path = out_dir / "page.html"
    md_path = out_dir / "page.md"
    meta_path = out_dir / "meta.json"

    methods_attempted: list[str] = []
    methods_succeeded: list[str] = []
    errors: list[str] = []

    final_url = url
    content_type = None
    html: str | None = None
    markdown: str | None = None
    markdown_source = "none"
    html_source = "none"

    parsed_input = urlparse(url)
    logger.info("archive.start url=%s out_dir=%s", url, out_dir)

    try:
        if parsed_input.scheme == "file":
            methods_attempted.append("html_file")
            final_url, html, content_type = _read_file_url(url)
            methods_succeeded.append("html_file")
            html_source = "file"
        elif parsed_input.scheme in {"http", "https"}:
            if requests is not None:
                methods_attempted.append("html_requests")
                try:
                    final_url, html, content_type = _fetch_html_requests(url=url, timeout=timeout)
                    methods_succeeded.append("html_requests")
                    html_source = "requests"
                except Exception as req_exc:
                    errors.append(f"html_requests_error: {type(req_exc).__name__}: {req_exc}")
                    methods_attempted.append("html_urllib")
                    final_url, html, content_type = _fetch_html_urllib(url=url, timeout=timeout)
                    methods_succeeded.append("html_urllib")
                    html_source = "urllib"
            else:
                methods_attempted.append("html_urllib")
                final_url, html, content_type = _fetch_html_urllib(url=url, timeout=timeout)
                methods_succeeded.append("html_urllib")
                html_source = "urllib"
        else:
            methods_attempted.append("html_urllib")
            final_url, html, content_type = _fetch_html_urllib(url=url, timeout=timeout)
            methods_succeeded.append("html_urllib")
            html_source = "urllib"

        html_path.write_text(html, encoding="utf-8")
        logger.info("archive.html_saved path=%s", html_path)
    except Exception as exc:  # pragma: no cover
        errors.append(f"html_fetch_error: {type(exc).__name__}: {exc}")
        logger.exception("archive.html_fetch_failed url=%s", url)

    if html:
        parsed_final = urlparse(final_url)
        if parsed_final.scheme in {"http", "https"}:
            methods_attempted.append("markdown_new")
            try:
                markdown = _fetch_markdown_via_markdown_new(final_url=final_url, timeout=timeout)
                methods_succeeded.append("markdown_new")
                markdown_source = "markdown_new"
            except Exception as exc:  # pragma: no cover
                errors.append(f"markdown_new_error: {type(exc).__name__}: {exc}")
                logger.warning("archive.markdown_new_failed url=%s error=%s", final_url, exc)

        if not markdown:
            methods_attempted.append("local_html_to_markdown")
            try:
                markdown = _html_to_markdown(html)
                methods_succeeded.append("local_html_to_markdown")
                markdown_source = "local_html_to_markdown"
            except Exception as exc:  # pragma: no cover
                errors.append(f"local_markdown_error: {type(exc).__name__}: {exc}")
                logger.exception("archive.local_markdown_failed url=%s", final_url)

        if markdown:
            md_path.write_text(markdown, encoding="utf-8")
            logger.info("archive.markdown_saved path=%s", md_path)

    has_html = html_path.exists()
    has_md = md_path.exists()
    status = _status_from_outputs(has_html=has_html, has_md=has_md)

    html_hash = _sha256_text(html) if html else None
    markdown_hash = _sha256_text(markdown) if markdown else None
    content_hash_preferred = markdown_hash or html_hash

    previous_entries = find_previous_same_url_day(url_original=url, date=run_date, index_file=index_file)
    dedup = _resolve_dedup_info(
        previous_entries=previous_entries,
        content_hash_preferred=content_hash_preferred,
        html_hash=html_hash,
        markdown_hash=markdown_hash,
    )

    elapsed_ms = int((time.perf_counter() - start) * 1000)

    files: dict[str, str | None] = {
        "page_html": str(html_path) if has_html else None,
        "page_md": str(md_path) if has_md else None,
        "meta_json": str(meta_path),
    }

    meta: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "url_original": url,
        "url_final": final_url,
        "domain": domain,
        "timestamp_utc": timestamp,
        "date": run_date,
        "status": status,
        "methods_attempted": methods_attempted,
        "methods_succeeded": methods_succeeded,
        "markdown_source": markdown_source,
        "html_source": html_source,
        "content_type": content_type,
        "files": files,
        "errors": errors,
        "elapsed_ms": elapsed_ms,
        "snapshot_path": str(out_dir),
        "html_hash": html_hash,
        "markdown_hash": markdown_hash,
        "content_hash_preferred": content_hash_preferred,
        "dedup": dedup,
    }

    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

    if status in {"ok", "partial"}:
        index_entry = {
            "snapshot_id": snapshot_id,
            "run_id": run_id,
            "url_original": url,
            "url_final": final_url,
            "domain": domain,
            "timestamp_utc": timestamp,
            "date": run_date,
            "status": status,
            "markdown_source": markdown_source,
            "html_source": html_source,
            "snapshot_path": str(out_dir),
            "elapsed_ms": elapsed_ms,
            "html_hash": html_hash,
            "markdown_hash": markdown_hash,
            "content_hash_preferred": content_hash_preferred,
            "is_duplicate_content": dedup.get("is_duplicate_content", False),
            "match_reason": dedup.get("match_reason", "none"),
        }
        append_snapshot_index_entry(index_entry, index_file=index_file)

    logger.info("archive.done status=%s meta=%s", status, meta_path)

    return ArchiveResult(
        snapshot_id=snapshot_id,
        run_id=run_id,
        status=status,
        output_dir=out_dir,
        meta_path=meta_path,
    )
