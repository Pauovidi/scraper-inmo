from __future__ import annotations

import re
from datetime import datetime, timezone

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None

from src.parsers.models import ParsedRecord
from src.parsers.snapshot_bridge import SnapshotBundle

PRICE_RE = re.compile(r"\d[\d\.,\s]{2,}\s?€|€\s?\d[\d\.,\s]{2,}", re.IGNORECASE)
SURFACE_RE = re.compile(r"\b\d{1,4}\s?(?:m2|m²|metros?)\b", re.IGNORECASE)
ROOMS_RE = re.compile(r"\b\d{1,2}\s?(?:hab(?:itaciones)?\.?|dormitorios|rooms?)\b", re.IGNORECASE)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _compact(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned or None


def _from_selectors(soup, selectors: list[str]) -> str | None:
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        value = _compact(node.get_text(" ", strip=True))
        if value:
            return value
    return None


def _regex_find(pattern: re.Pattern[str], text: str | None) -> str | None:
    if not text:
        return None
    match = pattern.search(text)
    if not match:
        return None
    return _compact(match.group(0))


def parse_pisos_detail_snapshot(bundle: SnapshotBundle, parser_key: str = "pisos_detail") -> ParsedRecord:
    meta = bundle.meta
    html = bundle.html or ""
    markdown = bundle.markdown or ""
    combined = "\n".join(part for part in [markdown, html] if part)

    title = None
    price_text = None
    location_text = None
    surface_text = None
    rooms_text = None
    description_text = None
    extracted_links: list[str] = []

    if BeautifulSoup is not None and html:
        soup = BeautifulSoup(html, "lxml")

        title = _from_selectors(
            soup,
            [
                "meta[property='og:title']",
                "h1",
                "[class*='title']",
            ],
        )
        if title and title.lower().startswith("content="):
            title = _compact(title.replace("content=", ""))

        price_text = _from_selectors(
            soup,
            [
                "[class*='price']",
                "[id*='price']",
                "meta[property='product:price:amount']",
            ],
        )
        if not price_text:
            price_text = _regex_find(PRICE_RE, combined)

        location_text = _from_selectors(
            soup,
            [
                "[class*='location']",
                "[class*='address']",
                "[class*='zone']",
            ],
        )

        info_text = _from_selectors(soup, ["[class*='features']", "[class*='characteristics']", "ul"]) or combined
        surface_text = _regex_find(SURFACE_RE, info_text)
        rooms_text = _regex_find(ROOMS_RE, info_text)

        description_text = _from_selectors(
            soup,
            [
                "[class*='description']",
                "[id*='description']",
                "article",
            ],
        )

        for a in soup.find_all("a"):
            href = a.get("href")
            if href:
                extracted_links.append(str(href).strip())

    if not title:
        t = _regex_find(re.compile(r"^#\s+(.+)$", re.MULTILINE), markdown)
        if t:
            title = _compact(t.replace("#", ""))

    if not price_text:
        price_text = _regex_find(PRICE_RE, combined)
    if not surface_text:
        surface_text = _regex_find(SURFACE_RE, combined)
    if not rooms_text:
        rooms_text = _regex_find(ROOMS_RE, combined)

    if not description_text:
        description_text = _compact(combined[:1500])

    fields_present = sum(1 for value in [title, price_text, location_text, surface_text, rooms_text, description_text] if value)

    if fields_present >= 4:
        parse_status = "ok"
    elif fields_present >= 2:
        parse_status = "partial"
    else:
        parse_status = "error"

    parse_errors: list[str] = []
    if not (html or markdown):
        parse_errors.append("missing_html_and_markdown")

    confidence = 0.35 + (fields_present * 0.1)
    confidence = max(0.2, min(0.98, confidence))

    # Detail parser is intended for property pages; keep unknown only on very low signal.
    page_kind = "detail" if fields_present >= 2 else "unknown"

    dedup_links: list[str] = []
    seen: set[str] = set()
    for link in extracted_links:
        if not link or link in seen:
            continue
        seen.add(link)
        dedup_links.append(link)

    return ParsedRecord(
        parser_key=parser_key,
        source_domain=meta.get("domain", "unknown-domain"),
        snapshot_id=meta.get("snapshot_id", ""),
        run_id=meta.get("run_id", ""),
        snapshot_path=meta.get("snapshot_path", str(bundle.snapshot_path)),
        url_original=meta.get("url_original", ""),
        url_final=meta.get("url_final", ""),
        page_kind=page_kind,
        title=title,
        price_text=price_text,
        location_text=location_text,
        surface_text=surface_text,
        rooms_text=rooms_text,
        description_text=description_text,
        extracted_links=dedup_links[:200],
        extracted_at=_now_iso(),
        parse_status=parse_status,
        parse_errors=parse_errors,
        confidence_score=round(confidence, 2),
    )
