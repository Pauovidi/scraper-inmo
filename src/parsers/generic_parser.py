from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None

from src.parsers.models import ParsedRecord
from src.parsers.snapshot_bridge import SnapshotBundle

PRICE_RE = re.compile(r"(?:€|eur|euro|euros|\$|£)\s?\d[\d\.,\s]*|\d[\d\.,\s]*(?:€|eur|euro|euros)", re.IGNORECASE)
SURFACE_RE = re.compile(r"\b\d{1,4}\s?(?:m2|m²|metros cuadrados|sqm)\b", re.IGNORECASE)
ROOMS_RE = re.compile(r"\b\d{1,2}\s?(?:habitaciones|hab\.?|rooms?)\b", re.IGNORECASE)
LOCATION_HINT_RE = re.compile(r"\b(?:bizkaia|bilbao|madrid|barcelona|valencia|sevilla)\b", re.IGNORECASE)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _extract_title_from_html(html: str | None) -> str | None:
    if not html:
        return None
    if BeautifulSoup is None:
        m = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
        if m:
            return re.sub(r"\s+", " ", m.group(1)).strip()
        return None

    soup = BeautifulSoup(html, "lxml")
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(" ", strip=True)
    if soup.title and soup.title.get_text(strip=True):
        return soup.title.get_text(" ", strip=True)
    return None


def _extract_links(html: str | None) -> list[str]:
    if not html:
        return []
    if BeautifulSoup is None:
        links = re.findall(r'href=["\']([^"\']+)["\']', html, re.IGNORECASE)
    else:
        soup = BeautifulSoup(html, "lxml")
        links = [a.get("href", "").strip() for a in soup.find_all("a") if a.get("href")]

    deduped: list[str] = []
    seen: set[str] = set()
    for link in links:
        if not link or link in seen:
            continue
        seen.add(link)
        deduped.append(link)
    return deduped[:200]


def _find_first(pattern: re.Pattern[str], text: str | None) -> str | None:
    if not text:
        return None
    m = pattern.search(text)
    if not m:
        return None
    return m.group(0).strip()


def _extract_location(text: str | None) -> str | None:
    if not text:
        return None
    m = LOCATION_HINT_RE.search(text)
    if m:
        return m.group(0)
    return None


def _description(markdown: str | None, html: str | None) -> str | None:
    source = markdown or html
    if not source:
        return None
    cleaned = re.sub(r"\s+", " ", source).strip()
    if not cleaned:
        return None
    return cleaned[:1200]


def _page_kind(url: str, links_count: int, has_price: bool, has_surface: bool, title: str | None) -> str:
    low = url.lower()
    title_low = (title or "").lower()

    detail_tokens = ["/inmueble/", "/ficha", "/detalle", "id-"]
    listing_tokens = ["/buscar", "/alquiler", "/venta", "resultados", "/naves"]

    if any(token in low for token in detail_tokens):
        return "detail"

    if any(token in low for token in listing_tokens) and links_count >= 5:
        return "listing"

    # Detail pages usually show structured facts and fewer outbound links.
    if (has_price and has_surface and links_count <= 15) or "nave" in title_low and has_price:
        return "detail"

    if links_count >= 20:
        return "listing"

    return "unknown"


def _confidence(fields_present: int) -> float:
    return max(0.1, min(1.0, fields_present / 7))


def parse_generic_snapshot(bundle: SnapshotBundle, parser_key: str = "generic") -> ParsedRecord:
    meta = bundle.meta
    markdown = bundle.markdown
    html = bundle.html

    combined_text = "\n".join(part for part in [markdown, html] if part)

    title = _extract_title_from_html(html)
    price_text = _find_first(PRICE_RE, combined_text)
    surface_text = _find_first(SURFACE_RE, combined_text)
    rooms_text = _find_first(ROOMS_RE, combined_text)
    location_text = _extract_location(combined_text)
    links = _extract_links(html)
    desc = _description(markdown, html)

    fields_present = sum(
        1
        for value in [title, price_text, location_text, surface_text, rooms_text, desc]
        if value
    )

    if fields_present >= 3:
        parse_status = "ok"
    elif fields_present >= 1:
        parse_status = "partial"
    else:
        parse_status = "error"

    errors: list[str] = []
    if not (html or markdown):
        errors.append("missing_html_and_markdown")

    return ParsedRecord(
        parser_key=parser_key,
        source_domain=meta.get("domain", "unknown-domain"),
        snapshot_id=meta.get("snapshot_id", ""),
        run_id=meta.get("run_id", ""),
        snapshot_path=meta.get("snapshot_path", str(bundle.snapshot_path)),
        url_original=meta.get("url_original", ""),
        url_final=meta.get("url_final", ""),
        page_kind=_page_kind(
            meta.get("url_final", ""),
            len(links),
            bool(price_text),
            bool(surface_text),
            title,
        ),
        title=title,
        price_text=price_text,
        location_text=location_text,
        surface_text=surface_text,
        rooms_text=rooms_text,
        description_text=desc,
        extracted_links=links,
        extracted_at=_now_iso(),
        parse_status=parse_status,
        parse_errors=errors,
        confidence_score=round(_confidence(fields_present), 2),
    )

