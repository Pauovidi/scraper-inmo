from __future__ import annotations

import re

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None

from src.parsers.models import ParsedRecord
from src.parsers.normalization import normalize_price, normalize_rooms_count, normalize_surface_sqm
from src.parsers.snapshot_bridge import SnapshotBundle
from src.utils.time_utils import now_utc_iso

PRICE_RE = re.compile(r"\d[\d\.,\s]{2,}\s?€|€\s?\d[\d\.,\s]{2,}", re.IGNORECASE)
SURFACE_RE = re.compile(r"\b\d{1,4}\s?(?:m2|m²|metros?)\b", re.IGNORECASE)
ROOMS_RE = re.compile(r"\b\d{1,2}\s?(?:hab(?:itaciones)?\.?|dormitorios|rooms?)\b", re.IGNORECASE)


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
        if node.name == "meta":
            value = _compact(node.get("content"))
        else:
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


def _resolve_page_kind(url: str, links_count: int, has_price: bool, has_surface: bool, has_rooms: bool) -> str:
    low = url.lower()

    listing_hints = [
        "/inmobiliaria",
        "/agencia",
        "/perfil",
        "clientid=",
        "/todas-las-zonas/l",
    ]
    detail_hints = ["/d?", "/d", "/inmueble/", "/detalle", "/ficha"]

    if any(token in low for token in listing_hints):
        return "listing"

    if any(token in low for token in detail_hints):
        return "detail"

    if has_price and has_surface and has_rooms and links_count <= 10:
        return "detail"

    if links_count >= 15:
        return "listing"

    return "unknown"


def parse_fotocasa_detail_snapshot(bundle: SnapshotBundle, parser_key: str = "fotocasa_detail") -> ParsedRecord:
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

        price_text = _from_selectors(
            soup,
            [
                "[class*='price']",
                "[class*='Price']",
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

        info_text = _from_selectors(soup, ["[class*='feature']", "[class*='characteristic']", "ul", "section"]) or combined
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
        title = _regex_find(re.compile(r"^#\s+(.+)$", re.MULTILINE), markdown)
        if title:
            title = _compact(title.replace("#", ""))

    if not price_text:
        price_text = _regex_find(PRICE_RE, combined)
    if not surface_text:
        surface_text = _regex_find(SURFACE_RE, combined)
    if not rooms_text:
        rooms_text = _regex_find(ROOMS_RE, combined)
    if not description_text:
        description_text = _compact(combined[:1500])

    price_value, price_currency = normalize_price(price_text, combined)
    surface_sqm = normalize_surface_sqm(surface_text, combined)
    rooms_count = normalize_rooms_count(rooms_text, combined)

    dedup_links: list[str] = []
    seen: set[str] = set()
    for link in extracted_links:
        if not link or link in seen:
            continue
        seen.add(link)
        dedup_links.append(link)

    page_kind = _resolve_page_kind(
        str(meta.get("url_final", "")),
        len(extracted_links),
        bool(price_value),
        bool(surface_sqm),
        bool(rooms_count),
    )

    fields_present = sum(1 for value in [title, price_text, location_text, surface_text, rooms_text, description_text] if value)
    parse_errors: list[str] = []

    if not (html or markdown):
        parse_status = "error"
        parse_errors.append("missing_html_and_markdown")
    elif page_kind != "detail":
        if fields_present >= 3:
            parse_status = "partial"
        else:
            parse_status = "error"
        parse_errors.append("non_detail_page_kind")
    elif fields_present >= 4 and price_value is not None and location_text and (surface_sqm is not None or rooms_count is not None):
        parse_status = "ok"
    elif fields_present >= 2:
        parse_status = "partial"
    else:
        parse_status = "error"

    confidence = 0.25 + (fields_present * 0.08)
    if page_kind != "detail":
        confidence = min(confidence * 0.6, 0.55)
    if price_value is None:
        confidence -= 0.12
    if not location_text:
        confidence -= 0.08
    confidence = max(0.1, min(0.98, confidence))

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
        price_value=price_value,
        price_currency=price_currency,
        location_text=location_text,
        surface_text=surface_text,
        surface_sqm=surface_sqm,
        rooms_text=rooms_text,
        rooms_count=rooms_count,
        description_text=description_text,
        extracted_links=dedup_links[:200],
        extracted_at=now_utc_iso(),
        parse_status=parse_status,
        parse_errors=parse_errors,
        confidence_score=round(confidence, 2),
    )



