from __future__ import annotations

import re
from urllib.parse import urlparse

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

LOCATION_STOPWORDS = {
    "espana",
    "españa",
    "todas las zonas",
    "comprar",
    "alquilar",
    "locales",
    "viviendas",
    "fotocasa",
}
DESCRIPTION_NOISE_PATTERNS = [
    r"conecta tu hogar",
    r"elige la fibra",
    r"comprueba cobertura",
    r"ver tarifas",
    r"ofrecido por",
    r"contactar",
    r"llamar",
    r"hace \d+ d[ií]as",
    r"top\+",
    r"que mejor se adapte a ti",
]


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


def _clean_location_candidate(value: str | None) -> str | None:
    cleaned = _compact(value)
    if not cleaned:
        return None

    low = cleaned.lower()
    if any(stop in low for stop in LOCATION_STOPWORDS):
        return None

    if len(cleaned) < 3 or len(cleaned) > 80:
        return None

    if not re.search(r"[a-záéíóúñ]", low, re.IGNORECASE):
        return None

    return cleaned


def _title_location(title: str | None) -> str | None:
    if not title:
        return None
    normalized = re.sub(r"\s+", " ", title)
    parts = [p.strip() for p in normalized.split(",") if p.strip()]
    if len(parts) >= 2:
        for part in reversed(parts):
            candidate = _clean_location_candidate(part)
            if candidate:
                return candidate
    return None


def _url_location(url: str | None) -> str | None:
    if not url:
        return None
    path = urlparse(url).path
    pieces = [p for p in path.split("/") if p]
    for idx, part in enumerate(pieces):
        if part in {"local-comercial", "vivienda", "piso", "nave", "locales-comerciales"} and idx + 1 < len(pieces):
            candidate = pieces[idx + 1].replace("-", " ").strip()
            return _clean_location_candidate(candidate)
    return None


def _extract_location(soup, title: str | None, url: str | None, combined: str) -> str | None:
    selector_candidates = [
        "[class*='location']",
        "[class*='address']",
        "[class*='zone']",
        "meta[property='og:locality']",
        "[itemprop*='addressLocality']",
    ]
    for selector in selector_candidates:
        candidate = _from_selectors(soup, [selector])
        candidate = _clean_location_candidate(candidate)
        if candidate:
            return candidate

    breadcrumb_candidates: list[str] = []
    for selector in ["nav[aria-label*='miga'] a", "[class*='breadcrumb'] a", "[data-testid*='breadcrumb'] a"]:
        for node in soup.select(selector):
            text = _compact(node.get_text(" ", strip=True))
            if text:
                breadcrumb_candidates.append(text)

    for item in reversed(breadcrumb_candidates):
        candidate = _clean_location_candidate(item)
        if candidate:
            return candidate

    from_title = _clean_location_candidate(_title_location(title))
    if from_title:
        return from_title

    from_url = _clean_location_candidate(_url_location(url))
    if from_url:
        return from_url

    fallback = _regex_find(re.compile(r"\b(?:bilbao|bizkaia|sestao|barakaldo|getxo|durango)\b", re.IGNORECASE), combined)
    return _clean_location_candidate(fallback)


def _clean_description(text: str | None) -> str | None:
    cleaned = _compact(text)
    if not cleaned:
        return None

    for pattern in DESCRIPTION_NOISE_PATTERNS:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r"[*_`#]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .,-")
    if not cleaned:
        return None
    if len(cleaned) < 30:
        return None
    return cleaned[:1500]


def _extract_description(soup, combined: str) -> str | None:
    selectors = [
        "[data-testid*='description']",
        "[class*='detail-description']",
        "[class*='description']",
        "[id*='description']",
        "meta[property='og:description']",
        "meta[name='description']",
        "article",
        "main",
    ]
    for selector in selectors:
        candidate = _from_selectors(soup, [selector])
        cleaned = _clean_description(candidate)
        if cleaned:
            return cleaned

    lines = [line.strip() for line in (combined or "").splitlines() if line.strip()]
    fallback_text = " ".join(lines[:20]) if lines else combined
    return _clean_description(fallback_text)


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

        info_text = _from_selectors(soup, ["[class*='feature']", "[class*='characteristic']", "ul", "section"]) or combined
        surface_text = _regex_find(SURFACE_RE, info_text)
        rooms_text = _regex_find(ROOMS_RE, info_text)

        description_text = _extract_description(soup, combined)
        location_text = _extract_location(soup, title, str(meta.get("url_final", "")), combined)

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
    if not description_text and markdown:
        description_text = _clean_description(markdown[:2500])

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

    has_price = price_value is not None
    has_surface_or_rooms = surface_sqm is not None or rooms_count is not None
    has_location = bool(location_text)
    has_description = bool(description_text and len(description_text) >= 60)

    page_kind = _resolve_page_kind(
        str(meta.get("url_final", "")),
        len(extracted_links),
        has_price,
        surface_sqm is not None,
        rooms_count is not None,
    )

    fields_present = sum(1 for value in [title, price_text, location_text, surface_text, rooms_text, description_text] if value)
    parse_errors: list[str] = []

    if not (html or markdown):
        parse_status = "error"
        parse_errors.append("missing_html_and_markdown")
    elif page_kind != "detail":
        parse_status = "partial" if fields_present >= 3 else "error"
        parse_errors.append("non_detail_page_kind")
    elif has_price and has_surface_or_rooms and has_location and fields_present >= 4:
        parse_status = "ok"
    elif has_price and has_surface_or_rooms and (has_location or has_description):
        parse_status = "partial"
    elif fields_present >= 2:
        parse_status = "partial"
    else:
        parse_status = "error"

    confidence = 0.2
    if page_kind == "detail":
        confidence += 0.2
    if has_price:
        confidence += 0.2
    if surface_sqm is not None:
        confidence += 0.12
    if rooms_count is not None:
        confidence += 0.08
    if has_location:
        confidence += 0.15
    if has_description:
        confidence += 0.1
    else:
        confidence -= 0.08

    if parse_status == "partial":
        confidence = min(confidence, 0.79)
    elif parse_status == "error":
        confidence = min(confidence, 0.49)

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
