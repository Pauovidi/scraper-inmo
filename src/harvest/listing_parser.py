from __future__ import annotations

import json
import re
from collections.abc import Iterable
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

try:
    from bs4 import BeautifulSoup, Tag  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None
    Tag = object  # type: ignore

from src.harvest.models import ListingCandidate
from src.parsers.normalization import normalize_price
from src.parsers.snapshot_bridge import SnapshotBundle
from src.utils.listing_identity import canonicalize_url, extract_external_id, resolve_listing_identity
from src.utils.time_utils import now_utc_iso

PRICE_TEXT_RE = re.compile(
    r"(?:€|eur|euro|euros)\s?\d[\d\.,\s]*|\d[\d\.,\s]*(?:€|eur|euro|euros)",
    re.IGNORECASE,
)
SURFACE_TEXT_RE = re.compile(r"\b\d[\d\.,\s]{0,8}\s?(?:m2|m²|metros cuadrados|metros|sqm)\b", re.IGNORECASE)
ROOMS_TEXT_RE = re.compile(r"\b\d{1,2}\s?(?:hab(?:itaciones)?\.?|dormitorios|rooms?)\b", re.IGNORECASE)
CARD_HINT_TOKENS = ("card", "item", "result", "listing", "property", "article", "row", "ad", "result")
QUERY_DROP_KEYS = {
    "from",
    "gbpv",
    "isgalleryopen",
    "iszoomgalleryopen",
    "page",
    "pagina",
    "currentpage",
}
DETAIL_PATTERNS_BY_PORTAL = {
    "fotocasa.es": [r"/\d+/d(?:\?|$)", r"/inmueble/\d+"],
    "idealista.com": [r"/inmueble/\d+/?$"],
    "milanuncios.com": [r"-\d+\.htm(?:\?|$)"],
    "pisos.com": [r"/inmueble/\d+"],
    "yaencontre.com": [r"/inmueble/\d+/?$", r"/\d+/?$"],
}
EXCLUDE_PATTERNS_BY_PORTAL = {
    "fotocasa.es": [r"/todas-las-zonas/l(?:\?|$)", r"/mapa", r"/inmobiliaria", r"/agencia", r"/perfil"],
    "idealista.com": [r"/pagina-\d+\.htm$", r"/agencia", r"/obra-nueva", r"/mapa", r"/perfil"],
    "milanuncios.com": [r"/inmobiliaria", r"/profesional", r"/s?buscador", r"/m?is-anuncios"],
    "pisos.com": [r"/alquiler/?$", r"/venta/?$", r"/obra-nueva", r"/inmobiliaria", r"/agencia"],
    "yaencontre.com": [r"/alquiler/", r"/venta/", r"/obra-nueva", r"/agencia", r"/inmobiliaria", r"/mapa"],
}


def _clean_text(value: object | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned or None


def _portal_patterns(source_domain: str) -> list[str]:
    return DETAIL_PATTERNS_BY_PORTAL.get(source_domain, [r"/inmueble/", r"/detalle", r"/ficha", r"\d{6,}"])


def _portal_excludes(source_domain: str) -> list[str]:
    base = [r"/inmobiliaria", r"/agencia", r"/perfil", r"/contacto", r"/login", r"javascript:", r"mailto:", r"tel:"]
    return [*base, *EXCLUDE_PATTERNS_BY_PORTAL.get(source_domain, [])]


def normalize_candidate_url(url: str, *, base_url: str) -> str:
    absolute = urljoin(base_url, url)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return ""

    query = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=False) if key.lower() not in QUERY_DROP_KEYS]
    normalized = urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, parsed.params, urlencode(query, doseq=True), ""))
    return canonicalize_url(normalized)


def is_candidate_detail_url(url: str, *, source_domain: str) -> bool:
    if not url:
        return False

    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    if source_domain and netloc != source_domain and not netloc.endswith(f".{source_domain}"):
        return False

    low = url.lower()
    for pattern in _portal_excludes(source_domain):
        if re.search(pattern, low):
            return False

    return any(re.search(pattern, low) for pattern in _portal_patterns(source_domain))


def _attr_candidates(tag: Tag, keys: Iterable[str]) -> list[str]:
    out: list[str] = []
    for key in keys:
        value = tag.get(key)
        if not value:
            continue
        if isinstance(value, (list, tuple)):
            out.extend(str(item) for item in value if item)
        else:
            out.append(str(value))
    return out


def _card_score(tag: Tag) -> int:
    score = 0
    name = getattr(tag, "name", "")
    if name in {"article", "li", "section"}:
        score += 3

    attrs = " ".join(_attr_candidates(tag, ["class", "id", "data-testid", "data-test"]))
    for token in CARD_HINT_TOKENS:
        if token in attrs.lower():
            score += 2

    text = _clean_text(tag.get_text(" ", strip=True)) or ""
    if len(text) >= 30:
        score += 1
    if len(text) >= 120:
        score += 1
    return score


def _find_card(anchor: Tag) -> Tag:
    chosen: Tag = anchor
    best_score = -1
    for parent in [anchor, *list(anchor.parents)[:6]]:
        if getattr(parent, "name", "") not in {"a", "div", "article", "li", "section"}:
            continue
        score = _card_score(parent)
        if score > best_score:
            chosen = parent
            best_score = score
    return chosen


def _find_text_by_attr(card: Tag, tokens: tuple[str, ...]) -> str | None:
    for tag in card.find_all(True):
        attrs = " ".join(_attr_candidates(tag, ["class", "id", "data-testid", "data-test"]))
        attrs_low = attrs.lower()
        if not any(token in attrs_low for token in tokens):
            continue
        text = _clean_text(tag.get_text(" ", strip=True))
        if text:
            return text
    return None


def _title_text(card: Tag, anchor: Tag) -> str | None:
    for candidate in [anchor.get_text(" ", strip=True), _find_text_by_attr(card, ("title", "headline", "subject"))]:
        cleaned = _clean_text(candidate)
        if cleaned and len(cleaned) >= 5:
            return cleaned

    for heading_name in ("h1", "h2", "h3", "h4", "strong"):
        heading = card.find(heading_name)
        if heading:
            cleaned = _clean_text(heading.get_text(" ", strip=True))
            if cleaned and len(cleaned) >= 5:
                return cleaned
    return None


def _extract_external_id(card: Tag, candidate_url: str, source_domain: str) -> str | None:
    keys = (
        "data-id",
        "data-adid",
        "data-ad-id",
        "data-listing-id",
        "data-property-id",
        "data-re-ad-id",
        "id",
    )
    values = _attr_candidates(card, keys)
    values.extend(_attr_candidates(next(iter(card.find_all("a", href=True)), card), keys))
    for value in values:
        if not value:
            continue
        match = re.search(r"(\d{5,})", value)
        if match:
            return match.group(1)

    return extract_external_id(source_domain, candidate_url)


def _regex_text(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    return _clean_text(match.group(0)) if match else None


def _location_text(card: Tag, text: str) -> str | None:
    direct = _find_text_by_attr(card, ("location", "district", "zone", "address", "municip"))
    if direct:
        return direct

    segments = [segment.strip() for segment in re.split(r"[|\n·]", text) if segment.strip()]
    for segment in segments:
        if PRICE_TEXT_RE.search(segment) or SURFACE_TEXT_RE.search(segment):
            continue
        if any(char.isalpha() for char in segment) and len(segment) <= 80:
            return _clean_text(segment)
    return None


def candidate_from_card(
    *,
    anchor: Tag,
    bundle: SnapshotBundle,
    source_domain: str,
    parser_key: str,
    job_name: str,
    harvest_run_id: str,
    page_number: int,
    card_position: int,
) -> ListingCandidate | None:
    base_url = str(bundle.meta.get("url_final") or bundle.meta.get("url_original") or "")
    href = _clean_text(anchor.get("href"))
    if not href:
        return None

    candidate_url = normalize_candidate_url(href, base_url=base_url)
    if not is_candidate_detail_url(candidate_url, source_domain=source_domain):
        return None

    card = _find_card(anchor)
    raw_text = _clean_text(card.get_text(" ", strip=True))
    if not raw_text or len(raw_text) < 12:
        return None

    title_text = _title_text(card, anchor)
    price_text = _find_text_by_attr(card, ("price", "amount")) or _regex_text(PRICE_TEXT_RE, raw_text)
    location_text = _location_text(card, raw_text)
    surface_text = _find_text_by_attr(card, ("surface", "size", "meters")) or _regex_text(SURFACE_TEXT_RE, raw_text)
    rooms_text = _find_text_by_attr(card, ("room", "habit")) or _regex_text(ROOMS_TEXT_RE, raw_text)
    external_id = _extract_external_id(card, candidate_url, source_domain)

    price_value, _ = normalize_price(price_text, raw_text)
    provisional = {
        "source_domain": source_domain,
        "url_final": candidate_url,
        "external_id": external_id,
        "title": title_text,
        "price_text": price_text,
        "price_value": price_value,
        "location_text": location_text,
        "surface_text": surface_text,
        "rooms_text": rooms_text,
    }
    identity = resolve_listing_identity(provisional)
    dedupe_key = external_id or identity["canonical_url"] or identity["listing_key"]
    dedupe_method = "external_id" if external_id else identity["dedupe_method"]

    return ListingCandidate(
        job_name=job_name,
        harvest_run_id=harvest_run_id,
        source_domain=source_domain,
        parser_key=parser_key,
        listing_page_url=base_url,
        listing_start_url=str((bundle.meta.get("extra") or {}).get("listing_start_url") or base_url),
        listing_snapshot_path=str(bundle.meta.get("snapshot_path", bundle.snapshot_path)),
        listing_snapshot_id=str(bundle.meta.get("snapshot_id", "")),
        listing_snapshot_run_id=str(bundle.meta.get("run_id", "")),
        page_number=page_number,
        card_position=card_position,
        candidate_url=candidate_url,
        canonical_url=identity["canonical_url"] or canonicalize_url(candidate_url),
        title_text=title_text,
        price_text=price_text,
        location_text=location_text,
        surface_text=surface_text,
        rooms_text=rooms_text,
        external_id=external_id,
        listing_key=identity["listing_key"],
        dedupe_key=dedupe_key,
        dedupe_method=dedupe_method,
        raw_text=raw_text[:600],
        discovered_at=now_utc_iso(),
    )


def extract_listing_candidates(
    bundle: SnapshotBundle,
    *,
    job_name: str,
    harvest_run_id: str,
    source_domain: str,
    parser_key: str,
    page_number: int,
) -> list[ListingCandidate]:
    html = bundle.html or ""
    if not html:
        return []

    if BeautifulSoup is None:
        links = re.findall(r'href=["\']([^"\']+)["\']', html, re.IGNORECASE)
        candidates: list[ListingCandidate] = []
        for position, href in enumerate(links, start=1):
            candidate_url = normalize_candidate_url(href, base_url=str(bundle.meta.get("url_final") or bundle.meta.get("url_original") or ""))
            if not is_candidate_detail_url(candidate_url, source_domain=source_domain):
                continue
            identity = resolve_listing_identity({"source_domain": source_domain, "url_final": candidate_url})
            candidates.append(
                ListingCandidate(
                    job_name=job_name,
                    harvest_run_id=harvest_run_id,
                    source_domain=source_domain,
                    parser_key=parser_key,
                    listing_page_url=str(bundle.meta.get("url_final") or bundle.meta.get("url_original") or ""),
                    listing_start_url=str((bundle.meta.get("extra") or {}).get("listing_start_url") or ""),
                    listing_snapshot_path=str(bundle.meta.get("snapshot_path", bundle.snapshot_path)),
                    listing_snapshot_id=str(bundle.meta.get("snapshot_id", "")),
                    listing_snapshot_run_id=str(bundle.meta.get("run_id", "")),
                    page_number=page_number,
                    card_position=position,
                    candidate_url=candidate_url,
                    canonical_url=identity["canonical_url"],
                    title_text=None,
                    price_text=None,
                    location_text=None,
                    surface_text=None,
                    rooms_text=None,
                    external_id=identity["external_id"] or None,
                    listing_key=identity["listing_key"],
                    dedupe_key=identity["external_id"] or identity["canonical_url"] or identity["listing_key"],
                    dedupe_method=identity["dedupe_method"],
                    raw_text=None,
                    discovered_at=now_utc_iso(),
                )
            )
        return dedupe_candidates(candidates)

    soup = BeautifulSoup(html, "lxml")
    candidates: list[ListingCandidate] = []
    for position, anchor in enumerate(soup.find_all("a", href=True), start=1):
        candidate = candidate_from_card(
            anchor=anchor,
            bundle=bundle,
            source_domain=source_domain,
            parser_key=parser_key,
            job_name=job_name,
            harvest_run_id=harvest_run_id,
            page_number=page_number,
            card_position=position,
        )
        if candidate is not None:
            candidates.append(candidate)

    return dedupe_candidates(candidates)


def dedupe_candidates(candidates: list[ListingCandidate]) -> list[ListingCandidate]:
    deduped: dict[str, ListingCandidate] = {}
    for candidate in candidates:
        key = candidate.dedupe_key or candidate.listing_key or candidate.candidate_url
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = candidate
            continue

        existing_score = sum(1 for value in [existing.title_text, existing.price_text, existing.location_text, existing.surface_text, existing.rooms_text] if value)
        candidate_score = sum(1 for value in [candidate.title_text, candidate.price_text, candidate.location_text, candidate.surface_text, candidate.rooms_text] if value)
        if candidate_score > existing_score:
            deduped[key] = candidate
    return list(deduped.values())


def candidates_to_jsonl_rows(candidates: list[ListingCandidate]) -> list[dict[str, object]]:
    return [candidate.to_dict() for candidate in candidates]


def load_candidates_jsonl(path: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows
