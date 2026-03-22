from __future__ import annotations

import json
import re
from collections.abc import Iterable

try:
    from bs4 import BeautifulSoup, Tag  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None
    Tag = object  # type: ignore

from src.harvest.models import ListingCandidate, ListingParseReport
from src.harvest.portals import (
    PortalStrategy,
    get_portal_strategy,
    is_detail_candidate_url_for_strategy,
    normalize_candidate_url_for_strategy,
)
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


def _clean_text(value: object | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned or None


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


def _select_cards(soup: BeautifulSoup, strategy: PortalStrategy) -> list[Tag]:
    selected: list[Tag] = []
    seen: set[int] = set()
    for selector in strategy.card_selectors:
        for tag in soup.select(selector):
            identifier = id(tag)
            if identifier in seen:
                continue
            if getattr(tag, "name", "") == "a":
                if not tag.get("href"):
                    continue
            elif not tag.find("a", href=True):
                continue
            seen.add(identifier)
            selected.append(tag)

    if selected:
        return selected

    for anchor in soup.find_all("a", href=True):
        card = _find_card(anchor)
        identifier = id(card)
        if identifier in seen:
            continue
        seen.add(identifier)
        selected.append(card)
    return selected


def _select_card_anchors(card: Tag, strategy: PortalStrategy) -> list[Tag]:
    selected: list[Tag] = []
    seen: set[int] = set()

    if getattr(card, "name", "") == "a" and card.get("href"):
        identifier = id(card)
        seen.add(identifier)
        selected.append(card)

    for selector in strategy.detail_link_selectors:
        for anchor in card.select(selector):
            identifier = id(anchor)
            if identifier in seen:
                continue
            if not anchor.get("href"):
                continue
            seen.add(identifier)
            selected.append(anchor)

    if selected:
        return selected
    return [anchor for anchor in card.find_all("a", href=True)]


def _candidate_rejection_reason(
    *,
    source_domain: str,
    strategy: PortalStrategy,
    href: str | None,
    candidate_url: str,
) -> str:
    href_value = _clean_text(href) or ""
    if not href_value:
        return "missing_href"
    if not candidate_url:
        return "invalid_candidate_url"

    low = candidate_url.lower()
    for pattern in strategy.reject_patterns:
        if re.search(pattern, low):
            return "rejected_by_url_rules"

    if strategy.is_detail_candidate_url_fn is not None:
        if not is_detail_candidate_url_for_strategy(strategy, candidate_url):
            return "non_detail_url"
    elif not any(re.search(pattern, low) for pattern in strategy.detail_patterns):
        return "non_detail_url"
    return "unknown_rejection"


def _candidate_from_anchor(
    *,
    anchor: Tag,
    card: Tag,
    bundle: SnapshotBundle,
    source_domain: str,
    parser_key: str,
    job_name: str,
    harvest_run_id: str,
    page_number: int,
    card_position: int,
    strategy: PortalStrategy,
) -> tuple[ListingCandidate | None, str | None]:
    base_url = str(bundle.meta.get("url_final") or bundle.meta.get("url_original") or "")
    href = _clean_text(anchor.get("href"))
    if not href:
        return None, "missing_href"

    effective_card = _find_card(anchor) if getattr(card, "name", "") == "a" else card

    candidate_url = normalize_candidate_url_for_strategy(strategy, href, base_url=base_url)
    if not candidate_url:
        return None, "invalid_candidate_url"
    if not is_detail_candidate_url_for_strategy(strategy, candidate_url) and not (
        strategy.is_detail_candidate_url_fn is None and any(re.search(pattern, candidate_url.lower()) for pattern in strategy.detail_patterns)
    ):
        return None, _candidate_rejection_reason(
            source_domain=source_domain,
            strategy=strategy,
            href=href,
            candidate_url=candidate_url,
        )

    raw_text = _clean_text(effective_card.get_text(" ", strip=True))
    if not raw_text or len(raw_text) < 12:
        return None, "card_text_too_short"

    title_text = _title_text(effective_card, anchor)
    price_text = _find_text_by_attr(effective_card, ("price", "amount")) or _regex_text(PRICE_TEXT_RE, raw_text)
    location_text = _location_text(effective_card, raw_text)
    surface_text = _find_text_by_attr(effective_card, ("surface", "size", "meters")) or _regex_text(SURFACE_TEXT_RE, raw_text)
    rooms_text = _find_text_by_attr(effective_card, ("room", "habit")) or _regex_text(ROOMS_TEXT_RE, raw_text)
    external_id = _extract_external_id(effective_card, candidate_url, source_domain)

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

    return (
        ListingCandidate(
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
        ),
        None,
    )


def _increment_reason(report: ListingParseReport, reason: str) -> None:
    report.candidates_rejected_by_rules += 1
    report.rejection_reasons[reason] = report.rejection_reasons.get(reason, 0) + 1


def extract_listing_candidates_with_report(
    bundle: SnapshotBundle,
    *,
    job_name: str,
    harvest_run_id: str,
    source_domain: str,
    parser_key: str,
    page_number: int,
    source_config: dict[str, object] | None = None,
) -> ListingParseReport:
    html = bundle.html or ""
    report = ListingParseReport()
    if not html:
        return report

    if source_domain == "milanuncios.com":
        from src.harvest.portals.milanuncios import milanuncios_is_blocked_listing_html

        if milanuncios_is_blocked_listing_html(html):
            report.rejection_reasons["blocked_listing_page"] = 1
            return report

    strategy = get_portal_strategy(source_domain, source_config=source_config)

    if BeautifulSoup is None:
        links = re.findall(r'href=["\']([^"\']+)["\']', html, re.IGNORECASE)
        raw_candidates: list[ListingCandidate] = []
        for position, href in enumerate(links, start=1):
            candidate_url = normalize_candidate_url(source_domain, href, base_url=str(bundle.meta.get("url_final") or bundle.meta.get("url_original") or ""), source_config=source_config)
            if not is_detail_candidate_url(candidate_url, source_domain=source_domain, source_config=source_config):
                _increment_reason(report, "non_detail_url")
                continue
            identity = resolve_listing_identity({"source_domain": source_domain, "url_final": candidate_url})
            raw_candidates.append(
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
        report.cards_detected = len(links)
        report.candidates_emitted = len(raw_candidates)
        report.candidates = dedupe_candidates(raw_candidates)
        report.candidates_deduped_out = max(0, len(raw_candidates) - len(report.candidates))
        return report

    soup = BeautifulSoup(html, "lxml")
    cards = _select_cards(soup, strategy)
    report.cards_detected = len(cards)

    raw_candidates: list[ListingCandidate] = []
    for card_position, card in enumerate(cards, start=1):
        anchors = _select_card_anchors(card, strategy)
        if not anchors:
            _increment_reason(report, "missing_anchor")
            continue

        best_rejection = "non_detail_url"
        accepted = False
        for anchor in anchors:
            candidate, rejection_reason = _candidate_from_anchor(
                anchor=anchor,
                card=card,
                bundle=bundle,
                source_domain=source_domain,
                parser_key=parser_key,
                job_name=job_name,
                harvest_run_id=harvest_run_id,
                page_number=page_number,
                card_position=card_position,
                strategy=strategy,
            )
            if candidate is not None:
                raw_candidates.append(candidate)
                accepted = True
                break
            if rejection_reason:
                best_rejection = rejection_reason

        if not accepted:
            _increment_reason(report, best_rejection)

    deduped = dedupe_candidates(raw_candidates)
    report.candidates_emitted = len(raw_candidates)
    report.candidates_deduped_out = max(0, len(raw_candidates) - len(deduped))
    report.candidates = deduped
    return report


def normalize_candidate_url(
    source_domain: str,
    url: str,
    *,
    base_url: str,
    source_config: dict[str, object] | None = None,
) -> str:
    strategy = get_portal_strategy(source_domain, source_config=source_config)
    return normalize_candidate_url_for_strategy(strategy, url, base_url=base_url)


def is_candidate_detail_url(
    url: str,
    *,
    source_domain: str,
    source_config: dict[str, object] | None = None,
) -> bool:
    return is_detail_candidate_url(url, source_domain=source_domain, source_config=source_config)


def is_detail_candidate_url(
    url: str,
    *,
    source_domain: str,
    source_config: dict[str, object] | None = None,
) -> bool:
    if not url:
        return False

    strategy = get_portal_strategy(source_domain, source_config=source_config)
    parsed = re.match(r"^https?://([^/]+)", url.lower())
    if parsed is not None:
        netloc = parsed.group(1)
        if source_domain and netloc != source_domain and not netloc.endswith(f".{source_domain}"):
            return False

    low = url.lower()
    for pattern in strategy.reject_patterns:
        if re.search(pattern, low):
            return False

    if strategy.is_detail_candidate_url_fn is not None:
        return is_detail_candidate_url_for_strategy(strategy, url)

    return any(re.search(pattern, low) for pattern in strategy.detail_patterns)


def extract_listing_candidates(
    bundle: SnapshotBundle,
    *,
    job_name: str,
    harvest_run_id: str,
    source_domain: str,
    parser_key: str,
    page_number: int,
    source_config: dict[str, object] | None = None,
) -> list[ListingCandidate]:
    report = extract_listing_candidates_with_report(
        bundle,
        job_name=job_name,
        harvest_run_id=harvest_run_id,
        source_domain=source_domain,
        parser_key=parser_key,
        page_number=page_number,
        source_config=source_config,
    )
    return report.candidates


def dedupe_candidates(candidates: list[ListingCandidate]) -> list[ListingCandidate]:
    deduped: dict[str, ListingCandidate] = {}
    for candidate in candidates:
        key = candidate.dedupe_key or candidate.listing_key or candidate.candidate_url
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = candidate
            continue

        existing_score = sum(
            1
            for value in [existing.title_text, existing.price_text, existing.location_text, existing.surface_text, existing.rooms_text]
            if value
        )
        candidate_score = sum(
            1
            for value in [candidate.title_text, candidate.price_text, candidate.location_text, candidate.surface_text, candidate.rooms_text]
            if value
        )
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
