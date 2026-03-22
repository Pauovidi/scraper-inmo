from __future__ import annotations

from typing import Any

from src.utils.listing_identity import (
    canonicalize_url,
    clean_text as _clean_text,
    extract_external_id,
    float_or_none as _float_or_none,
    int_or_none as _int_or_none,
    portal_slug,
    resolve_listing_identity,
)

PORTAL_ORDER = ["fotocasa", "idealista", "milanuncios", "pisos", "yaencontre"]
PORTAL_LABELS = {
    "fotocasa": "Fotocasa",
    "idealista": "Idealista",
    "milanuncios": "Milanuncios",
    "pisos": "Pisos",
    "yaencontre": "Yaencontre",
}
def portal_label(portal: str) -> str:
    return PORTAL_LABELS.get(portal, portal.title() if portal else "Desconocido")


def normalize_listing_record(record: dict[str, Any]) -> dict[str, Any]:
    identity = resolve_listing_identity(record)
    url_final = _clean_text(record.get("url_final") or record.get("url_original"))

    return {
        "portal": identity["portal"],
        "source_domain": _clean_text(record.get("source_domain")) or "unknown-domain",
        "listing_key": identity["listing_key"],
        "external_id": identity["external_id"] or None,
        "canonical_url": identity["canonical_url"] or None,
        "dedupe_method": identity["dedupe_method"],
        "url_final": url_final or None,
        "title": _clean_text(record.get("title")) or None,
        "price_text": _clean_text(record.get("price_text")) or None,
        "price_value": _float_or_none(record.get("price_value")),
        "location_text": _clean_text(record.get("location_text")) or None,
        "surface_sqm": _float_or_none(record.get("surface_sqm")),
        "rooms_count": _int_or_none(record.get("rooms_count")),
        "parser_key": _clean_text(record.get("parser_key")) or None,
        "parse_status": _clean_text(record.get("parse_status")) or None,
    }


def dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for record in records:
        normalized = normalize_listing_record(record)
        deduped[str(normalized["listing_key"])] = normalized
    return list(deduped.values())
