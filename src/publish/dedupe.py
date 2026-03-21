from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import urlsplit, urlunsplit

PORTAL_ORDER = ["fotocasa", "idealista", "milanuncios", "pisos", "yaencontre"]
PORTAL_LABELS = {
    "fotocasa": "Fotocasa",
    "idealista": "Idealista",
    "milanuncios": "Milanuncios",
    "pisos": "Pisos",
    "yaencontre": "Yaencontre",
}


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _float_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def portal_slug(source_domain: Any) -> str:
    domain = _clean_text(source_domain).lower()
    if "fotocasa" in domain:
        return "fotocasa"
    if "idealista" in domain:
        return "idealista"
    if "milanuncios" in domain:
        return "milanuncios"
    if "pisos" in domain:
        return "pisos"
    if "yaencontre" in domain:
        return "yaencontre"
    return domain or "desconocido"


def portal_label(portal: str) -> str:
    return PORTAL_LABELS.get(portal, portal.title() if portal else "Desconocido")


def canonicalize_url(url: Any) -> str:
    raw = _clean_text(url)
    if not raw:
        return ""

    parts = urlsplit(raw)
    path = re.sub(r"/+$", "", parts.path or "")
    normalized_path = path or "/"
    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            normalized_path,
            "",
            "",
        )
    )


def extract_external_id(source_domain: Any, url: Any) -> str | None:
    portal = portal_slug(source_domain)
    candidate = canonicalize_url(url)
    if not candidate:
        return None

    path = urlsplit(candidate).path
    patterns_by_portal = {
        "fotocasa": [r"/(\d+)/d$", r"/(\d+)/?$"],
        "idealista": [r"/inmueble/(\d+)/?$", r"/(\d+)/?$"],
        "milanuncios": [r"-(\d+)\.htm$", r"/(\d+)\.htm$"],
        "pisos": [r"/(\d+)_", r"/(\d+)(?:/)?$"],
        "yaencontre": [r"/inmueble/(\d+)/?$", r"/(\d+)(?:/)?$"],
    }

    for pattern in patterns_by_portal.get(portal, [r"/(\d{5,})(?:/)?$"]):
        match = re.search(pattern, path, flags=re.IGNORECASE)
        if match:
            return match.group(1)

    generic_matches = re.findall(r"(\d{5,})", path)
    return generic_matches[-1] if generic_matches else None


def resolve_listing_identity(record: dict[str, Any]) -> dict[str, str]:
    source_domain = _clean_text(record.get("source_domain")) or "unknown-domain"
    url_final = _clean_text(record.get("url_final") or record.get("url_original"))
    portal = portal_slug(source_domain)

    external_id = _clean_text(record.get("external_id")) or extract_external_id(source_domain, url_final) or ""
    if external_id:
        return {
            "portal": portal,
            "external_id": external_id,
            "canonical_url": canonicalize_url(url_final),
            "dedupe_method": "external_id",
            "listing_key": f"{source_domain}:id:{external_id}",
        }

    canonical_url = canonicalize_url(url_final)
    if canonical_url:
        return {
            "portal": portal,
            "external_id": "",
            "canonical_url": canonical_url,
            "dedupe_method": "canonical_url",
            "listing_key": f"{source_domain}:url:{canonical_url}",
        }

    fingerprint_source = "|".join(
        [
            source_domain,
            _clean_text(record.get("title")).lower(),
            _clean_text(record.get("price_value") or record.get("price_text")).lower(),
            _clean_text(record.get("location_text")).lower(),
            _clean_text(record.get("surface_sqm") or record.get("surface_text")).lower(),
        ]
    )
    fingerprint = hashlib.sha256(fingerprint_source.encode("utf-8")).hexdigest()[:16]
    return {
        "portal": portal,
        "external_id": "",
        "canonical_url": "",
        "dedupe_method": "fingerprint",
        "listing_key": f"{source_domain}:fingerprint:{fingerprint}",
    }


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
