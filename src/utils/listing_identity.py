from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import urlsplit, urlunsplit


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def float_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def int_or_none(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def portal_slug(source_domain: Any) -> str:
    domain = clean_text(source_domain).lower()
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


def canonicalize_url(url: Any) -> str:
    raw = clean_text(url)
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
    source_domain = clean_text(record.get("source_domain")) or "unknown-domain"
    url_final = clean_text(record.get("url_final") or record.get("url_original"))
    portal = portal_slug(source_domain)

    external_id = clean_text(record.get("external_id")) or extract_external_id(source_domain, url_final) or ""
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
            clean_text(record.get("title")).lower(),
            clean_text(record.get("price_value") or record.get("price_text")).lower(),
            clean_text(record.get("location_text")).lower(),
            clean_text(record.get("surface_sqm") or record.get("surface_text")).lower(),
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

