from __future__ import annotations

import re
import unicodedata
from typing import Any, Mapping

from src.config import load_province_catalog

PROVINCE_CATALOG = load_province_catalog()
SPAIN_PROVINCES = list(PROVINCE_CATALOG["provinces"])
DEMO_TARGET_PROVINCES = list(PROVINCE_CATALOG.get("demo_target_provinces", []))

BLOCKED_TEXT_PATTERNS = [
    r"sentimos la interrupci",
    r"pardon our interruption",
    r"access denied",
    r"verify you are human",
    r"verifica que eres humano",
    r"verifica que eres una persona",
    r"security check",
    r"captcha",
    r"geetest",
    r"noindex nofollow",
]

PROVINCE_ALIASES: dict[str, tuple[str, ...]] = {
    province: tuple(str(alias) for alias in aliases)
    for province, aliases in dict(PROVINCE_CATALOG.get("aliases", {})).items()
}

CITY_TO_PROVINCE: dict[str, str] = {
    str(city): str(province)
    for city, province in dict(PROVINCE_CATALOG.get("city_to_province", {})).items()
}


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _normalize_text(value: object | None) -> str:
    if value is None:
        return ""
    text = _strip_accents(str(value)).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _contains_alias(text: str, alias: str) -> bool:
    pattern = rf"(?<![a-z0-9]){re.escape(alias)}(?![a-z0-9])"
    return re.search(pattern, text) is not None


def normalize_province_name(value: object | None) -> str | None:
    text = _normalize_text(value)
    if not text:
        return None

    for province in SPAIN_PROVINCES:
        canonical = _normalize_text(province)
        if text == canonical:
            return province

    for province, aliases in PROVINCE_ALIASES.items():
        if text in {_normalize_text(alias) for alias in aliases}:
            return province
    return None


def infer_record_province(record: Mapping[str, Any]) -> str | None:
    existing = normalize_province_name(record.get("province"))
    if existing:
        return existing

    fragments = [
        record.get("location_text"),
        record.get("title"),
        record.get("url_final"),
        record.get("canonical_url"),
        record.get("breadcrumbs"),
        record.get("breadcrumb_text"),
        record.get("breadcrumbs_text"),
        record.get("raw_text"),
    ]
    normalized_fragments = [_normalize_text(fragment) for fragment in fragments if fragment]
    combined = " ".join(fragment for fragment in normalized_fragments if fragment)
    if not combined:
        return None

    for province in SPAIN_PROVINCES:
        alias_candidates = {_normalize_text(province)}
        alias_candidates.update(_normalize_text(alias) for alias in PROVINCE_ALIASES.get(province, ()))
        for alias in alias_candidates:
            if alias and _contains_alias(combined, alias):
                return province

    for city, province in CITY_TO_PROVINCE.items():
        if _contains_alias(combined, _normalize_text(city)):
            return province

    return None


def is_blocked_client_record(record: Mapping[str, Any]) -> bool:
    combined = " ".join(
        _normalize_text(record.get(field))
        for field in ["title", "location_text", "price_text", "url_final", "canonical_url", "raw_text"]
        if record.get(field)
    )
    if not combined:
        return False
    return any(re.search(pattern, combined) for pattern in BLOCKED_TEXT_PATTERNS)


def normalize_client_record(record: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized["province"] = infer_record_province(record)
    return normalized
