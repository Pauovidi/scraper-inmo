from __future__ import annotations

import re
import unicodedata
from typing import Any, Mapping

SPAIN_PROVINCES = [
    "A Coruña",
    "Álava",
    "Albacete",
    "Alicante",
    "Almería",
    "Asturias",
    "Ávila",
    "Badajoz",
    "Barcelona",
    "Bizkaia",
    "Burgos",
    "Cáceres",
    "Cádiz",
    "Cantabria",
    "Castellón",
    "Ceuta",
    "Ciudad Real",
    "Córdoba",
    "Cuenca",
    "Girona",
    "Granada",
    "Guadalajara",
    "Gipuzkoa",
    "Huelva",
    "Huesca",
    "Illes Balears",
    "Jaén",
    "La Rioja",
    "Las Palmas",
    "León",
    "Lleida",
    "Lugo",
    "Madrid",
    "Málaga",
    "Melilla",
    "Murcia",
    "Navarra",
    "Ourense",
    "Palencia",
    "Pontevedra",
    "Salamanca",
    "Santa Cruz de Tenerife",
    "Segovia",
    "Sevilla",
    "Soria",
    "Tarragona",
    "Teruel",
    "Toledo",
    "Valencia",
    "Valladolid",
    "Zamora",
    "Zaragoza",
]

BLOCKED_TEXT_PATTERNS = [
    r"sentimos la interrupcion",
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
    "A Coruña": ("a coruna", "la coruna"),
    "Álava": ("alava", "araba"),
    "Alicante": ("alicante", "alacant"),
    "Asturias": ("asturias", "oviedo"),
    "Bizkaia": ("bizkaia", "vizcaya"),
    "Castellón": ("castellon", "castello"),
    "Gipuzkoa": ("gipuzkoa", "guipuzcoa"),
    "Illes Balears": ("illes balears", "islas baleares", "baleares"),
    "La Rioja": ("la rioja", "rioja"),
    "Navarra": ("navarra", "nafarroa"),
    "Ourense": ("ourense", "orense"),
    "Valencia": ("valencia", "valencia/valencia"),
}

CITY_TO_PROVINCE: dict[str, str] = {
    "abadiño": "Bizkaia",
    "ajangiz": "Bizkaia",
    "amorebieta": "Bizkaia",
    "arrigorriaga": "Bizkaia",
    "barakaldo": "Bizkaia",
    "basauri": "Bizkaia",
    "bilbao": "Bizkaia",
    "derio": "Bizkaia",
    "durango": "Bizkaia",
    "elorrio": "Bizkaia",
    "erandio": "Bizkaia",
    "etxebarri": "Bizkaia",
    "galdakao": "Bizkaia",
    "gatika": "Bizkaia",
    "gernika": "Bizkaia",
    "guernica": "Bizkaia",
    "igorre": "Bizkaia",
    "iurreta": "Bizkaia",
    "lezama": "Bizkaia",
    "mungia": "Bizkaia",
    "orozko": "Bizkaia",
    "ortuella": "Bizkaia",
    "portugalete": "Bizkaia",
    "sestao": "Bizkaia",
    "sondika": "Bizkaia",
    "trapagaran": "Bizkaia",
    "ugao": "Bizkaia",
    "vizcaya": "Bizkaia",
    "zamudio": "Bizkaia",
    "zierbena": "Bizkaia",
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

