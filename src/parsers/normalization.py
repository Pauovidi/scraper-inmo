from __future__ import annotations

import re


def _to_float(number_text: str) -> float | None:
    value = number_text.strip().replace(" ", "")
    if not value:
        return None

    if "," in value and "." in value:
        if value.rfind(",") > value.rfind("."):
            value = value.replace(".", "")
            value = value.replace(",", ".")
        else:
            value = value.replace(",", "")
    elif "," in value:
        parts = value.split(",")
        if len(parts[-1]) in {1, 2}:
            value = value.replace(".", "")
            value = value.replace(",", ".")
        else:
            value = value.replace(",", "")
    elif "." in value:
        parts = value.split(".")
        if len(parts) > 1 and all(len(part) == 3 for part in parts[1:]):
            value = "".join(parts)

    try:
        return float(value)
    except ValueError:
        return None


def normalize_price(price_text: str | None, fallback_text: str | None = None) -> tuple[float | None, str | None]:
    source = " ".join(part for part in [price_text, fallback_text] if part)
    if not source:
        return None, None

    lower = source.lower()
    currency = None
    if "€" in source or "eur" in lower or "euro" in lower:
        currency = "EUR"
    elif "$" in source or "usd" in lower:
        currency = "USD"
    elif "£" in source or "gbp" in lower:
        currency = "GBP"

    match = re.search(r"\d[\d\.,\s]{1,}", source)
    if not match:
        return None, currency

    value = _to_float(match.group(0))
    return value, currency


def normalize_surface_sqm(surface_text: str | None, fallback_text: str | None = None) -> float | None:
    source = " ".join(part for part in [surface_text, fallback_text] if part)
    if not source:
        return None

    match = re.search(r"(\d[\d\.,\s]{0,})\s?(?:m2|m²|metros\s?cuadrados|metros?)", source, re.IGNORECASE)
    if not match:
        return None

    return _to_float(match.group(1))


def normalize_rooms_count(rooms_text: str | None, fallback_text: str | None = None) -> int | None:
    source = " ".join(part for part in [rooms_text, fallback_text] if part)
    if not source:
        return None

    match = re.search(r"\b(\d{1,2})\s?(?:hab(?:itaciones)?\.?|dormitorios|rooms?)\b", source, re.IGNORECASE)
    if not match:
        return None

    try:
        return int(match.group(1))
    except ValueError:
        return None
