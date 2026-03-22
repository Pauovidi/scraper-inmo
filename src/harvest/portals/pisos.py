from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from src.harvest.portals import PortalStrategy

PISOS_LISTING_START_URLS = [
    "https://www.pisos.com/alquiler/naves-vizcaya_bizkaia/",
    "https://www.pisos.com/alquiler/naves-bilbao/",
    "https://www.pisos.com/alquiler/naves-barakaldo/",
    "https://www.pisos.com/alquiler/naves-basauri/",
]

PISOS_DETAIL_PATTERNS = [
    r"/alquilar/[^/]+/\d+(?:_[^/]+)?/?$",
    r"/alquilar/[^/]*-\d+(?:_[^/]+)?/?$",
    r"/inmueble/\d+/?$",
]

PISOS_REJECT_PATTERNS = [
    r"/alquiler/?$",
    r"/venta/?$",
    r"/naves-vizcaya_bizkaia/\d+/?$",
    r"/inmobiliaria",
    r"/agencia",
    r"/mapa",
    r"/obra-nueva",
    r"/login",
]

PISOS_QUERY_DROP_KEYS = (
    "pagina",
    "page",
    "from",
    "utm_source",
    "utm_medium",
    "utm_campaign",
)


def pisos_normalize_candidate_url(url: str, *, base_url: str) -> str:
    absolute = urljoin(base_url, url)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return ""

    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=False)
        if key.lower() not in PISOS_QUERY_DROP_KEYS
    ]
    path = parsed.path if parsed.path.endswith("/") else f"{parsed.path}/"
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, parsed.params, urlencode(query, doseq=True), ""))


def pisos_is_detail_candidate_url(url: str) -> bool:
    low = (url or "").lower()
    if not low:
        return False

    for pattern in PISOS_REJECT_PATTERNS:
        if re.search(pattern, low):
            return False
    return any(re.search(pattern, low) for pattern in PISOS_DETAIL_PATTERNS)


def pisos_listing_start_urls() -> list[str]:
    return list(PISOS_LISTING_START_URLS)


PISOS_STRATEGY = PortalStrategy(
    source_domain="pisos.com",
    card_selectors=(
        "a.ad-preview__title[href]",
    ),
    detail_link_selectors=(
        "a.ad-preview__title[href]",
    ),
    detail_patterns=tuple(PISOS_DETAIL_PATTERNS),
    reject_patterns=tuple(PISOS_REJECT_PATTERNS),
    query_drop_keys=PISOS_QUERY_DROP_KEYS,
    listing_start_urls=tuple(PISOS_LISTING_START_URLS),
    max_listing_pages=4,
    listing_page_url_template="{base}{page}/",
    normalize_candidate_url_fn=pisos_normalize_candidate_url,
    is_detail_candidate_url_fn=pisos_is_detail_candidate_url,
)
