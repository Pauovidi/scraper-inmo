from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from src.harvest.portals import PortalStrategy
from src.utils.listing_identity import canonicalize_url

YAENCONTRE_LISTING_START_URLS = [
    "https://www.yaencontre.com/alquiler/naves/bizkaia",
    "https://www.yaencontre.com/venta/naves/bizkaia",
]

YAENCONTRE_LISTING_PAGE_PARAM = "page"
YAENCONTRE_MAX_LISTING_PAGES = 5

YAENCONTRE_DETAIL_PATTERNS = [
    r"/inmueble/\d+(?:/|$)",
    r"/\d{5,}(?:/|$)",
    r"/[a-z0-9-]+-\d{5,}(?:\.htm)?(?:\?|/|$)",
]

YAENCONTRE_LISTING_EXCLUDES = [
    r"/alquiler/",
    r"/venta/",
    r"/obra-nueva",
    r"/inmobiliaria",
    r"/agencia",
    r"/perfil",
    r"/mapa",
    r"/buscador",
    r"/resultados",
    r"/filtros?",
    r"/contacto",
    r"/favoritos",
    r"/alertas",
    r"/login",
    r"javascript:",
    r"mailto:",
    r"tel:",
]

YAENCONTRE_QUERY_DROP_KEYS = {
    "from",
    "page",
    "pagina",
    "offset",
    "limit",
    "sort",
    "orden",
    "order",
    "ref",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
}


def yaencontre_listing_start_urls() -> list[str]:
    return list(YAENCONTRE_LISTING_START_URLS)


def yaencontre_normalize_candidate_url(url: str, *, base_url: str) -> str:
    absolute = urljoin(base_url, url)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return ""

    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=False)
        if key.lower() not in YAENCONTRE_QUERY_DROP_KEYS
    ]
    normalized = urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, parsed.params, urlencode(query, doseq=True), ""))
    return canonicalize_url(normalized)


def yaencontre_is_detail_candidate_url(url: str) -> bool:
    low = (url or "").lower()
    if not low:
        return False

    for pattern in YAENCONTRE_LISTING_EXCLUDES:
        if re.search(pattern, low):
            return False

    return any(re.search(pattern, low) for pattern in YAENCONTRE_DETAIL_PATTERNS)


YAENCONTRE_STRATEGY = PortalStrategy(
    source_domain="yaencontre.com",
    card_selectors=(
        "article",
        "li",
        "[class*='property']",
        "[class*='card']",
        "[data-testid*='property']",
    ),
    detail_link_selectors=(
        "a[href*='/inmueble/']",
        "a[href*='-']",
    ),
    detail_patterns=tuple(YAENCONTRE_DETAIL_PATTERNS),
    reject_patterns=tuple(YAENCONTRE_LISTING_EXCLUDES),
    query_drop_keys=tuple(YAENCONTRE_QUERY_DROP_KEYS),
    listing_start_urls=tuple(YAENCONTRE_LISTING_START_URLS),
    max_listing_pages=YAENCONTRE_MAX_LISTING_PAGES,
    listing_page_param=YAENCONTRE_LISTING_PAGE_PARAM,
    normalize_candidate_url_fn=yaencontre_normalize_candidate_url,
    is_detail_candidate_url_fn=yaencontre_is_detail_candidate_url,
)
