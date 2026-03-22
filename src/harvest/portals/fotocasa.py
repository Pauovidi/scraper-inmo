from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from src.harvest.portals import PortalStrategy
from src.utils.listing_identity import canonicalize_url

FOTOCASA_LISTING_START_URLS = [
    "https://www.fotocasa.es/es/alquiler/naves-industriales/bizkaia-provincia/todas-las-zonas/l",
    "https://www.fotocasa.es/es/comprar/naves-industriales/bizkaia-provincia/todas-las-zonas/l",
    "https://www.fotocasa.es/es/alquiler/naves-industriales/poi/cercanias-zorrotza-bizkaia/l",
    "https://www.fotocasa.es/es/alquiler/naves-industriales/bizkaia-provincia/duranguesado/l",
    "https://www.fotocasa.es/es/alquiler/naves-industriales/bizkaia-provincia/uribe/l",
    "https://www.fotocasa.es/es/comprar/locales/bizkaia-provincia/todas-las-zonas/l",
]

FOTOCASA_LISTING_PAGE_PARAM = "pagina"
FOTOCASA_MAX_LISTING_PAGES = 4

FOTOCASA_DETAIL_PATTERNS = [
    r"/\d+/d(?:\?|$)",
    r"/inmueble/\d+",
    r"/es/(alquiler|comprar)/naves-industriales/",
    r"/es/(alquiler|comprar)/locales/",
]

FOTOCASA_LISTING_EXCLUDES = [
    r"/inmobiliaria",
    r"/agencia",
    r"/perfil",
    r"/l/\d+(?:\?|$)",
    r"/crear-alerta",
    r"/contactar",
    r"/contacto",
    r"/mapa",
    r"/favoritos",
    r"/share",
    r"/whatsapp",
    r"javascript:",
    r"mailto:",
    r"tel:",
]


def fotocasa_listing_start_urls() -> list[str]:
    return list(FOTOCASA_LISTING_START_URLS)


def fotocasa_normalize_candidate_url(url: str, *, base_url: str) -> str:
    absolute = urljoin(base_url, url)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return ""

    query = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=False) if key.lower() not in {"from", "multimedia", "isgalleryopen", "iszoomgalleryopen", "page", "pagina"}]
    normalized = urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, parsed.params, urlencode(query, doseq=True), ""))
    return canonicalize_url(normalized)


def fotocasa_is_detail_candidate_url(url: str) -> bool:
    low = url.lower()
    if not low:
        return False

    for pattern in FOTOCASA_LISTING_EXCLUDES:
        if re.search(pattern, low):
            return False

    return any(re.search(pattern, low) for pattern in FOTOCASA_DETAIL_PATTERNS)


FOTOCASA_STRATEGY = PortalStrategy(
    source_domain="fotocasa.es",
    card_selectors=(
        "article",
        "[class*='re-Card']",
        "[class*='re-SearchResult']",
        "[data-testid*='card']",
        "[data-testid*='search']",
    ),
    detail_link_selectors=(
        "a[href*='/d']",
        "a[href*='/comprar/']",
        "a[href*='/alquiler/']",
    ),
    detail_patterns=tuple(FOTOCASA_DETAIL_PATTERNS),
    reject_patterns=tuple(FOTOCASA_LISTING_EXCLUDES),
    query_drop_keys=("from", "multimedia", "isgalleryopen", "iszoomgalleryopen", "page", "pagina"),
    listing_start_urls=tuple(FOTOCASA_LISTING_START_URLS),
    max_listing_pages=FOTOCASA_MAX_LISTING_PAGES,
    listing_page_param=FOTOCASA_LISTING_PAGE_PARAM,
    normalize_candidate_url_fn=fotocasa_normalize_candidate_url,
    is_detail_candidate_url_fn=fotocasa_is_detail_candidate_url,
)
