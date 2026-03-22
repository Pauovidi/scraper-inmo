from __future__ import annotations

import re

from src.harvest.portals import PortalStrategy

IDEALISTA_DETAIL_PATTERNS = [
    r"/inmueble/\d+/?$",
]

IDEALISTA_REJECT_PATTERNS = [
    r"/agencia",
    r"/obra-nueva",
    r"/perfil",
    r"/mapa",
    r"/inmobiliaria",
    r"/favoritos",
    r"/publicar",
    r"/login",
    r"/pagina-\d+\.htm$",
]

IDEALISTA_QUERY_DROP_KEYS = (
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "pagina",
    "page",
)


def idealista_is_detail_candidate_url(url: str) -> bool:
    low = (url or "").lower()
    if not low:
        return False

    for pattern in IDEALISTA_REJECT_PATTERNS:
        if re.search(pattern, low):
            return False
    return any(re.search(pattern, low) for pattern in IDEALISTA_DETAIL_PATTERNS)


IDEALISTA_STRATEGY = PortalStrategy(
    source_domain="idealista.com",
    card_selectors=(
        "article",
        "li",
        "[data-testid*='listing']",
        "[data-testid*='property']",
        "[class*='item']",
        "[class*='card']",
    ),
    detail_link_selectors=("a[href*='/inmueble/']",),
    detail_patterns=tuple(IDEALISTA_DETAIL_PATTERNS),
    reject_patterns=tuple(IDEALISTA_REJECT_PATTERNS),
    query_drop_keys=IDEALISTA_QUERY_DROP_KEYS,
    listing_start_urls=("https://www.idealista.com/alquiler-naves/bizkaia/",),
    max_listing_pages=4,
    listing_page_url_template="https://www.idealista.com/alquiler-naves/bizkaia/pagina-{page}.htm",
    is_detail_candidate_url_fn=idealista_is_detail_candidate_url,
)
