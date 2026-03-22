from __future__ import annotations

import re

from src.harvest.portals import PortalStrategy

MILANUNCIOS_DETAIL_PATTERNS = [
    r"-\d+\.htm(?:\?|$)",
]

MILANUNCIOS_REJECT_PATTERNS = [
    r"/inmobiliaria",
    r"/profesional",
    r"/mis-anuncios",
    r"/buscador",
    r"/s?buscador",
    r"/login",
    r"/registro",
]

MILANUNCIOS_QUERY_DROP_KEYS = (
    "pagina",
    "page",
    "from",
    "ref",
    "utm_source",
    "utm_medium",
    "utm_campaign",
)


def milanuncios_is_detail_candidate_url(url: str) -> bool:
    low = (url or "").lower()
    if not low:
        return False

    for pattern in MILANUNCIOS_REJECT_PATTERNS:
        if re.search(pattern, low):
            return False
    return any(re.search(pattern, low) for pattern in MILANUNCIOS_DETAIL_PATTERNS)


MILANUNCIOS_STRATEGY = PortalStrategy(
    source_domain="milanuncios.com",
    card_selectors=(
        "article[data-testid='AD_CARD']",
        "article[data-testid*='AD_CARD']",
        "article",
        "[class*='AdCard']",
    ),
    detail_link_selectors=(
        "a.ma-AdCardListingV2-TitleLink[href]",
        "a[href$='.htm']",
    ),
    detail_patterns=tuple(MILANUNCIOS_DETAIL_PATTERNS),
    reject_patterns=tuple(MILANUNCIOS_REJECT_PATTERNS),
    query_drop_keys=MILANUNCIOS_QUERY_DROP_KEYS,
    listing_start_urls=("https://www.milanuncios.com/alquiler-de-naves-industriales-en-vizcaya/",),
    max_listing_pages=5,
    listing_page_param="pagina",
    is_detail_candidate_url_fn=milanuncios_is_detail_candidate_url,
)
