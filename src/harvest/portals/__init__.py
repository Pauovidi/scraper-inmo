from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from src.utils.listing_identity import canonicalize_url

DEFAULT_CARD_SELECTORS = (
    "article",
    "li",
    "[data-testid*='listing']",
    "[data-testid*='property']",
    "[class*='card']",
    "[class*='item']",
    "[class*='result']",
    "[class*='listing']",
)


@dataclass(frozen=True)
class PortalStrategy:
    source_domain: str
    card_selectors: tuple[str, ...] = DEFAULT_CARD_SELECTORS
    detail_link_selectors: tuple[str, ...] = ()
    detail_patterns: tuple[str, ...] = ()
    reject_patterns: tuple[str, ...] = ()
    query_drop_keys: tuple[str, ...] = ()
    listing_start_urls: tuple[str, ...] = ()
    max_listing_pages: int | None = None
    listing_page_param: str | None = None
    listing_page_url_template: str | None = None
    normalize_candidate_url_fn: Callable[[str, str], str] | None = None
    is_detail_candidate_url_fn: Callable[[str], bool] | None = None


def _default_normalize_candidate_url(url: str, *, base_url: str, strategy: PortalStrategy) -> str:
    absolute = urljoin(base_url, url)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return ""

    drop_keys = {key.lower() for key in strategy.query_drop_keys}
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=False)
        if key.lower() not in drop_keys
    ]
    normalized = urlunparse(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path,
            parsed.params,
            urlencode(query, doseq=True),
            "",
        )
    )
    return canonicalize_url(normalized)


def _merge_strategies(strategy: PortalStrategy, source_config: Mapping[str, object] | None = None) -> PortalStrategy:
    if source_config is None:
        return strategy

    card_selectors = tuple(
        str(item)
        for item in (source_config.get("listing_card_selectors") or strategy.card_selectors)
        if str(item).strip()
    )
    detail_patterns = tuple(
        str(item)
        for item in (source_config.get("listing_detail_url_patterns") or strategy.detail_patterns)
        if str(item).strip()
    )
    reject_patterns = tuple(
        str(item)
        for item in (source_config.get("listing_reject_url_patterns") or strategy.reject_patterns)
        if str(item).strip()
    )
    return PortalStrategy(
        source_domain=strategy.source_domain,
        card_selectors=card_selectors or strategy.card_selectors,
        detail_link_selectors=strategy.detail_link_selectors,
        detail_patterns=detail_patterns or strategy.detail_patterns,
        reject_patterns=reject_patterns or strategy.reject_patterns,
        query_drop_keys=strategy.query_drop_keys,
        listing_start_urls=tuple(
            str(item) for item in (source_config.get("listing_start_urls") or strategy.listing_start_urls) if str(item).strip()
        ),
        max_listing_pages=int(source_config.get("max_listing_pages", strategy.max_listing_pages or 1) or 1),
        listing_page_param=str(source_config.get("listing_page_param") or strategy.listing_page_param or "").strip() or None,
        listing_page_url_template=str(source_config.get("listing_page_url_template") or strategy.listing_page_url_template or "").strip() or None,
        normalize_candidate_url_fn=strategy.normalize_candidate_url_fn,
        is_detail_candidate_url_fn=strategy.is_detail_candidate_url_fn,
    )


def normalize_candidate_url_for_strategy(strategy: PortalStrategy, url: str, *, base_url: str) -> str:
    if strategy.normalize_candidate_url_fn is not None:
        return strategy.normalize_candidate_url_fn(url, base_url=base_url)
    return _default_normalize_candidate_url(url, base_url=base_url, strategy=strategy)


def is_detail_candidate_url_for_strategy(strategy: PortalStrategy, url: str) -> bool:
    if strategy.is_detail_candidate_url_fn is not None:
        return strategy.is_detail_candidate_url_fn(url)
    return False


from src.harvest.portals.fotocasa import FOTOCASA_STRATEGY
from src.harvest.portals.idealista import IDEALISTA_STRATEGY
from src.harvest.portals.milanuncios import MILANUNCIOS_STRATEGY
from src.harvest.portals.pisos import PISOS_STRATEGY
from src.harvest.portals.yaencontre import YAENCONTRE_STRATEGY

PORTAL_STRATEGIES = {
    FOTOCASA_STRATEGY.source_domain: FOTOCASA_STRATEGY,
    IDEALISTA_STRATEGY.source_domain: IDEALISTA_STRATEGY,
    MILANUNCIOS_STRATEGY.source_domain: MILANUNCIOS_STRATEGY,
    PISOS_STRATEGY.source_domain: PISOS_STRATEGY,
    YAENCONTRE_STRATEGY.source_domain: YAENCONTRE_STRATEGY,
}


def get_portal_strategy(source_domain: str, source_config: Mapping[str, object] | None = None) -> PortalStrategy:
    base = PORTAL_STRATEGIES.get(source_domain, PortalStrategy(source_domain=source_domain))
    return _merge_strategies(base, source_config=source_config)


__all__ = [
    "PortalStrategy",
    "DEFAULT_CARD_SELECTORS",
    "PORTAL_STRATEGIES",
    "get_portal_strategy",
    "is_detail_candidate_url_for_strategy",
    "normalize_candidate_url_for_strategy",
]
