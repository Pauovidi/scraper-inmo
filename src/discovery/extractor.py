from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None

from src.parsers.snapshot_bridge import SnapshotBundle

BASE_EXCLUDE = [
    "login",
    "signin",
    "register",
    "contact",
    "favorit",
    "share",
    "whatsapp",
    "mailto:",
    "tel:",
    "javascript:",
    "#",
]

RULES_BY_PARSER_KEY = {
    "generic": {
        "include": [],
        "exclude": [r"/blog", r"/ayuda", r"/privacidad"],
    },
    "generic_listing": {
        "include": [r"/inmueble", r"/ficha", r"/detalle", r"/anuncio"],
        "exclude": [r"/contacto", r"/favoritos", r"/mapa"],
    },
    # First portal-specific discovery ruleset
    "idealista_listing": {
        "include": [r"/inmueble/", r"\d{6,}"],
        "exclude": [r"/agencia", r"/obra-nueva", r"/mapa"],
    },
}


def _extract_html_links(html: str | None) -> list[str]:
    if not html:
        return []

    if BeautifulSoup is None:
        return re.findall(r'href=["\']([^"\']+)["\']', html, re.IGNORECASE)

    soup = BeautifulSoup(html, "lxml")
    out: list[str] = []
    for a in soup.find_all("a"):
        href = a.get("href")
        if href:
            out.append(str(href).strip())
    return out


def _extract_markdown_links(markdown: str | None) -> list[str]:
    if not markdown:
        return []
    return re.findall(r"\[[^\]]+\]\(([^\)]+)\)", markdown)


def _normalize_urls(links: list[str], base_url: str) -> list[str]:
    out: list[str] = []
    for link in links:
        if not link:
            continue
        abs_url = urljoin(base_url, link)
        parsed = urlparse(abs_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        out.append(abs_url)
    return out


def _domain_match(url: str, allowed_domain: str | None) -> bool:
    if not allowed_domain:
        return True
    netloc = urlparse(url).netloc.lower()
    return netloc == allowed_domain or netloc.endswith(f".{allowed_domain}")


def _base_excluded(url: str) -> bool:
    low = url.lower()
    return any(token in low for token in BASE_EXCLUDE)


def _apply_rule_filter(url: str, parser_key: str) -> bool:
    rules = RULES_BY_PARSER_KEY.get(parser_key, RULES_BY_PARSER_KEY["generic"])
    low = url.lower()

    for pattern in rules.get("exclude", []):
        if re.search(pattern, low):
            return False

    includes = rules.get("include", [])
    if not includes:
        return True

    return any(re.search(pattern, low) for pattern in includes)


def discover_candidate_urls(
    bundle: SnapshotBundle,
    *,
    parser_key: str,
    allowed_domain: str | None,
) -> list[str]:
    meta = bundle.meta
    base_url = meta.get("url_final") or meta.get("url_original") or ""

    raw_links = _extract_html_links(bundle.html) + _extract_markdown_links(bundle.markdown)
    normalized = _normalize_urls(raw_links, base_url=base_url)

    dedup: list[str] = []
    seen: set[str] = set()
    for url in normalized:
        if url in seen:
            continue
        seen.add(url)

        if not _domain_match(url, allowed_domain):
            continue
        if _base_excluded(url):
            continue
        if not _apply_rule_filter(url, parser_key=parser_key):
            continue

        dedup.append(url)

    return dedup
