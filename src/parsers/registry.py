from __future__ import annotations

from typing import Callable

from src.config import load_source_by_domain
from src.parsers.generic_parser import parse_generic_snapshot
from src.parsers.models import ParsedRecord
from src.parsers.pisos_detail_parser import parse_pisos_detail_snapshot
from src.parsers.snapshot_bridge import SnapshotBundle

ParserFn = Callable[[SnapshotBundle, str], ParsedRecord]

PARSER_REGISTRY: dict[str, ParserFn] = {
    "pisos_detail": parse_pisos_detail_snapshot,
}


def get_parser(parser_key: str) -> ParserFn:
    return PARSER_REGISTRY.get(parser_key, parse_generic_snapshot)


def resolve_parser_key_for_domain(domain: str) -> str:
    try:
        source = load_source_by_domain(domain)
        return str(source.get("parser_key", "generic"))
    except Exception:
        return "generic"


def parse_with_registry(bundle: SnapshotBundle) -> ParsedRecord:
    domain = str(bundle.meta.get("domain", "unknown-domain"))
    parser_key = resolve_parser_key_for_domain(domain)
    parser = get_parser(parser_key)
    return parser(bundle, parser_key)
