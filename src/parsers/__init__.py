"""Parsers package."""

from src.parsers.models import ParsedRecord
from src.parsers.registry import get_parser, parse_with_registry, resolve_parser_key_for_domain
from src.parsers.runner import parse_discovered, parse_job_run, parse_snapshot
from src.parsers.snapshot_bridge import SnapshotBridge, SnapshotBundle

__all__ = [
    "ParsedRecord",
    "SnapshotBridge",
    "SnapshotBundle",
    "get_parser",
    "resolve_parser_key_for_domain",
    "parse_with_registry",
    "parse_snapshot",
    "parse_job_run",
    "parse_discovered",
]
