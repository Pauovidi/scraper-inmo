from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class ListingPagePlan:
    source_domain: str
    parser_key: str
    listing_start_url: str
    listing_page_url: str
    page_number: int
    max_listing_pages: int
    rate_limit_seconds: float
    timeout_seconds: int
    pagination_strategy: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ListingCandidate:
    job_name: str
    harvest_run_id: str
    source_domain: str
    parser_key: str
    listing_page_url: str
    listing_start_url: str
    listing_snapshot_path: str
    listing_snapshot_id: str
    listing_snapshot_run_id: str
    page_number: int
    card_position: int
    candidate_url: str
    canonical_url: str
    title_text: str | None
    price_text: str | None
    location_text: str | None
    surface_text: str | None
    rooms_text: str | None
    external_id: str | None
    listing_key: str
    dedupe_key: str
    dedupe_method: str
    raw_text: str | None
    discovered_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

