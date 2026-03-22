from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class DiscoveredUrl:
    job_name: str
    run_id: str
    source_domain: str
    parser_key: str
    parent_snapshot_id: str
    parent_run_id: str
    parent_snapshot_path: str
    page_kind: str
    discovered_url: str
    discovered_at: str
    external_id: str | None = None
    candidate_listing_key: str | None = None
    title_text: str | None = None
    price_text: str | None = None
    location_text: str | None = None
    surface_text: str | None = None
    rooms_text: str | None = None
    listing_page_url: str | None = None
    acquisition_type: str | None = None
    selection_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
