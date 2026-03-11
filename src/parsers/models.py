from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class ParsedRecord:
    parser_key: str
    source_domain: str
    snapshot_id: str
    run_id: str
    snapshot_path: str
    url_original: str
    url_final: str
    page_kind: str
    title: str | None
    price_text: str | None
    price_value: float | None
    price_currency: str | None
    location_text: str | None
    surface_text: str | None
    surface_sqm: float | None
    rooms_text: str | None
    rooms_count: int | None
    description_text: str | None
    extracted_links: list[str] = field(default_factory=list)
    extracted_at: str = ""
    parse_status: str = "error"
    parse_errors: list[str] = field(default_factory=list)
    confidence_score: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
