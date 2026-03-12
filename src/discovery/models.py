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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
