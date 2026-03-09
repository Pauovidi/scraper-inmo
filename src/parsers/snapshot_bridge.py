from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.archiver.index import load_snapshot_meta, resolve_meta_path


@dataclass
class SnapshotBundle:
    snapshot_path: Path
    html: str | None
    markdown: str | None
    meta: dict[str, Any]


class SnapshotBridge:
    @staticmethod
    def load(snapshot_path: str | Path) -> SnapshotBundle:
        meta_path = resolve_meta_path(snapshot_path)
        meta = load_snapshot_meta(meta_path)

        html = None
        markdown = None

        html_file = meta.get("files", {}).get("page_html")
        md_file = meta.get("files", {}).get("page_md")

        if html_file:
            html_path = Path(html_file)
            if html_path.exists():
                html = html_path.read_text(encoding="utf-8", errors="replace")

        if md_file:
            md_path = Path(md_file)
            if md_path.exists():
                markdown = md_path.read_text(encoding="utf-8", errors="replace")

        return SnapshotBundle(
            snapshot_path=Path(meta.get("snapshot_path", meta_path.parent)),
            html=html,
            markdown=markdown,
            meta=meta,
        )
