from __future__ import annotations

import time
from dataclasses import asdict
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from src.archiver import archive_url
from src.harvest.models import ListingPagePlan


def build_listing_page_plan(source: dict[str, object]) -> list[ListingPagePlan]:
    listing_start_urls = [str(url) for url in source.get("listing_start_urls", []) if url]
    page_start = int(source.get("listing_page_start", 1) or 1)
    max_listing_pages = int(source.get("max_listing_pages", 1) or 1)
    rate_limit_seconds = float(source.get("rate_limit_seconds", 0) or 0)
    timeout_seconds = int(source.get("timeout_seconds", 20) or 20)

    plans: list[ListingPagePlan] = []
    for start_url in listing_start_urls:
        for page_number in range(page_start, page_start + max_listing_pages):
            listing_page_url, strategy = build_listing_page_url(
                start_url=start_url,
                page_number=page_number,
                page_start=page_start,
                page_param=source.get("listing_page_param"),
                page_url_template=source.get("listing_page_url_template"),
                first_page_uses_start_url=bool(source.get("listing_first_page_uses_start_url", True)),
            )
            plans.append(
                ListingPagePlan(
                    source_domain=str(source.get("domain", "")),
                    parser_key=str(source.get("parser_key", "generic")),
                    listing_start_url=start_url,
                    listing_page_url=listing_page_url,
                    page_number=page_number,
                    max_listing_pages=max_listing_pages,
                    rate_limit_seconds=rate_limit_seconds,
                    timeout_seconds=timeout_seconds,
                    pagination_strategy=strategy,
                )
            )
    return plans


def build_listing_page_url(
    *,
    start_url: str,
    page_number: int,
    page_start: int = 1,
    page_param: object | None = None,
    page_url_template: object | None = None,
    first_page_uses_start_url: bool = True,
) -> tuple[str, str]:
    if first_page_uses_start_url and page_number == page_start:
        return start_url, "start_url"

    if page_url_template:
        return str(page_url_template).format(base=start_url, page=page_number), "template"

    page_param_name = str(page_param or "page").strip()
    parsed = urlparse(start_url)
    query_items = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_items[page_param_name] = str(page_number)
    page_url = urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(query_items, doseq=True),
            parsed.fragment,
        )
    )
    return page_url, "query_param"


def fetch_listing_pages(
    *,
    plans: list[ListingPagePlan],
    archive_fn=archive_url,
    sleep_fn: Callable[[float], None] = time.sleep,
    output_base_dir: Path | None = None,
    snapshot_index_file: Path | None = None,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []

    for idx, plan in enumerate(plans):
        archive_result = archive_fn(
            url=plan.listing_page_url,
            timeout=plan.timeout_seconds,
            output_base_dir=output_base_dir,
            index_file=snapshot_index_file,
            page_kind_hint="listing",
            snapshot_role="listing_page",
            source_domain_override=plan.source_domain,
            slug_hint=f"listing-page-{plan.page_number:03d}",
            extra_meta={
                "listing_start_url": plan.listing_start_url,
                "listing_page_url": plan.listing_page_url,
                "page_number": plan.page_number,
                "pagination_strategy": plan.pagination_strategy,
                "source_domain": plan.source_domain,
                "parser_key": plan.parser_key,
            },
        )

        results.append(
            {
                "plan": asdict(plan),
                "status": archive_result.status,
                "snapshot_id": archive_result.snapshot_id,
                "snapshot_run_id": archive_result.run_id,
                "snapshot_path": str(archive_result.output_dir),
                "meta_path": str(archive_result.meta_path),
            }
        )

        if plan.rate_limit_seconds > 0 and idx < len(plans) - 1:
            sleep_fn(plan.rate_limit_seconds)

    return results

