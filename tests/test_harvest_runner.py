from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.harvest.listing_fetcher import build_listing_page_plan, build_listing_page_url
from src.harvest.runner import harvest_listings
from src.publish.history import write_master_records


class HarvestRunnerTests(unittest.TestCase):
    def test_build_listing_page_url_supports_query_param_and_template(self) -> None:
        query_url, query_strategy = build_listing_page_url(
            start_url="https://example.com/search",
            page_number=2,
            page_param="pagina",
        )
        self.assertEqual(query_strategy, "query_param")
        self.assertEqual(query_url, "https://example.com/search?pagina=2")

        template_url, template_strategy = build_listing_page_url(
            start_url="https://example.com/search/",
            page_number=3,
            page_url_template="https://example.com/search/pagina-{page}.htm",
        )
        self.assertEqual(template_strategy, "template")
        self.assertEqual(template_url, "https://example.com/search/pagina-3.htm")

    def test_build_listing_page_plan_respects_config(self) -> None:
        plans = build_listing_page_plan(
            {
                "domain": "idealista.com",
                "parser_key": "idealista_listing",
                "listing_start_urls": ["https://www.idealista.com/alquiler-naves/bizkaia/"],
                "max_listing_pages": 3,
                "listing_page_start": 1,
                "listing_page_url_template": "https://www.idealista.com/alquiler-naves/bizkaia/pagina-{page}.htm",
                "rate_limit_seconds": 2,
                "timeout_seconds": 20,
            }
        )

        self.assertEqual(len(plans), 3)
        self.assertEqual(plans[0].listing_page_url, "https://www.idealista.com/alquiler-naves/bizkaia/")
        self.assertEqual(plans[1].listing_page_url, "https://www.idealista.com/alquiler-naves/bizkaia/pagina-2.htm")
        self.assertEqual(plans[2].page_number, 3)

    def test_harvest_listings_persists_outputs_and_merges_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            history_root = tmp_path / "history"
            today_iso = date.today().isoformat()
            write_master_records(
                [
                    {
                        "portal": "idealista",
                        "source_domain": "idealista.com",
                        "listing_key": "idealista.com:id:99887777",
                        "external_id": "99887777",
                        "canonical_url": "https://www.idealista.com/inmueble/99887777",
                        "dedupe_method": "external_id",
                        "url_final": "https://www.idealista.com/inmueble/99887777",
                        "title": "Existente hoy",
                        "price_text": "100.000 €",
                        "price_value": 100000.0,
                        "location_text": "Bilbao",
                        "surface_sqm": 120.0,
                        "rooms_count": 1,
                        "first_seen_date": today_iso,
                        "last_seen_date": today_iso,
                        "seen_count": 1,
                        "workflow_status": "pending",
                        "workflow_updated_at": f"{today_iso}T00:00:00Z",
                        "workflow_note": None,
                        "parser_key": "idealista_listing",
                        "parse_status": "ok",
                    }
                ],
                root_dir=history_root,
            )

            html_by_page = {
                "https://www.idealista.com/alquiler-naves/bizkaia/": """
                    <article class="listing-card" data-id="99887766">
                      <a href="/inmueble/99887766/">Nave industrial en Bilbao</a>
                      <span class="price">230.000 €</span>
                      <span class="location">Bilbao</span>
                      <span class="surface">450 m2</span>
                    </article>
                    <article class="listing-card" data-id="99887777">
                      <a href="/inmueble/99887777/">Existente hoy</a>
                      <span class="price">100.000 €</span>
                      <span class="location">Bilbao</span>
                    </article>
                """,
                "https://www.idealista.com/alquiler-naves/bizkaia/pagina-2.htm": """
                    <article class="listing-card" data-id="99887766">
                      <a href="/inmueble/99887766/">Nave industrial en Bilbao</a>
                      <span class="price">230.000 €</span>
                      <span class="location">Bilbao</span>
                      <span class="surface">450 m2</span>
                    </article>
                """,
            }

            def fake_archive(*, url: str, **_: object):
                page_no = "001" if url.endswith("/bizkaia/") else "002"
                out_dir = tmp_path / "snapshots" / page_no
                out_dir.mkdir(parents=True, exist_ok=True)
                (out_dir / "page.html").write_text(html_by_page[url], encoding="utf-8")
                meta = {
                    "snapshot_id": f"snap_{page_no}",
                    "run_id": f"run_{page_no}",
                    "domain": "idealista.com",
                    "url_original": url,
                    "url_final": url,
                    "snapshot_path": str(out_dir),
                    "files": {
                        "page_html": str(out_dir / "page.html"),
                        "page_md": None,
                        "meta_json": str(out_dir / "meta.json"),
                    },
                    "extra": {
                        "listing_start_url": "https://www.idealista.com/alquiler-naves/bizkaia/",
                    },
                }
                (out_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
                return SimpleNamespace(
                    status="ok",
                    snapshot_id=f"snap_{page_no}",
                    run_id=f"run_{page_no}",
                    output_dir=out_dir,
                    meta_path=out_dir / "meta.json",
                )

            harvest_plan = {
                "job": {"job_name": "bizkaia_naves_smoke"},
                "included_sources": [
                    {
                        "domain": "idealista.com",
                        "parser_key": "idealista_listing",
                        "listing_start_urls": ["https://www.idealista.com/alquiler-naves/bizkaia/"],
                        "max_listing_pages": 2,
                        "listing_page_start": 1,
                        "listing_page_url_template": "https://www.idealista.com/alquiler-naves/bizkaia/pagina-{page}.htm",
                        "listing_first_page_uses_start_url": True,
                        "rate_limit_seconds": 0,
                        "timeout_seconds": 20,
                    }
                ],
                "excluded_sources": [],
            }

            with patch("src.harvest.runner.resolve_job_harvest_plan", return_value=harvest_plan):
                summary = harvest_listings(
                    job_name="bizkaia_naves_smoke",
                    linked_run_id="jobrun_001",
                    merge_into_discovery=True,
                    harvest_root_dir=tmp_path / "harvest",
                    discovery_root_dir=tmp_path / "discovered",
                    history_root_dir=history_root,
                    archive_fn=fake_archive,
                    sleep_fn=lambda _: None,
                )

            portal_summary = summary["portal_summaries"]["idealista"]
            self.assertEqual(portal_summary["listing_pages_requested"], 2)
            self.assertEqual(portal_summary["listing_pages_ok"], 2)
            self.assertEqual(portal_summary["cards_detected"], 3)
            self.assertEqual(portal_summary["candidates_unique_count"], 2)
            self.assertEqual(portal_summary["candidates_passed_to_detail_count"], 1)
            self.assertEqual(portal_summary["candidates_skipped_known_today_count"], 1)
            self.assertEqual(summary["execution_mode"], "parallel_by_portal")

            candidates_path = Path(portal_summary["candidates_path"])
            self.assertTrue(candidates_path.exists())
            candidate_rows = [json.loads(line) for line in candidates_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(candidate_rows), 2)
            self.assertEqual(sum(1 for row in candidate_rows if row["selected_for_detail"]), 1)

            discovery_path = Path(summary["discovery_merge"]["discovered_output_path"])
            self.assertTrue(discovery_path.exists())
            discovered_rows = [json.loads(line) for line in discovery_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(discovered_rows), 1)
            self.assertEqual(discovered_rows[0]["discovered_url"], "https://www.idealista.com/inmueble/99887766")


if __name__ == "__main__":
    unittest.main()
