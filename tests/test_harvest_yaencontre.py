from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.config.loader import load_source_by_domain
from src.harvest.listing_fetcher import build_listing_page_plan
from src.harvest.listing_parser import dedupe_candidates, extract_listing_candidates
from src.harvest.runner import harvest_listings
from src.parsers.snapshot_bridge import SnapshotBundle


def _bundle(html: str, *, url: str) -> SnapshotBundle:
    return SnapshotBundle(
        snapshot_path=Path("/tmp/yaencontre-listing"),
        html=html,
        markdown=None,
        meta={
            "domain": "yaencontre.com",
            "url_original": url,
            "url_final": url,
            "snapshot_id": "snap_ya_001",
            "run_id": "run_ya_001",
            "snapshot_path": "/tmp/yaencontre-listing",
            "extra": {"listing_start_url": "https://www.yaencontre.com/alquiler/naves/bizkaia"},
        },
    )


class YaencontreHarvestTests(unittest.TestCase):
    def test_config_and_listing_pagination_are_present(self) -> None:
        source = load_source_by_domain("yaencontre.com")

        self.assertTrue(source["harvest_enabled"])
        self.assertEqual(source["listing_page_param"], "page")
        self.assertGreaterEqual(int(source["max_listing_pages"]), 5)
        self.assertGreaterEqual(len(source["listing_start_urls"]), 1)

        plans = build_listing_page_plan(source)
        self.assertGreaterEqual(len(plans), 1)
        self.assertEqual(plans[0].source_domain, "yaencontre.com")
        self.assertEqual(plans[0].pagination_strategy, "start_url")

    def test_extracts_detail_cards_and_skips_listing_urls(self) -> None:
        html = """
        <html>
          <body>
            <article class="listing-card" data-id="99112233">
              <a href="/inmueble/99112233/">Nave industrial en Bilbao</a>
              <span class="price">210.000 €</span>
              <span class="location">Bilbao</span>
              <span class="surface">410 m2</span>
              <span class="rooms">2 hab.</span>
            </article>
            <article class="listing-card" data-id="99112233">
              <a href="/inmueble/99112233?from=listado">Nave industrial en Bilbao</a>
              <span class="price">210.000 €</span>
              <span class="location">Bilbao</span>
            </article>
            <article class="listing-card">
              <a href="/alquiler/naves/bizkaia?page=2">Página de listado</a>
            </article>
            <article class="listing-card">
              <a href="/agencia/empresa-demo/">Perfil de agencia</a>
            </article>
          </body>
        </html>
        """

        candidates = extract_listing_candidates(
            _bundle(html, url="https://www.yaencontre.com/alquiler/naves/bizkaia"),
            job_name="bizkaia_naves_smoke",
            harvest_run_id="harvest_ya_001",
            source_domain="yaencontre.com",
            parser_key="generic_listing",
            page_number=1,
        )

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.source_domain, "yaencontre.com")
        self.assertEqual(candidate.external_id, "99112233")
        self.assertTrue(candidate.candidate_url.endswith("/inmueble/99112233"))
        self.assertEqual(candidate.title_text, "Nave industrial en Bilbao")
        self.assertEqual(candidate.price_text, "210.000 €")
        self.assertEqual(candidate.location_text, "Bilbao")
        self.assertEqual(candidate.surface_text, "410 m2")
        self.assertEqual(candidate.rooms_text, "2 hab.")

    def test_dedupe_keeps_one_candidate_for_same_url_variants(self) -> None:
        html = """
        <html>
          <body>
            <article class="listing-card" data-id="99112234">
              <a href="/inmueble/99112234/">Primera versión</a>
              <span class="price">190.000 €</span>
            </article>
            <article class="listing-card" data-id="99112234">
              <a href="https://www.yaencontre.com/inmueble/99112234?utm_source=newsletter">Segunda versión</a>
              <span class="price">190.000 €</span>
              <span class="location">Barakaldo</span>
            </article>
          </body>
        </html>
        """

        candidates = extract_listing_candidates(
            _bundle(html, url="https://www.yaencontre.com/alquiler/naves/bizkaia"),
            job_name="bizkaia_naves_smoke",
            harvest_run_id="harvest_ya_002",
            source_domain="yaencontre.com",
            parser_key="generic_listing",
            page_number=1,
        )

        deduped = dedupe_candidates(candidates)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].external_id, "99112234")
        self.assertEqual(deduped[0].location_text, "Barakaldo")

    def test_harvest_runner_persists_yaencontre_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

            html_by_page = {
                "https://www.yaencontre.com/alquiler/naves/bizkaia": """
                    <article class="listing-card" data-id="99112235">
                      <a href="/inmueble/99112235/">Nave en Bilbao</a>
                      <span class="price">230.000 €</span>
                      <span class="location">Bilbao</span>
                    </article>
                    <article class="listing-card">
                      <a href="/alquiler/naves/bizkaia?page=2">Listado siguiente</a>
                    </article>
                """,
                "https://www.yaencontre.com/alquiler/naves/bizkaia?page=2": """
                    <article class="listing-card" data-id="99112236">
                      <a href="/inmueble/99112236/">Nave en Basauri</a>
                      <span class="price">240.000 €</span>
                      <span class="location">Basauri</span>
                    </article>
                """,
            }

            def fake_archive(*, url: str, **_: object):
                page_id = "001" if url.endswith("/bizkaia") else "002"
                out_dir = tmp_path / "snapshots" / page_id
                out_dir.mkdir(parents=True, exist_ok=True)
                (out_dir / "page.html").write_text(html_by_page[url], encoding="utf-8")
                meta = {
                    "snapshot_id": f"snap_{page_id}",
                    "run_id": f"run_{page_id}",
                    "domain": "yaencontre.com",
                    "url_original": url,
                    "url_final": url,
                    "snapshot_path": str(out_dir),
                    "files": {
                        "page_html": str(out_dir / "page.html"),
                        "page_md": None,
                        "meta_json": str(out_dir / "meta.json"),
                    },
                    "extra": {
                        "listing_start_url": "https://www.yaencontre.com/alquiler/naves/bizkaia",
                    },
                }
                (out_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
                return SimpleNamespace(
                    status="ok",
                    snapshot_id=f"snap_{page_id}",
                    run_id=f"run_{page_id}",
                    output_dir=out_dir,
                    meta_path=out_dir / "meta.json",
                )

            harvest_plan = {
                "job": {"job_name": "bizkaia_naves_smoke"},
                "included_sources": [
                    {
                        "domain": "yaencontre.com",
                        "parser_key": "generic_listing",
                        "listing_start_urls": ["https://www.yaencontre.com/alquiler/naves/bizkaia"],
                        "max_listing_pages": 2,
                        "listing_page_start": 1,
                        "listing_page_param": "page",
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
                    linked_run_id="jobrun_ya_001",
                    merge_into_discovery=True,
                    harvest_root_dir=tmp_path / "harvest",
                    discovery_root_dir=tmp_path / "discovered",
                    history_root_dir=tmp_path / "history",
                    archive_fn=fake_archive,
                    sleep_fn=lambda _: None,
                )

            portal_summary = summary["portal_summaries"]["yaencontre"]
            self.assertEqual(portal_summary["listing_pages_requested"], 2)
            self.assertEqual(portal_summary["candidates_unique_count"], 2)
            self.assertEqual(portal_summary["candidates_passed_to_detail_count"], 2)

            candidates_path = Path(portal_summary["candidates_path"])
            self.assertTrue(candidates_path.exists())
            candidate_rows = [json.loads(line) for line in candidates_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(candidate_rows), 2)
            self.assertTrue(all(row["selected_for_detail"] for row in candidate_rows))

            discovery_path = Path(summary["discovery_merge"]["discovered_output_path"])
            self.assertTrue(discovery_path.exists())
            discovered_rows = [json.loads(line) for line in discovery_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(discovered_rows), 2)
            self.assertEqual({row["discovered_url"] for row in discovered_rows}, {
                "https://www.yaencontre.com/inmueble/99112235",
                "https://www.yaencontre.com/inmueble/99112236",
            })


if __name__ == "__main__":
    unittest.main()

