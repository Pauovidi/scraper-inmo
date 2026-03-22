from __future__ import annotations

import unittest
from pathlib import Path

from src.config.loader import load_source_by_domain
from src.harvest.listing_fetcher import build_listing_page_plan
from src.harvest.listing_parser import dedupe_candidates, extract_listing_candidates
from src.harvest.portals.pisos import pisos_is_detail_candidate_url, pisos_listing_start_urls
from src.parsers import parse_snapshot
from src.parsers.snapshot_bridge import SnapshotBundle


def _bundle(html: str, *, url: str) -> SnapshotBundle:
    return SnapshotBundle(
        snapshot_path=Path("/tmp/pisos-listing"),
        html=html,
        markdown=None,
        meta={
            "domain": "pisos.com",
            "url_original": url,
            "url_final": url,
            "snapshot_id": "snap_pisos_001",
            "run_id": "run_pisos_001",
            "snapshot_path": "/tmp/pisos-listing",
            "extra": {"listing_start_url": url},
        },
    )


class PisosHarvestTests(unittest.TestCase):
    def test_pisos_config_uses_real_seeds_and_path_pagination(self) -> None:
        source = load_source_by_domain("pisos.com")
        self.assertEqual(source["listing_start_urls"], pisos_listing_start_urls())

        plans = build_listing_page_plan(source)
        self.assertGreaterEqual(len(plans), 4)
        self.assertEqual(plans[0].listing_page_url, "https://www.pisos.com/alquiler/naves-vizcaya_bizkaia/")
        self.assertEqual(plans[1].listing_page_url, "https://www.pisos.com/alquiler/naves-vizcaya_bizkaia/2/")

    def test_extracts_detail_from_ad_preview_title_and_dedupes(self) -> None:
        html = """
        <html>
          <body>
            <article class="ad-preview" data-id="88776655">
              <a class="ad-preview__title" href="/alquilar/nave-industrial-bilbao/88776655_1/">Nave en Bilbao</a>
              <span class="price">2.400 €</span>
              <span class="location">Bilbao</span>
              <span class="surface">500 m2</span>
            </article>
            <article class="ad-preview">
              <a class="ad-preview__title" href="/alquilar/nave-industrial-bilbao/88776655_1/?from=listado">Nave en Bilbao duplicada</a>
              <span class="price">2.400 €</span>
              <span class="surface">500 m2</span>
            </article>
            <article class="ad-preview">
              <a href="/alquiler/naves-vizcaya_bizkaia/2/">Siguiente listado</a>
            </article>
          </body>
        </html>
        """

        candidates = extract_listing_candidates(
            _bundle(html, url="https://www.pisos.com/alquiler/naves-vizcaya_bizkaia/"),
            job_name="bizkaia_naves_smoke",
            harvest_run_id="harvest_pisos_001",
            source_domain="pisos.com",
            parser_key="pisos_detail",
            page_number=1,
            source_config=load_source_by_domain("pisos.com"),
        )

        deduped = dedupe_candidates(candidates)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].external_id, "88776655")
        self.assertEqual(deduped[0].location_text, "Bilbao")
        self.assertEqual(deduped[0].surface_text, "500 m2")
        self.assertTrue(pisos_is_detail_candidate_url("https://www.pisos.com/alquilar/nave-industrial-bilbao/88776655_1/"))
        self.assertFalse(pisos_is_detail_candidate_url("https://www.pisos.com/alquiler/naves-vizcaya_bizkaia/2/"))

    def test_real_pisos_detail_snapshot_is_classified_as_detail(self) -> None:
        snapshot_path = (
            Path(__file__).resolve().parents[1]
            / "data"
            / "snapshots"
            / "pisos.com"
            / "2026-03-22"
            / "nave-industrial-galdakao-centro-urbano-50015621961-100500"
            / "20260322T183141Z_db474a3b"
        )

        record = parse_snapshot(snapshot_path)

        self.assertEqual(record["parser_key"], "pisos_detail")
        self.assertEqual(record["source_domain"], "pisos.com")
        self.assertEqual(record["page_kind"], "detail")
        self.assertEqual(record["parse_status"], "ok")
        self.assertIn("Galdakao", record["title"])
        self.assertTrue(record["price_text"])
        self.assertTrue(record["surface_text"])


if __name__ == "__main__":
    unittest.main()
