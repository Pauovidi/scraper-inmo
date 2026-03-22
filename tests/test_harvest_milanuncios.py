from __future__ import annotations

import unittest
from pathlib import Path

from src.config.loader import load_source_by_domain
from src.harvest.listing_parser import extract_listing_candidates
from src.harvest.portals.milanuncios import milanuncios_is_detail_candidate_url
from src.parsers.snapshot_bridge import SnapshotBundle


def _bundle(html: str, *, url: str) -> SnapshotBundle:
    return SnapshotBundle(
        snapshot_path=Path("/tmp/milanuncios-listing"),
        html=html,
        markdown=None,
        meta={
            "domain": "milanuncios.com",
            "url_original": url,
            "url_final": url,
            "snapshot_id": "snap_ma_001",
            "run_id": "run_ma_001",
            "snapshot_path": "/tmp/milanuncios-listing",
            "extra": {"listing_start_url": url},
        },
    )


class MilanunciosHarvestTests(unittest.TestCase):
    def test_milanuncios_config_expands_listing_pages(self) -> None:
        source = load_source_by_domain("milanuncios.com")
        self.assertTrue(source["harvest_enabled"])
        self.assertGreaterEqual(int(source["max_listing_pages"]), 5)
        self.assertEqual(source["listing_page_param"], "pagina")

    def test_extracts_detail_from_title_link_in_ad_card(self) -> None:
        html = """
        <html>
          <body>
            <article data-testid="AD_CARD" data-id="111222333">
              <a class="ma-AdCardV2-link" href="/alquiler-de-naves-industriales-en-vizcaya/">Wrapper listing</a>
              <a class="ma-AdCardListingV2-TitleLink" href="/nave-industrial-en-bilbao-vizcaya-111222333.htm">Nave en Bilbao</a>
              <span class="price">1.800 eur</span>
              <span class="location">Bilbao</span>
              <span class="surface">420 m2</span>
            </article>
            <article data-testid="AD_CARD" data-id="111222444">
              <a class="ma-AdCardListingV2-TitleLink" href="/nave-industrial-en-barakaldo-vizcaya-111222444.htm">Nave en Barakaldo</a>
              <span class="price">2.100 eur</span>
              <span class="location">Barakaldo</span>
            </article>
            <article data-testid="AD_CARD">
              <a href="/profesional/agencia-demo/">Perfil profesional</a>
            </article>
          </body>
        </html>
        """

        candidates = extract_listing_candidates(
            _bundle(html, url="https://www.milanuncios.com/alquiler-de-naves-industriales-en-vizcaya/"),
            job_name="bizkaia_naves_smoke",
            harvest_run_id="harvest_ma_001",
            source_domain="milanuncios.com",
            parser_key="generic_listing",
            page_number=1,
            source_config=load_source_by_domain("milanuncios.com"),
        )

        self.assertEqual(len(candidates), 2)
        self.assertEqual(candidates[0].external_id, "111222333")
        self.assertTrue(candidates[0].candidate_url.endswith("111222333.htm"))
        self.assertEqual(candidates[0].location_text, "Bilbao")
        self.assertEqual(candidates[0].surface_text, "420 m2")
        self.assertTrue(milanuncios_is_detail_candidate_url("https://www.milanuncios.com/nave-industrial-en-bilbao-vizcaya-111222333.htm"))
        self.assertFalse(milanuncios_is_detail_candidate_url("https://www.milanuncios.com/profesional/agencia-demo/"))


if __name__ == "__main__":
    unittest.main()
