from __future__ import annotations

import unittest
from pathlib import Path

from src.config.loader import load_source_by_domain
from src.harvest.listing_parser import extract_listing_candidates, extract_listing_candidates_with_report
from src.harvest.portals.milanuncios import milanuncios_is_blocked_listing_html, milanuncios_is_detail_candidate_url
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
    def test_config_keeps_volume_friendly_milanuncios_listing_rules(self) -> None:
        source = load_source_by_domain("milanuncios.com")
        self.assertTrue(source["harvest_enabled"])
        self.assertGreaterEqual(int(source["max_listing_pages"]), 5)
        self.assertEqual(source["listing_page_param"], "pagina")
        self.assertIn("a.ma-AdCardListingV2-TitleLink[href]", source["listing_detail_link_selectors"])
        self.assertIn("article[data-testid='AD_CARD']", source["listing_card_selectors"])

    def test_extracts_real_milanuncios_cards_from_title_link(self) -> None:
        html = """
        <html>
          <body>
            <div class="ma-AdList">
              <article class="ma-AdCardV2 ma-AdCardV2--listingCard3AdsPerRow" data-testid="AD_CARD">
                <a class="ma-AdCardV2-link" href="/alquiler-de-naves-industriales-en-vizcaya/"></a>
                <a class="ma-AdCardListingV2-TitleLink" href="/alquiler-de-naves-industriales-en-bilbao-vizcaya/bilbao-479664210.htm">
                  <h2>Bilbao</h2>
                </a>
                <span class="ma-AdPrice-value">1.875 €</span>
                <span class="ma-AdLocation-text">Bilbao (Bizkaia)</span>
                <span class="ma-AdTag-label" title="500 m²">500 m²</span>
              </article>
              <article class="ma-AdCardV2 ma-AdCardV2--listingCard3AdsPerRow" data-testid="AD_CARD">
                <a class="ma-AdCardListingV2-TitleLink" href="/alquiler-de-naves-industriales-en-barakaldo-vizcaya/barakaldo-545524184.htm">
                  <h2>Barakaldo</h2>
                </a>
                <span class="ma-AdPrice-value">5.000 €</span>
                <span class="ma-AdLocation-text">Barakaldo (Bizkaia)</span>
                <span class="ma-AdTag-label" title="600 m²">600 m²</span>
              </article>
              <article class="ma-AdCardV2 ma-AdCardV2--listingCard3AdsPerRow" data-testid="AD_CARD">
                <a class="ma-AdCardListingV2-TitleLink" href="/alquiler-de-naves-industriales-en-galdakao|galdacano|galdakano-vizcaya/galdakao-581292696.htm">
                  <h2>Galdakao</h2>
                </a>
                <span class="ma-AdPrice-value">8.500 €</span>
                <span class="ma-AdLocation-text">Galdakao/Galdacano/Galdakano (Bizkaia)</span>
                <span class="ma-AdTag-label" title="1860 m²">1860 m²</span>
              </article>
              <article class="ma-AdCardV2 ma-AdCardV2--listingCard3AdsPerRow" data-testid="AD_CARD">
                <a class="ma-AdCardListingV2-TitleLink" href="/alquiler-de-naves-industriales-en-derio-vizcaya/derio-527979185.htm">
                  <h2>Derio</h2>
                </a>
                <span class="ma-AdPrice-value">4.000 €</span>
                <span class="ma-AdLocation-text">Derio (Bizkaia)</span>
                <span class="ma-AdTag-label" title="582 m²">582 m²</span>
              </article>
              <article class="ma-AdCardV2 ma-AdCardV2--listingCard3AdsPerRow" data-testid="AD_CARD">
                <a class="ma-AdCardListingV2-TitleLink" href="/alquiler-de-naves-industriales-en-orozko-vizcaya/orozko-546171235.htm">
                  <h2>Orozko</h2>
                </a>
                <span class="ma-AdPrice-value">10.000 €</span>
                <span class="ma-AdLocation-text">Orozko (Bizkaia)</span>
                <span class="ma-AdTag-label" title="1920 m²">1920 m²</span>
              </article>
              <article class="ma-AdCardV2">
                <a href="/profesional/agencia-demo/">Perfil profesional</a>
              </article>
            </div>
          </body>
        </html>
        """

        source = load_source_by_domain("milanuncios.com")
        candidates = extract_listing_candidates(
            _bundle(html, url="https://www.milanuncios.com/alquiler-de-naves-industriales-en-vizcaya/"),
            job_name="bizkaia_naves_smoke",
            harvest_run_id="harvest_ma_001",
            source_domain="milanuncios.com",
            parser_key="milanuncios_listing",
            page_number=1,
            source_config=source,
        )

        self.assertEqual(len(candidates), 5)
        first = candidates[0]
        self.assertEqual(first.source_domain, "milanuncios.com")
        self.assertEqual(first.external_id, "479664210")
        self.assertEqual(first.title_text, "Bilbao")
        self.assertEqual(first.price_text, "1.875 €")
        self.assertEqual(first.location_text, "Bilbao (Bizkaia)")
        self.assertEqual(first.surface_text, "500 m²")
        self.assertTrue(first.candidate_url.endswith("479664210.htm"))
        self.assertTrue(milanuncios_is_detail_candidate_url(first.candidate_url))
        self.assertFalse(milanuncios_is_detail_candidate_url("https://www.milanuncios.com/alquiler-de-naves-industriales-en-vizcaya/"))

    def test_dedupes_duplicate_detail_url_and_keeps_richer_card(self) -> None:
        html = """
        <html>
          <body>
            <article data-testid="AD_CARD">
              <a class="ma-AdCardListingV2-TitleLink" href="/alquiler-de-naves-industriales-en-bilbao-vizcaya/bilbao-479664210.htm">Bilbao</a>
              <span class="ma-AdPrice-value">1.875 €</span>
            </article>
            <article data-testid="AD_CARD">
              <a class="ma-AdCardListingV2-TitleLink" href="/alquiler-de-naves-industriales-en-bilbao-vizcaya/bilbao-479664210.htm">Bilbao</a>
              <span class="ma-AdPrice-value">1.875 €</span>
              <span class="ma-AdLocation-text">Bilbao (Bizkaia)</span>
              <span class="ma-AdTag-label" title="500 m²">500 m²</span>
            </article>
          </body>
        </html>
        """

        candidates = extract_listing_candidates(
            _bundle(html, url="https://www.milanuncios.com/alquiler-de-naves-industriales-en-vizcaya/?pagina=2"),
            job_name="bizkaia_naves_smoke",
            harvest_run_id="harvest_ma_002",
            source_domain="milanuncios.com",
            parser_key="milanuncios_listing",
            page_number=2,
            source_config=load_source_by_domain("milanuncios.com"),
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].external_id, "479664210")
        self.assertEqual(candidates[0].location_text, "Bilbao (Bizkaia)")
        self.assertEqual(candidates[0].surface_text, "500 m²")

    def test_blocked_listing_page_is_reported(self) -> None:
        html = """
        <html>
          <head><title>Pardon Our Interruption</title></head>
          <body>
            <h1>Pardon Our Interruption</h1>
            <p>Please complete the security check to access Milanuncios.</p>
          </body>
        </html>
        """

        self.assertTrue(milanuncios_is_blocked_listing_html(html))
        report = extract_listing_candidates_with_report(
            _bundle(html, url="https://www.milanuncios.com/alquiler-de-naves-industriales-en-vizcaya/"),
            job_name="bizkaia_naves_smoke",
            harvest_run_id="harvest_ma_003",
            source_domain="milanuncios.com",
            parser_key="milanuncios_listing",
            page_number=1,
            source_config=load_source_by_domain("milanuncios.com"),
        )

        self.assertEqual(report.cards_detected, 0)
        self.assertEqual(report.candidates_emitted, 0)
        self.assertEqual(report.candidates_deduped_out, 0)
        self.assertEqual(report.rejection_reasons.get("blocked_listing_page"), 1)


if __name__ == "__main__":
    unittest.main()
