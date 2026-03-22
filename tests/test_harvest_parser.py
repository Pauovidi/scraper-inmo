from __future__ import annotations

import unittest
from pathlib import Path

from src.harvest.listing_parser import dedupe_candidates, extract_listing_candidates
from src.harvest.models import ListingCandidate
from src.parsers.snapshot_bridge import SnapshotBundle


def _bundle(html: str, *, url: str, source_domain: str) -> SnapshotBundle:
    return SnapshotBundle(
        snapshot_path=Path("/tmp/listing"),
        html=html,
        markdown=None,
        meta={
            "domain": source_domain,
            "url_original": url,
            "url_final": url,
            "snapshot_id": "snap_001",
            "run_id": "run_001",
            "snapshot_path": "/tmp/listing",
            "extra": {"listing_start_url": url},
        },
    )


class HarvestParserTests(unittest.TestCase):
    def test_extract_listing_candidates_from_listing_html(self) -> None:
        html = """
        <html>
          <body>
            <article class="listing-card" data-id="99887766">
              <a href="/inmueble/99887766/">Nave industrial en Bilbao</a>
              <span class="price">230.000 €</span>
              <span class="location">Bilbao</span>
              <span class="surface">450 m2</span>
              <span class="rooms">2 hab.</span>
            </article>
            <article class="listing-card" data-id="99887777">
              <a href="/inmueble/99887777/">Pabellón en Barakaldo</a>
              <span class="price">180.000 €</span>
              <span class="location">Barakaldo</span>
              <span class="surface">320 m2</span>
            </article>
            <article class="listing-card">
              <a href="/agencia/inmobiliaria-foo/">Perfil agencia</a>
            </article>
          </body>
        </html>
        """

        candidates = extract_listing_candidates(
            _bundle(html, url="https://www.idealista.com/alquiler-naves/bizkaia/", source_domain="idealista.com"),
            job_name="bizkaia_naves_smoke",
            harvest_run_id="harvest_001",
            source_domain="idealista.com",
            parser_key="idealista_listing",
            page_number=1,
        )

        self.assertEqual(len(candidates), 2)
        first = candidates[0]
        self.assertEqual(first.source_domain, "idealista.com")
        self.assertEqual(first.external_id, "99887766")
        self.assertEqual(first.title_text, "Nave industrial en Bilbao")
        self.assertEqual(first.price_text, "230.000 €")
        self.assertEqual(first.location_text, "Bilbao")
        self.assertEqual(first.surface_text, "450 m2")
        self.assertEqual(first.rooms_text, "2 hab.")
        self.assertTrue(first.candidate_url.endswith("/inmueble/99887766"))

    def test_dedupe_candidates_prefers_richer_candidate(self) -> None:
        poorer = ListingCandidate(
            job_name="job",
            harvest_run_id="harvest",
            source_domain="pisos.com",
            parser_key="pisos_detail",
            listing_page_url="https://www.pisos.com/alquiler/",
            listing_start_url="https://www.pisos.com/alquiler/",
            listing_snapshot_path="/tmp/snapshot1",
            listing_snapshot_id="snap1",
            listing_snapshot_run_id="run1",
            page_number=1,
            card_position=1,
            candidate_url="https://www.pisos.com/inmueble/123456789",
            canonical_url="https://www.pisos.com/inmueble/123456789",
            title_text="Nave",
            price_text=None,
            location_text=None,
            surface_text=None,
            rooms_text=None,
            external_id="123456789",
            listing_key="pisos.com:id:123456789",
            dedupe_key="123456789",
            dedupe_method="external_id",
            raw_text=None,
            discovered_at="2026-03-22T00:00:00Z",
        )
        richer = ListingCandidate(
            **{
                **poorer.to_dict(),
                "price_text": "200.000 €",
                "location_text": "Bilbao",
                "surface_text": "450 m2",
            }
        )

        deduped = dedupe_candidates([poorer, richer])
        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].price_text, "200.000 €")
        self.assertEqual(deduped[0].location_text, "Bilbao")


if __name__ == "__main__":
    unittest.main()

