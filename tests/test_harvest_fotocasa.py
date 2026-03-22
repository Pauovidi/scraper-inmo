from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.config.loader import load_sources
from src.harvest.listing_parser import extract_listing_candidates
from src.harvest.portals.fotocasa import (
    fotocasa_is_detail_candidate_url,
    fotocasa_listing_start_urls,
    fotocasa_normalize_candidate_url,
)
from src.parsers.snapshot_bridge import SnapshotBundle


class FotocasaHarvestTests(unittest.TestCase):
    def test_fotocasa_config_expands_listing_seeds_and_pagination(self) -> None:
        sources = load_sources()
        fotocasa = next(item for item in sources if item["domain"] == "fotocasa.es")

        self.assertTrue(fotocasa["harvest_enabled"])
        self.assertEqual(fotocasa["max_listing_pages"], 4)
        self.assertEqual(fotocasa["listing_page_param"], "pagina")
        self.assertEqual(len(fotocasa["listing_start_urls"]), 6)
        self.assertEqual(fotocasa["listing_start_urls"], fotocasa_listing_start_urls())

    def test_fotocasa_listing_parser_emits_detail_candidates_and_filters_lists(self) -> None:
        fixture_path = Path(__file__).resolve().parents[1] / "fixtures" / "fotocasa_listing_sample.html"
        html = fixture_path.read_text(encoding="utf-8")

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            snapshot_dir = tmp_path / "snapshots" / "fotocasa.es" / "2026-03-22" / "list" / "run-001"
            snapshot_dir.mkdir(parents=True, exist_ok=True)

            bundle = SnapshotBundle(
                snapshot_path=snapshot_dir,
                html=html,
                markdown=None,
                meta={
                    "snapshot_id": "snap_fotocasa_listing_001",
                    "run_id": "run_fotocasa_listing_001",
                    "url_original": "https://www.fotocasa.es/es/comprar/locales/bizkaia-provincia/todas-las-zonas/l",
                    "url_final": "https://www.fotocasa.es/es/comprar/locales/bizkaia-provincia/todas-las-zonas/l",
                    "domain": "fotocasa.es",
                    "snapshot_path": str(snapshot_dir),
                    "extra": {
                        "listing_start_url": "https://www.fotocasa.es/es/comprar/locales/bizkaia-provincia/todas-las-zonas/l"
                    },
                },
            )

            candidates = extract_listing_candidates(
                bundle,
                job_name="bizkaia_naves_smoke",
                harvest_run_id="harvest_run_001",
                source_domain="fotocasa.es",
                parser_key="fotocasa_listing",
                page_number=1,
            )

        self.assertEqual(len(candidates), 2)
        self.assertEqual([candidate.external_id for candidate in candidates], ["123456789", "987654321"])
        self.assertTrue([candidate.candidate_url for candidate in candidates][0].endswith("123456789/d"))
        self.assertEqual(candidates[0].dedupe_method, "external_id")
        self.assertEqual(candidates[0].price_text, "450.000 €")
        self.assertEqual(candidates[0].location_text, "Bilbao, Bizkaia")
        self.assertEqual(candidates[0].surface_text, "980 m2")
        self.assertEqual(candidates[0].rooms_text, "4 habitaciones")

        self.assertTrue(fotocasa_is_detail_candidate_url("https://www.fotocasa.es/es/comprar/locales/bizkaia-provincia/bilbao-123456789/d?from=list"))
        self.assertFalse(fotocasa_is_detail_candidate_url("https://www.fotocasa.es/es/inmobiliaria/agencia-bilbao/"))
        self.assertFalse(fotocasa_is_detail_candidate_url("https://www.fotocasa.es/es/comprar/locales/bizkaia-provincia/todas-las-zonas/l/2"))

    def test_fotocasa_url_normalization_drops_listing_query_params(self) -> None:
        normalized = fotocasa_normalize_candidate_url(
            "/es/comprar/locales/bizkaia-provincia/bilbao-123456789/d?from=list&multimedia=true&pagina=2",
            base_url="https://www.fotocasa.es/es/comprar/locales/bizkaia-provincia/todas-las-zonas/l",
        )

        self.assertEqual(normalized, "https://www.fotocasa.es/es/comprar/locales/bizkaia-provincia/bilbao-123456789/d")


if __name__ == "__main__":
    unittest.main()
