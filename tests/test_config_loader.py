from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.config.loader import load_jobs, load_province_catalog, load_sources, resolve_job_start_urls


class ConfigLoaderTests(unittest.TestCase):
    def test_load_sources_from_repo_config(self) -> None:
        sources = load_sources()
        domains = {item["domain"] for item in sources}
        self.assertIn("fotocasa.es", domains)
        self.assertIn("idealista.com", domains)
        self.assertIn("milanuncios.com", domains)
        self.assertIn("pisos.com", domains)
        self.assertIn("yaencontre.com", domains)
        by_domain = {item["domain"]: item for item in sources}
        self.assertTrue(by_domain["fotocasa.es"]["harvest_enabled"])
        self.assertGreaterEqual(by_domain["idealista.com"]["max_listing_pages"], 4)
        self.assertGreaterEqual(by_domain["milanuncios.com"]["max_listing_pages"], 5)
        self.assertEqual(
            by_domain["pisos.com"]["listing_start_urls"][0],
            "https://www.pisos.com/alquiler/naves-vizcaya_bizkaia/",
        )
        self.assertGreaterEqual(len(by_domain["yaencontre.com"]["listing_start_urls"]), 2)

    def test_load_jobs_and_resolve_start_urls(self) -> None:
        jobs = load_jobs()
        names = {job["job_name"] for job in jobs}
        self.assertIn("bizkaia_naves", names)
        self.assertIn("inmoscraper_demo_spain", names)

        urls = resolve_job_start_urls("bizkaia_naves")
        self.assertGreaterEqual(len(urls), 1)

        demo_job = next(job for job in jobs if job["job_name"] == "inmoscraper_demo_spain")
        self.assertIn("Madrid", demo_job["target_provinces"])
        self.assertEqual(demo_job["geography_catalog"], "provinces.yaml")

    def test_load_province_catalog_from_config(self) -> None:
        catalog = load_province_catalog()
        self.assertEqual(len(catalog["provinces"]), 52)
        self.assertIn("Bizkaia", catalog["provinces"])
        self.assertIn("Madrid", catalog["demo_target_provinces"])
        self.assertEqual(catalog["aliases"]["Bizkaia"], ["bizkaia", "vizcaya"])
        self.assertEqual(catalog["city_to_province"]["Bilbao"], "Bizkaia")

    def test_validation_missing_required_field_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            bad_sources = tmp_path / "sources"
            bad_jobs = tmp_path / "jobs"
            bad_sources.mkdir(parents=True)
            bad_jobs.mkdir(parents=True)

            # Missing required field: domain
            (bad_sources / "bad.yaml").write_text(
                '{"enabled": true, "mode": "seed_only", "start_urls": ["https://x"], '
                '"rate_limit_seconds": 1, "login_allowed": false, "archiver_enabled": true, '
                '"parser_key": "generic", "notes": "bad source"}',
                encoding="utf-8",
            )

            with patch("src.config.loader.sources_dir", return_value=bad_sources), patch(
                "src.config.loader.jobs_dir", return_value=bad_jobs
            ):
                with self.assertRaises(ValueError):
                    load_sources()


if __name__ == "__main__":
    unittest.main()
