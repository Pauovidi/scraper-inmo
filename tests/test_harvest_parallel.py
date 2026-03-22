from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.harvest.runner import harvest_listings


class HarvestParallelTests(unittest.TestCase):
    def test_harvest_runs_in_parallel_by_portal_and_keeps_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

            html_by_url = {
                "https://www.milanuncios.com/alquiler-de-naves-industriales-en-vizcaya/": """
                    <article data-testid="AD_CARD" data-id="111222333">
                      <a class="ma-AdCardListingV2-TitleLink" href="/nave-industrial-en-bilbao-vizcaya-111222333.htm">Nave Bilbao</a>
                      <span class="price">1.800 eur</span>
                    </article>
                """,
                "https://www.pisos.com/alquiler/naves-vizcaya_bizkaia/": """
                    <article class="ad-preview" data-id="88776655">
                      <a class="ad-preview__title" href="/alquilar/nave-industrial-bilbao/88776655_1/">Nave Bilbao</a>
                      <span class="price">2.400 €</span>
                    </article>
                """,
            }

            def fake_archive(*, url: str, **_: object):
                slug = "milanuncios" if "milanuncios" in url else "pisos"
                out_dir = tmp_path / "snapshots" / slug
                out_dir.mkdir(parents=True, exist_ok=True)
                (out_dir / "page.html").write_text(html_by_url[url], encoding="utf-8")
                meta = {
                    "snapshot_id": f"snap_{slug}",
                    "run_id": f"run_{slug}",
                    "domain": "milanuncios.com" if slug == "milanuncios" else "pisos.com",
                    "url_original": url,
                    "url_final": url,
                    "snapshot_path": str(out_dir),
                    "files": {
                        "page_html": str(out_dir / "page.html"),
                        "page_md": None,
                        "meta_json": str(out_dir / "meta.json"),
                    },
                    "extra": {"listing_start_url": url},
                }
                (out_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
                return SimpleNamespace(
                    status="ok",
                    snapshot_id=f"snap_{slug}",
                    run_id=f"run_{slug}",
                    output_dir=out_dir,
                    meta_path=out_dir / "meta.json",
                )

            harvest_plan = {
                "job": {"job_name": "bizkaia_naves_smoke"},
                "included_sources": [
                    {
                        "domain": "milanuncios.com",
                        "parser_key": "generic_listing",
                        "listing_start_urls": ["https://www.milanuncios.com/alquiler-de-naves-industriales-en-vizcaya/"],
                        "max_listing_pages": 1,
                        "listing_page_start": 1,
                        "listing_page_param": "pagina",
                        "listing_first_page_uses_start_url": True,
                        "rate_limit_seconds": 0,
                        "timeout_seconds": 20,
                    },
                    {
                        "domain": "pisos.com",
                        "parser_key": "pisos_detail",
                        "listing_start_urls": ["https://www.pisos.com/alquiler/naves-vizcaya_bizkaia/"],
                        "max_listing_pages": 1,
                        "listing_page_start": 1,
                        "listing_page_url_template": "{base}{page}/",
                        "listing_first_page_uses_start_url": True,
                        "rate_limit_seconds": 0,
                        "timeout_seconds": 20,
                    },
                ],
                "excluded_sources": [],
            }

            with patch("src.harvest.runner.resolve_job_harvest_plan", return_value=harvest_plan):
                summary = harvest_listings(
                    job_name="bizkaia_naves_smoke",
                    harvest_root_dir=tmp_path / "harvest",
                    discovery_root_dir=tmp_path / "discovered",
                    history_root_dir=tmp_path / "history",
                    archive_fn=fake_archive,
                    sleep_fn=lambda _: None,
                )

            self.assertEqual(summary["execution_mode"], "parallel_by_portal")
            self.assertEqual(summary["max_workers"], 2)
            self.assertIn("milanuncios", summary["portal_summaries"])
            self.assertIn("pisos", summary["portal_summaries"])
            self.assertEqual(summary["totals"]["candidates_sent_to_detail"], 2)
            self.assertTrue(Path(summary["portal_summaries"]["milanuncios"]["candidates_path"]).exists())
            self.assertTrue(Path(summary["portal_summaries"]["pisos"]["listing_pages_manifest_path"]).exists())


if __name__ == "__main__":
    unittest.main()
