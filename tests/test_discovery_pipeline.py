from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.archiver.service import archive_url
from src.discovery.extractor import discover_candidate_urls
from src.discovery.runner import archive_discovered, discover_job_run
from src.jobs.index import append_job_run_index_entry
from src.parsers.snapshot_bridge import SnapshotBundle


class DiscoveryPipelineTests(unittest.TestCase):
    def test_extract_normalize_dedupe_and_filter(self) -> None:
        html = """
        <html><body>
          <a href="/inmueble/12345678/">detail a</a>
          <a href="https://www.example.com/inmueble/12345678/">detail a dup</a>
          <a href="/contacto">contact</a>
          <a href="/agencia/foo">agency</a>
          <a href="https://outside.test/inmueble/87654321/">outside</a>
        </body></html>
        """
        markdown = "[md detail](https://www.example.com/inmueble/99999999/)"

        bundle = SnapshotBundle(
            snapshot_path=Path("/tmp/snapshot"),
            html=html,
            markdown=markdown,
            meta={
                "url_original": "https://www.example.com/alquiler-naves/",
                "url_final": "https://www.example.com/alquiler-naves/",
            },
        )

        urls = discover_candidate_urls(
            bundle,
            parser_key="idealista_listing",
            allowed_domain="example.com",
        )

        self.assertIn("https://www.example.com/inmueble/12345678/", urls)
        self.assertIn("https://www.example.com/inmueble/99999999/", urls)
        self.assertEqual(len(urls), 2)

    def test_discover_job_run_persists_discovered_urls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

            fixture = tmp_path / "listing.html"
            fixture.write_text(
                """
                <html><head><title>Listado</title></head>
                <body>
                  <a href="https://example.com/inmueble/11111111/">A</a>
                  <a href="https://example.com/inmueble/11111111/">A duplicate</a>
                  <a href="/inmueble/22222222/">B relative</a>
                </body></html>
                """,
                encoding="utf-8",
            )

            arch = archive_url(
                url=fixture.as_uri(),
                output_base_dir=tmp_path / "snapshots",
                index_file=tmp_path / "index" / "snapshots_index.jsonl",
            )

            manifest_dir = tmp_path / "job_runs" / "job_demo" / "run_001"
            manifest_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = manifest_dir / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "job_name": "job_demo",
                        "run_id": "run_001",
                        "snapshot_paths": [str(arch.output_dir)],
                    }
                ),
                encoding="utf-8",
            )

            job_runs_index = tmp_path / "index" / "job_runs_index.jsonl"
            append_job_run_index_entry(
                {
                    "job_name": "job_demo",
                    "run_id": "run_001",
                    "timestamp_utc_start": "2026-03-10T00:00:00Z",
                    "timestamp_utc_end": "2026-03-10T00:00:01Z",
                    "total_urls": 1,
                    "ok_count": 1,
                    "partial_count": 0,
                    "error_count": 0,
                    "manifest_path": str(manifest_path),
                },
                index_file=job_runs_index,
            )

            summary = discover_job_run(
                job_name="job_demo",
                run_id="run_001",
                output_root_dir=tmp_path / "discovered" / "job_runs",
                job_runs_index_file=job_runs_index,
                discovery_index_file=tmp_path / "index" / "discovery_runs_index.jsonl",
            )

            self.assertEqual(summary["job_name"], "job_demo")
            self.assertGreaterEqual(summary["discovered_urls_count"], 1)

            output_path = Path(summary["discovered_output_path"])
            self.assertTrue(output_path.exists())

            rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertGreaterEqual(len(rows), 1)
            unique_urls = {row["discovered_url"] for row in rows}
            self.assertEqual(len(rows), len(unique_urls))

            index_path = tmp_path / "index" / "discovery_runs_index.jsonl"
            self.assertTrue(index_path.exists())
            index_rows = [json.loads(line) for line in index_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(index_rows), 1)
            self.assertEqual(index_rows[0]["run_id"], "run_001")

    def test_archive_discovered_passes_listing_context_to_archiver(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out_dir = tmp_path / "discovered" / "job_runs" / "job_demo" / "run_002"
            out_dir.mkdir(parents=True, exist_ok=True)
            discovered_path = out_dir / "discovered_urls.jsonl"
            discovered_path.write_text(
                json.dumps(
                    {
                        "discovered_url": "https://www.pisos.com/alquilar/nave_industrial-errekaldeberri48002-62596158719_102200/",
                        "source_domain": "pisos.com",
                        "listing_page_url": "https://www.pisos.com/alquiler/naves-vizcaya_bizkaia/",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            calls: list[dict[str, object]] = []

            def fake_archive_url(**kwargs):
                calls.append(kwargs)
                snapshot_dir = tmp_path / "snapshots" / "row_1"
                snapshot_dir.mkdir(parents=True, exist_ok=True)
                return SimpleNamespace(status="ok", output_dir=snapshot_dir)

            with patch("src.discovery.runner.archive_url", side_effect=fake_archive_url):
                summary = archive_discovered(job_name="job_demo", run_id="run_002", output_root_dir=tmp_path / "discovered" / "job_runs")

            self.assertEqual(summary["ok_count"], 1)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["session_warmup_url"], "https://www.pisos.com/alquiler/naves-vizcaya_bizkaia/")
            self.assertEqual(calls[0]["request_headers"]["Referer"], "https://www.pisos.com/alquiler/naves-vizcaya_bizkaia/")


if __name__ == "__main__":
    unittest.main()
