from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.archiver.service import archive_url
from src.jobs.index import append_job_run_index_entry
from src.parsers.runner import parse_job_run, parse_snapshot


class ParserPipelineTests(unittest.TestCase):
    def test_parse_snapshot_with_local_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            fixture = tmp_path / "listing.html"
            fixture.write_text(
                """
                <html><head><title>Nave en Bilbao</title></head>
                <body>
                  <h1>Nave industrial en Bilbao</h1>
                  <p>Precio: 250000 EUR</p>
                  <p>Superficie: 1200 m2</p>
                  <p>3 habitaciones</p>
                  <a href="https://example.com/a">A</a>
                </body></html>
                """,
                encoding="utf-8",
            )

            arch = archive_url(url=fixture.as_uri(), output_base_dir=tmp_path / "snapshots", index_file=tmp_path / "idx.jsonl")
            record = parse_snapshot(arch.output_dir)

            self.assertEqual(record["parse_status"], "ok")
            self.assertEqual(record["parser_key"], "generic")
            self.assertTrue(record["title"])
            self.assertTrue(record["price_text"])
            self.assertTrue(record["surface_text"])
            self.assertGreaterEqual(len(record["extracted_links"]), 1)

    def test_registry_fallback_to_generic_for_unknown_domain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            fixture = tmp_path / "simple.html"
            fixture.write_text("<html><body><h1>Simple</h1></body></html>", encoding="utf-8")

            arch = archive_url(url=fixture.as_uri(), output_base_dir=tmp_path / "snapshots", index_file=tmp_path / "idx.jsonl")
            record = parse_snapshot(arch.output_dir)

            self.assertEqual(record["source_domain"], "local-file")
            self.assertEqual(record["parser_key"], "generic")

    def test_parse_job_run_persists_jsonl_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            fixture = tmp_path / "job_seed.html"
            fixture.write_text("<html><body><h1>Job Seed</h1><p>Precio: 100000 EUR</p></body></html>", encoding="utf-8")

            arch = archive_url(url=fixture.as_uri(), output_base_dir=tmp_path / "snapshots", index_file=tmp_path / "snap_idx.jsonl")

            manifest_dir = tmp_path / "job_runs" / "job_demo" / "run_001"
            manifest_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = manifest_dir / "manifest.json"
            manifest = {
                "job_name": "job_demo",
                "run_id": "run_001",
                "snapshot_paths": [str(arch.output_dir)],
            }
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            job_index = tmp_path / "index" / "job_runs_index.jsonl"
            append_job_run_index_entry(
                {
                    "job_name": "job_demo",
                    "run_id": "run_001",
                    "timestamp_utc_start": "2026-03-09T00:00:00Z",
                    "timestamp_utc_end": "2026-03-09T00:00:01Z",
                    "total_urls": 1,
                    "ok_count": 1,
                    "partial_count": 0,
                    "error_count": 0,
                    "manifest_path": str(manifest_path),
                },
                index_file=job_index,
            )

            summary = parse_job_run(
                job_name="job_demo",
                run_id="run_001",
                output_root_dir=tmp_path / "parsed" / "job_runs",
                parse_runs_index_file=tmp_path / "index" / "parse_runs_index.jsonl",
                job_runs_index_file=job_index,
            )

            self.assertEqual(summary["parsed_count"], 1)
            parsed_output = Path(summary["parsed_output_path"])
            self.assertTrue(parsed_output.exists())
            lines = [line for line in parsed_output.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(lines), 1)


if __name__ == "__main__":
    unittest.main()

