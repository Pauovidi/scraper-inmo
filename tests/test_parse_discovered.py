from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.parsers.runner import parse_discovered, parse_snapshot


def _write_snapshot(snapshot_dir: Path, *, domain: str, url_final: str, html: str, markdown: str = "") -> Path:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    html_path = snapshot_dir / "page.html"
    md_path = snapshot_dir / "page.md"
    meta_path = snapshot_dir / "meta.json"

    html_path.write_text(html, encoding="utf-8")
    md_path.write_text(markdown, encoding="utf-8")

    meta = {
        "snapshot_id": "snap_test_001",
        "run_id": "run_test_001",
        "url_original": url_final,
        "url_final": url_final,
        "domain": domain,
        "timestamp_utc": "2026-03-10T10:00:00Z",
        "status": "ok",
        "files": {
            "page_html": str(html_path),
            "page_md": str(md_path),
            "meta_json": str(meta_path),
        },
        "errors": [],
        "snapshot_path": str(snapshot_dir),
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return snapshot_dir


class ParseDiscoveredTests(unittest.TestCase):
    def test_pisos_detail_parser_extracts_key_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            snapshot_dir = _write_snapshot(
                tmp_path / "snapshots" / "pisos.com" / "2026-03-10" / "abc" / "run1",
                domain="pisos.com",
                url_final="https://www.pisos.com/inmueble/123456789/",
                html="""
                <html>
                  <head><title>Nave industrial en Bilbao</title></head>
                  <body>
                    <h1>Nave industrial en Bilbao</h1>
                    <div class="price">235.000 €</div>
                    <div class="location">Bilbao, Bizkaia</div>
                    <ul class="features">
                      <li>1200 m2</li>
                      <li>3 habitaciones</li>
                    </ul>
                    <section class="description">Nave en buen estado para uso logístico.</section>
                  </body>
                </html>
                """,
            )

            record = parse_snapshot(snapshot_dir)

            self.assertEqual(record["parser_key"], "pisos_detail")
            self.assertEqual(record["page_kind"], "detail")
            self.assertTrue(record["title"])
            self.assertTrue(record["price_text"])
            self.assertTrue(record["location_text"])
            self.assertTrue(record["surface_text"])
            self.assertTrue(record["rooms_text"])
            self.assertGreaterEqual(record["confidence_score"], 0.6)

    def test_parse_discovered_persists_parsed_details_and_exports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            job_name = "bizkaia_naves"
            run_id = "run_discovered_001"

            snapshot_dir = _write_snapshot(
                tmp_path / "snapshots" / "pisos.com" / "2026-03-10" / "def" / "run2",
                domain="pisos.com",
                url_final="https://www.pisos.com/inmueble/987654321/",
                html="""
                <html>
                  <body>
                    <h1>Nave en alquiler</h1>
                    <div class="price">1.600 €</div>
                    <div class="location">Barakaldo, Bizkaia</div>
                    <p>Superficie 450 m2</p>
                    <p>2 habitaciones</p>
                    <div class="description">Activo para industria ligera.</div>
                  </body>
                </html>
                """,
            )

            discovered_run_dir = tmp_path / "discovered" / "job_runs" / job_name / run_id
            discovered_run_dir.mkdir(parents=True, exist_ok=True)
            (discovered_run_dir / "archive_summary.json").write_text(
                json.dumps(
                    {
                        "job_name": job_name,
                        "run_id": run_id,
                        "total_urls": 1,
                        "ok_count": 1,
                        "partial_count": 0,
                        "error_count": 0,
                        "archived_snapshot_paths": [str(snapshot_dir)],
                    }
                ),
                encoding="utf-8",
            )

            summary = parse_discovered(
                job_name=job_name,
                run_id=run_id,
                discovery_root_dir=tmp_path / "discovered" / "job_runs",
                parsed_root_dir=tmp_path / "parsed" / "discovered",
                export_root_dir=tmp_path / "exports",
                parse_runs_index_file=tmp_path / "index" / "parse_runs_index.jsonl",
            )

            self.assertEqual(summary["parsed_count"], 1)
            self.assertEqual(summary["error_count"], 0)

            parsed_details = Path(summary["parsed_details_path"])
            self.assertTrue(parsed_details.exists())
            parsed_lines = [json.loads(line) for line in parsed_details.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(parsed_lines), 1)
            self.assertEqual(parsed_lines[0]["parser_key"], "pisos_detail")

            export_jsonl = Path(summary["export_jsonl_path"])
            export_csv = Path(summary["export_csv_path"])
            self.assertTrue(export_jsonl.exists())
            self.assertTrue(export_csv.exists())

            export_rows = [json.loads(line) for line in export_jsonl.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(export_rows), 1)
            self.assertIn("source_domain", export_rows[0])
            self.assertIn("parse_status", export_rows[0])


if __name__ == "__main__":
    unittest.main()
