from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.parsers.runner import parse_snapshot


class FotocasaDetailParserTests(unittest.TestCase):
    def test_fotocasa_detail_parser_extracts_normalized_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            snapshot_dir = tmp_path / "snapshots" / "fotocasa.es" / "2026-03-10" / "abc" / "run1"
            snapshot_dir.mkdir(parents=True, exist_ok=True)

            html_path = snapshot_dir / "page.html"
            md_path = snapshot_dir / "page.md"
            meta_path = snapshot_dir / "meta.json"

            html_path.write_text(
                """
                <html>
                  <head><title>Nave en venta en Bilbao</title></head>
                  <body>
                    <h1>Nave en venta en Bilbao</h1>
                    <div class="price">450.000 €</div>
                    <div class="location">Bilbao, Bizkaia</div>
                    <ul>
                      <li>980 m2</li>
                      <li>4 habitaciones</li>
                    </ul>
                    <section class="description">Inmueble ideal para actividad logística.</section>
                  </body>
                </html>
                """,
                encoding="utf-8",
            )
            md_path.write_text("", encoding="utf-8")

            meta = {
                "snapshot_id": "snap_fotocasa_001",
                "run_id": "run_fotocasa_001",
                "url_original": "https://www.fotocasa.es/es/comprar/vivienda/bilbao/188901695/d?from=list",
                "url_final": "https://www.fotocasa.es/es/comprar/vivienda/bilbao/188901695/d?from=list",
                "domain": "fotocasa.es",
                "files": {
                    "page_html": str(html_path),
                    "page_md": str(md_path),
                    "meta_json": str(meta_path),
                },
                "snapshot_path": str(snapshot_dir),
            }
            meta_path.write_text(json.dumps(meta), encoding="utf-8")

            record = parse_snapshot(snapshot_dir)

            self.assertEqual(record["parser_key"], "fotocasa_detail")
            self.assertEqual(record["page_kind"], "detail")
            self.assertEqual(record["parse_status"], "ok")
            self.assertEqual(record["price_currency"], "EUR")
            self.assertEqual(record["price_value"], 450000.0)
            self.assertEqual(record["surface_sqm"], 980.0)
            self.assertEqual(record["rooms_count"], 4)
            self.assertTrue(record["title"])
            self.assertTrue(record["price_text"])


if __name__ == "__main__":
    unittest.main()
