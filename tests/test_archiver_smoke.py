from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.archiver.service import archive_url


class ArchiverSmokeTests(unittest.TestCase):
    def test_file_url_smoke_generates_meta_hashes_and_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            html_fixture = tmp_path / "sample.html"
            html_fixture.write_text(
                """
                <html>
                    <body>
                        <h1>Nave industrial</h1>
                        <p>Superficie: 1200 m2</p>
                    </body>
                </html>
                """,
                encoding="utf-8",
            )

            output_base = tmp_path / "snapshots"
            index_file = tmp_path / "index" / "snapshots_index.jsonl"
            result = archive_url(
                url=html_fixture.as_uri(),
                timeout=5,
                output_base_dir=output_base,
                index_file=index_file,
            )

            self.assertEqual(result.status, "ok")
            self.assertTrue(result.meta_path.exists())

            meta = json.loads(result.meta_path.read_text(encoding="utf-8"))
            self.assertEqual(meta["html_source"], "file")
            self.assertEqual(meta["markdown_source"], "local_html_to_markdown")
            self.assertIsNotNone(meta["html_hash"])
            self.assertIsNotNone(meta["markdown_hash"])
            self.assertEqual(meta["content_hash_preferred"], meta["markdown_hash"])
            self.assertEqual(meta["dedup"]["is_duplicate_content"], False)
            self.assertTrue(index_file.exists())

    def test_markdown_new_failure_falls_back_to_local_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            output_base = tmp_path / "snapshots"
            index_file = tmp_path / "index" / "snapshots_index.jsonl"

            with patch("src.archiver.service.requests", object()), patch(
                "src.archiver.service._fetch_html_requests",
                return_value=("https://example.com/ficha", "<html><body><h1>Ficha</h1></body></html>", "text/html"),
            ), patch(
                "src.archiver.service._fetch_markdown_via_markdown_new",
                side_effect=RuntimeError("markdown.new unavailable"),
            ):
                result = archive_url(
                    url="https://example.com/ficha",
                    timeout=5,
                    output_base_dir=output_base,
                    index_file=index_file,
                )

            meta = json.loads(result.meta_path.read_text(encoding="utf-8"))
            self.assertEqual(meta["status"], "ok")
            self.assertIn("markdown_new", meta["methods_attempted"])
            self.assertNotIn("markdown_new", meta["methods_succeeded"])
            self.assertEqual(meta["markdown_source"], "local_html_to_markdown")
            self.assertEqual(meta["html_source"], "requests")

    def test_markdown_new_success_is_tracked_in_meta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            output_base = tmp_path / "snapshots"
            index_file = tmp_path / "index" / "snapshots_index.jsonl"

            with patch("src.archiver.service.requests", object()), patch(
                "src.archiver.service._fetch_html_requests",
                return_value=("https://example.com/ficha", "<html><body><h1>Ficha</h1></body></html>", "text/html"),
            ), patch(
                "src.archiver.service._fetch_markdown_via_markdown_new",
                return_value="# Ficha\n\nContenido",
            ):
                result = archive_url(
                    url="https://example.com/ficha",
                    timeout=5,
                    output_base_dir=output_base,
                    index_file=index_file,
                )

            meta = json.loads(result.meta_path.read_text(encoding="utf-8"))
            self.assertEqual(meta["status"], "ok")
            self.assertIn("markdown_new", meta["methods_attempted"])
            self.assertIn("markdown_new", meta["methods_succeeded"])
            self.assertEqual(meta["markdown_source"], "markdown_new")


if __name__ == "__main__":
    unittest.main()
