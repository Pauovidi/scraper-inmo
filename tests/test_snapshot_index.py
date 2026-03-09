from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.archiver.index import list_snapshots, load_snapshot_meta, read_index_entries
from src.archiver.service import archive_url
from src.parsers.snapshot_bridge import SnapshotBridge


class SnapshotIndexTests(unittest.TestCase):
    def test_index_and_dedup_for_same_url_same_day(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            fixture = tmp_path / "sample.html"
            fixture.write_text("<html><body><h1>A</h1><p>Alpha</p></body></html>", encoding="utf-8")

            output_base = tmp_path / "snapshots"
            index_file = tmp_path / "index" / "snapshots_index.jsonl"
            url = fixture.as_uri()

            first = archive_url(url=url, output_base_dir=output_base, index_file=index_file)
            first_meta = json.loads(first.meta_path.read_text(encoding="utf-8"))

            second = archive_url(url=url, output_base_dir=output_base, index_file=index_file)
            second_meta = json.loads(second.meta_path.read_text(encoding="utf-8"))

            self.assertNotEqual(first.output_dir, second.output_dir)
            self.assertEqual(first_meta["dedup"]["is_duplicate_content"], False)
            self.assertEqual(second_meta["dedup"]["is_duplicate_content"], True)
            self.assertGreaterEqual(second_meta["dedup"]["same_url_same_day_previous_count"], 1)

            entries = read_index_entries(index_file=index_file)
            self.assertEqual(len(entries), 2)
            self.assertIn("content_hash_preferred", entries[0])

    def test_list_snapshots_filters_and_load_meta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            fixture = tmp_path / "sample.html"
            fixture.write_text("<html><body><h1>B</h1></body></html>", encoding="utf-8")

            output_base = tmp_path / "snapshots"
            index_file = tmp_path / "index" / "snapshots_index.jsonl"

            result = archive_url(url=fixture.as_uri(), output_base_dir=output_base, index_file=index_file)
            meta = json.loads(result.meta_path.read_text(encoding="utf-8"))

            by_domain = list_snapshots(domain="local-file", index_file=index_file)
            by_status = list_snapshots(status="ok", index_file=index_file)
            by_date = list_snapshots(date=meta["date"], index_file=index_file)

            self.assertGreaterEqual(len(by_domain), 1)
            self.assertGreaterEqual(len(by_status), 1)
            self.assertGreaterEqual(len(by_date), 1)

            loaded_meta = load_snapshot_meta(result.output_dir)
            self.assertEqual(loaded_meta["snapshot_id"], meta["snapshot_id"])

            bundle = SnapshotBridge.load(result.output_dir)
            self.assertIsNotNone(bundle.html)
            self.assertIsNotNone(bundle.markdown)
            self.assertEqual(bundle.meta["snapshot_id"], meta["snapshot_id"])


if __name__ == "__main__":
    unittest.main()


