from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.jobs.index import list_job_runs, load_job_run_manifest
from src.jobs.runner import run_job


class JobRunnerTests(unittest.TestCase):
    def test_run_job_generates_manifest_and_index_with_dedup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            cfg_sources = tmp_path / "sources"
            cfg_jobs = tmp_path / "jobs"
            cfg_sources.mkdir(parents=True)
            cfg_jobs.mkdir(parents=True)

            fixture = tmp_path / "seed.html"
            fixture.write_text("<html><body><h1>Seed</h1></body></html>", encoding="utf-8")
            file_url = fixture.as_uri()

            (cfg_sources / "a.yaml").write_text(
                json.dumps(
                    {
                        "domain": "a.test",
                        "enabled": True,
                        "mode": "seed_only",
                        "start_urls": [file_url],
                        "rate_limit_seconds": 0,
                        "login_allowed": False,
                        "archiver_enabled": True,
                        "parser_key": "generic",
                        "notes": "A",
                    }
                ),
                encoding="utf-8",
            )
            (cfg_sources / "b.yaml").write_text(
                json.dumps(
                    {
                        "domain": "b.test",
                        "enabled": True,
                        "mode": "seed_only",
                        "start_urls": [file_url],
                        "rate_limit_seconds": 0,
                        "login_allowed": False,
                        "archiver_enabled": True,
                        "parser_key": "generic",
                        "notes": "B duplicate URL",
                    }
                ),
                encoding="utf-8",
            )
            (cfg_sources / "c.yaml").write_text(
                json.dumps(
                    {
                        "domain": "c.test",
                        "enabled": False,
                        "mode": "seed_only",
                        "start_urls": [file_url],
                        "rate_limit_seconds": 0,
                        "login_allowed": False,
                        "archiver_enabled": True,
                        "parser_key": "generic",
                        "notes": "Disabled",
                    }
                ),
                encoding="utf-8",
            )
            (cfg_sources / "d.yaml").write_text(
                json.dumps(
                    {
                        "domain": "d.test",
                        "enabled": True,
                        "mode": "seed_only",
                        "start_urls": [file_url],
                        "rate_limit_seconds": 0,
                        "login_allowed": False,
                        "archiver_enabled": False,
                        "parser_key": "generic",
                        "notes": "Archiver disabled",
                    }
                ),
                encoding="utf-8",
            )

            (cfg_jobs / "job.yaml").write_text(
                json.dumps(
                    {
                        "job_name": "job_test",
                        "sources": ["a.test", "b.test", "c.test", "d.test"],
                        "filters": {},
                        "max_urls": 100,
                        "notes": "Job for runner tests",
                    }
                ),
                encoding="utf-8",
            )

            snapshots_out = tmp_path / "snapshots"
            manifest_root = tmp_path / "job_runs"
            snap_index = tmp_path / "index" / "snapshots_index.jsonl"
            job_runs_index = tmp_path / "index" / "job_runs_index.jsonl"

            with patch("src.config.loader.sources_dir", return_value=cfg_sources), patch(
                "src.config.loader.jobs_dir", return_value=cfg_jobs
            ):
                result = run_job(
                    "job_test",
                    archive_output_base_dir=snapshots_out,
                    snapshot_index_file=snap_index,
                    manifest_root_dir=manifest_root,
                    job_runs_index_file=job_runs_index,
                )

            self.assertEqual(result.total_urls, 1)
            self.assertEqual(result.ok_count, 1)
            self.assertEqual(result.partial_count, 0)
            self.assertEqual(result.error_count, 0)
            self.assertTrue(result.manifest_path.exists())

            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["job_name"], "job_test")
            self.assertEqual(manifest["total_urls"], 1)
            self.assertEqual(manifest["duplicate_start_urls_skipped"], 1)
            self.assertIn("a.test", manifest["sources_resolved"]["included"])
            excluded_domains = {row["domain"] for row in manifest["sources_resolved"]["excluded"]}
            self.assertIn("c.test", excluded_domains)
            self.assertIn("d.test", excluded_domains)
            self.assertEqual(len(manifest["snapshot_paths"]), 1)

            rows = list_job_runs(job_name="job_test", index_file=job_runs_index)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], result.run_id)

            loaded_manifest = load_job_run_manifest("job_test", result.run_id, index_file=job_runs_index)
            self.assertEqual(loaded_manifest["run_id"], result.run_id)


if __name__ == "__main__":
    unittest.main()
