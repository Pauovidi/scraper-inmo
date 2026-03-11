from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.pipeline.index import list_pipeline_runs, load_pipeline_run_manifest
from src.pipeline.runner import run_job_full


class PipelineRunnerTests(unittest.TestCase):
    def _make_fakes(self, tmp_path: Path, run_id: str, calls: dict[str, int]):
        discovered_out = tmp_path / "discovered" / "job_runs" / "bizkaia_naves" / run_id / "discovered_urls.jsonl"
        archive_summary_path = discovered_out.parent / "archive_summary.json"
        parsed_details = tmp_path / "parsed" / "discovered" / "bizkaia_naves" / run_id / "parsed_details.jsonl"
        parse_summary = parsed_details.parent / "summary.json"
        export_jsonl = tmp_path / "exports" / "bizkaia_naves" / run_id / "properties.jsonl"
        export_csv = tmp_path / "exports" / "bizkaia_naves" / run_id / "properties.csv"

        def fake_run_job(*, job_name: str):
            calls["run"] += 1
            manifest_path = tmp_path / "job_runs" / job_name / run_id / "manifest.json"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(json.dumps({"job_name": job_name, "run_id": run_id, "snapshot_paths": []}), encoding="utf-8")
            return SimpleNamespace(
                job_name=job_name,
                run_id=run_id,
                manifest_path=manifest_path,
                total_urls=1,
                ok_count=1,
                partial_count=0,
                error_count=0,
            )

        def fake_discover(*, job_name: str, run_id: str):
            calls["discover"] += 1
            discovered_out.parent.mkdir(parents=True, exist_ok=True)
            discovered_out.write_text("", encoding="utf-8")
            (discovered_out.parent / "summary.json").write_text(
                json.dumps({"job_name": job_name, "run_id": run_id, "discovered_output_path": str(discovered_out)}),
                encoding="utf-8",
            )
            return {"discovered_output_path": str(discovered_out)}

        def fake_archive(*, job_name: str, run_id: str):
            calls["archive"] += 1
            archive_summary_path.parent.mkdir(parents=True, exist_ok=True)
            archive_summary_path.write_text(
                json.dumps(
                    {
                        "job_name": job_name,
                        "run_id": run_id,
                        "ok_count": 1,
                        "partial_count": 0,
                        "error_count": 0,
                        "archived_snapshot_paths": [str(tmp_path / "snapshots" / "x")],
                    }
                ),
                encoding="utf-8",
            )
            return {"ok_count": 1, "partial_count": 0, "error_count": 0}

        def fake_parse(*, job_name: str, run_id: str):
            calls["parse"] += 1
            parsed_details.parent.mkdir(parents=True, exist_ok=True)
            parsed_details.write_text("{}\n", encoding="utf-8")
            parse_summary.write_text(json.dumps({"parsed_details_path": str(parsed_details)}), encoding="utf-8")
            export_jsonl.parent.mkdir(parents=True, exist_ok=True)
            export_jsonl.write_text("{}\n", encoding="utf-8")
            export_csv.write_text("source_domain,price_value\n", encoding="utf-8")
            return {
                "parsed_details_path": str(parsed_details),
                "export_jsonl_path": str(export_jsonl),
                "export_csv_path": str(export_csv),
            }

        return fake_run_job, fake_discover, fake_archive, fake_parse

    def test_run_job_full_generates_manifest_and_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            calls = {"run": 0, "discover": 0, "archive": 0, "parse": 0}
            fake_run_job, fake_discover, fake_archive, fake_parse = self._make_fakes(tmp_path, "jobrun_001", calls)

            result = run_job_full(
                job_name="bizkaia_naves",
                run_job_fn=fake_run_job,
                discover_fn=fake_discover,
                archive_discovered_fn=fake_archive,
                parse_discovered_fn=fake_parse,
                pipeline_root_dir=tmp_path / "pipeline_runs",
                pipeline_index_file=tmp_path / "index" / "pipeline_runs_index.jsonl",
                log_fn=lambda _: None,
            )

            self.assertEqual(result.status, "completed")
            self.assertEqual(calls, {"run": 1, "discover": 1, "archive": 1, "parse": 1})
            self.assertTrue(result.manifest_path.exists())

            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "completed")
            self.assertEqual(manifest["job_run_id"], "jobrun_001")
            self.assertEqual(manifest["discovery_run_id"], "jobrun_001")
            self.assertIn("export_paths", manifest)

            rows = list_pipeline_runs(index_file=tmp_path / "index" / "pipeline_runs_index.jsonl")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["pipeline_run_id"], result.pipeline_run_id)

    def test_resume_skips_completed_steps_without_duplicate_index_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            calls = {"run": 0, "discover": 0, "archive": 0, "parse": 0}
            fake_run_job, fake_discover, fake_archive, fake_parse = self._make_fakes(tmp_path, "jobrun_010", calls)

            first = run_job_full(
                job_name="bizkaia_naves",
                run_job_fn=fake_run_job,
                discover_fn=fake_discover,
                archive_discovered_fn=fake_archive,
                parse_discovered_fn=fake_parse,
                pipeline_root_dir=tmp_path / "pipeline_runs",
                pipeline_index_file=tmp_path / "index" / "pipeline_runs_index.jsonl",
                log_fn=lambda _: None,
            )

            second = run_job_full(
                job_name="bizkaia_naves",
                resume=True,
                run_job_fn=fake_run_job,
                discover_fn=fake_discover,
                archive_discovered_fn=fake_archive,
                parse_discovered_fn=fake_parse,
                pipeline_root_dir=tmp_path / "pipeline_runs",
                pipeline_index_file=tmp_path / "index" / "pipeline_runs_index.jsonl",
                log_fn=lambda _: None,
            )

            self.assertEqual(second.pipeline_run_id, first.pipeline_run_id)
            self.assertEqual(calls, {"run": 1, "discover": 1, "archive": 1, "parse": 1})

            rows = list_pipeline_runs(index_file=tmp_path / "index" / "pipeline_runs_index.jsonl")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["pipeline_run_id"], first.pipeline_run_id)

            manifest = json.loads(second.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["step_statuses"]["run-job"], "skipped")
            self.assertEqual(manifest["step_statuses"]["discover-job-run"], "skipped")
            self.assertEqual(manifest["step_statuses"]["archive-discovered"], "skipped")
            self.assertEqual(manifest["step_statuses"]["parse-discovered"], "skipped")

    def test_resume_force_parse_reexecutes_only_parse(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            calls = {"run": 0, "discover": 0, "archive": 0, "parse": 0}
            fake_run_job, fake_discover, fake_archive, fake_parse = self._make_fakes(tmp_path, "jobrun_020", calls)

            run_job_full(
                job_name="bizkaia_naves",
                run_job_fn=fake_run_job,
                discover_fn=fake_discover,
                archive_discovered_fn=fake_archive,
                parse_discovered_fn=fake_parse,
                pipeline_root_dir=tmp_path / "pipeline_runs",
                pipeline_index_file=tmp_path / "index" / "pipeline_runs_index.jsonl",
                log_fn=lambda _: None,
            )

            run_job_full(
                job_name="bizkaia_naves",
                resume=True,
                force_parse=True,
                run_job_fn=fake_run_job,
                discover_fn=fake_discover,
                archive_discovered_fn=fake_archive,
                parse_discovered_fn=fake_parse,
                pipeline_root_dir=tmp_path / "pipeline_runs",
                pipeline_index_file=tmp_path / "index" / "pipeline_runs_index.jsonl",
                log_fn=lambda _: None,
            )

            self.assertEqual(calls["run"], 1)
            self.assertEqual(calls["discover"], 1)
            self.assertEqual(calls["archive"], 1)
            self.assertEqual(calls["parse"], 2)

    def test_load_pipeline_manifest_from_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            calls = {"run": 0, "discover": 0, "archive": 0, "parse": 0}
            fake_run_job, fake_discover, fake_archive, fake_parse = self._make_fakes(tmp_path, "jobrun_030", calls)

            result = run_job_full(
                job_name="bizkaia_naves",
                run_job_fn=fake_run_job,
                discover_fn=fake_discover,
                archive_discovered_fn=fake_archive,
                parse_discovered_fn=fake_parse,
                pipeline_root_dir=tmp_path / "pipeline_runs",
                pipeline_index_file=tmp_path / "index" / "pipeline_runs_index.jsonl",
                log_fn=lambda _: None,
            )

            manifest = load_pipeline_run_manifest(
                job_name="bizkaia_naves",
                pipeline_run_id=result.pipeline_run_id,
                index_file=tmp_path / "index" / "pipeline_runs_index.jsonl",
            )
            self.assertEqual(manifest["pipeline_run_id"], result.pipeline_run_id)
            self.assertEqual(manifest["job_run_id"], "jobrun_030")


if __name__ == "__main__":
    unittest.main()
