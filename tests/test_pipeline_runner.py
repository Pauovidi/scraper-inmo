from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.pipeline.runner import run_job_full


class PipelineRunnerTests(unittest.TestCase):
    def test_run_job_full_generates_manifest_and_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            calls = {"run": 0, "discover": 0, "archive": 0, "parse": 0}

            discovered_out = tmp_path / "discovered" / "job_runs" / "bizkaia_naves" / "jobrun_001" / "discovered_urls.jsonl"
            archive_summary_path = discovered_out.parent / "archive_summary.json"
            parsed_details = tmp_path / "parsed" / "discovered" / "bizkaia_naves" / "jobrun_001" / "parsed_details.jsonl"
            parse_summary = parsed_details.parent / "summary.json"
            export_jsonl = tmp_path / "exports" / "bizkaia_naves" / "jobrun_001" / "properties.jsonl"
            export_csv = tmp_path / "exports" / "bizkaia_naves" / "jobrun_001" / "properties.csv"

            def fake_run_job(*, job_name: str):
                calls["run"] += 1
                manifest_path = tmp_path / "job_runs" / job_name / "jobrun_001" / "manifest.json"
                manifest_path.parent.mkdir(parents=True, exist_ok=True)
                manifest_path.write_text(json.dumps({"job_name": job_name, "run_id": "jobrun_001", "snapshot_paths": []}), encoding="utf-8")
                return SimpleNamespace(
                    job_name=job_name,
                    run_id="jobrun_001",
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
                export_csv.write_text("source_domain\n", encoding="utf-8")
                return {
                    "parsed_details_path": str(parsed_details),
                    "export_jsonl_path": str(export_jsonl),
                    "export_csv_path": str(export_csv),
                }

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
            self.assertEqual(manifest["step_statuses"]["run-job"], "completed")
            self.assertEqual(manifest["step_statuses"]["parse-discovered"], "completed")
            self.assertIn("export_paths", manifest)

            index_path = tmp_path / "index" / "pipeline_runs_index.jsonl"
            self.assertTrue(index_path.exists())
            lines = [json.loads(line) for line in index_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(lines), 1)
            self.assertEqual(lines[0]["pipeline_run_id"], result.pipeline_run_id)

    def test_resume_skips_completed_steps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            calls = {"run": 0, "discover": 0, "archive": 0, "parse": 0}

            discovered_out = tmp_path / "discovered" / "job_runs" / "bizkaia_naves" / "jobrun_001" / "discovered_urls.jsonl"
            archive_summary_path = discovered_out.parent / "archive_summary.json"
            parsed_details = tmp_path / "parsed" / "discovered" / "bizkaia_naves" / "jobrun_001" / "parsed_details.jsonl"
            parse_summary = parsed_details.parent / "summary.json"
            export_jsonl = tmp_path / "exports" / "bizkaia_naves" / "jobrun_001" / "properties.jsonl"
            export_csv = tmp_path / "exports" / "bizkaia_naves" / "jobrun_001" / "properties.csv"

            def fake_run_job(*, job_name: str):
                calls["run"] += 1
                manifest_path = tmp_path / "job_runs" / job_name / "jobrun_001" / "manifest.json"
                manifest_path.parent.mkdir(parents=True, exist_ok=True)
                manifest_path.write_text(json.dumps({"job_name": job_name, "run_id": "jobrun_001", "snapshot_paths": []}), encoding="utf-8")
                return SimpleNamespace(job_name=job_name, run_id="jobrun_001", manifest_path=manifest_path)

            def fake_discover(*, job_name: str, run_id: str):
                calls["discover"] += 1
                discovered_out.parent.mkdir(parents=True, exist_ok=True)
                discovered_out.write_text("", encoding="utf-8")
                (discovered_out.parent / "summary.json").write_text("{}", encoding="utf-8")
                return {"discovered_output_path": str(discovered_out)}

            def fake_archive(*, job_name: str, run_id: str):
                calls["archive"] += 1
                archive_summary_path.parent.mkdir(parents=True, exist_ok=True)
                archive_summary_path.write_text(
                    json.dumps({"archived_snapshot_paths": [str(tmp_path / "snapshots" / "x")], "error_count": 0}),
                    encoding="utf-8",
                )
                return {"ok_count": 1, "partial_count": 0, "error_count": 0}

            def fake_parse(*, job_name: str, run_id: str):
                calls["parse"] += 1
                parsed_details.parent.mkdir(parents=True, exist_ok=True)
                parsed_details.write_text("{}\n", encoding="utf-8")
                parse_summary.write_text("{}", encoding="utf-8")
                export_jsonl.parent.mkdir(parents=True, exist_ok=True)
                export_jsonl.write_text("{}\n", encoding="utf-8")
                export_csv.write_text("source_domain\n", encoding="utf-8")
                return {
                    "parsed_details_path": str(parsed_details),
                    "export_jsonl_path": str(export_jsonl),
                    "export_csv_path": str(export_csv),
                }

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
            self.assertEqual(first.status, "completed")
            self.assertEqual(calls, {"run": 1, "discover": 1, "archive": 1, "parse": 1})

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
            self.assertEqual(second.status, "completed")
            self.assertEqual(second.pipeline_run_id, first.pipeline_run_id)
            self.assertEqual(calls, {"run": 1, "discover": 1, "archive": 1, "parse": 1})

            manifest = json.loads(second.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["step_statuses"]["run-job"], "skipped")
            self.assertEqual(manifest["step_statuses"]["discover-job-run"], "skipped")
            self.assertEqual(manifest["step_statuses"]["archive-discovered"], "skipped")
            self.assertEqual(manifest["step_statuses"]["parse-discovered"], "skipped")

    def test_resume_force_parse_reexecutes_only_parse(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            calls = {"run": 0, "discover": 0, "archive": 0, "parse": 0}

            discovered_out = tmp_path / "discovered" / "job_runs" / "bizkaia_naves" / "jobrun_002" / "discovered_urls.jsonl"
            archive_summary_path = discovered_out.parent / "archive_summary.json"
            parsed_details = tmp_path / "parsed" / "discovered" / "bizkaia_naves" / "jobrun_002" / "parsed_details.jsonl"
            parse_summary = parsed_details.parent / "summary.json"
            export_jsonl = tmp_path / "exports" / "bizkaia_naves" / "jobrun_002" / "properties.jsonl"
            export_csv = tmp_path / "exports" / "bizkaia_naves" / "jobrun_002" / "properties.csv"

            def fake_run_job(*, job_name: str):
                calls["run"] += 1
                manifest_path = tmp_path / "job_runs" / job_name / "jobrun_002" / "manifest.json"
                manifest_path.parent.mkdir(parents=True, exist_ok=True)
                manifest_path.write_text(json.dumps({"job_name": job_name, "run_id": "jobrun_002", "snapshot_paths": []}), encoding="utf-8")
                return SimpleNamespace(job_name=job_name, run_id="jobrun_002", manifest_path=manifest_path)

            def fake_discover(*, job_name: str, run_id: str):
                calls["discover"] += 1
                discovered_out.parent.mkdir(parents=True, exist_ok=True)
                discovered_out.write_text("", encoding="utf-8")
                (discovered_out.parent / "summary.json").write_text("{}", encoding="utf-8")
                return {"discovered_output_path": str(discovered_out)}

            def fake_archive(*, job_name: str, run_id: str):
                calls["archive"] += 1
                archive_summary_path.parent.mkdir(parents=True, exist_ok=True)
                archive_summary_path.write_text(json.dumps({"archived_snapshot_paths": [str(tmp_path / "snapshots" / "x")]}), encoding="utf-8")
                return {"ok_count": 1, "partial_count": 0, "error_count": 0}

            def fake_parse(*, job_name: str, run_id: str):
                calls["parse"] += 1
                parsed_details.parent.mkdir(parents=True, exist_ok=True)
                parsed_details.write_text("{}\n", encoding="utf-8")
                parse_summary.write_text("{}", encoding="utf-8")
                export_jsonl.parent.mkdir(parents=True, exist_ok=True)
                export_jsonl.write_text("{}\n", encoding="utf-8")
                export_csv.write_text("source_domain\n", encoding="utf-8")
                return {
                    "parsed_details_path": str(parsed_details),
                    "export_jsonl_path": str(export_jsonl),
                    "export_csv_path": str(export_csv),
                }

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


if __name__ == "__main__":
    unittest.main()
