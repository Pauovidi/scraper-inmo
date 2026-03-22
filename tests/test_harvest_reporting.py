from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.harvest.reporting import build_funnel_report


class HarvestReportingTests(unittest.TestCase):
    def test_build_funnel_report_aggregates_portal_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            harvest_dir = tmp_path / "harvest" / "2026-03-22"
            harvest_dir.mkdir(parents=True, exist_ok=True)
            harvest_summary_path = harvest_dir / "summary.json"
            harvest_summary_path.write_text(
                json.dumps(
                    {
                        "job_name": "bizkaia_naves_smoke",
                        "harvest_date": "2026-03-22",
                        "portal_summaries": {
                            "fotocasa": {
                                "source_domain": "fotocasa.es",
                                "listing_pages_attempted": 4,
                                "listing_pages_ok": 4,
                                "listing_pages_error": 0,
                                "cards_detected": 12,
                                "candidates_emitted": 8,
                                "candidates_deduped_out": 2,
                                "candidates_rejected_by_rules": 4,
                                "candidates_sent_to_detail": 6,
                            },
                            "milanuncios": {
                                "source_domain": "milanuncios.com",
                                "listing_pages_attempted": 3,
                                "listing_pages_ok": 3,
                                "listing_pages_error": 0,
                                "cards_detected": 15,
                                "candidates_emitted": 15,
                                "candidates_deduped_out": 0,
                                "candidates_rejected_by_rules": 0,
                                "candidates_sent_to_detail": 15,
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

            archive_summary_path = tmp_path / "discovered" / "job_runs" / "bizkaia_naves_smoke" / "jobrun_001" / "archive_summary.json"
            archive_summary_path.parent.mkdir(parents=True, exist_ok=True)
            archive_summary_path.write_text(
                json.dumps(
                    {
                        "results": [
                            {
                                "source_domain": "fotocasa.es",
                                "discovered_url": "https://www.fotocasa.es/es/comprar/locales/bizkaia-provincia/bilbao-123456789/d",
                                "status": "ok",
                                "snapshot_path": "/tmp/fotocasa_detail_1",
                            },
                            {
                                "source_domain": "milanuncios.com",
                                "discovered_url": "https://www.milanuncios.com/nave-industrial-en-bilbao-vizcaya-111222333.htm",
                                "status": "ok",
                                "snapshot_path": "/tmp/milanuncios_detail_1",
                            },
                            {
                                "source_domain": "milanuncios.com",
                                "discovered_url": "https://www.milanuncios.com/nave-industrial-en-barakaldo-vizcaya-111222444.htm",
                                "status": "error",
                                "snapshot_path": None,
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            parsed_dir = tmp_path / "parsed" / "discovered" / "bizkaia_naves_smoke" / "jobrun_001"
            parsed_dir.mkdir(parents=True, exist_ok=True)
            (parsed_dir / "parsed_details.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "source_domain": "fotocasa.es",
                                "page_kind": "detail",
                                "parse_status": "ok",
                            }
                        ),
                        json.dumps(
                            {
                                "source_domain": "milanuncios.com",
                                "page_kind": "detail",
                                "parse_status": "partial",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (parsed_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "errors": [
                            {
                                "snapshot_path": "/tmp/milanuncios_detail_1",
                                "error": "ValueError: broken parser",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            report = build_funnel_report(
                job_name="bizkaia_naves_smoke",
                run_id="jobrun_001",
                harvest_summary_path=harvest_summary_path,
                harvest_root_dir=tmp_path / "harvest",
                discovery_root_dir=tmp_path / "discovered" / "job_runs",
                parsed_root_dir=tmp_path / "parsed" / "discovered",
            )

            self.assertTrue(Path(report["output_path"]).exists())
            self.assertEqual(report["portal_reports"]["fotocasa"]["detail_archive_ok"], 1)
            self.assertEqual(report["portal_reports"]["fotocasa"]["parsed_detail_ok"], 1)
            self.assertEqual(report["portal_reports"]["milanuncios"]["detail_archive_ok"], 1)
            self.assertEqual(report["portal_reports"]["milanuncios"]["detail_archive_error"], 1)
            self.assertEqual(report["portal_reports"]["milanuncios"]["parsed_detail_partial"], 1)
            self.assertEqual(report["portal_reports"]["milanuncios"]["parsed_detail_error"], 1)
            self.assertEqual(report["totals"]["candidates_sent_to_detail"], 21)


if __name__ == "__main__":
    unittest.main()
