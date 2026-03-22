from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from app.streamlit_app import _apply_history_filters
from src.publish.history import load_master_map, load_published_summary
from src.publish.runner import _resolve_pipeline_manifest, publish_records, set_listing_status
from src.publish.status_store import read_status_map


def _record(
    *,
    source_domain: str,
    url_final: str,
    title: str,
    price_text: str,
    price_value: float,
    location_text: str,
    surface_sqm: float,
    rooms_count: int,
    parser_key: str,
    parse_status: str = "ok",
) -> dict[str, object]:
    return {
        "source_domain": source_domain,
        "url_final": url_final,
        "title": title,
        "price_text": price_text,
        "price_value": price_value,
        "location_text": location_text,
        "surface_sqm": surface_sqm,
        "rooms_count": rooms_count,
        "parser_key": parser_key,
        "parse_status": parse_status,
    }


class PublishRunnerTests(unittest.TestCase):
    def test_resolve_pipeline_manifest_prefers_richer_same_day_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_root = root / "pipeline"
            older_manifest_path = pipeline_root / "run_a" / "manifest.json"
            newer_manifest_path = pipeline_root / "run_b" / "manifest.json"
            older_export = pipeline_root / "run_a" / "properties.csv"
            newer_export = pipeline_root / "run_b" / "properties.csv"
            older_manifest_path.parent.mkdir(parents=True, exist_ok=True)
            newer_manifest_path.parent.mkdir(parents=True, exist_ok=True)
            older_export.write_text("source_domain,title\npisos.com,Nave A\npisos.com,Nave B\n", encoding="utf-8")
            newer_export.write_text("source_domain,title\npisos.com,Nave C\n", encoding="utf-8")
            older_manifest = {
                "timestamp_utc_start": "2026-03-22T10:00:00Z",
                "export_paths": {"csv": str(older_export)},
            }
            newer_manifest = {
                "timestamp_utc_start": "2026-03-22T12:00:00Z",
                "export_paths": {"csv": str(newer_export)},
            }
            older_manifest_path.write_text(json.dumps(older_manifest), encoding="utf-8")
            newer_manifest_path.write_text(json.dumps(newer_manifest), encoding="utf-8")

            with patch(
                "src.publish.runner.list_pipeline_runs",
                return_value=[
                    {"manifest_path": str(newer_manifest_path), "timestamp_utc_start": "2026-03-22T12:00:00Z"},
                    {"manifest_path": str(older_manifest_path), "timestamp_utc_start": "2026-03-22T10:00:00Z"},
                ],
            ), patch("src.publish.runner.run_job_full") as mocked_run_job_full:
                manifest, manifest_path, pipeline_executed = _resolve_pipeline_manifest(
                    job_name="internal_job_name",
                    publish_date="2026-03-22",
                )

            self.assertFalse(pipeline_executed)
            self.assertEqual(manifest_path, older_manifest_path)
            self.assertEqual(manifest["export_paths"]["csv"], str(older_export))
            mocked_run_job_full.assert_not_called()

    def test_publish_records_tracks_history_and_only_publishes_new_listings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            history_root = root / "history"
            published_root = root / "published"

            day1_records = [
                _record(
                    source_domain="fotocasa.es",
                    url_final="https://www.fotocasa.es/es/comprar/vivienda/sestao/188901695/d",
                    title="Nave en Sestao",
                    price_text="130.000 €",
                    price_value=130000.0,
                    location_text="Sestao",
                    surface_sqm=110.0,
                    rooms_count=3,
                    parser_key="fotocasa_detail",
                ),
                _record(
                    source_domain="fotocasa.es",
                    url_final="https://www.fotocasa.es/es/comprar/vivienda/sestao/188901695/d?from=list",
                    title="Nave en Sestao duplicada",
                    price_text="130.000 €",
                    price_value=130000.0,
                    location_text="Sestao",
                    surface_sqm=110.0,
                    rooms_count=3,
                    parser_key="fotocasa_detail",
                ),
                _record(
                    source_domain="pisos.com",
                    url_final="https://www.pisos.com/inmueble/123456789/",
                    title="Pabellón en Bilbao",
                    price_text="235.000 €",
                    price_value=235000.0,
                    location_text="Bilbao",
                    surface_sqm=450.0,
                    rooms_count=2,
                    parser_key="pisos_detail",
                ),
            ]

            summary_day1 = publish_records(
                job_name="bizkaia_naves_smoke",
                records=day1_records,
                publish_date="2026-03-21",
                history_root_dir=history_root,
                published_root_dir=published_root,
            )

            self.assertEqual(summary_day1["input_records_count"], 3)
            self.assertEqual(summary_day1["deduped_records_count"], 2)
            self.assertEqual(summary_day1["new_listings_count"], 2)
            self.assertEqual(summary_day1["portal_counts"]["fotocasa"], 1)
            self.assertEqual(summary_day1["portal_counts"]["pisos"], 1)

            for portal in ["fotocasa", "idealista", "milanuncios", "pisos", "yaencontre", "all"]:
                self.assertTrue(Path(summary_day1["output_paths"][portal]).exists())

            summary_path = Path(summary_day1["summary_path"])
            self.assertTrue(summary_path.exists())
            persisted_summary = load_published_summary("2026-03-21", root_dir=published_root)
            self.assertIsNotNone(persisted_summary)
            self.assertEqual(persisted_summary["new_listings_count"], 2)

            master_map = load_master_map(root_dir=history_root)
            self.assertEqual(len(master_map), 2)
            fotocasa_key = "fotocasa.es:id:188901695"
            self.assertIn(fotocasa_key, master_map)
            self.assertEqual(master_map[fotocasa_key]["first_seen_date"], "2026-03-21")
            self.assertEqual(master_map[fotocasa_key]["last_seen_date"], "2026-03-21")
            self.assertEqual(master_map[fotocasa_key]["seen_count"], 1)
            self.assertEqual(master_map[fotocasa_key]["workflow_status"], "pending")

            day2_records = [
                _record(
                    source_domain="fotocasa.es",
                    url_final="https://www.fotocasa.es/es/comprar/vivienda/sestao/188901695/d",
                    title="Nave en Sestao",
                    price_text="130.000 €",
                    price_value=130000.0,
                    location_text="Sestao",
                    surface_sqm=110.0,
                    rooms_count=3,
                    parser_key="fotocasa_detail",
                ),
                _record(
                    source_domain="idealista.com",
                    url_final="https://www.idealista.com/inmueble/99887766/",
                    title="Local en Portugalete",
                    price_text="95.000 €",
                    price_value=95000.0,
                    location_text="Portugalete",
                    surface_sqm=85.0,
                    rooms_count=1,
                    parser_key="generic_parser",
                    parse_status="partial",
                ),
            ]

            summary_day2 = publish_records(
                job_name="bizkaia_naves_smoke",
                records=day2_records,
                publish_date="2026-03-22",
                history_root_dir=history_root,
                published_root_dir=published_root,
            )

            self.assertEqual(summary_day2["deduped_records_count"], 2)
            self.assertEqual(summary_day2["new_listings_count"], 1)
            self.assertEqual(summary_day2["portal_counts"]["fotocasa"], 0)
            self.assertEqual(summary_day2["portal_counts"]["idealista"], 1)

            master_map = load_master_map(root_dir=history_root)
            self.assertEqual(master_map[fotocasa_key]["first_seen_date"], "2026-03-21")
            self.assertEqual(master_map[fotocasa_key]["last_seen_date"], "2026-03-22")
            self.assertEqual(master_map[fotocasa_key]["seen_count"], 2)

            all_csv_path = Path(summary_day2["output_paths"]["all"])
            with all_csv_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["source_domain"], "idealista.com")

    def test_set_listing_status_persists_when_listing_reappears(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            history_root = root / "history"
            published_root = root / "published"

            summary = publish_records(
                job_name="bizkaia_naves_smoke",
                records=[
                    _record(
                        source_domain="fotocasa.es",
                        url_final="https://www.fotocasa.es/es/comprar/vivienda/sestao/188901695/d",
                        title="Nave en Sestao",
                        price_text="130.000 €",
                        price_value=130000.0,
                        location_text="Sestao",
                        surface_sqm=110.0,
                        rooms_count=3,
                        parser_key="fotocasa_detail",
                    )
                ],
                publish_date="2026-03-21",
                history_root_dir=history_root,
                published_root_dir=published_root,
            )

            master_map = load_master_map(root_dir=history_root)
            listing_key = next(iter(master_map.keys()))

            status_payload = set_listing_status(
                listing_key=listing_key,
                status="processed",
                note="Cliente revisado",
                history_root_dir=history_root,
            )
            self.assertEqual(status_payload["workflow_status"], "processed")
            self.assertEqual(status_payload["workflow_note"], "Cliente revisado")

            publish_records(
                job_name="bizkaia_naves_smoke",
                records=[
                    _record(
                        source_domain="fotocasa.es",
                        url_final="https://www.fotocasa.es/es/comprar/vivienda/sestao/188901695/d",
                        title="Nave en Sestao",
                        price_text="130.000 €",
                        price_value=130000.0,
                        location_text="Sestao",
                        surface_sqm=110.0,
                        rooms_count=3,
                        parser_key="fotocasa_detail",
                    )
                ],
                publish_date="2026-03-22",
                history_root_dir=history_root,
                published_root_dir=published_root,
            )

            master_map = load_master_map(root_dir=history_root)
            status_map = read_status_map(root_dir=history_root)
            self.assertEqual(master_map[listing_key]["workflow_status"], "processed")
            self.assertEqual(master_map[listing_key]["workflow_note"], "Cliente revisado")
            self.assertEqual(status_map[listing_key]["workflow_status"], "processed")
            self.assertEqual(status_map[listing_key]["workflow_note"], "Cliente revisado")

            day2_summary = load_published_summary("2026-03-22", root_dir=published_root)
            self.assertIsNotNone(day2_summary)
            self.assertEqual(day2_summary["new_listings_count"], 0)
            self.assertEqual(Path(summary["output_paths"]["all"]).name, "all.csv")

    def test_history_filters_support_portal_status_date_and_text(self) -> None:
        dataframe = pd.DataFrame(
            [
                {
                    "portal": "fotocasa",
                    "workflow_status": "processed",
                    "first_seen_date": "2026-03-21",
                    "last_seen_date": "2026-03-22",
                    "title": "Nave en Sestao",
                    "location_text": "Sestao",
                },
                {
                    "portal": "pisos",
                    "workflow_status": "pending",
                    "first_seen_date": "2026-03-22",
                    "last_seen_date": "2026-03-22",
                    "title": "Pabellón en Bilbao",
                    "location_text": "Bilbao",
                },
            ]
        )

        filtered = _apply_history_filters(
            dataframe,
            portal_filter="fotocasa",
            status_filter="processed",
            date_filter="2026-03-22",
            search_text="sestao",
        )

        self.assertEqual(len(filtered.index), 1)
        self.assertEqual(filtered.iloc[0]["portal"], "fotocasa")
        self.assertEqual(filtered.iloc[0]["workflow_status"], "processed")


if __name__ == "__main__":
    unittest.main()
