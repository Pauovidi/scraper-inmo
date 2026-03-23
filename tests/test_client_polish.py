from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from app.streamlit_app import _apply_history_filters, _format_visible_date, _format_visible_datetime, _tab_labels
from src.publish.client_cleaning import infer_record_province, is_blocked_client_record, normalize_province_name
from src.publish.dedupe import PORTAL_LABELS
from src.publish.history import load_master_map
from src.publish.runner import publish_records


class ClientPolishTests(unittest.TestCase):
    def test_province_normalization_and_inference(self) -> None:
        self.assertEqual(normalize_province_name("vizcaya"), "Bizkaia")
        self.assertEqual(
            infer_record_province(
                {
                    "location_text": "Bilbao",
                    "url_final": "https://www.milanuncios.com/alquiler-de-naves-industriales-en-bilbao-vizcaya/bilbao-479664210.htm",
                }
            ),
            "Bizkaia",
        )
        self.assertIsNone(infer_record_province({"location_text": "Centro ciudad"}))

    def test_blocked_records_are_detected(self) -> None:
        self.assertTrue(is_blocked_client_record({"title": "Pardon our interruption"}))
        self.assertTrue(is_blocked_client_record({"title": "Sentimos la interrupción"}))
        self.assertTrue(is_blocked_client_record({"title": "SENTIMOS LA INTERRUPCIĂ\x93N"}))
        self.assertFalse(is_blocked_client_record({"title": "Nave industrial en Bilbao"}))

    def test_publish_records_excludes_blocked_rows_and_persists_province(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            history_root = root / "history"
            published_root = root / "published"

            summary = publish_records(
                job_name="internal_job_name",
                publish_date="2026-03-22",
                history_root_dir=history_root,
                published_root_dir=published_root,
                records=[
                    {
                        "source_domain": "milanuncios.com",
                        "url_final": "https://www.milanuncios.com/alquiler-de-naves-industriales-en-bilbao-vizcaya/bilbao-479664210.htm",
                        "title": "Nave en Bilbao",
                        "price_text": "1.875 €",
                        "price_value": 1875.0,
                        "location_text": "Bilbao (Bizkaia)",
                        "surface_sqm": 500.0,
                        "rooms_count": 0,
                        "parser_key": "generic_parser",
                        "parse_status": "ok",
                    },
                    {
                        "source_domain": "milanuncios.com",
                        "url_final": "https://www.milanuncios.com/captcha.htm",
                        "title": "Pardon our interruption",
                        "price_text": "",
                        "price_value": None,
                        "location_text": "",
                        "surface_sqm": None,
                        "rooms_count": None,
                        "parser_key": "generic_parser",
                        "parse_status": "partial",
                    },
                ],
            )

            self.assertEqual(summary["deduped_records_count"], 1)
            self.assertEqual(summary["new_listings_count"], 1)

            master_map = load_master_map(root_dir=history_root)
            self.assertEqual(len(master_map), 1)
            only_row = next(iter(master_map.values()))
            self.assertEqual(only_row["province"], "Bizkaia")

    def test_history_filters_support_province_multiselect(self) -> None:
        dataframe = pd.DataFrame(
            [
                {"portal": "pisos", "workflow_status": "pending", "first_seen_date": "2026-03-22", "last_seen_date": "2026-03-22", "title": "Nave en Bilbao", "province": "Bizkaia"},
                {"portal": "fotocasa", "workflow_status": "processed", "first_seen_date": "2026-03-22", "last_seen_date": "2026-03-22", "title": "Local en Valencia", "province": "Valencia"},
                {"portal": "idealista", "workflow_status": "pending", "first_seen_date": "2026-03-22", "last_seen_date": "2026-03-22", "title": "Oficina en Madrid", "province": "Madrid"},
            ]
        )

        filtered = _apply_history_filters(
            dataframe,
            portal_filter="Todos",
            status_filter="Todos",
            date_filter="Todas",
            search_text="",
            selected_provinces=["Bizkaia", "Valencia"],
        )

        self.assertEqual(len(filtered.index), 2)
        self.assertEqual(set(filtered["province"].tolist()), {"Bizkaia", "Valencia"})

    def test_visible_dates_use_european_format(self) -> None:
        self.assertEqual(_format_visible_date("2026-03-23"), "23-03-2026")
        self.assertEqual(_format_visible_datetime("2026-03-23T06:24:02Z"), "23-03-2026 06:24")

    def test_portal_labels_and_tabs_use_client_copy(self) -> None:
        self.assertEqual(PORTAL_LABELS["pisos"], "Pisos.com")
        self.assertIn("PISOS.COM", _tab_labels())
        self.assertIn("HISTÓRICO", _tab_labels())

    def test_main_view_avoids_internal_operational_copy(self) -> None:
        source = Path("app/streamlit_app.py").read_text(encoding="utf-8")
        self.assertIn("Guardar cambios de estado", source)
        self.assertNotIn("Cómo cambiar el estado", source)
        self.assertNotIn("Ejecución diaria", source)
        self.assertNotIn("actualización diaria", source.lower())

    def test_main_view_does_not_expose_internal_bizkaia_job_names(self) -> None:
        source = Path("app/streamlit_app.py").read_text(encoding="utf-8").lower()
        self.assertNotIn("bizkaia_naves", source)
        self.assertNotIn("bizkaia_naves_smoke", source)


if __name__ == "__main__":
    unittest.main()
