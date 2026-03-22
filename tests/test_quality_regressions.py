from __future__ import annotations

import unittest
from pathlib import Path

from src.discovery.extractor import discover_candidate_urls
from src.parsers.fotocasa_detail_parser import parse_fotocasa_detail_snapshot
from src.parsers.generic_parser import parse_generic_snapshot
from src.parsers.normalization import normalize_price
from src.parsers.snapshot_bridge import SnapshotBundle


class QualityRegressionsTests(unittest.TestCase):
    def test_discovery_excludes_fotocasa_profile_listing_url(self) -> None:
        html = """
        <html><body>
          <a href="https://www.fotocasa.es/es/inmobiliaria-global-urma/comprar/inmuebles/espana/todas-las-zonas/l?clientId=9202753024785">perfil</a>
          <a href="https://www.fotocasa.es/es/comprar/vivienda/bilbao/188901695/d?from=list&multimedia=image&isGalleryOpen=true">detail-image</a>
          <a href="https://www.fotocasa.es/es/comprar/vivienda/bilbao/188901695/d?from=list&multimedia=map&isZoomGalleryOpen=true">detail-map</a>
        </body></html>
        """
        bundle = SnapshotBundle(
            snapshot_path=Path("/tmp/snapshot"),
            html=html,
            markdown="",
            meta={
                "url_original": "https://www.fotocasa.es/es/comprar/locales/bizkaia-provincia/todas-las-zonas/l",
                "url_final": "https://www.fotocasa.es/es/comprar/locales/bizkaia-provincia/todas-las-zonas/l",
            },
        )

        urls = discover_candidate_urls(bundle, parser_key="fotocasa_detail", allowed_domain="fotocasa.es")

        self.assertEqual(len(urls), 1)
        self.assertEqual(urls[0], "https://www.fotocasa.es/es/comprar/vivienda/bilbao/188901695/d")

    def test_normalize_price_does_not_convert_precio_label_to_one(self) -> None:
        value, currency = normalize_price("Precio", "1 / 13 GLOBAL URMA · Tu partner inmobiliario")
        self.assertIsNone(value)
        self.assertIsNone(currency)

    def test_fotocasa_non_detail_page_is_not_ok(self) -> None:
        html = """
        <html><body>
          <h1>10 inmuebles de GLOBAL URMA en venta en España.</h1>
          <div class="price">Precio</div>
          <ul><li>90 m²</li><li>3 dormitorios</li></ul>
          <a href="/es/comprar/vivienda/barakaldo/188901695/d?from=list">detalle</a>
          <a href="/es/comprar/pisos/espana/todas-las-zonas/l">listado</a>
        </body></html>
        """
        bundle = SnapshotBundle(
            snapshot_path=Path("/tmp/snapshot"),
            html=html,
            markdown="",
            meta={
                "snapshot_id": "s1",
                "run_id": "r1",
                "snapshot_path": "/tmp/snapshot",
                "domain": "fotocasa.es",
                "url_original": "https://www.fotocasa.es/es/inmobiliaria-global-urma/comprar/inmuebles/espana/todas-las-zonas/l?clientId=9202753024785",
                "url_final": "https://www.fotocasa.es/es/inmobiliaria-global-urma/comprar/inmuebles/espana/todas-las-zonas/l?clientId=9202753024785",
            },
        )

        record = parse_fotocasa_detail_snapshot(bundle)

        self.assertEqual(record.page_kind, "listing")
        self.assertIn(record.parse_status, {"partial", "error"})
        self.assertIsNone(record.price_value)
        self.assertLess(record.confidence_score, 0.7)

    def test_milanuncios_detail_url_with_price_and_surface_is_treated_as_detail(self) -> None:
        html = """
        <html><body>
          <title>¡Ups! Algo se detuvo</title>
          <h1>¡Ups! Algo se detuvo</h1>
          <div>Bilbao</div>
          <div>1.875 €</div>
          <div>500 m²</div>
        </body></html>
        """
        bundle = SnapshotBundle(
            snapshot_path=Path("/tmp/milanuncios"),
            html=html,
            markdown="",
            meta={
                "snapshot_id": "s2",
                "run_id": "r2",
                "snapshot_path": "/tmp/milanuncios",
                "domain": "milanuncios.com",
                "url_original": "https://www.milanuncios.com/alquiler-de-naves-industriales-en-bilbao-vizcaya/bilbao-479664210.htm",
                "url_final": "https://www.milanuncios.com/alquiler-de-naves-industriales-en-bilbao-vizcaya/bilbao-479664210.htm",
            },
        )

        record = parse_generic_snapshot(bundle, "generic")

        self.assertEqual(record.page_kind, "detail")
        self.assertIn(record.parse_status, {"ok", "partial"})
        self.assertEqual(record.price_value, 1875.0)
        self.assertEqual(record.surface_sqm, 500.0)


if __name__ == "__main__":
    unittest.main()
