# -*- coding: utf-8 -*-
# AGENTE VIZCAYA V11 - COMBO MEJORES SCRAPERS + DROPS IDEALISTA / INDOMIO / BERAIBER
# Python 3.11.x | deps: requests, beautifulsoup4, lxml, pandas, openpyxl

import sys
import os
import re
import time
import json
import argparse
import datetime
import glob
import random
from typing import List, Dict, Tuple, Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup


# ===================== CONFIG GLOBAL =====================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE_DIR, "out")
DIAG_DIR = os.path.join(BASE_DIR, "diag")
DROP_DIR = os.path.join(BASE_DIR, "drops", "idealista")   # Idealista
INDOMIO_DROP_DIR = os.path.join(BASE_DIR, "drops", "indomio")
BERAIBER_DROP_DIR = os.path.join(BASE_DIR, "drops", "beraiber")

for d in (OUT_DIR, DIAG_DIR, DROP_DIR, INDOMIO_DROP_DIR, BERAIBER_DROP_DIR):
    os.makedirs(d, exist_ok=True)

UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edg/126.0 Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
]

BASE_HEADERS = {"Accept-Language": "es-ES,es;q=0.9"}


def _pick_headers() -> Dict[str, str]:
    return {
        **BASE_HEADERS,
        "User-Agent": random.choice(UAS),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,*/*;q=0.8"
        ),
        "Referer": "https://www.google.com/",
    }


def _human_delay():
    time.sleep(random.uniform(1.0, 3.0))  # fiabilidad > velocidad


def get_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").replace("\xa0", " ")).strip()


def to_int_m2(txt: str) -> Optional[int]:
    if not txt:
        return None
    s = txt.replace("\xa0", " ")
    m = re.search(
        r"(\d{1,3}(?:[\.,]\d{3})+|\d+(?:[\.,]\d+)?)\s*m²",
        s,
        re.I,
    )
    if not m:
        m = re.search(
            r"(\d{1,3}(?:[\.,]\d{3})+|\d+(?:[\.,]\d+)?)\s*m2",
            s,
            re.I,
        )
    if not m:
        return None
    v = m.group(1).replace(".", "").replace(",", ".")
    try:
        return int(float(v))
    except Exception:
        return None


def parse_price_block(txt: str, m2: Optional[int] = None) -> Tuple[Optional[float], Optional[float], Optional[int]]:
    s = txt.replace("\xa0", " ")
    m = re.search(r"(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?)\s*€", s)
    price = float(m.group(1).replace(".", "").replace(",", ".")) if m else None
    unit = (price / m2) if (price and m2) else None
    return price, unit, m2


def guess_localidad(txt: str) -> Optional[str]:
    if not txt:
        return None
    pats = [
        "Bilbao", "Barakaldo", "Getxo", "Santurtzi", "Sestao",
        "Portugalete", "Basauri", "Leioa", "Erandio", "Galdakao",
        "Durango", "Ermua", "Muskiz", "Zamudio", "Amorebieta",
        "Zalla", "Balmaseda", "Gernika", "Mungia", "Abadiño",
        "Etxebarri", "Derio", "Trapagaran", "Abanto", "Zierbena",
    ]
    for p in pats:
        if re.search(rf"\b{re.escape(p)}\b", txt, re.I):
            return p
    return None


def clasificar_operacion(title: str, body: str, url: str) -> str:
    txt = " ".join([title or "", body or "", url or ""]).lower()
    if "alquiler" in txt or "/alquiler" in txt or "mes" in txt:
        return "alquiler"
    if "/venta" in txt or "venta" in txt:
        return "venta"
    return "N/D"


def _save_diag_html(name: str, html: str):
    try:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(DIAG_DIR, f"{name}_{ts}.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass


def http_get(url: str, timeout: int = 20000, retries: int = 3) -> Optional[str]:
    """
    GET robusto: UA rotatorio, tolera 403/406 si hay cuerpo, backoff
    con jitter, fallback milanuncios www/es, delay humano entre intentos.
    timeout en milisegundos.
    """
    u = url
    backoff = 0.9
    for i in range(retries + 1):
        _human_delay()
        try:
            r = requests.get(
                u,
                headers=_pick_headers(),
                timeout=timeout / 1000,
                allow_redirects=True,
            )
            if r.text and (200 <= r.status_code < 400 or r.status_code in (403, 406)):
                if r.status_code in (403, 406):
                    _save_diag_html("resp_403_406", r.text)
                return r.text
            if r.status_code in (429, 503):
                time.sleep(backoff + random.uniform(0.2, 0.8))
                backoff *= 1.8
                continue
            if r.status_code == 404 and "milanuncios.com" in u and i == 0:
                # alternar www/es
                if "://www." in u:
                    u = u.replace("://www.", "://es.")
                else:
                    u = u.replace("://es.", "://www.")
                time.sleep(0.7)
                continue
        except Exception:
            pass
        if i < retries:
            time.sleep(backoff + random.uniform(0.2, 0.8))
            backoff *= 1.6
        else:
            break
    sys.stderr.write(f"[WARN] GET fail {url}\n")
    return None


JSON_LD_SEL = "script[type='application/ld+json']"


def parse_ld_json(soup: BeautifulSoup) -> dict:
    data = {}
    for s in soup.select(JSON_LD_SEL):
        try:
            j = json.loads(s.get_text(strip=True))
            if isinstance(j, list):
                for x in j:
                    if isinstance(x, dict):
                        data.update(x)
            elif isinstance(j, dict):
                data.update(j)
        except Exception:
            continue
    return data


def ld_pick(d: dict, *keys):
    for k in keys:
        if k in d and d[k]:
            return d[k]
    return None


def normalize_raw_item(it: Dict[str, object], fuente: str) -> Dict[str, object]:
    """
    Normaliza un dict de cualquier crawler a columnas comunes:
    link, localidad, m2, precio, operacion, detalles, fuente, fecha.
    """
    now_iso = datetime.datetime.now().strftime("%Y-%m-%d")
    return {
        "link": it.get("link", ""),
        "localidad": it.get("localidad", "") or "",
        "m2": it.get("m2", "") or "",
        "precio": it.get("precio", "") or "",
        "operacion": it.get("operacion", "") or "N/D",
        "detalles": it.get("detalles", "") or "",
        "fuente": it.get("fuente", fuente) or fuente,
        "fecha": it.get("fecha", now_iso) or now_iso,
    }


def write_diag(name: str, rows: List[Dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    p = os.path.join(DIAG_DIR, f"{datetime.datetime.now():%Y%m%d_%H%M%S}_{name}.csv")
    df.to_csv(p, index=False, encoding="utf-8-sig")


# ================== PABELLONES & NAVES (parser texto) ==================

def _pab_normalize(text: str) -> str:
    import unicodedata
    text = (text or "").upper()
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def _pab_is_title(line: str) -> bool:
    n = _pab_normalize(line)
    TITLE_PREFIXES = (
        "SE VENDE",
        "SE VENDEN",
        "SE ALQUILA",
        "SE ALQUILAN",
        "ALQUILER PABELLON",
        "ALQUILER PABELLON LOGISTICA",
        "ALQUILER PABELLON LOGISTICA POLIGONO",
        "ALQUILER PABELLON LOGISTICA. ZAMUDIO",
    )
    if any(n.startswith(pref) for pref in TITLE_PREFIXES):
        return True
    if n.startswith("EN ZONA "):
        return True
    if n.startswith("PABELLONES LOGISTICA"):
        return True
    if n.startswith("SE VENDEN PARCELAS NUEVO POLIGONO"):
        return True
    if n.startswith("SE ALQUILAN PABELLONES LOGISTICA EN EL SUPERPUERTO"):
        return True
    return False


def parse_pabellones_html(html: str) -> List[Dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    lines = [t.strip() for t in soup.stripped_strings if t.strip()]
    properties: List[Dict[str, object]] = []
    i = 0
    total = len(lines)

    while i < total:
        line = lines[i]
        if not _pab_is_title(line):
            i += 1
            continue
        title = line
        caracteristicas: List[str] = []
        i += 1
        while i < total:
            l2 = lines[i].strip()
            n2 = _pab_normalize(l2)
            if _pab_is_title(l2):
                break
            if n2.startswith("PABELLONES Y NAVES VIZCAYA"):
                i += 1
                continue
            if n2.startswith("MAPA DEL SITIO") or n2.startswith("CONSULTE NUESTRAS OFERTAS"):
                i += 1
                continue
            if l2.startswith("- "):
                l2 = l2[2:].strip()
            if len(l2) > 2:
                caracteristicas.append(l2)
            i += 1
        properties.append(
            {
                "titulo": title,
                "caracteristicas": caracteristicas,
            }
        )
    return properties


def _pab_static_fallback() -> List[Dict[str, object]]:
    # Resumen estático (noviembre 2025); abreviado
    return [
        {
            "titulo": "Se vende parcela industrial en Trapagaran",
            "caracteristicas": [
                "Parcela de 14.000 m²",
                "Junto a la autovía A-8, a 5 minutos de Bilbao",
                "Zona comercial en expansión junto a centro comercial",
                "Indicada para logística, grandes almacenes y última milla",
            ],
        },
        {
            "titulo": "Se vende parcela industrial en el Txorierri",
            "caracteristicas": [
                "Superficie de 10.000 m²",
                "Muy buena ubicación, junto a la autovía A-8",
                "Situada en una zona de grandes empresas",
            ],
        },
        # ... resto del listado estático original ...
    ]


def _pab_to_item(item: Dict[str, object], base_url: str) -> Dict[str, object]:
    titulo = str(item.get("titulo", "")).strip()
    caracteristicas = item.get("caracteristicas") or []
    cuerpo = " ".join([titulo] + list(caracteristicas))
    m2v = to_int_m2(cuerpo)
    pr, _, _ = parse_price_block(cuerpo, m2v)
    return {
        "link": base_url,
        "localidad": guess_localidad(cuerpo) or "",
        "detalles": clean_text(cuerpo),
        "m2": m2v,
        "precio": pr,
        "operacion": clasificar_operacion(cuerpo, cuerpo, base_url),
        "fuente": "Pabellones y Naves Vizcaya",
        "fecha": datetime.datetime.now().strftime("%Y-%m-%d"),
    }


def fetch_pabellones() -> List[Dict[str, object]]:
    urls = [
        "https://www.pabellonesynavesvizcaya.com/",
        "https://www.pabellonesynavesvizcaya.com/pabellones-logistica/",
    ]
    out: List[Dict[str, object]] = []
    # 1) Intento requests
    print("2025-11-09 [INFO] Pabellones: intentando extracción dinámica con requests…")
    for u in urls:
        print(f"2025-11-09 [INFO] Pabellones: descargando {u}")
        html = http_get(u, timeout=20000, retries=2)
        if not html:
            continue
        props = parse_pabellones_html(html)
        print(f"2025-11-09 [INFO] Pabellones: {len(props)} propiedades detectadas en {u}")
        for p in props:
            out.append(_pab_to_item(p, u))
    if out:
        print(f"2025-11-09 [INFO] → Pabellones: {len(out)} propiedades")
        return out
    # 2) Fallback estático
    print("2025-11-09 [INFO] Pabellones: usando listado estático precompilado")
    for p in _pab_static_fallback():
        out.append(_pab_to_item(p, urls[0]))
    print(f"2025-11-09 [INFO] → Pabellones (estático): {len(out)} propiedades")
    return out


# ================== INMOERCILLA (listados + estático mejorado) ==================

def _inmoercilla_parse_list_page(html: str, operacion: str) -> List[Dict[str, object]]:
    """
    Extrae naves de una página de listado de Inmoercilla
    SIN entrar en las fichas (para evitar errores 500).
    """
    soup = get_soup(html)
    out: List[Dict[str, object]] = []

    # En los HTML que pasaste, cada tarjeta de resultado es un <div class="card custom-card-info">.
    cards = soup.select("div.card.custom-card-info")
    if not cards:
        # Fallback genérico por si cambian clases
        cards = soup.select("div.card")

    for c in cards:
        # Enlace principal de la propiedad
        a = c.select_one("a[href*='/inmuebles/'], a[href*='/property/']")
        if not a:
            continue

        href = a.get("href") or ""
        if href.startswith("http"):
            url_item = href
        elif href.startswith("/"):
            url_item = "https://www.inmoercilla.com" + href
        else:
            url_item = "https://www.inmoercilla.com/" + href

        # Texto completo de la tarjeta
        meta = clean_text(c.get_text(" "))

        # Filtro mínimo para asegurarnos de que hablamos de naves / pabellones
        if not re.search(r"\b(nave|pabell[óo]n)\b", meta, re.I):
            continue

        # m² y precio usando los helpers globales
        m2v = to_int_m2(meta)
        pr, _, _ = parse_price_block(meta, m2v)

        # Localidad: primero intentamos con el icono de ubicación, si existe
        localidad = ""
        loc_icon = c.select_one("i.fa-location-dot, i.fa-location-dot ~ span, i.fa-location-dot ~ strong")
        if loc_icon and loc_icon.parent:
            localidad = clean_text(loc_icon.parent.get_text(" "))
        if not localidad:
            localidad = guess_localidad(meta) or ""

        # Operación: usamos el hint (alquiler/venta) y, si el texto dice otra cosa, lo corregimos
        op_txt = meta.lower()
        op = operacion
        if "alquiler" in op_txt:
            op = "alquiler"
        elif "venta" in op_txt:
            op = "venta"

        out.append(
            {
                "link": url_item,
                "localidad": localidad,
                "m2": m2v,
                "precio": pr,
                "operacion": op,
                "detalles": meta[:700],
                "fuente": "Inmoercilla",
            }
        )

    return out


def _inmoercilla_list_crawl() -> List[Dict[str, object]]:
    """
    Recorre todas las páginas de listado de Inmoercilla (alquiler y venta)
    usando la paginación ?page=N, SIN visitar las fichas individuales.
    """
    base_alq = "https://www.inmoercilla.com/naves-pabellones-industriales-alquiler"
    base_ven = "https://www.inmoercilla.com/naves-pabellones-industriales-venta"
    out: List[Dict[str, object]] = []

    for base_url, oper in [(base_alq, "alquiler"), (base_ven, "venta")]:
        print(f"2025-11-09 [INFO] Inmoercilla: {base_url}")
        for page in range(1, 9):
            page_url = base_url if page == 1 else f"{base_url}?page={page}"
            html = http_get(page_url, timeout=20000, retries=2)
            if not html:
                break
            chunk = _inmoercilla_parse_list_page(html, operacion=oper)
            if not chunk and page > 1:
                # si ya no hay tarjetas en esta página, dejamos de paginar
                break
            out.extend(chunk)

    return out


def _inmoercilla_static_v5() -> List[Dict[str, object]]:
    """
    Fallback estático de las 3 referencias 'PA77xx' que usabas en la V5.

    IMPORTANTE: ya NO hacemos GET a esas URLs (devuelven 500),
    sólo las añadimos como registros mínimos.
    """
    urls = [
        "https://inmoercilla.com/property/pa7755",
        "https://inmoercilla.com/property/pa7756",
        "https://inmoercilla.com/property/pa7754",
    ]
    out: List[Dict[str, object]] = []
    for url in urls:
        out.append(
            {
                "link": url,
                "localidad": "",
                "m2": None,
                "precio": None,
                "operacion": "N/D",
                "detalles": "",
                "fuente": "Inmoercilla (estático)",
            }
        )
    return out


def fetch_inmoercilla() -> List[Dict[str, object]]:
    """
    Punto de entrada principal para Inmoercilla:

    1) Recorre listados de alquiler y venta (paginados) y normaliza.
    2) Añade las 3 referencias estáticas V5 sin hacer peticiones a sus fichas.
    """
    # 1) Listados dinámicos
    items: List[Dict[str, object]] = []
    try:
        list_items = _inmoercilla_list_crawl()
        items.extend(list_items)
    except Exception as e:
        print(f"[WARN] Inmoercilla: error en listados: {e}")

    # 2) Fallback estático V5
    try:
        static_items = _inmoercilla_static_v5()
        items.extend(static_items)
    except Exception as e:
        print(f"[WARN] Inmoercilla: error en estático V5: {e}")

    # Normalización a formato común del agente
    out = [normalize_raw_item(r, r.get("fuente", "Inmoercilla")) for r in items]
    print(
        f"2025-11-09 [INFO] → Inmoercilla: {len(out)} propiedades "
        "(combo listados + estático)"
    )
    return out


# ================== QUORUM ==================

def fetch_detail_quorum(url: str) -> Tuple[Optional[float], Optional[int], Optional[str], str]:
    html = http_get(url, timeout=20000, retries=2)
    if not html:
        return None, None, None, ""
    s = get_soup(html)
    t = clean_text(s.get_text(" "))
    ld = parse_ld_json(s)
    price = None
    offers = ld.get("offers")
    if isinstance(offers, dict) and offers.get("price"):
        try:
            price = float(offers["price"])
        except Exception:
            price = None
    if price is None:
        pr, _, _ = parse_price_block(t)
        price = pr
    m2v = to_int_m2(str(ld_pick(ld, "floorSize", "area", "size"))) or to_int_m2(t)
    loc = ld_pick(ld, "addressLocality", "address")
    if isinstance(loc, dict):
        loc = loc.get("addressLocality") or loc.get("addressRegion")
    if not loc:
        bc = s.select_one(
            ".breadcrumb, .breadcrumbs, nav[aria-label*='breadcrumb']"
        )
        loc = (
            guess_localidad(clean_text(bc.get_text(" "))) if bc else guess_localidad(t)
        )
    return price, m2v, loc, t[:600]


def crawl_quorum_list(
    url: str = (
        "https://www.inmobiliariaquorum.com/propiedades/?status=any&"
        "location=any&child-location=any&type=nave-industrial&"
        "max-price=any&bedrooms=any&bathrooms=any&min-area&property-id&keyword"
    ),
    base: str = "https://www.inmobiliariaquorum.com",
    max_pages: int = 5,
) -> List[Dict]:
    out: List[Dict] = []
    next_url = url
    for _ in range(max_pages):
        html = http_get(next_url, timeout=20000, retries=2)
        if not html:
            break
        s = get_soup(html)
        cards = (
            s.select("article, .property, .listing, .property-item, .property-card")
            or s.select("a[href]")
        )
        for c in cards:
            a = c.select_one("a[href]") if hasattr(c, "select_one") else None
            if not a:
                continue
            href = a.get("href") or ""
            url_item = (
                href
                if href.startswith("http")
                else base.rstrip("/") + "/" + href.lstrip("/")
            )
            if not (
                re.search(r"inmobiliariaquorum\.com", url_item)
                and (
                    re.search(r"/(propiedad|property|inmueble)/?", url_item)
                    or len(
                        [
                            p
                            for p in re.sub(r"^https?://[^/]+", "", url_item).split("/")
                            if p
                        ]
                    )
                    >= 2
                )
            ):
                continue
            meta = clean_text(
                (c.get_text(" ") if hasattr(c, "get_text") else a.get_text(" "))
            )
            precio, m2v, loc, snippet = fetch_detail_quorum(url_item)
            if not (
                precio
                or m2v
                or re.search(r"\b(nave|pabell[óo]n)\b", meta, re.I)
            ):
                continue
            out.append(
                {
                    "fuente_origen": "quorum",
                    "link": url_item,
                    "localidad": loc or guess_localidad(meta) or "",
                    "m2": m2v,
                    "precio": precio,
                    "operacion": clasificar_operacion("", meta, url_item),
                    "detalles": snippet or meta[:500],
                    "_diag_text": (snippet or meta)[:600],
                }
            )
        nxt = s.select_one("a[rel='next'], .pagination a.next, a.page-next")
        if nxt and nxt.get("href"):
            h = nxt.get("href")
            next_url = (
                h if h.startswith("http") else base.rstrip("/") + "/" + h.lstrip("/")
            )
            time.sleep(0.8)
        else:
            break
    return out


def fetch_quorum() -> List[Dict[str, object]]:
    raw = crawl_quorum_list()
    out = [normalize_raw_item(r, "Quorum") for r in raw]
    print(f"2025-11-09 [INFO] → Quorum: {len(out)} propiedades")
    return out


# ================== MJI ==================

def crawl_mji_seeds() -> List[Dict]:
    urls = [
        "https://mjinavesypabellones.com/",
        "https://mjinavesypabellones.com/naves-industriales/",
    ]
    out: List[Dict] = []
    for u in urls:
        html = http_get(u, timeout=15000, retries=2)
        if not html:
            continue
        s = get_soup(html)
        for a in s.select(
            "a[href*='nave'], a[href*='pabellon'], a[href*='industrial']"
        ):
            href = a.get("href") or ""
            link = href if href.startswith("http") else u.rstrip("/") + "/" + href.lstrip("/")
            txt = clean_text(a.get_text(" "))
            if len(txt) < 5:
                continue
            out.append(
                {
                    "fuente_origen": "mji",
                    "link": link,
                    "detalles": txt[:300],
                }
            )
    return out[:50]


def _mji_static_v5() -> List[Dict[str, object]]:
    base = "https://mjinavesypabellones.com"
    urls = [
        f"{base}/property/7821/",
        f"{base}/property/6800/",
        f"{base}/property/arrigorriaga",
        f"{base}/property/etxebarri",
        f"{base}/property/gallarta",
        f"{base}/property/pabellon-bolueta",
    ]
    out: List[Dict[str, object]] = []
    for url in urls:
        html = http_get(url, timeout=20000, retries=1)
        if not html:
            continue
        s = get_soup(html)
        detalle = s.get_text(" ")
        cuerpo = clean_text(detalle)
        m2v = to_int_m2(cuerpo)
        pr, _, _ = parse_price_block(cuerpo, m2v)
        out.append(
            {
                "link": url,
                "localidad": guess_localidad(cuerpo) or "",
                "m2": m2v,
                "precio": pr,
                "operacion": clasificar_operacion("", cuerpo, url),
                "detalles": cuerpo[:700],
                "fuente": "MJI (estático)",
            }
        )
    return out


def fetch_mji() -> List[Dict[str, object]]:
    seeds = crawl_mji_seeds()
    static_items = _mji_static_v5()
    raw: List[Dict[str, object]] = []
    for it in seeds:
        raw.append(
            {
                "link": it["link"],
                "localidad": "",
                "m2": None,
                "precio": None,
                "operacion": "N/D",
                "detalles": it.get("detalles", ""),
                "fuente": "MJI",
            }
        )
    raw.extend(static_items)
    out = [normalize_raw_item(r, r.get("fuente", "MJI")) for r in raw]
    print(f"2025-11-09 [INFO] → MJI: {len(out)} propiedades")
    return out

# ================== MILANUNCIOS (v7 mejorado, adaptado a http_get(timeout=...)) ==================

def crawl_milanuncios() -> List[Dict]:
    list_urls = [
        "https://www.milanuncios.com/naves-industriales-en-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-etxebarri-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-arrigorriaga-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-barakaldo-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-muskiz-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-mungia-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-zamudio-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-lemoa-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-ugao-miraballes-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-erandio-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-gallarta-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-portugalete-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-gernika-lumo-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-abadino-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-getxo-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-durango-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-amorebieta-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-igorret-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-alonsotegui-vizcaya/",
        "https://www.milanuncios.com/naves-industriales-en-ortuella-vizcaya/",
    ]

    out: List[Dict] = []
    seen: set[str] = set()

    for list_url in list_urls:
        # ⬅️ AQUÍ EL CAMBIO IMPORTANTE: timeout=..., no timeout_ms=...
        html = http_get(list_url, timeout=15000, retries=3)
        if not html:
            continue

        s = get_soup(html)  # reutilizamos tu helper, que usa BeautifulSoup("lxml")

        # Solo anuncios reales: URLs que terminan en "-<digits>.htm"
        for a in s.select("a[href]"):
            href = a.get("href") or ""
            if not re.search(r"-\d+\.htm$", href):
                continue

            link = (
                href
                if href.startswith("http")
                else "https://www.milanuncios.com" + href
            )
            if link in seen:
                continue
            seen.add(link)

            txt = clean_text(a.get_text(" "))
            if len(txt) < 5:
                continue

            m2v = to_int_m2(txt)
            precio, _, _ = parse_price_block(txt, m2v)

            out.append(
                {
                    "link": link,
                    "localidad": guess_localidad(txt) or "",
                    "m2": m2v,
                    "precio": precio,
                    "operacion": clasificar_operacion("", txt, link),
                    "detalles": txt[:400],
                    "fuente": "Milanuncios",
                }
            )

    return out


def fetch_milanuncios() -> List[Dict[str, object]]:
    raw = crawl_milanuncios()
    out = [normalize_raw_item(r, "Milanuncios") for r in raw]
    print(f"2025-11-09 [INFO] → Milanuncios: {len(out)} propiedades (ligero)")
    return out


# --------------------------------------------------------------------
# BERAIBER (v9 + drops)
# --------------------------------------------------------------------

def _beraiber_links_from_html(html: str) -> List[str]:
    soup = get_soup(html)
    links = []
    for a in soup.select("a[href*='/es/propiedad/'], a[href*='/es/property/']"):
        href = a.get("href") or ""
        if not href:
            continue
        if not href.startswith("http"):
            href = "https://www.beraiber.com" + href
        if "beraiber.com" not in href:
            continue
        links.append(href.split("#")[0])
    return sorted(set(links))


def _beraiber_sitemaps_candidates() -> List[str]:
    base = "https://www.beraiber.com"
    return [
        f"{base}/sitemap.xml",
        f"{base}/sitemap_index.xml",
        f"{base}/property-sitemap.xml",
        f"{base}/properties-sitemap.xml",
        f"{base}/propiedad-sitemap.xml",
        f"{base}/post-sitemap.xml",
        f"{base}/page-sitemap.xml",
        f"{base}/real-estate-property-sitemap.xml",
    ]


def _beraiber_fetch_detail(url: str) -> Dict[str, object]:
    html = http_get(url, timeout=20000, retries=2)
    if not html:
        return {
            "fuente_origen": "beraiber",
            "link": url,
            "detalles": "",
            "precio": None,
            "m2": None,
        }
    s = get_soup(html)
    t = clean_text(s.get_text(" "))
    ld = parse_ld_json(s)
    price = None
    offers = ld.get("offers")
    if isinstance(offers, dict) and offers.get("price"):
        try:
            price = float(offers["price"])
        except Exception:
            price = None
    if price is None:
        pr, _, _ = parse_price_block(t)
        price = pr
    m2v = to_int_m2(t)
    loc = ld_pick(ld, "addressLocality", "address")
    if isinstance(loc, dict):
        loc = loc.get("addressLocality") or loc.get("addressRegion")
    if not loc:
        loc = guess_localidad(t)
    return {
        "fuente_origen": "beraiber",
        "link": url,
        "localidad": loc or "",
        "m2": m2v,
        "precio": price,
        "detalles": t[:600],
    }


def crawl_beraiber_from_drops() -> List[Dict[str, object]]:
    """
    Recorre los HTML guardados en drops/beraiber y extrae enlaces
    /es/propiedad/... de las páginas de listado para convertirlos en
    items normalizados, intentando además rascar datos de cada ficha.
    """
    items: List[Dict[str, object]] = []
    if not os.path.isdir(BERAIBER_DROP_DIR):
        return items

    files = sorted(glob.glob(os.path.join(BERAIBER_DROP_DIR, "*.html")))
    if not files:
        return items

    all_links = set()
    for fp in files:
        try:
            html = open(fp, "r", encoding="utf-8", errors="ignore").read()
        except Exception:
            continue
        for lk in _beraiber_links_from_html(html):
            all_links.add(lk)

    all_links = sorted(all_links)
    if not all_links:
        return items

    print(f"2025-11-09 [INFO] Beraiber (drops): {len(all_links)} enlaces de propiedad detectados")

    for lk in all_links:
        d = _beraiber_fetch_detail(lk)
        items.append(d)
    return items


def crawl_beraiber() -> List[Dict[str, object]]:
    """
    Lógica combinada:
    1) Intenta sitemaps oficiales.
    2) Intenta un buscador sencillo.
    3) Añade lo que haya en drops/beraiber (listados guardados a mano).
    """
    out: List[Dict[str, object]] = []

    # 1) Sitemaps (best effort)
    for sm in _beraiber_sitemaps_candidates():
        html = http_get(sm, timeout=15000, retries=1)
        if not html:
            continue
        links = re.findall(
            r"https?://beraiber\.com/es/propiedad/[^\s<]+", html, re.I
        )
        links = sorted(set(lk.strip() for lk in links))
        for lk in links:
            out.append(_beraiber_fetch_detail(lk))
        if out:
            break

    # 2) Buscador simple si no hay nada aún
    if not out:
        buscador_url = (
            "https://www.beraiber.com/es/buscador/?operacion=&tipologia=naves"
        )
        html = http_get(buscador_url, timeout=15000, retries=2)
        if html:
            for lk in _beraiber_links_from_html(html):
                out.append(_beraiber_fetch_detail(lk))

    # 3) Añadir Beraiber (drops)
    drops_items = crawl_beraiber_from_drops()
    out.extend(drops_items)

    # deduplicar por link
    ded: List[Dict[str, object]] = []
    seen = set()
    for it in out:
        lk = it.get("link")
        if not lk or lk in seen:
            continue
        seen.add(lk)
        ded.append(it)
    print(f"2025-11-09 [INFO] → Beraiber: {len(ded)} propiedades")
    return ded


def fetch_beraiber() -> List[Dict[str, object]]:
    raw = crawl_beraiber()
    out = [normalize_raw_item(r, "Beraiber") for r in raw]
    return out


# --------------------------------------------------------------------
# IDEALISTA / INDOMIO - resumen + procesar HTMLs guardados en drops/
# (esta parte ya estaba en v8 y la dejamos tal cual)
# --------------------------------------------------------------------

INIT_JSON_PATTERNS = [
    re.compile(r"__INITIAL_STATE__\s*=\s*(\{.*?\})\s*;", re.S),
    re.compile(r"window\.appState\s*=\s*(\{.*?\})\s*;", re.S),
    re.compile(r"__NEXT_DATA__\"?\s*type=\"application/json\">(\{.*?\})</script>", re.S),
]


def _extract_items_from_blob(blob: str, origin: str) -> List[Dict[str, object]]:
    items: List[Dict[str, object]] = []
    try:
        j = json.loads(blob)
    except Exception:
        return items

    def walk(x):
        if isinstance(x, dict):
            url = x.get("url") or x.get("detailUrl")
            title = x.get("title") or x.get("subtitle") or ""
            if url and any(dom in url for dom in ("idealista.com", "indomio.es", "indomio.it", "indomio.com")):
                txt = json.dumps(x, ensure_ascii=False)
                m2v = to_int_m2(txt)
                pr, _, _ = parse_price_block(txt, m2v)
                items.append(
                    {
                        "fuente_origen": origin,
                        "link": url,
                        "detalles": (title or "")[:500] or txt[:500],
                        "precio": pr,
                        "m2": m2v,
                        "localidad": guess_localidad(txt),
                        "operacion": clasificar_operacion(title, txt, url),
                        "_diag_text": txt[:600],
                    }
                )
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(j)
    return items


def crawl_idealista_from_drops() -> List[Dict[str, object]]:
    items: List[Dict[str, object]] = []
    files = sorted(glob.glob(os.path.join(DROP_DIR, "*.html")))
    for fp in files:
        try:
            html = open(fp, "r", encoding="utf-8", errors="ignore").read()
        except Exception:
            continue
        # HTML directo
        s = get_soup(html)
        for a in s.select(
            "a[href*='/inmueble/'], a.item-link, "
            "[data-test*='item-link'] a[href], article a[href]"
        ):
            href = a.get("href") or ""
            link = href if href.startswith("http") else f"https://www.idealista.com{href}"
            if "idealista.com" not in link:
                continue
            txt = clean_text(a.get_text(" "))
            if not txt:
                continue
            m2v = to_int_m2(txt)
            pr, _, _ = parse_price_block(txt, m2v)
            items.append(
                {
                    "fuente_origen": "idealista_drops",
                    "link": link,
                    "detalles": txt[:500],
                    "precio": pr,
                    "m2": m2v,
                    "localidad": guess_localidad(txt),
                    "operacion": clasificar_operacion("", txt, link),
                    "_diag_text": txt[:600],
                }
            )
        # JSON embebido
        blob = None
        for pat in INIT_JSON_PATTERNS:
            m = pat.search(html or "")
            if m:
                blob = m.group(1)
                break
        if blob:
            items.extend(_extract_items_from_blob(blob, "idealista_drops_blob"))

    # deduplicar
    ded: List[Dict[str, object]] = []
    seen = set()
    for it in items:
        lk = it.get("link")
        if not lk or lk in seen:
            continue
        seen.add(lk)
        ded.append(it)
    print(f"2025-11-09 [INFO] Idealista (drops): {len(ded)} propiedades deduplicadas")
    return ded


def crawl_indomio_from_drops() -> List[Dict[str, object]]:
    items: List[Dict[str, object]] = []
    files = sorted(glob.glob(os.path.join(INDOMIO_DROP_DIR, "*.html")))
    for fp in files:
        try:
            html = open(fp, "r", encoding="utf-8", errors="ignore").read()
        except Exception:
            continue
        s = get_soup(html)
        for a in s.select("a[href*='/naves-'], a[href*='/nav-'], a[href*='/industrial'], a[href*='/inmueble/']"):
            href = a.get("href") or ""
            if not href:
                continue
            if href.startswith("http"):
                link = href
            else:
                link = "https://www.indomio.es" + href
            if "indomio." not in link:
                continue
            txt = clean_text(a.get_text(" "))
            if not txt:
                continue
            m2v = to_int_m2(txt)
            pr, _, _ = parse_price_block(txt, m2v)
            items.append(
                {
                    "fuente_origen": "indomio_drops",
                    "link": link,
                    "detalles": txt[:500],
                    "precio": pr,
                    "m2": m2v,
                    "localidad": guess_localidad(txt),
                    "operacion": clasificar_operacion("", txt, link),
                    "_diag_text": txt[:600],
                }
            )
        blob = None
        for pat in INIT_JSON_PATTERNS:
            m = pat.search(html or "")
            if m:
                blob = m.group(1)
                break
        if blob:
            items.extend(_extract_items_from_blob(blob, "indomio_drops_blob"))

    ded: List[Dict[str, object]] = []
    seen = set()
    for it in items:
        lk = it.get("link")
        if not lk or lk in seen:
            continue
        seen.add(lk)
        ded.append(it)
    print(f"2025-11-09 [INFO] Indomio (drops): {len(ded)} propiedades deduplicadas")
    return ded


def fetch_idealista_summary() -> List[Dict[str, object]]:
    # Sólo una fila resumen para tener trazabilidad
    return [
        {
            "link": "https://www.idealista.com/venta-naves/vizcaya/",
            "localidad": "Vizcaya",
            "m2": "",
            "precio": "",
            "operacion": "N/D",
            "detalles": "Idealista: acceso con captcha; se usan HTMLs guardados en drops/idealista.",
            "fuente": "Idealista (resumen)",
            "fecha": datetime.datetime.now().strftime("%Y-%m-%d"),
        }
    ]


def fetch_indomio_summary() -> List[Dict[str, object]]:
    return [
        {
            "link": "https://www.indomio.es/naves-industriales/vizcaya-provincia/",
            "localidad": "Vizcaya",
            "m2": "",
            "precio": "",
            "operacion": "N/D",
            "detalles": "Indomio: acceso con captcha; se usan HTMLs guardados en drops/indomio.",
            "fuente": "Indomio (resumen)",
            "fecha": datetime.datetime.now().strftime("%Y-%m-%d"),
        }
    ]


# --------------------------------------------------------------------
# EXPORT EXCEL (mismo formato de las versiones anteriores)
# --------------------------------------------------------------------

def guardar_en_excel(datos: List[Dict[str, object]]) -> None:
    if not datos:
        print("[INFO] No hay datos que exportar.")
        return
    df = pd.DataFrame(datos)

    now_iso = datetime.datetime.now().strftime("%Y-%m-%d")
    if "fecha" not in df.columns:
        df["fecha"] = now_iso
    df["fecha"] = df["fecha"].fillna(now_iso)

    if "fuente" not in df.columns:
        df["fuente"] = "N/D"
    df["fuente"] = df["fuente"].fillna("N/D")

    if "operacion" not in df.columns:
        df["operacion"] = "N/D"
    df["operacion"] = df["operacion"].fillna("N/D").str.lower()

    df_out = pd.DataFrame(
        {
            "Link": df.get("link", ""),
            "Ubicación": df.get("localidad", ""),
            "Detalles": df.get("detalles", ""),
            "m²": df.get("m2", ""),
            "Precio": df.get("precio", ""),
            "Fecha": df.get("fecha", now_iso),
            "Fuente": df.get("fuente", "N/D"),
        }
    )

    op = df["operacion"]
    mask_alquiler = op.str.contains("alquiler", na=False)
    mask_venta = op.str.contains("venta", na=False)
    mask_nd = ~(mask_alquiler | mask_venta)

    df_alquiler = df_out[mask_alquiler].copy()
    df_venta = df_out[mask_venta | mask_nd].copy()

    fecha = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    fname = f"naves_bizkaia_v11_{fecha}.xlsx"
    out_path = os.path.join(OUT_DIR, fname)

    with pd.ExcelWriter(out_path) as writer:
        df_alquiler.to_excel(writer, sheet_name="Alquiler", index=False)
        df_venta.to_excel(writer, sheet_name="Venta", index=False)

    print(f"[INFO] Datos guardados en archivo Excel: {out_path}")


# --------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------

def main():
    print("2025-11-09 [INFO] Iniciando agente V11 (scraper combinado sin Playwright)")

    all_items: List[Dict[str, object]] = []

    # 1) Pabellones (texto)
    all_items.extend(fetch_pabellones())

    # 2) Inmoercilla (listados + estático v5)
    all_items.extend(fetch_inmoercilla())

    # 3) Quorum
    all_items.extend(fetch_quorum())

    # 4) MJI
    all_items.extend(fetch_mji())

    # 5) Milanuncios (versión buena múltiples listados)
    all_items.extend(fetch_milanuncios())

    # 6) Beraiber (web + drops)
    all_items.extend(fetch_beraiber())

    # 7) Idealista / Indomio (drops + resumen)
    all_items.extend(fetch_idealista_summary())
    all_items.extend(fetch_indomio_summary())
    all_items.extend(crawl_idealista_from_drops())
    all_items.extend(crawl_indomio_from_drops())

    # Resumen por fuente
    from collections import defaultdict

    per = defaultdict(int)
    for r in all_items:
        per[r.get("fuente", r.get("fuente_origen", "?"))] += 1
    print(f"2025-11-09 [INFO] Resumen por fuente: {json.dumps(per, ensure_ascii=False)}")
    print(f"2025-11-09 [INFO] Total registros: {len(all_items)}")

    # Deduplicar por link
    ded: List[Dict[str, object]] = []
    seen = set()
    for it in all_items:
        lk = it.get("link")
        if not lk or lk in seen:
            continue
        seen.add(lk)
        ded.append(it)

    guardar_en_excel(ded)


if __name__ == "__main__":
    main()
