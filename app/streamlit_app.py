from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.publish import PORTAL_LABELS, PORTAL_ORDER, list_published_dates, load_master_records, load_published_summary, set_listing_status
from src.publish.client_cleaning import SPAIN_PROVINCES, is_blocked_client_record, normalize_client_record
from src.utils.paths import history_dir, published_dir

APP_NAME = "Inmoscraper"
EMPTY_LABEL = "Sin dato"

STATUS_LABELS = {
    "pending": "Pendiente",
    "processed": "Procesado",
    "discarded": "Descartado",
}
STATUS_BY_LABEL = {label: key for key, label in STATUS_LABELS.items()}
VISIBLE_PORTAL_TABS = ["NUEVOS HOY"]


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if not path or not path.exists() or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _parse_iso_date(value: object | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_iso_datetime(value: object | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        parsed_date = _parse_iso_date(text)
        if parsed_date is None:
            return None
        return datetime(parsed_date.year, parsed_date.month, parsed_date.day)


def _empty_master_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "portal",
            "source_domain",
            "listing_key",
            "url_final",
            "title",
            "price_text",
            "price_value",
            "location_text",
            "province",
            "surface_sqm",
            "rooms_count",
            "first_seen_date",
            "last_seen_date",
            "seen_count",
            "workflow_status",
            "workflow_updated_at",
            "workflow_note",
            "parser_key",
            "parse_status",
        ]
    )


def _normalize_client_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return _empty_master_dataframe()

    rows: list[dict[str, Any]] = []
    for record in dataframe.to_dict(orient="records"):
        normalized = normalize_client_record(record)
        if is_blocked_client_record(normalized):
            continue
        rows.append(normalized)

    if not rows:
        return _empty_master_dataframe()

    normalized_df = pd.DataFrame(rows)
    for column in _empty_master_dataframe().columns:
        if column not in normalized_df.columns:
            normalized_df[column] = None
    return normalized_df


def _load_master_dataframe() -> pd.DataFrame:
    records = load_master_records()
    if not records:
        return _empty_master_dataframe()
    return _normalize_client_dataframe(pd.DataFrame(records))


def _published_day_dir(publish_date: str) -> Path:
    return published_dir() / publish_date


def _load_published_dataframe(publish_date: str, portal: str) -> pd.DataFrame:
    csv_path = _published_day_dir(publish_date) / f"{portal}.csv"
    if not csv_path.exists():
        return pd.DataFrame()
    try:
        dataframe = pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame()
    return _normalize_client_dataframe(dataframe)


def _merge_master_data(published_df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    if published_df.empty:
        return published_df.copy()
    if master_df.empty or "listing_key" not in published_df.columns:
        return _normalize_client_dataframe(published_df)

    master_fields = [
        "listing_key",
        "workflow_status",
        "workflow_updated_at",
        "workflow_note",
        "first_seen_date",
        "last_seen_date",
        "seen_count",
        "title",
        "price_text",
        "price_value",
        "location_text",
        "province",
        "surface_sqm",
        "rooms_count",
        "url_final",
        "parse_status",
    ]
    master_subset = master_df[[field for field in master_fields if field in master_df.columns]].copy()
    master_subset = master_subset.drop_duplicates(subset=["listing_key"], keep="last")
    merged = published_df.merge(master_subset, on="listing_key", how="left", suffixes=("", "_master"))

    for column in master_subset.columns:
        if column == "listing_key":
            continue
        master_column = f"{column}_master"
        if master_column in merged.columns:
            if column in merged.columns:
                merged[column] = merged[master_column].where(merged[master_column].notna(), merged[column])
            else:
                merged[column] = merged[master_column]
            merged = merged.drop(columns=[master_column])
    return _normalize_client_dataframe(merged)


def _format_text(value: object | None) -> str:
    if value is None:
        return EMPTY_LABEL
    text = str(value).strip()
    return text if text else EMPTY_LABEL


def _format_visible_date(value: object | None) -> str:
    parsed = _parse_iso_date(value)
    if parsed is not None:
        return parsed.strftime("%d-%m-%Y")
    return _format_text(value)


def _format_visible_datetime(value: object | None) -> str:
    parsed = _parse_iso_datetime(value)
    if parsed is not None:
        if parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0:
            return parsed.strftime("%d-%m-%Y")
        return parsed.strftime("%d-%m-%Y %H:%M")
    return _format_text(value)


def _format_surface(value: object | None) -> str:
    if value in {"", None} or pd.isna(value):
        return EMPTY_LABEL
    return f"{value} m²"


def _format_rooms(value: object | None) -> str:
    if value in {"", None} or pd.isna(value):
        return EMPTY_LABEL
    try:
        return str(int(float(value)))
    except Exception:
        return _format_text(value)


def _tab_labels() -> list[str]:
    portal_tabs = [PORTAL_LABELS[portal].upper() for portal in PORTAL_ORDER]
    return VISIBLE_PORTAL_TABS + portal_tabs + ["HISTÓRICO", "TÉCNICO"]


def _inject_client_styles() -> None:
    st.markdown(
        """
        <style>
        div[data-baseweb="tab-list"] {
            gap: 0.65rem;
            flex-wrap: wrap;
            margin-bottom: 1rem;
        }
        button[data-baseweb="tab"] {
            border: 1px solid rgba(15, 23, 42, 0.18);
            border-radius: 999px;
            padding: 0.72rem 1.1rem;
            background: #f8fafc;
            color: #0f172a;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }
        button[data-baseweb="tab"][aria-selected="true"] {
            background: #0f172a;
            color: #ffffff;
            border-color: #0f172a;
        }
        div[data-testid="stMetric"] {
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 18px;
            padding: 0.8rem 0.9rem;
            background: #ffffff;
        }
        .status-guide {
            border: 1px solid rgba(14, 116, 144, 0.15);
            border-radius: 18px;
            padding: 0.9rem 1rem;
            background: linear-gradient(180deg, rgba(240, 249, 255, 1) 0%, rgba(248, 250, 252, 1) 100%);
            margin-bottom: 0.8rem;
        }
        .status-guide strong {
            display: block;
            margin-bottom: 0.2rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_status_guidance() -> None:
    st.markdown(
        """
        <div class="status-guide">
          <strong>Cómo cambiar el estado</strong>
          Usa la columna <strong>ESTADO</strong> para marcar cada anuncio como <strong>Pendiente</strong>, <strong>Procesado</strong> o <strong>Descartado</strong>, y después pulsa <strong>Guardar cambios de estado</strong>.
        </div>
        """,
        unsafe_allow_html=True,
    )


def _sort_client_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe
    working = dataframe.copy()
    for field in ["last_seen_date", "first_seen_date", "title", "portal"]:
        if field not in working.columns:
            working[field] = ""
    return working.sort_values(
        by=["last_seen_date", "first_seen_date", "portal", "title"],
        ascending=[False, False, True, True],
        kind="stable",
    )


def _prepare_view_dataframe(dataframe: pd.DataFrame, *, include_portal: bool = True) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame()

    working = _sort_client_dataframe(dataframe)
    working["workflow_status"] = working["workflow_status"].fillna("pending")
    working["Estado"] = working["workflow_status"].map(STATUS_LABELS).fillna("Pendiente")
    working["Portal"] = working["portal"].fillna("").apply(lambda value: PORTAL_LABELS.get(str(value), str(value).title()))
    working["Título"] = working["title"].apply(_format_text)
    working["Precio"] = working["price_text"].apply(_format_text)
    working["Ubicación"] = working["location_text"].apply(_format_text)
    working["Provincia"] = working["province"].apply(_format_text)
    working["Superficie"] = working["surface_sqm"].apply(_format_surface)
    working["Habitaciones"] = working["rooms_count"].apply(_format_rooms)
    working["Enlace"] = working["url_final"].apply(lambda value: value if value and str(value).strip() else "")
    working["Primera detección"] = working["first_seen_date"].apply(_format_visible_date)
    working["Última detección"] = working["last_seen_date"].apply(_format_visible_date)
    working["Veces visto"] = working["seen_count"].fillna(0).astype(int)

    columns = [
        "listing_key",
        "Estado",
        "Portal",
        "Título",
        "Precio",
        "Ubicación",
        "Provincia",
        "Superficie",
        "Habitaciones",
        "Primera detección",
        "Última detección",
        "Enlace",
    ]
    if not include_portal:
        columns.remove("Portal")
    return working[columns].set_index("listing_key")


def _render_status_editor(dataframe: pd.DataFrame, *, key_prefix: str, include_portal: bool = True) -> None:
    if dataframe.empty:
        st.info("No hay anuncios para mostrar con los filtros actuales.")
        return

    _render_status_guidance()
    view_df = _prepare_view_dataframe(dataframe, include_portal=include_portal)
    disabled_columns = [
        column
        for column in [
            "Portal",
            "Título",
            "Precio",
            "Ubicación",
            "Provincia",
            "Superficie",
            "Habitaciones",
            "Enlace",
            "Primera detección",
            "Última detección",
        ]
        if column in view_df.columns
    ]
    edited_df = st.data_editor(
        view_df,
        width="stretch",
        hide_index=True,
        key=f"editor_{key_prefix}",
        column_config={
            "Estado": st.column_config.SelectboxColumn("Estado", options=list(STATUS_BY_LABEL.keys()), required=True, width="medium"),
            "Portal": st.column_config.TextColumn("Portal", width="small"),
            "Título": st.column_config.TextColumn("Título", width="large"),
            "Precio": st.column_config.TextColumn("Precio", width="small"),
            "Ubicación": st.column_config.TextColumn("Ubicación", width="medium"),
            "Provincia": st.column_config.TextColumn("Provincia", width="small"),
            "Superficie": st.column_config.TextColumn("Superficie", width="small"),
            "Habitaciones": st.column_config.TextColumn("Habitaciones", width="small"),
            "Primera detección": st.column_config.TextColumn("Primera detección", width="small"),
            "Última detección": st.column_config.TextColumn("Última detección", width="small"),
            "Enlace": st.column_config.LinkColumn("Enlace", display_text="Abrir anuncio", width="small"),
        },
        disabled=disabled_columns,
    )

    if st.button("Guardar cambios de estado", key=f"save_{key_prefix}", type="primary"):
        changes = 0
        for listing_key in edited_df.index:
            original_status = str(view_df.loc[listing_key, "Estado"])
            edited_status = str(edited_df.loc[listing_key, "Estado"])
            if original_status == edited_status:
                continue
            set_listing_status(listing_key=str(listing_key), status=STATUS_BY_LABEL[edited_status])
            changes += 1

        if changes:
            st.success(f"Se guardaron {changes} cambio(s).")
            st.rerun()
        else:
            st.info("No había cambios para guardar.")


def _apply_history_filters(
    dataframe: pd.DataFrame,
    *,
    portal_filter: str,
    status_filter: str,
    date_filter: str,
    search_text: str,
    selected_provinces: list[str] | None = None,
) -> pd.DataFrame:
    filtered = dataframe.copy()

    if portal_filter != "Todos" and "portal" in filtered.columns:
        filtered = filtered[filtered["portal"].fillna("") == portal_filter]

    if status_filter != "Todos" and "workflow_status" in filtered.columns:
        filtered = filtered[filtered["workflow_status"].fillna("") == status_filter]

    provinces = [province for province in (selected_provinces or []) if province]
    if provinces and "province" in filtered.columns:
        filtered = filtered[filtered["province"].fillna("").isin(provinces)]

    if date_filter != "Todas":
        filtered = filtered[
            (filtered["first_seen_date"].fillna("") == date_filter)
            | (filtered["last_seen_date"].fillna("") == date_filter)
        ]

    query = search_text.strip()
    if query and not filtered.empty:
        search_base = filtered.fillna("").astype(str)
        matches = search_base.apply(lambda series: series.str.contains(query, case=False, regex=False, na=False)).any(axis=1)
        filtered = filtered[matches]

    return filtered


def _filter_by_provinces(dataframe: pd.DataFrame, selected_provinces: list[str]) -> pd.DataFrame:
    if dataframe.empty or not selected_provinces or "province" not in dataframe.columns:
        return dataframe
    return dataframe[dataframe["province"].fillna("").isin(selected_provinces)]


def _select_all_provinces() -> None:
    st.session_state["province_filter"] = list(SPAIN_PROVINCES)


def _clear_provinces() -> None:
    st.session_state["province_filter"] = []


def _visible_summary_payload(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "publicado_el": _format_visible_datetime(summary.get("published_at")),
        "nuevos_del_dia": summary.get("new_listings_count"),
        "historico_total": summary.get("history_total_count"),
        "rutas": {
            "historico_maestro": str(history_dir() / "listings_master.jsonl"),
            "estados": str(history_dir() / "listing_status.jsonl"),
            "publicacion_diaria": str(_published_day_dir(str(summary.get("publish_date") or "")) / "summary.json") if summary.get("publish_date") else None,
        },
    }


def render() -> None:
    st.set_page_config(page_title=APP_NAME, layout="wide")
    _inject_client_styles()
    st.title(APP_NAME)
    st.caption("Panel cliente de oportunidades inmobiliarias en España")

    if st.button("Actualizar panel"):
        st.rerun()

    published_dates = list_published_dates()
    if not published_dates:
        st.warning("Todavía no hay publicaciones listas para mostrar.")
        st.stop()

    with st.sidebar:
        st.header("Filtros")
        st.caption("Cobertura preparada para provincias de España")
        selected_date = st.selectbox("Fecha de actualización", published_dates, format_func=_format_visible_date)
        province_controls = st.columns(2)
        province_controls[0].button("Todas", on_click=_select_all_provinces, use_container_width=True)
        province_controls[1].button("Limpiar", on_click=_clear_provinces, use_container_width=True)
        selected_provinces = st.multiselect(
            "Provincias",
            SPAIN_PROVINCES,
            key="province_filter",
            placeholder="Si no seleccionas ninguna, se muestran todas",
        )

    summary = load_published_summary(selected_date) or {}
    master_df = _load_master_dataframe()
    today_all_df = _merge_master_data(_load_published_dataframe(selected_date, "all"), master_df)
    portal_frames = {
        portal: _merge_master_data(_load_published_dataframe(selected_date, portal), master_df)
        for portal in PORTAL_ORDER
    }

    total_pending = int((master_df["workflow_status"].fillna("pending") == "pending").sum()) if not master_df.empty else 0
    portal_counts = {portal: len(frame.index) for portal, frame in portal_frames.items()}

    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("Actualización", _format_visible_date(selected_date))
    metric_col2.metric("Nuevos hoy", len(today_all_df.index))
    metric_col3.metric("Histórico total", len(master_df.index))
    metric_col4.metric("Pendientes", total_pending)

    st.caption(f"Última actualización visible: {_format_visible_datetime(summary.get('published_at'))}")

    portal_metric_cols = st.columns(len(PORTAL_ORDER))
    for idx, portal in enumerate(PORTAL_ORDER):
        portal_metric_cols[idx].metric(PORTAL_LABELS[portal], portal_counts.get(portal, 0))

    tabs = st.tabs(_tab_labels())

    with tabs[0]:
        st.subheader(f"Nuevos del día · {_format_visible_date(selected_date)}")
        visible_today = _filter_by_provinces(today_all_df, selected_provinces)
        st.caption(f"Mostrando {len(visible_today.index)} anuncio(s) visibles")
        _render_status_editor(visible_today, key_prefix=f"{selected_date}_all", include_portal=True)

    for index, portal in enumerate(PORTAL_ORDER, start=1):
        with tabs[index]:
            portal_df = _filter_by_provinces(portal_frames[portal], selected_provinces)
            st.subheader(PORTAL_LABELS[portal])
            st.caption(f"Mostrando {len(portal_df.index)} anuncio(s)")
            _render_status_editor(portal_df, key_prefix=f"{selected_date}_{portal}", include_portal=False)

    with tabs[len(PORTAL_ORDER) + 1]:
        st.subheader("Histórico")
        st.caption(f"Fecha de referencia: {_format_visible_date(selected_date)}")
        if master_df.empty:
            st.info("No hay histórico disponible todavía.")
        else:
            filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
            portal_filter = filter_col1.selectbox(
                "Portal",
                ["Todos"] + PORTAL_ORDER,
                format_func=lambda value: "Todos" if value == "Todos" else PORTAL_LABELS.get(value, value.title()),
            )
            status_filter = filter_col2.selectbox(
                "Estado",
                ["Todos", "pending", "processed", "discarded"],
                format_func=lambda value: "Todos" if value == "Todos" else STATUS_LABELS.get(value, value),
            )
            known_dates = sorted(
                {
                    str(value)
                    for value in pd.concat(
                        [
                            master_df["first_seen_date"].dropna().astype(str),
                            master_df["last_seen_date"].dropna().astype(str),
                        ]
                    ).tolist()
                    if value
                },
                reverse=True,
            )
            date_filter = filter_col3.selectbox(
                "Fecha",
                ["Todas"] + known_dates,
                format_func=lambda value: "Todas" if value == "Todas" else _format_visible_date(value),
            )
            search_text = filter_col4.text_input("Búsqueda")

            filtered_history = _apply_history_filters(
                master_df,
                portal_filter=portal_filter,
                status_filter=status_filter,
                date_filter=date_filter,
                search_text=search_text,
                selected_provinces=selected_provinces,
            )
            st.caption(f"Mostrando {len(filtered_history.index)} anuncio(s)")
            _render_status_editor(filtered_history, key_prefix=f"history_{selected_date}", include_portal=True)

    with tabs[len(PORTAL_ORDER) + 2]:
        st.subheader("Técnico")
        st.caption("Información secundaria mínima para soporte de la demo.")
        st.text_input("Histórico maestro", value=str(history_dir() / "listings_master.jsonl"), disabled=True)
        st.text_input("Estados", value=str(history_dir() / "listing_status.jsonl"), disabled=True)
        st.text_input("Publicación diaria", value=str(_published_day_dir(selected_date) / "summary.json"), disabled=True)
        st.json(_visible_summary_payload({"publish_date": selected_date, **summary}))


if __name__ == "__main__":
    render()
