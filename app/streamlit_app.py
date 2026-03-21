from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.publish import (
    PORTAL_LABELS,
    PORTAL_ORDER,
    list_published_dates,
    load_master_records,
    load_published_summary,
    set_listing_status,
)
from src.utils.paths import history_dir, published_dir

STATUS_LABELS = {
    "pending": "Pendiente",
    "processed": "Procesado",
    "discarded": "Descartado",
}
STATUS_BY_LABEL = {label: key for key, label in STATUS_LABELS.items()}


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if not path or not path.exists() or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


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


def _load_master_dataframe() -> pd.DataFrame:
    records = load_master_records()
    if not records:
        return _empty_master_dataframe()

    dataframe = pd.DataFrame(records)
    for column in _empty_master_dataframe().columns:
        if column not in dataframe.columns:
            dataframe[column] = None
    return dataframe


def _published_day_dir(publish_date: str) -> Path:
    return published_dir() / publish_date


def _load_published_dataframe(publish_date: str, portal: str) -> pd.DataFrame:
    csv_path = _published_day_dir(publish_date) / f"{portal}.csv"
    if not csv_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame()


def _merge_master_data(published_df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    if published_df.empty:
        return published_df.copy()
    if master_df.empty or "listing_key" not in published_df.columns:
        return published_df.copy()

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
    return merged


def _prepare_view_dataframe(dataframe: pd.DataFrame, include_portal: bool) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame()

    working = dataframe.copy()
    working["workflow_status"] = working["workflow_status"].fillna("pending")
    working["price_text"] = working["price_text"].fillna("")
    working["location_text"] = working["location_text"].fillna("")
    working["surface_sqm"] = working["surface_sqm"].fillna("")
    working["rooms_count"] = working["rooms_count"].fillna("")
    working["url_final"] = working["url_final"].fillna("")
    working["first_seen_date"] = working["first_seen_date"].fillna("")
    working["last_seen_date"] = working["last_seen_date"].fillna("")
    working["seen_count"] = working["seen_count"].fillna(0)

    working["Estado"] = working["workflow_status"].map(STATUS_LABELS).fillna("Pendiente")
    working["Título"] = working["title"].fillna("")
    working["Precio"] = working["price_text"].fillna("")
    working["Ubicación"] = working["location_text"].fillna("")
    working["Superficie"] = working["surface_sqm"].apply(lambda value: f"{value} m²" if value not in {"", None} and pd.notna(value) else "")
    working["Habitaciones"] = working["rooms_count"].apply(lambda value: int(value) if str(value) not in {"", "nan"} else "")
    working["Enlace"] = working["url_final"].fillna("")
    working["Primera detección"] = working["first_seen_date"].fillna("")
    working["Última detección"] = working["last_seen_date"].fillna("")
    working["Veces visto"] = working["seen_count"].fillna(0).astype(int)
    working["Portal"] = working["portal"].fillna("").apply(lambda value: PORTAL_LABELS.get(str(value), str(value).title()))

    columns = [
        "listing_key",
        "Estado",
        "Título",
        "Precio",
        "Ubicación",
        "Superficie",
        "Habitaciones",
        "Enlace",
        "Primera detección",
        "Última detección",
    ]
    if include_portal:
        columns.insert(1, "Portal")
        columns.append("Veces visto")

    return working[columns].set_index("listing_key")


def _render_status_editor(dataframe: pd.DataFrame, *, key_prefix: str, include_portal: bool = False) -> None:
    if dataframe.empty:
        st.info("No hay anuncios para mostrar.")
        return

    view_df = _prepare_view_dataframe(dataframe, include_portal=include_portal)
    disabled_columns = [
        column
        for column in [
            "Portal",
            "Título",
            "Precio",
            "Ubicación",
            "Superficie",
            "Habitaciones",
            "Enlace",
            "Primera detección",
            "Última detección",
            "Veces visto",
        ]
        if column in view_df.columns
    ]
    edited_df = st.data_editor(
        view_df,
        width="stretch",
        hide_index=True,
        key=f"editor_{key_prefix}",
        column_config={
            "Estado": st.column_config.SelectboxColumn(
                "Estado",
                options=list(STATUS_BY_LABEL.keys()),
                required=True,
            ),
            "Enlace": st.column_config.LinkColumn("Enlace", display_text="Abrir anuncio"),
        },
        disabled=disabled_columns,
    )

    if st.button("Guardar estados", key=f"save_{key_prefix}"):
        changes = 0
        for listing_key in edited_df.index:
            original_status = str(view_df.loc[listing_key, "Estado"])
            edited_status = str(edited_df.loc[listing_key, "Estado"])
            if original_status == edited_status:
                continue
            set_listing_status(
                listing_key=str(listing_key),
                status=STATUS_BY_LABEL[edited_status],
            )
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
) -> pd.DataFrame:
    filtered = dataframe.copy()

    if portal_filter != "Todos" and "portal" in filtered.columns:
        filtered = filtered[filtered["portal"].fillna("") == portal_filter]

    if status_filter != "Todos" and "workflow_status" in filtered.columns:
        filtered = filtered[filtered["workflow_status"].fillna("") == status_filter]

    if date_filter != "Todas":
        filtered = filtered[
            (filtered["first_seen_date"].fillna("") == date_filter)
            | (filtered["last_seen_date"].fillna("") == date_filter)
        ]

    query = search_text.strip()
    if query and not filtered.empty:
        search_base = filtered.fillna("").astype(str)
        matches = search_base.apply(
            lambda series: series.str.contains(query, case=False, regex=False, na=False)
        ).any(axis=1)
        filtered = filtered[matches]

    return filtered


def render() -> None:
    st.set_page_config(page_title="Panel de anuncios", layout="wide")
    st.title("Panel de anuncios")
    st.caption("Visor local v2 orientado a cliente")

    if st.button("Actualizar vista"):
        st.rerun()

    published_dates = list_published_dates()
    if not published_dates:
        st.warning("Todavía no hay publicaciones diarias. Ejecuta `python -m src.main publish-daily --job bizkaia_naves_smoke`.")
        st.stop()

    with st.sidebar:
        st.header("Vista")
        selected_date = st.selectbox("Fecha publicada", published_dates)

    summary = load_published_summary(selected_date) or {}
    if not summary:
        st.warning("No se encontró el resumen diario. Se mostrarán los datos disponibles sin métricas consolidadas.")
    master_df = _load_master_dataframe()
    today_all_df = _load_published_dataframe(selected_date, "all")
    today_all_df = _merge_master_data(today_all_df, master_df)

    total_pending = int((master_df["workflow_status"].fillna("pending") == "pending").sum()) if not master_df.empty else 0
    portal_counts = summary.get("portal_counts", {}) if isinstance(summary, dict) else {}

    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("Actualización", selected_date)
    metric_col2.metric("Nuevos hoy", int(summary.get("new_listings_count", len(today_all_df.index)) or 0))
    metric_col3.metric("Histórico total", int(summary.get("history_total_count", len(master_df.index)) or 0))
    metric_col4.metric("Pendientes", total_pending)

    st.caption(
        f"Última actualización: {summary.get('published_at', 'sin dato')}"
        f" | Job: {summary.get('job_name', 'sin dato')}"
    )

    portal_metric_cols = st.columns(len(PORTAL_ORDER))
    for idx, portal in enumerate(PORTAL_ORDER):
        portal_metric_cols[idx].metric(PORTAL_LABELS[portal], int(portal_counts.get(portal, 0) or 0))

    tabs = st.tabs([PORTAL_LABELS[portal] for portal in PORTAL_ORDER] + ["Histórico", "Técnico"])

    for index, portal in enumerate(PORTAL_ORDER):
        with tabs[index]:
            portal_df = _load_published_dataframe(selected_date, portal)
            portal_df = _merge_master_data(portal_df, master_df)
            st.subheader(f"{PORTAL_LABELS[portal]}: nuevos de hoy")
            if portal_df.empty:
                st.info("Sin anuncios nuevos hoy para este portal.")
            else:
                _render_status_editor(portal_df, key_prefix=f"{selected_date}_{portal}")

    with tabs[len(PORTAL_ORDER)]:
        st.subheader("Histórico")
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
            date_filter = filter_col3.selectbox("Fecha", ["Todas"] + known_dates)
            search_text = filter_col4.text_input("Búsqueda")

            filtered_history = _apply_history_filters(
                master_df,
                portal_filter=portal_filter,
                status_filter=status_filter,
                date_filter=date_filter,
                search_text=search_text,
            )
            st.caption(f"Mostrando {len(filtered_history.index)} anuncio(s)")
            _render_status_editor(filtered_history, key_prefix=f"history_{selected_date}", include_portal=True)

    with tabs[len(PORTAL_ORDER) + 1]:
        st.subheader("Técnico")
        st.text_input("Ruta histórico maestro", value=str(history_dir() / "listings_master.jsonl"), disabled=True)
        st.text_input("Ruta estados", value=str(history_dir() / "listing_status.jsonl"), disabled=True)
        st.text_input("Ruta summary", value=str(_published_day_dir(selected_date) / "summary.json"), disabled=True)
        st.json(summary)

        manifest_path = Path(str(summary.get("source_manifest_path", ""))) if summary.get("source_manifest_path") else None
        manifest_payload = _read_json(manifest_path)
        if manifest_payload is not None:
            st.markdown("**Manifest del pipeline origen**")
            st.json(manifest_payload)


if __name__ == "__main__":
    render()
