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

from src.utils.paths import pipeline_runs_dir


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if not path or not path.exists() or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _read_jsonl_records(path: Path | None) -> list[dict[str, Any]]:
    if not path or not path.exists() or not path.is_file():
        return []

    records: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line:
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict):
                    records.append(value)
    except OSError:
        return []
    return records


def _path_from_value(value: Any) -> Path | None:
    if not value:
        return None
    try:
        return Path(str(value))
    except (TypeError, ValueError):
        return None


def list_jobs() -> list[str]:
    root = pipeline_runs_dir()
    if not root.exists():
        return []
    return sorted([entry.name for entry in root.iterdir() if entry.is_dir()])


def list_pipeline_run_ids(job_name: str) -> list[str]:
    job_dir = pipeline_runs_dir() / job_name
    if not job_dir.exists():
        return []

    runs = [entry.name for entry in job_dir.iterdir() if entry.is_dir()]
    return sorted(runs, reverse=True)


def _manifest_path(job_name: str, pipeline_run_id: str) -> Path:
    return pipeline_runs_dir() / job_name / pipeline_run_id / "manifest.json"


def _resolve_output_paths(job_name: str, pipeline_run_id: str, manifest: dict[str, Any]) -> dict[str, Path | None]:
    export_paths = manifest.get("export_paths", {}) or {}
    output_paths = manifest.get("output_paths", {}) or {}
    job_run_id = str(manifest.get("output_job_run_id") or manifest.get("job_run_id") or "")

    discovered_dir = REPO_ROOT / "data" / "discovered" / "job_runs" / job_name / job_run_id
    parsed_dir = REPO_ROOT / "data" / "parsed" / "discovered" / job_name / job_run_id
    exports_dir = REPO_ROOT / "data" / "exports" / job_name / job_run_id

    return {
        "manifest": _manifest_path(job_name, pipeline_run_id),
        "discovery_summary": _path_from_value(manifest.get("discovery_summary_path")) or discovered_dir / "summary.json",
        "archive_summary": _path_from_value(manifest.get("archive_discovered_summary_path")) or discovered_dir / "archive_summary.json",
        "parse_summary": _path_from_value(manifest.get("parse_discovered_summary_path")) or parsed_dir / "summary.json",
        "properties_csv": _path_from_value(export_paths.get("csv")) or _path_from_value(output_paths.get("export_csv_path")) or exports_dir / "properties.csv",
        "properties_jsonl": _path_from_value(export_paths.get("jsonl")) or _path_from_value(output_paths.get("export_jsonl_path")) or exports_dir / "properties.jsonl",
        "job_manifest": _path_from_value(manifest.get("job_manifest_path")),
    }


def load_pipeline_context(job_name: str, pipeline_run_id: str) -> dict[str, Any]:
    manifest_path = _manifest_path(job_name, pipeline_run_id)
    manifest = _read_json(manifest_path)
    if manifest is None:
        return {
            "manifest": None,
            "paths": {
                "manifest": manifest_path,
            },
            "discovery_summary": None,
            "archive_summary": None,
            "parse_summary": None,
            "dataframe": pd.DataFrame(),
            "export_source": None,
            "warnings": [f"No se pudo leer el manifest: {manifest_path}"],
        }

    paths = _resolve_output_paths(job_name=job_name, pipeline_run_id=pipeline_run_id, manifest=manifest)
    discovery_summary = _read_json(paths["discovery_summary"])
    archive_summary = _read_json(paths["archive_summary"])
    parse_summary = _read_json(paths["parse_summary"])

    dataframe = pd.DataFrame()
    export_source: str | None = None
    warnings: list[str] = []

    csv_path = paths["properties_csv"]
    jsonl_path = paths["properties_jsonl"]

    if csv_path and csv_path.exists():
        try:
            dataframe = pd.read_csv(csv_path)
            export_source = "csv"
        except Exception as exc:  # pragma: no cover
            warnings.append(f"No se pudo leer el CSV: {csv_path} ({type(exc).__name__})")

    if dataframe.empty and jsonl_path and jsonl_path.exists():
        records = _read_jsonl_records(jsonl_path)
        if records:
            dataframe = pd.DataFrame(records)
            export_source = "jsonl"
        else:
            warnings.append(f"El JSONL esta vacio o no se pudo leer: {jsonl_path}")

    if dataframe.empty and export_source is None:
        warnings.append("No hay export disponible para esta ejecucion.")

    return {
        "manifest": manifest,
        "paths": paths,
        "discovery_summary": discovery_summary,
        "archive_summary": archive_summary,
        "parse_summary": parse_summary,
        "dataframe": dataframe,
        "export_source": export_source,
        "warnings": warnings,
    }


def _apply_filters(
    dataframe: pd.DataFrame,
    source_domain: str,
    parse_status: str,
    search_text: str,
) -> pd.DataFrame:
    filtered = dataframe.copy()

    if source_domain != "Todos" and "source_domain" in filtered.columns:
        filtered = filtered[filtered["source_domain"].fillna("") == source_domain]

    if parse_status != "Todos" and "parse_status" in filtered.columns:
        filtered = filtered[filtered["parse_status"].fillna("") == parse_status]

    query = search_text.strip()
    if query and not filtered.empty:
        search_base = filtered.fillna("").astype(str)
        matches = search_base.apply(
            lambda series: series.str.contains(query, case=False, regex=False, na=False)
        ).any(axis=1)
        filtered = filtered[matches]

    return filtered


def _status_metric(discovery_summary: dict[str, Any] | None, archive_summary: dict[str, Any] | None, parse_summary: dict[str, Any] | None, dataframe: pd.DataFrame) -> None:
    discovered = discovery_summary.get("discovered_urls_count", 0) if discovery_summary else 0
    archive_ok = archive_summary.get("ok_count", 0) if archive_summary else 0
    archive_partial = archive_summary.get("partial_count", 0) if archive_summary else 0
    archive_error = archive_summary.get("error_count", 0) if archive_summary else 0
    parsed = parse_summary.get("parsed_count", 0) if parse_summary else 0
    detail = parse_summary.get("detail_count", 0) if parse_summary else 0
    parse_error = parse_summary.get("error_count", 0) if parse_summary else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Discovered URLs", discovered)
    col2.metric("Archive OK / Partial / Error", f"{archive_ok} / {archive_partial} / {archive_error}")
    col3.metric("Parse Parsed / Detail / Error", f"{parsed} / {detail} / {parse_error}")
    col4.metric("Filas exportadas", int(len(dataframe.index)))


def _show_paths(paths: dict[str, Path | None]) -> None:
    st.subheader("Rutas")
    for label, path in paths.items():
        st.text_input(label, value=str(path) if path else "", disabled=True)


def _show_json_block(title: str, payload: dict[str, Any] | None, path: Path | None) -> None:
    st.markdown(f"**{title}**")
    if payload is None:
        st.warning(f"Archivo ausente o no legible: {path}")
        return
    st.json(payload)


def render() -> None:
    st.set_page_config(page_title="Scraper Results Viewer", layout="wide")
    st.title("Visor local de resultados")
    st.caption(f"Repositorio: {REPO_ROOT}")

    if st.button("Refresh"):
        st.rerun()

    jobs = list_jobs()
    if not jobs:
        st.warning("No se detectaron jobs en data/pipeline_runs.")
        st.stop()

    with st.sidebar:
        st.header("Seleccion")
        selected_job = st.selectbox("Job", jobs)
        run_ids = list_pipeline_run_ids(selected_job)
        if not run_ids:
            st.warning("No hay pipeline runs para el job seleccionado.")
            st.stop()
        selected_run = st.selectbox("Pipeline run", run_ids)

    context = load_pipeline_context(job_name=selected_job, pipeline_run_id=selected_run)
    manifest = context["manifest"]
    dataframe = context["dataframe"]

    for warning in context["warnings"]:
        st.warning(warning)

    if manifest is None:
        st.stop()

    summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
    summary_col1.metric("Pipeline run", str(manifest.get("pipeline_run_id", selected_run)))
    summary_col2.metric("Job run", str(manifest.get("job_run_id", "")))
    summary_col3.metric("Status", str(manifest.get("status", "unknown")))
    summary_col4.metric("Export source", context["export_source"] or "none")

    st.caption(
        f"Inicio: {manifest.get('timestamp_utc_start', '-')}"
        f" | Fin: {manifest.get('timestamp_utc_end', '-')}"
    )

    _status_metric(
        discovery_summary=context["discovery_summary"],
        archive_summary=context["archive_summary"],
        parse_summary=context["parse_summary"],
        dataframe=dataframe,
    )

    with st.expander("Ver rutas relevantes", expanded=False):
        _show_paths(context["paths"])

    tab_data, tab_json = st.tabs(["Datos", "JSON"])

    with tab_data:
        download_col1, download_col2 = st.columns(2)
        csv_path = context["paths"].get("properties_csv")
        jsonl_path = context["paths"].get("properties_jsonl")

        if csv_path and csv_path.exists():
            download_col1.download_button(
                "Descargar properties.csv",
                data=csv_path.read_bytes(),
                file_name=csv_path.name,
                mime="text/csv",
            )
        else:
            download_col1.info("properties.csv no disponible")

        if jsonl_path and jsonl_path.exists():
            download_col2.download_button(
                "Descargar properties.jsonl",
                data=jsonl_path.read_bytes(),
                file_name=jsonl_path.name,
                mime="application/json",
            )
        else:
            download_col2.info("properties.jsonl no disponible")

        filter_col1, filter_col2, filter_col3 = st.columns(3)
        source_options = ["Todos"]
        if "source_domain" in dataframe.columns:
            source_options.extend(sorted(value for value in dataframe["source_domain"].dropna().astype(str).unique()))
        status_options = ["Todos"]
        if "parse_status" in dataframe.columns:
            status_options.extend(sorted(value for value in dataframe["parse_status"].dropna().astype(str).unique()))

        selected_source = filter_col1.selectbox("Filtrar source_domain", source_options)
        selected_status = filter_col2.selectbox("Filtrar parse_status", status_options)
        search_text = filter_col3.text_input("Busqueda simple")

        filtered = _apply_filters(
            dataframe=dataframe,
            source_domain=selected_source,
            parse_status=selected_status,
            search_text=search_text,
        )

        st.caption(f"Mostrando {len(filtered.index)} fila(s)")
        if filtered.empty:
            st.info("No hay filas para los filtros actuales.")
        else:
            st.dataframe(filtered, width="stretch", hide_index=True)

    with tab_json:
        _show_json_block("Manifest global del pipeline", manifest, context["paths"].get("manifest"))
        _show_json_block("Discovery summary", context["discovery_summary"], context["paths"].get("discovery_summary"))
        _show_json_block("Archive summary", context["archive_summary"], context["paths"].get("archive_summary"))
        _show_json_block("Parse summary", context["parse_summary"], context["paths"].get("parse_summary"))


if __name__ == "__main__":
    render()
