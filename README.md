# Scraper Inmobiliario Bizkaia

Proyecto Python incremental con baseline legacy preservado y pipeline modular: archivado, jobs, discovery, parsing y export.

## Estado actual
- Baseline legacy intacto (`agent_naves_bizkaia_v14.py`).
- Archiver con `markdown.new` + fallback local.
- Pipeline completo `run-job-full` con resume y manifest global.
- Parsers específicos de detalle:
  - `pisos_detail` (`pisos.com`)
  - `fotocasa_detail` (`fotocasa.es`)
- Fallback genérico siempre disponible.

## CLI principal
```powershell
python -m src.main run-job --job bizkaia_naves
python -m src.main discover-job-run --job bizkaia_naves --run-id <run_id>
python -m src.main archive-discovered --job bizkaia_naves --run-id <run_id>
python -m src.main parse-discovered --job bizkaia_naves --run-id <run_id>
python -m src.main run-job-full --job bizkaia_naves
```

## run-job-full
Comando:

```powershell
python -m src.main run-job-full --job bizkaia_naves
```

Flags:

```powershell
python -m src.main run-job-full --job bizkaia_naves --resume
python -m src.main run-job-full --job bizkaia_naves --resume --force-discovery
python -m src.main run-job-full --job bizkaia_naves --resume --force-archive-discovered
python -m src.main run-job-full --job bizkaia_naves --resume --force-parse
```

Orquestación:
1. `run-job`
2. `discover-job-run`
3. `archive-discovered`
4. `parse-discovered`
5. export JSONL/CSV

## Consulta de pipeline runs
```powershell
python -m src.main list-pipeline-runs
python -m src.main list-pipeline-runs --job bizkaia_naves
python -m src.main show-pipeline-run --job bizkaia_naves --pipeline-run-id <id>
```

## Manifest global de pipeline
Ruta:
- `data/pipeline_runs/{job_name}/{pipeline_run_id}/manifest.json`

Campos clave:
- `pipeline_run_id`
- `job_name`
- `timestamp_utc_start`
- `timestamp_utc_end`
- `status` (`completed`, `partial`, `failed`)
- `job_run_id`
- `discovery_run_id`
- `archive_discovered_summary_path`
- `parse_discovered_summary_path`
- `export_paths`
- `output_job_run_id`
- `output_paths`
- `step_statuses`
- `errors_summary`

Índice global:
- `data/index/pipeline_runs_index.jsonl`

## Resume / skip-existing
Con `--resume`, el pipeline reutiliza la última ejecución del job y salta etapas completas cuando detecta outputs válidos:
- `run-job`: requiere `job_manifest_path`.
- `discover-job-run`: requiere `discovery_summary_path` y `discovered_output_path`.
- `archive-discovered`: requiere `archive_discovered_summary_path` con `archived_snapshot_paths`.
- `parse-discovered`: requiere `parse_discovered_summary_path` y exports (`jsonl`, `csv`).

`--force-discovery`, `--force-archive-discovered`, `--force-parse` fuerzan reejecución selectiva durante resume.

## Schema normalizado de parsing
Además de campos textuales, ahora se incluyen:
- `price_value` (float)
- `price_currency` (str, p.ej. `EUR`)
- `surface_sqm` (float)
- `rooms_count` (int)

Si no hay extracción fiable, se mantiene `null` y se conserva el texto original (`price_text`, `surface_text`, `rooms_text`).

## Exports finales
Ruta:
- `data/exports/{job_name}/{job_run_id}/properties.jsonl`
- `data/exports/{job_name}/{job_run_id}/properties.csv`

Columnas:
- `source_domain`
- `url_final`
- `title`
- `price_text`
- `price_value`
- `price_currency`
- `location_text`
- `surface_text`
- `surface_sqm`
- `rooms_text`
- `rooms_count`
- `description_text`
- `confidence_score`
- `snapshot_path`
- `parser_key`
- `parse_status`

## Otros outputs
- Discovery:
  - `data/discovered/job_runs/{job_name}/{run_id}/discovered_urls.jsonl`
  - `data/discovered/job_runs/{job_name}/{run_id}/summary.json`
  - `data/discovered/job_runs/{job_name}/{run_id}/archive_summary.json`
- Parse discovered:
  - `data/parsed/discovered/{job_name}/{run_id}/parsed_details.jsonl`
  - `data/parsed/discovered/{job_name}/{run_id}/summary.json`

## Tests
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

## Limitaciones actuales
- Parsers específicos aún heurísticos (no parser completo por portal).
- No hay crawling profundo ni anti-bot avanzado.
- Sin Playwright/OCR/login automático en esta fase.

## Job smoke de validación
Para validaciones rápidas y controladas (evitando ruido por seeds con 403/404), se añade:
- `config/jobs/bizkaia_naves_smoke.yaml`

Ejecución:
```powershell
python -m src.main run-job-full --job bizkaia_naves_smoke
```

Notas de calidad actuales:
- Discovery ahora excluye URLs de perfil/agencia/listado intermedio y canonicaliza parámetros de tracking/galería para deduplicar.
- El parser de detalle baja `parse_status`/`confidence_score` cuando faltan señales mínimas de ficha fiable.
- La normalización de precio evita falsos positivos tipo `"Precio" -> 1.0`.
