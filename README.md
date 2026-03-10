# Scraper Inmobiliario Bizkaia

Proyecto Python incremental con baseline legacy preservado y pipeline modular: archivado, jobs, discovery, parsing y exports.

## Estado actual
- Baseline legacy intacto (`agent_naves_bizkaia_v14.py`).
- Archiver con `markdown.new` + fallback local.
- Índices globales para snapshots, jobs, parse y pipeline runs.
- Configuración por fuentes y jobs (`config/sources`, `config/jobs`).
- Discovery + archive-discovered.
- parse-discovered + exports JSONL/CSV.
- Parser específico inicial de detalle para `pisos.com` (`parser_key=pisos_detail`) con fallback genérico.

## Comandos principales
```powershell
python -m src.main run-job --job bizkaia_naves
python -m src.main discover-job-run --job bizkaia_naves --run-id <run_id>
python -m src.main archive-discovered --job bizkaia_naves --run-id <run_id>
python -m src.main parse-discovered --job bizkaia_naves --run-id <run_id>
```

## Full Pipeline
Nuevo comando:

```powershell
python -m src.main run-job-full --job bizkaia_naves
```

Flags de control:

```powershell
python -m src.main run-job-full --job bizkaia_naves --resume
python -m src.main run-job-full --job bizkaia_naves --resume --force-discovery
python -m src.main run-job-full --job bizkaia_naves --resume --force-archive-discovered
python -m src.main run-job-full --job bizkaia_naves --resume --force-parse
```

`run-job-full` orquesta en orden:
1. `run-job`
2. `discover-job-run`
3. `archive-discovered`
4. `parse-discovered`
5. export final (JSONL + CSV)

## Manifest global de pipeline
Ruta:

`data/pipeline_runs/{job_name}/{pipeline_run_id}/manifest.json`

Campos principales:
- `pipeline_run_id`
- `job_name`
- `timestamp_utc_start`
- `timestamp_utc_end`
- `status` (`completed`, `partial`, `failed`)
- `job_run_id`
- `discovery_run_id`
- `archive_discovered_summary_path`
- `parse_discovered_summary_path`
- `export_paths` (`jsonl`, `csv`)
- `step_statuses`
- `errors_summary`

Índice global:

`data/index/pipeline_runs_index.jsonl`

## Estrategia resume / skip-existing
Con `--resume` el pipeline intenta continuar la última ejecución del job y no rehace pasos ya completos si detecta outputs coherentes:
- `run-job`: se salta si existe `job_manifest_path` válido.
- `discover-job-run`: se salta si existen `discovery_summary_path` y `discovered_output_path`.
- `archive-discovered`: se salta si existe `archive_discovered_summary_path` con `archived_snapshot_paths`.
- `parse-discovered`: se salta si existe `parse_discovered_summary_path` y exports (`jsonl`, `csv`).

Flags `--force-*` permiten reejecutar selectivamente etapas concretas durante resume.

## Outputs
Discovery:
- `data/discovered/job_runs/{job_name}/{run_id}/discovered_urls.jsonl`
- `data/discovered/job_runs/{job_name}/{run_id}/summary.json`
- `data/discovered/job_runs/{job_name}/{run_id}/archive_summary.json`

Parsed details:
- `data/parsed/discovered/{job_name}/{run_id}/parsed_details.jsonl`
- `data/parsed/discovered/{job_name}/{run_id}/summary.json`

Exports de negocio:
- `data/exports/{job_name}/{run_id}/properties.jsonl`
- `data/exports/{job_name}/{run_id}/properties.csv`

Pipeline runs:
- `data/pipeline_runs/{job_name}/{pipeline_run_id}/manifest.json`
- `data/index/pipeline_runs_index.jsonl`

## Parser específico de detalle
`pisos.com` usa `parser_key: pisos_detail` y mejora extracción de:
- `title`
- `price_text`
- `location_text`
- `surface_text`
- `rooms_text`
- `description_text`
- `page_kind` (prioriza `detail` cuando hay señal suficiente)
- `confidence_score`

Si no existe parser específico para una fuente, el registry usa parser genérico.

## Configuración
### Source YAML (campos)
- `domain`
- `enabled`
- `mode`
- `start_urls`
- `rate_limit_seconds`
- `timeout_seconds`
- `login_allowed`
- `archiver_enabled`
- `parser_key`
- `notes`

### Job YAML (campos)
- `job_name`
- `sources`
- `filters`
- `max_urls`
- `notes`

## Tests
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

## Limitaciones actuales
- Resume usa estrategia básica de coherencia por existencia de outputs, no validación semántica profunda.
- No hay crawling profundo ni anti-bot avanzado.
- Sin Playwright/OCR/login automático en esta fase.
