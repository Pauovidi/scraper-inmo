# Scraper Inmobiliario Bizkaia

Proyecto Python incremental con baseline legacy preservado y pipeline modular de archivado, jobs y parsing normalizado.

## Estado actual
- Baseline legacy intacto (`agent_naves_bizkaia_v14.py`).
- Archiver con `markdown.new` + fallback local.
- Índice global de snapshots JSONL.
- Configuración por fuente y job (`config/sources`, `config/jobs`).
- Runner batch de jobs con manifiesto por ejecución.
- Capa de parsing normalizado sobre snapshots archivados.

## Configuración
```text
config/
|-- sources/
|   |-- idealista.yaml
|   |-- fotocasa.yaml
|   |-- pisos.yaml
|   `-- yaencontre.yaml
`-- jobs/
    `-- bizkaia_naves.yaml
```

### Source YAML (campos)
- `domain`
- `enabled`
- `mode`
- `start_urls`
- `rate_limit_seconds`
- `timeout_seconds` (opcional, default 20)
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

## Archivado
Ruta de snapshot:

`data/snapshots/{domain}/{yyyy-mm-dd}/{slug_or_hash}/{run_id}/`

- `snapshot_id` estable por URL
- `run_id` único por ejecución
- histórico real sin sobrescritura

## Runner de jobs
```powershell
python -m src.main run-job --job bizkaia_naves
```

Comportamiento:
- resuelve sources del job
- excluye `enabled=false` o `archiver_enabled=false`
- deduplica `start_urls`
- aplica `max_urls`
- archiva cada URL
- respeta `rate_limit_seconds` y `timeout_seconds` por source
- continúa en errores

### Manifest de job run
`data/job_runs/{job_name}/{run_id}/manifest.json`

Campos principales:
- `job_name`, `run_id`
- `timestamp_utc_start`, `timestamp_utc_end`
- `sources_resolved`
- `start_urls`, `duplicate_start_urls_skipped`
- `total_urls`, `ok_count`, `partial_count`, `error_count`
- `snapshot_paths`, `errors_summary`, `url_results`

### Índice de job runs
`data/index/job_runs_index.jsonl` (1 línea por ejecución)

## Parsing normalizado
### Schema base de salida
Cada registro parseado incluye:
- `parser_key`
- `source_domain`
- `snapshot_id`
- `run_id`
- `snapshot_path`
- `url_original`
- `url_final`
- `page_kind` (`listing`, `detail`, `unknown`)
- `title`
- `price_text`
- `location_text`
- `surface_text`
- `rooms_text`
- `description_text`
- `extracted_links`
- `extracted_at`
- `parse_status` (`ok`, `partial`, `error`)
- `parse_errors`
- `confidence_score`

### Registry de parsers
- toma `parser_key` desde `config/sources/*.yaml`
- si no hay parser específico disponible, fallback automático al parser genérico

### Comandos de parsing
```powershell
python -m src.main parse-snapshot --path "<snapshot_path>" --json
python -m src.main parse-job-run --job bizkaia_naves --run-id <run_id> --json
```

### Persistencia de parse outputs
`parse-job-run` guarda en:
- `data/parsed/job_runs/{job_name}/{run_id}/parsed.jsonl`
- `data/parsed/job_runs/{job_name}/{run_id}/summary.json`

Índice de parse runs:
- `data/index/parse_runs_index.jsonl`

## CLI adicional
```powershell
python -m src.main list-snapshots
python -m src.main list-sources
python -m src.main show-source --domain pisos.com
python -m src.main list-jobs
python -m src.main show-job --job bizkaia_naves
python -m src.main list-job-runs --job bizkaia_naves
python -m src.main show-job-run --job bizkaia_naves --run-id <run_id>
```

## Tests
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

## Limitaciones actuales
- Parser genérico heurístico, sin reglas específicas por portal.
- Extracción de `price/location/surface/rooms` basada en patrones simples.
- `markdown.new` y scraping HTTP dependen de red disponible del entorno.
