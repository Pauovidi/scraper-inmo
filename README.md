# Scraper Inmobiliario Bizkaia

Proyecto Python incremental con baseline legacy preservado y pipeline modular de archivado, jobs, discovery y parsing normalizado.

## Estado actual
- Baseline legacy intacto (`agent_naves_bizkaia_v14.py`).
- Archiver con estrategia explícita `markdown.new` + fallback local.
- Índice global de snapshots JSONL y deduplicación básica por hash.
- Configuración por fuente y job (`config/sources`, `config/jobs`).
- Runner batch de jobs con manifiesto por ejecución.
- Discovery de enlaces candidatos a fichas (`detail`) desde snapshots de `listing`.
- Parsing de snapshots y parseo específico de detalles descubiertos con export final útil.

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

- `snapshot_id` estable por URL.
- `run_id` único por ejecución.
- Histórico real sin sobrescritura.

## Runner de jobs
```powershell
python -m src.main run-job --job bizkaia_naves
```

Comportamiento:
- Resuelve sources del job.
- Excluye `enabled=false` o `archiver_enabled=false`.
- Deduplica `start_urls`.
- Aplica `max_urls`.
- Archiva cada URL.
- Respeta `rate_limit_seconds` y `timeout_seconds` por source.
- Continúa si una URL falla.

## Discovery de enlaces
### Comandos
```powershell
python -m src.main discover-job-run --job bizkaia_naves --run-id <run_id> --json
python -m src.main archive-discovered --job bizkaia_naves --run-id <run_id> --json
```

### Outputs de discovery
- `data/discovered/job_runs/{job_name}/{run_id}/discovered_urls.jsonl`
- `data/discovered/job_runs/{job_name}/{run_id}/summary.json`
- `data/discovered/job_runs/{job_name}/{run_id}/archive_summary.json`
- `data/index/discovery_runs_index.jsonl`

Formato base de `discovered_urls.jsonl` (1 línea = 1 URL):
- `job_name`
- `run_id`
- `source_domain`
- `parser_key`
- `parent_snapshot_id`
- `parent_run_id`
- `parent_snapshot_path`
- `page_kind`
- `discovered_url`
- `discovered_at`

## Parsing normalizado
### Schema base
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
- Usa `parser_key` desde `config/sources/*.yaml`.
- Parser específico de detalle inicial: `pisos_detail` (`pisos.com`).
- Si no hay parser específico para una source, fallback automático al parser genérico.

### Comandos de parsing
```powershell
python -m src.main parse-snapshot --path "<snapshot_path>" --json
python -m src.main parse-job-run --job bizkaia_naves --run-id <run_id> --json
python -m src.main parse-discovered --job bizkaia_naves --run-id <run_id> --json
```

## Parse de detalles descubiertos + exports
`parse-discovered` toma los snapshots de `archive-discovered` y persiste:

- `data/parsed/discovered/{job_name}/{run_id}/parsed_details.jsonl`
- `data/parsed/discovered/{job_name}/{run_id}/summary.json`

Además exporta subconjunto de negocio en:

- `data/exports/{job_name}/{run_id}/properties.jsonl`
- `data/exports/{job_name}/{run_id}/properties.csv`

Campos de negocio exportados:
- `source_domain`
- `url_final`
- `title`
- `price_text`
- `location_text`
- `surface_text`
- `rooms_text`
- `description_text`
- `confidence_score`
- `snapshot_path`
- `parser_key`
- `parse_status`

## Índices
- Snapshots: `data/index/snapshots_index.jsonl`
- Job runs: `data/index/job_runs_index.jsonl`
- Parse runs: `data/index/parse_runs_index.jsonl`
- Discovery runs: `data/index/discovery_runs_index.jsonl`

## CLI adicional
```powershell
python -m src.main archive --url "https://example.com"
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
- Parser específico implementado solo para detalle en `pisos.com`; resto sigue con parser genérico.
- Discovery y clasificación siguen siendo heurísticos (sin crawling profundo).
- Sin Playwright/OCR/login/anti-bot avanzado en esta fase.
- Algunos portales pueden devolver 403 en archivado HTTP directo.
