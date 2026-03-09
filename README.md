# Scraper Inmobiliario Bizkaia

Proyecto Python incremental con baseline legacy preservado y capas nuevas para archivado, índice global y configuración por fuentes/jobs.

## Estado actual
- Baseline legacy intacto (`agent_naves_bizkaia_v14.py`).
- Archiver v1.1 con `markdown.new` + fallback local.
- Índice global JSONL con hashes y deduplicación básica.
- SnapshotBridge para cargar snapshot desde disco.
- Capa de configuración YAML para fuentes y jobs.

## Estructura de configuración
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

## Formato source YAML
Campos obligatorios:
- `domain`
- `enabled`
- `mode`
- `start_urls`
- `rate_limit_seconds`
- `login_allowed`
- `archiver_enabled`
- `parser_key`
- `notes`

## Formato job YAML
Campos obligatorios:
- `job_name`
- `sources`
- `filters`
- `max_urls`
- `notes`

## Resolución de seeds por job
- El loader cruza `job.sources` con `config/sources/*.yaml`.
- Solo usa fuentes `enabled=true` y `archiver_enabled=true`.
- Devuelve `start_urls` deduplicadas.

## Snapshot path con histórico real (hotfix)
Antes se sobrescribía la misma ruta para URL+día.
Ahora cada ejecución genera `run_id` único y la ruta queda:

`data/snapshots/{domain}/{yyyy-mm-dd}/{slug_or_hash}/{run_id}/`

- `snapshot_id` sigue siendo estable por URL.
- `run_id` identifica cada ejecución concreta.
- `meta.json` e índice JSONL incluyen ambos (`snapshot_id`, `run_id`).

## Índice global
Archivo:
- `data/index/snapshots_index.jsonl`

Cada entrada guarda, entre otros:
- `snapshot_id`, `run_id`
- `url_original`, `url_final`, `domain`
- `timestamp_utc`, `date`, `status`
- `markdown_source`, `html_source`
- `snapshot_path`, `elapsed_ms`
- `html_hash`, `markdown_hash`, `content_hash_preferred`
- `is_duplicate_content`, `match_reason`

## CLI
### Archivar
```powershell
python -m src.main archive --url "https://example.com"
```

### Listar snapshots
```powershell
python -m src.main list-snapshots
python -m src.main list-snapshots --domain local-file --status ok
python -m src.main list-snapshots --date 2026-03-09 --json
```

### Configuración
```powershell
python -m src.main list-sources
python -m src.main show-source --domain pisos.com
python -m src.main list-jobs
python -m src.main show-job --job bizkaia_naves
```

### Legacy
```powershell
python -m src.main legacy -- --help
```

## Tests
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

## Limitaciones actuales
- Sin parsers por portal todavía (solo bridge genérico).
- Dedupe básica por URL/día/hash, no semántica avanzada.
- `markdown.new` depende de red externa; si falla, fallback local mantiene robustez.
