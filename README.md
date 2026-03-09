# Scraper Inmobiliario Bizkaia

Proyecto Python incremental con baseline legacy preservado y capas modulares para archivado, configuración y ejecución por jobs.

## Estado actual
- Baseline legacy intacto (`agent_naves_bizkaia_v14.py`).
- Archiver con `markdown.new` + fallback local.
- Índice global de snapshots JSONL.
- Configuración por fuente y por job (`config/sources`, `config/jobs`).
- Runner batch de jobs con manifiesto e índice de ejecuciones.

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

## Archivado y snapshots
Cada ejecución de `archive` guarda en:

`data/snapshots/{domain}/{yyyy-mm-dd}/{slug_or_hash}/{run_id}/`

- `snapshot_id`: estable por URL.
- `run_id`: único por ejecución.
- evita sobreescritura y conserva histórico real.

## Runner de jobs
Comando:
```powershell
python -m src.main run-job --job bizkaia_naves
```

Flujo:
1. Carga job YAML.
2. Resuelve fuentes asociadas.
3. Excluye fuentes `enabled=false` o `archiver_enabled=false`.
4. Deduplica `start_urls` entre fuentes.
5. Aplica `max_urls` del job.
6. Archiva cada URL con el archiver existente.
7. Respeta `rate_limit_seconds` de la fuente de cada URL.
8. Continúa aunque falle alguna URL.

### Manifest por ejecución
Ruta:
`data/job_runs/{job_name}/{run_id}/manifest.json`

Campos principales:
- `job_name`
- `run_id`
- `timestamp_utc_start`
- `timestamp_utc_end`
- `sources_resolved` (included/excluded)
- `start_urls`
- `duplicate_start_urls_skipped`
- `total_urls`
- `ok_count`
- `partial_count`
- `error_count`
- `snapshot_paths`
- `errors_summary`

### Índice de ejecuciones de jobs
Archivo:
`data/index/job_runs_index.jsonl`

Una línea por run con:
- `job_name`
- `run_id`
- `timestamp_utc_start`
- `timestamp_utc_end`
- `total_urls`
- `ok_count`
- `partial_count`
- `error_count`
- `manifest_path`

## CLI
### Archivo único
```powershell
python -m src.main archive --url "https://example.com"
```

### Snapshots
```powershell
python -m src.main list-snapshots
python -m src.main list-snapshots --domain local-file --status ok --json
```

### Config
```powershell
python -m src.main list-sources
python -m src.main show-source --domain pisos.com
python -m src.main list-jobs
python -m src.main show-job --job bizkaia_naves
```

### Jobs batch
```powershell
python -m src.main run-job --job bizkaia_naves
python -m src.main list-job-runs
python -m src.main list-job-runs --job bizkaia_naves
python -m src.main show-job-run --job bizkaia_naves --run-id <run_id>
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
- Sin parsers específicos por portal todavía.
- Dedupe de snapshots básica por URL+día+hash.
- `markdown.new` depende de disponibilidad de red.
