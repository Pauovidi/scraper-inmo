# Scraper Inmobiliario Bizkaia

Proyecto Python incremental con baseline legacy preservado y una v2 orientada a cliente final:

- histórico maestro de anuncios
- publicación diaria solo de anuncios nuevos
- separación por portal
- estados de trabajo persistentes
- visor local en Streamlit, en español

El scraper técnico sigue funcionando por CLI. La v2 reutiliza sus outputs para presentar resultados de forma más útil y mantenible.

## Qué hace la v2

La v2 parte de los exports generados por el pipeline técnico y construye una capa de publicación simple:

1. conserva un histórico maestro de anuncios
2. detecta qué anuncios son realmente nuevos en el día
3. genera salidas diarias por portal
4. permite marcar cada anuncio como:
   - `pending`
   - `processed`
   - `discarded`
5. mantiene ese estado aunque el anuncio reaparezca en días posteriores

## Baseline y estado actual

- Baseline legacy intacto: `agent_naves_bizkaia_v14.py`
- Archiver funcional con snapshots y metadatos
- Pipeline técnico completo:
  - `run-job`
  - `discover-job-run`
  - `archive-discovered`
  - `parse-discovered`
  - `run-job-full`
- Parsers específicos de detalle:
  - `pisos_detail`
  - `fotocasa_detail`
- Fallback genérico siempre disponible
- Publicación diaria v2:
  - histórico maestro
  - outputs diarios por portal
  - panel local para cliente

## Instalación

```powershell
python -m pip install -r requirements.txt
```

## CLI principal

Pipeline técnico:

```powershell
python -m src.main run-job-full --job bizkaia_naves
python -m src.main run-job-full --job bizkaia_naves_smoke --resume
```

Publicación diaria v2:

```powershell
python -m src.main publish-daily --job bizkaia_naves_smoke
```

Actualizar estado de un anuncio:

```powershell
python -m src.main set-listing-status --listing-key "fotocasa.es:id:188901695" --status processed
python -m src.main set-listing-status --listing-key "fotocasa.es:id:188901695" --status discarded --note "No encaja"
```

## Cómo funciona `publish-daily`

`publish-daily`:

1. reutiliza el último `pipeline_run` del día si ya existe
2. si no existe uno válido para hoy, ejecuta `run-job-full`
3. lee el export final (`properties.csv` o `properties.jsonl`)
4. deduplica anuncios con `listing_key`
5. actualiza el histórico maestro
6. genera los CSV diarios solo con anuncios nuevos de hoy
7. escribe un `summary.json` con métricas y rutas

La deduplicación histórica usa esta prioridad:

1. `external_id` del portal si se puede resolver
2. `canonical_url` / `url_final`
3. hash estable con portal + título + precio + ubicación + superficie

## Histórico y estados

Rutas principales:

- `data/history/listings_master.jsonl`
- `data/history/listing_status.jsonl`

Cada anuncio histórico conserva como mínimo:

- `source_domain`
- `listing_key`
- `url_final`
- `title`
- `price_text`
- `price_value`
- `location_text`
- `surface_sqm`
- `rooms_count`
- `first_seen_date`
- `last_seen_date`
- `seen_count`
- `workflow_status`
- `workflow_updated_at`
- `workflow_note`
- `parser_key`
- `parse_status`

Reglas de negocio:

- un anuncio nuevo entra como `pending`
- si luego se marca como `processed` o `discarded`, ese estado se conserva
- si reaparece otro día, se actualizan `last_seen_date` y `seen_count`
- no vuelve a salir como “nuevo” si ya existía antes
- el estado vivo del workflow se considera autoritativo en `data/history/`

## Outputs de publicación diaria

Rutas:

- `data/published/YYYY-MM-DD/fotocasa.csv`
- `data/published/YYYY-MM-DD/idealista.csv`
- `data/published/YYYY-MM-DD/milanuncios.csv`
- `data/published/YYYY-MM-DD/pisos.csv`
- `data/published/YYYY-MM-DD/yaencontre.csv`
- `data/published/YYYY-MM-DD/all.csv`
- `data/published/YYYY-MM-DD/summary.json`

Cada CSV diario contiene solo anuncios nuevos detectados ese día.

`all.csv` reúne todos los nuevos del día.

La app de Streamlit cruza esos CSV diarios con el histórico para mostrar el estado actual (`pending`, `processed`, `discarded`) aunque el CSV publicado se hubiera generado antes del último cambio de estado.

## Visor local en Streamlit

Arranque:

```powershell
streamlit run app/streamlit_app.py
```

La vista principal está en español y se centra en negocio:

- `Actualización`
- `Nuevos hoy`
- `Histórico total`
- `Pendientes`
- secciones por portal:
  - `Fotocasa`
  - `Idealista`
  - `Milanuncios`
  - `Pisos`
  - `Yaencontre`
- vista `Histórico`
- pestaña `Técnico` con rutas y JSONs de apoyo

En cada portal se muestran como mínimo:

- `Estado`
- `Título`
- `Precio`
- `Ubicación`
- `Superficie`
- `Habitaciones`
- `Enlace`
- `Fecha primera detección`
- `Fecha última detección`

El estado se puede cambiar directamente desde la app y queda persistido en disco.

## Estructura de datos relevante

```text
data/
  history/
    listings_master.jsonl
    listing_status.jsonl
  published/
    YYYY-MM-DD/
      fotocasa.csv
      idealista.csv
      milanuncios.csv
      pisos.csv
      yaencontre.csv
      all.csv
      summary.json
  pipeline_runs/
  exports/
  parsed/
  discovered/
  snapshots/
```

## Tests

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

## Limitaciones actuales

- La cobertura real sigue dependiendo de las fuentes que respondan bien.
- La representación por portal en la interfaz ya existe para 5 portales, aunque algunos puedan no aportar datos en una ejecución concreta.
- No hay panel multiusuario, autenticación ni CRM.
- No hay crawling profundo ni anti-bot avanzado en esta fase.
- La calidad final sigue siendo mejor en algunos portales que en otros.
