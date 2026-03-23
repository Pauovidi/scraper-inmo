# Demo local de Inmoscraper

## Archivo que debe abrir el cliente

Haz doble clic en:

- `demo\run_inmoscraper.bat`

Ese es el lanzador principal para Windows.

## Qué hace

1. intenta usar `.\.venv\Scripts\python.exe` si existe
2. si no existe, usa el `python` disponible en el sistema
3. arranca `app/streamlit_app.py` con Streamlit
4. espera a que el health endpoint responda
5. abre el navegador automáticamente en `http://127.0.0.1:8501`

## Requisitos mínimos

- Windows
- Python disponible
- dependencias instaladas con:

```powershell
python -m pip install -r requirements.txt
```

## Entrega recomendada

Entrega la carpeta del proyecto incluyendo:

- `demo/`
- `app/`
- `src/`
- `config/`
- `data/`
- `requirements.txt`

## Si algo falla

- prueba primero `demo\run_inmoscraper.bat`
- si el navegador no se abre solo, usa `http://127.0.0.1:8501`
- si falta Streamlit o alguna dependencia, instala `requirements.txt`

## Regenerar la demo local

No hace falta build especial: basta con actualizar el repo y volver a usar `demo\run_inmoscraper.bat`.
