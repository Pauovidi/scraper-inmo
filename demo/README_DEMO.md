# Demo local de Inmoscraper

## Lanzadores disponibles

- `demo\run_inmoscraper.bat`: fallback directo con Python del equipo
- `demo\run_inmoscraper.ps1`: alternativa PowerShell
- `dist_demo\Inmoscraper\Inmoscraper.exe`: opción entregable empaquetada para Windows

## Qué debe abrir el cliente

Para la entrega empaquetada:

- `dist_demo\Inmoscraper\Inmoscraper.exe`

Si hiciera falta un plan B dentro del repo:

- `demo\run_inmoscraper.bat`

## Qué hace

1. arranca Inmoscraper en `http://127.0.0.1:8501`
2. espera a que el servidor responda
3. abre el navegador automáticamente
4. mantiene la ventana del lanzador como proceso de control

## Requisitos mínimos

### Fallback con Python

- Windows
- Python disponible
- dependencias instaladas con:

```powershell
python -m pip install -r requirements.txt
```

### Paquete `.exe`

- Windows
- no necesita abrir Python manualmente
- conviene ejecutar la demo desde una carpeta de usuario con permisos de escritura

## Regenerar el paquete demo

```powershell
powershell -ExecutionPolicy Bypass -File scripts\build_demo_exe.ps1
```

Alternativa:

```bat
scripts\build_demo_exe.bat
```

## Resultado esperado

La build deja una carpeta lista para entregar:

- `dist_demo\Inmoscraper\Inmoscraper.exe`
- `dist_demo\Inmoscraper\README_DEMO.txt`
- `dist_demo\Inmoscraper\run_inmoscraper.bat`
- `dist_demo\Inmoscraper\run_inmoscraper.ps1`
- `dist_demo\Inmoscraper\data\history\...`
- `dist_demo\Inmoscraper\data\published\...`

## Limitaciones conocidas

- La demo empaquetada incluye el histórico/publicación actual del repo en el momento de la build.
- El `.exe` abre una app local en el navegador; no es una app web desplegada en la nube.
- Si SmartScreen muestra advertencias, hay que permitir la ejecución manualmente.
