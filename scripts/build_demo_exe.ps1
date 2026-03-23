param(
    [string]$DistRootName = "dist_demo"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$distRoot = Join-Path $repoRoot $DistRootName
$buildRoot = Join-Path $repoRoot "build_demo"
$packageRoot = Join-Path $distRoot "Inmoscraper"
$specPath = Join-Path $PSScriptRoot "inmoscraper_demo.spec"

Set-Location $repoRoot

Write-Host "[INFO] Preparando build de Inmoscraper..."

$pyInstaller = python -c "import importlib.util; print('yes' if importlib.util.find_spec('PyInstaller') else 'no')"

if ($pyInstaller.Trim() -ne "yes") {
    Write-Host "[INFO] Instalando PyInstaller..."
    python -m pip install pyinstaller
}

if (Test-Path $packageRoot) {
    try {
        Remove-Item $packageRoot -Recurse -Force
    }
    catch {
        if ($DistRootName -eq "dist_demo") {
            $distRoot = Join-Path $repoRoot "dist_demo_fixed"
            $packageRoot = Join-Path $distRoot "Inmoscraper"
            Write-Host "[WARN] dist_demo\\Inmoscraper estaba bloqueado. Se usará $packageRoot"
            if (Test-Path $packageRoot) {
                Remove-Item $packageRoot -Recurse -Force
            }
        }
        else {
            throw
        }
    }
}

python -m PyInstaller --noconfirm --clean --distpath $distRoot --workpath $buildRoot $specPath

if (-not (Test-Path $packageRoot)) {
    throw "No se ha generado la carpeta esperada: $packageRoot"
}

$dataRoot = Join-Path $packageRoot "data"
New-Item -ItemType Directory -Force -Path $dataRoot | Out-Null

Copy-Item (Join-Path $repoRoot "data\history") (Join-Path $packageRoot "data\history") -Recurse -Force
Copy-Item (Join-Path $repoRoot "data\published") (Join-Path $packageRoot "data\published") -Recurse -Force
Copy-Item (Join-Path $repoRoot "demo\README_DEMO.txt") (Join-Path $packageRoot "README_DEMO.txt") -Force

$batWrapper = @"
@echo off
setlocal
cd /d "%~dp0"
"%~dp0Inmoscraper.exe"
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
  echo.
  echo Inmoscraper no ha podido arrancar correctamente.
  pause
)
exit /b %EXIT_CODE%
"@
Set-Content -Path (Join-Path $packageRoot "run_inmoscraper.bat") -Value $batWrapper -Encoding ASCII

$psWrapper = @'
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
& (Join-Path $PSScriptRoot "Inmoscraper.exe")
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Inmoscraper no ha podido arrancar correctamente."
    Read-Host "Pulsa Enter para cerrar"
}
exit $LASTEXITCODE
'@
Set-Content -Path (Join-Path $packageRoot "run_inmoscraper.ps1") -Value $psWrapper -Encoding ASCII

Write-Host "[OK] Demo empaquetada en $packageRoot"
