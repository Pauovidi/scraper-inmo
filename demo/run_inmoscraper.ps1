$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..")
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"

if (Test-Path $venvPython) {
    $pythonExe = $venvPython
} else {
    $pythonExe = "python"
}

Set-Location $repoRoot
& $pythonExe (Join-Path $scriptDir "launch_inmoscraper.py")
$exitCode = $LASTEXITCODE

if ($exitCode -ne 0) {
    Write-Host ""
    Write-Host "Inmoscraper no ha podido arrancar correctamente."
    Read-Host "Pulsa Enter para cerrar" | Out-Null
}

exit $exitCode
