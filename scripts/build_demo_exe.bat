@echo off
setlocal

powershell -ExecutionPolicy Bypass -File "%~dp0build_demo_exe.ps1"
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo.
  echo La build de Inmoscraper no ha terminado correctamente.
  pause
)

exit /b %EXIT_CODE%
