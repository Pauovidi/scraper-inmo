@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "REPO_ROOT=%SCRIPT_DIR%.."
set "PYTHON_EXE=%REPO_ROOT%\.venv\Scripts\python.exe"

if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"

cd /d "%REPO_ROOT%"
"%PYTHON_EXE%" "%SCRIPT_DIR%launch_inmoscraper.py"
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo.
  echo Inmoscraper no ha podido arrancar correctamente.
  pause
)

exit /b %EXIT_CODE%
