from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _app_path() -> Path:
    return _repo_root() / "app" / "streamlit_app.py"


def _health_url(host: str, port: int) -> str:
    return f"http://{host}:{port}/_stcore/health"


def _app_url(host: str, port: int) -> str:
    return f"http://{host}:{port}"


def _health_ok(url: str, *, timeout: float = 2.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            payload = response.read().decode("utf-8", errors="ignore").strip().lower()
            return response.status == 200 and payload == "ok"
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def _wait_for_health(url: str, process: subprocess.Popen[bytes], *, timeout_seconds: int) -> tuple[bool, str]:
    deadline = time.time() + timeout_seconds
    last_error = "Tiempo de espera agotado"

    while time.time() < deadline:
        if process.poll() is not None:
            return False, f"Streamlit terminó antes de estar listo. Código de salida: {process.returncode}"

        if _health_ok(url):
            return True, ""

        try:
            with urllib.request.urlopen(url, timeout=2.0) as response:
                last_error = f"Health endpoint respondió {response.status}"
        except Exception as exc:  # pragma: no cover - solo diagnóstico interactivo
            last_error = str(exc)

        time.sleep(1)

    return False, last_error


def _python_command() -> list[str]:
    return [sys.executable or "python"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Launcher local de Inmoscraper para Windows.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8501)
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    app_path = _app_path()
    if not app_path.exists():
        print(f"[ERROR] No se encuentra la app de Inmoscraper en: {app_path}", flush=True)
        return 1

    if importlib.util.find_spec("streamlit") is None:
        print("[ERROR] Falta Streamlit en este entorno de Python. Instala dependencias con `python -m pip install -r requirements.txt`.", flush=True)
        return 1

    launch_url = _app_url(args.host, args.port)
    health_url = _health_url(args.host, args.port)

    if _health_ok(health_url):
        print(f"[OK] Inmoscraper ya está en marcha en {launch_url}", flush=True)
        if not args.no_browser:
            webbrowser.open(launch_url, new=1)
        return 0

    command = _python_command() + [
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.headless",
        "true",
        "--server.address",
        args.host,
        "--server.port",
        str(args.port),
    ]

    print("[INFO] Iniciando Inmoscraper...", flush=True)
    print(f"[INFO] URL esperada: {launch_url}", flush=True)
    process = subprocess.Popen(command, cwd=str(_repo_root()))

    ready, reason = _wait_for_health(health_url, process, timeout_seconds=args.timeout)
    if not ready:
        print(f"[ERROR] No se pudo iniciar Inmoscraper. {reason}", flush=True)
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        return 1

    print(f"[OK] Inmoscraper está listo en {launch_url}", flush=True)
    if args.no_browser:
        print("[INFO] Navegador automático desactivado.", flush=True)
    else:
        opened = webbrowser.open(launch_url, new=1)
        if opened:
            print("[INFO] Abriendo el navegador automáticamente...", flush=True)
        else:
            print(f"[INFO] No se pudo abrir el navegador automáticamente. Usa esta URL: {launch_url}", flush=True)

    print("[INFO] Cierra esta ventana para detener la demo.", flush=True)
    try:
        return process.wait()
    except KeyboardInterrupt:
        print("\n[INFO] Cerrando Inmoscraper...", flush=True)
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
