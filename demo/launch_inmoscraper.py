from __future__ import annotations

import argparse
import importlib.util
import os
import subprocess
import sys
import time
import threading
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path


def _resource_root() -> Path:
    if getattr(sys, "frozen", False):
        frozen_root = getattr(sys, "_MEIPASS", None)
        if frozen_root:
            return Path(str(frozen_root)).resolve()
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def _runtime_root() -> Path:
    configured = os.environ.get("INMOSCRAPER_RUNTIME_ROOT")
    if configured:
        return Path(configured).resolve()
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return _resource_root()


def _config_root() -> Path:
    return (_resource_root() / "config").resolve()


def _app_path() -> Path:
    return _resource_root() / "app" / "streamlit_app.py"


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


def _wait_for_health_url(url: str, *, timeout_seconds: int) -> tuple[bool, str]:
    deadline = time.time() + timeout_seconds
    last_error = "Tiempo de espera agotado"

    while time.time() < deadline:
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


def _streamlit_flag_options(host: str, port: int) -> dict[str, object]:
    return {
        "server.headless": True,
        "server.address": host,
        "server.port": port,
        "browser.gatherUsageStats": False,
        "global.developmentMode": False,
    }


def _server_command(host: str, port: int) -> list[str]:
    if getattr(sys, "frozen", False):
        return [sys.executable, "--serve", "--host", host, "--port", str(port)]
    return _python_command() + [
        "-m",
        "streamlit",
        "run",
        str(_app_path()),
        "--server.headless",
        "true",
        "--server.address",
        host,
        "--server.port",
        str(port),
    ]


def _serve_embedded_app(host: str, port: int) -> int:
    from streamlit.web.bootstrap import load_config_options, run

    app_path = _app_path()
    if not app_path.exists():
        print(f"[ERROR] No se encuentra la app de Inmoscraper en: {app_path}", flush=True)
        return 1

    os.environ.setdefault("INMOSCRAPER_RUNTIME_ROOT", str(_runtime_root()))
    os.environ.setdefault("INMOSCRAPER_CONFIG_ROOT", str(_config_root()))
    flag_options = _streamlit_flag_options(host, port)
    load_config_options(flag_options)
    run(str(app_path), False, [], flag_options)
    return 0


def _monitor_embedded_startup(*, health_url: str, launch_url: str, timeout_seconds: int, no_browser: bool) -> None:
    ready, reason = _wait_for_health_url(health_url, timeout_seconds=timeout_seconds)
    if not ready:
        print(f"[ERROR] No se pudo iniciar Inmoscraper. {reason}", flush=True)
        return

    print(f"[OK] Inmoscraper está listo en {launch_url}", flush=True)
    if no_browser:
        print("[INFO] Navegador automático desactivado.", flush=True)
        return

    opened = webbrowser.open(launch_url, new=1)
    if opened:
        print("[INFO] Abriendo el navegador automáticamente...", flush=True)
    else:
        print(f"[INFO] No se pudo abrir el navegador automáticamente. Usa esta URL: {launch_url}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Launcher local de Inmoscraper para Windows.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8501)
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument("--serve", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.serve:
        return _serve_embedded_app(args.host, args.port)

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

    if getattr(sys, "frozen", False):
        print("[INFO] Iniciando Inmoscraper...", flush=True)
        print(f"[INFO] URL esperada: {launch_url}", flush=True)
        thread = threading.Thread(
            target=_monitor_embedded_startup,
            kwargs={
                "health_url": health_url,
                "launch_url": launch_url,
                "timeout_seconds": args.timeout,
                "no_browser": args.no_browser,
            },
            daemon=True,
        )
        thread.start()
        print("[INFO] Cierra esta ventana para detener la demo.", flush=True)
        return _serve_embedded_app(args.host, args.port)

    command = _server_command(args.host, args.port)
    env = os.environ.copy()
    env["INMOSCRAPER_RUNTIME_ROOT"] = str(_runtime_root())
    env["INMOSCRAPER_CONFIG_ROOT"] = str(_config_root())

    print("[INFO] Iniciando Inmoscraper...", flush=True)
    print(f"[INFO] URL esperada: {launch_url}", flush=True)
    process = subprocess.Popen(command, cwd=str(_runtime_root()), env=env)

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
