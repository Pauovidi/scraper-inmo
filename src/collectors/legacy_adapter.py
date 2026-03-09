"""Legacy collector adapter."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def run_legacy(*legacy_args: str) -> int:
    repo_root = Path(__file__).resolve().parents[2]
    script = repo_root / "agent_naves_bizkaia_v14.py"
    if not script.exists():
        raise FileNotFoundError(f"Legacy script not found: {script}")

    cmd = [sys.executable, str(script), *legacy_args]
    return subprocess.run(cmd, cwd=repo_root).returncode
