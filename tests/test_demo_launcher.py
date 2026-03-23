from __future__ import annotations

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).resolve().parents[1] / "demo" / "launch_inmoscraper.py"
SPEC = importlib.util.spec_from_file_location("demo_launch_inmoscraper", MODULE_PATH)
assert SPEC and SPEC.loader
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


class DemoLauncherTests(unittest.TestCase):
    def test_runtime_root_uses_env_override(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with mock.patch.dict(os.environ, {"INMOSCRAPER_RUNTIME_ROOT": temp_dir}, clear=False):
                self.assertEqual(launcher._runtime_root(), Path(temp_dir).resolve())

    def test_server_command_uses_current_executable_when_frozen(self) -> None:
        with mock.patch.object(launcher.sys, "frozen", True, create=True):
            with mock.patch.object(launcher.sys, "executable", "C:\\demo\\Inmoscraper.exe"):
                command = launcher._server_command("127.0.0.1", 8501)

        self.assertEqual(command[0], "C:\\demo\\Inmoscraper.exe")
        self.assertIn("--serve", command)

    def test_streamlit_flag_options_disable_development_mode(self) -> None:
        options = launcher._streamlit_flag_options("127.0.0.1", 8501)

        self.assertEqual(options["server.address"], "127.0.0.1")
        self.assertEqual(options["server.port"], 8501)
        self.assertFalse(options["global.developmentMode"])

    def test_config_root_comes_from_resource_root(self) -> None:
        with mock.patch.object(launcher, "_resource_root", return_value=Path("C:/bundle")):
            self.assertEqual(launcher._config_root(), Path("C:/bundle/config"))


if __name__ == "__main__":
    unittest.main()
