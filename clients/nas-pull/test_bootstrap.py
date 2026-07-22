import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


BOOTSTRAP_PATH = Path(__file__).with_name("bootstrap.py")


def load_bootstrap():
    spec = importlib.util.spec_from_file_location("stoarama_nas_bootstrap", BOOTSTRAP_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BootstrapRecoveryTests(unittest.TestCase):
    def test_restores_previous_client_after_unclean_promoted_run(self):
        bootstrap = load_bootstrap()
        with tempfile.TemporaryDirectory() as state:
            root = Path(state)
            bootstrap.CURRENT = str(root / "stoarama_pull.py")
            bootstrap.PREVIOUS = str(root / "stoarama_pull.previous.py")
            bootstrap.RUNTIME = str(root / "runtime.json")
            bootstrap.CANDIDATE = str(root / "stoarama_pull.candidate.py")
            Path(bootstrap.CURRENT).write_text("raise RuntimeError('bad release')\n", encoding="utf-8")
            Path(bootstrap.PREVIOUS).write_text("CLIENT_VERSION = 'known-good'\n", encoding="utf-8")
            Path(bootstrap.RUNTIME).write_text(json.dumps({"exit": "running"}), encoding="utf-8")
            Path(bootstrap.CANDIDATE).write_text("candidate\n", encoding="utf-8")

            self.assertTrue(bootstrap.recover_previous())
            self.assertEqual(Path(bootstrap.CURRENT).read_text(encoding="utf-8"), "CLIENT_VERSION = 'known-good'\n")
            self.assertFalse(Path(bootstrap.CANDIDATE).exists())

    def test_clean_exit_keeps_current_client(self):
        bootstrap = load_bootstrap()
        with tempfile.TemporaryDirectory() as state:
            root = Path(state)
            bootstrap.CURRENT = str(root / "stoarama_pull.py")
            bootstrap.PREVIOUS = str(root / "stoarama_pull.previous.py")
            bootstrap.RUNTIME = str(root / "runtime.json")
            bootstrap.CANDIDATE = str(root / "stoarama_pull.candidate.py")
            Path(bootstrap.CURRENT).write_text("CLIENT_VERSION = 'current'\n", encoding="utf-8")
            Path(bootstrap.PREVIOUS).write_text("CLIENT_VERSION = 'previous'\n", encoding="utf-8")
            Path(bootstrap.RUNTIME).write_text(json.dumps({"exit": "clean"}), encoding="utf-8")

            self.assertFalse(bootstrap.recover_previous())
            self.assertEqual(Path(bootstrap.CURRENT).read_text(encoding="utf-8"), "CLIENT_VERSION = 'current'\n")


if __name__ == "__main__":
    unittest.main()
