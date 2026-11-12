import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("retire_apply", Path(__file__).with_name("apply.py"))
apply = importlib.util.module_from_spec(spec)
spec.loader.exec_module(apply)


class RepairStopTests(unittest.TestCase):
    def test_disjoint_manifest_does_not_require_repair_shutdown(self):
        with patch.object(apply.subprocess, "run") as run:
            apply.check_repair_stopped(["managed/acct-47/a.mp4"])
            run.assert_not_called()

    def test_overlap_requires_known_domain_and_unloaded_service(self):
        domain = subprocess.CompletedProcess([], 0, b"", b"")
        absent = subprocess.CompletedProcess([], 113, b"", b"Could not find service com.stoarama.repair")
        for job, allowed in [(absent, True),
                             (subprocess.CompletedProcess([], 0, b"state = not running", b""), False),
                             (subprocess.CompletedProcess([], 1, b"", b"permission denied"), False)]:
            with patch.object(apply.subprocess, "run", side_effect=[domain, job]):
                if allowed:
                    apply.check_repair_stopped(["repaired/managed/a"])
                else:
                    with self.assertRaises(RuntimeError):
                        apply.check_repair_stopped(["repaired/managed/a"])
        with patch.object(apply.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, b"", b"")):
            with self.assertRaises(RuntimeError):
                apply.check_repair_stopped(["repaired/a"])


if __name__ == "__main__":
    unittest.main()
