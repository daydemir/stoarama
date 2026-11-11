import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("probe", Path(__file__).with_name("verify_conditional_delete.py"))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class ProbeTests(unittest.TestCase):
    def run_probe(self, output, honors):
        operations = []
        def aws(args, **kwargs):
            operation = args[4]
            operations.append(operation)
            if operation == "put-object":
                return subprocess.CompletedProcess(args, 0, '{"ETag":"exact"}', "")
            if operation == "delete-object" and "00000000000000000000000000000000" in args[-1]:
                return subprocess.CompletedProcess(args, 1 if honors else 0, "", "(PreconditionFailed)" if honors else "")
            if operation == "get-object":
                Path(args[-1]).write_bytes(b"x" * 64)
            if operation == "head-object":
                return subprocess.CompletedProcess(args, 1, "", "(404)")
            return subprocess.CompletedProcess(args, 0, "{}", "")
        env = {"R2_ACCESS_KEY_ID": "test", "R2_SECRET_ACCESS_KEY": "test", "R2_ACCOUNT_ID": "test", "R2_BUCKET": "bucket"}
        with mock.patch.dict(os.environ, env), mock.patch.object(sys, "argv", ["probe", "--out", str(output)]), mock.patch.object(
                probe.subprocess, "run", side_effect=aws), mock.patch.object(probe.os, "urandom", side_effect=lambda n: b"x" * n), contextlib.redirect_stdout(io.StringIO()):
            probe.main()
        return operations

    def test_proof_requires_wrong_etag_rejection_and_preserved_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "proof.json"
            self.assertEqual(self.run_probe(output, True), ["put-object", "delete-object", "get-object", "delete-object", "head-object"])
            proof = json.loads(output.read_bytes())
            self.assertTrue(proof["wrong_etag_rejected"] and proof["object_preserved"] and proof["matching_delete_confirmed"])
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)

    def test_ignored_condition_never_produces_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "proof.json"
            with self.assertRaises(SystemExit):
                self.run_probe(output, False)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
