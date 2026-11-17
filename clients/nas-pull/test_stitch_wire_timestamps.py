"""Actual Go wire bytes, synthetic metadata; no app main, API, NAS or authority."""
import ast
import base64
import binascii
import datetime
import hashlib
import json
import re
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parent
SOURCE = (ROOT / "stoarama_pull.py").read_bytes()
VERIFIER = (ROOT / "verify_stitch_readback.py").read_bytes()
FIXTURE = json.loads((ROOT.parent.parent / "backend/internal/api/testdata/nas_stitch_wire_utc6.json").read_bytes())

# Load only the actual parser, not Config, startup, workers or credentials.
nodes = [node for node in ast.parse(SOURCE).body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
         and node.name in {"parse_certification_timestamp", "MediaCertificationError"}]
assert len(nodes) == 2 and all(not node.decorator_list for node in nodes)
PARSER = {"datetime": datetime, "re": re}
exec(compile(ast.Module(body=nodes, type_ignores=[]), "<exact client UTC parser>", "exec"), PARSER)

VERIFIER_NAMESPACE = {"__name__": "offline_wire_fixture_validator"}
exec(compile(VERIFIER, "<readback verifier>", "exec"), VERIFIER_NAMESPACE)
CORE = VERIFIER_NAMESPACE["load_core"](SOURCE, hashlib.sha256(SOURCE).hexdigest())
task_node = [node for node in ast.parse(SOURCE).body if isinstance(node, ast.FunctionDef) and node.name == "stitch_evidence_task"]
assert len(task_node) == 1 and not task_node[0].decorator_list
TASK = {"base64": base64, "binascii": binascii, "hashlib": hashlib,
        "stitch_proof_json": CORE.stitch_proof_json, "cleanup_json_bytes": CORE.cleanup_json_bytes,
        "StitchReadbackIntegrity": CORE.StitchReadbackIntegrity}
exec(compile(ast.Module(body=task_node, type_ignores=[]), "<exact task-byte validator>", "exec"), TASK)


class WireTimestampTests(unittest.TestCase):
    def test_fixture_is_closed_synthetic_and_covers_db_fraction_shapes(self):
        self.assertEqual(set(FIXTURE), {"schema", "synthetic", "cases"})
        self.assertEqual(FIXTURE["schema"], "stoarama.synthetic-stitch-wire-fixtures.v1")
        self.assertIs(FIXTURE["synthetic"], True)
        self.assertEqual(len(FIXTURE["cases"]), 9)
        self.assertEqual({len(row["legacy_nano_expiry"].split(".")[1][:-1])
                          if "." in row["legacy_nano_expiry"] else 0 for row in FIXTURE["cases"]},
                         {0, 1, 2, 3, 4, 5, 6})

    def test_actual_native_task_raw_parsed_outer_and_sha_match_without_rounding(self):
        for row in FIXTURE["cases"]:
            with self.subTest(case=row["name"]):
                body = CORE.stitch_proof_json(base64.b64decode(row["native_body_b64"], validate=True))
                raw = base64.b64decode(body["task_b64"], validate=True)
                self.assertEqual(TASK["stitch_evidence_task"](body), raw)
                self.assertEqual(hashlib.sha256(raw).hexdigest(), body["task_sha256"])
                self.assertEqual(CORE.stitch_proof_json(raw), body["task"])
                self.assertEqual(body["task"]["expires_at"], body["expires_at"])
                self.assertEqual(body["expires_at"], row["native_expiry"])
                self.assertRegex(body["expires_at"], r"\.\d{6}Z$")
                actual = PARSER["parse_certification_timestamp"](body["expires_at"], "expiry")
                expected = datetime.datetime.strptime(row["native_expiry"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc)
                self.assertEqual(actual, expected)
                old = CORE.stitch_proof_json(base64.b64decode(row["legacy_task_b64"], validate=True))
                self.assertEqual(old["expires_at"], row["legacy_nano_expiry"])
                old["expires_at"] = body["expires_at"]
                self.assertEqual(old, body["task"])

    def test_actual_readback_expiry_has_no_native_fallthrough(self):
        for row in FIXTURE["cases"]:
            with self.subTest(case=row["name"]):
                body = CORE.stitch_proof_json(base64.b64decode(row["readback_body_b64"], validate=True))
                self.assertEqual(set(body), {"schema", "mode", "profiles", "expires_at"})
                self.assertEqual(body["mode"], 2)
                self.assertEqual(body["expires_at"], row["readback_expiry"])
                PARSER["parse_certification_timestamp"](body["expires_at"], "expiry")

    def test_all_actual_new_server_timestamp_fields_pass_exact_readback_validator(self):
        for row in FIXTURE["cases"]:
            for name, definition in (("validation", CORE.StitchReadbackDefinition.VALIDATION),
                                     ("current", CORE.StitchReadbackDefinition.CURRENT)):
                with self.subTest(case=row["name"], response=name):
                    body = CORE.stitch_proof_json(base64.b64decode(row[name + "_body_b64"], validate=True))
                    CORE.stitch_readback_validate(body, definition)
                    fields = ["reader_heartbeat_received_at"]
                    fields += ["validated_at"] if name == "validation" else ["checked_at", "status_received_at", "source_scope_approved_until"]
                    for field in fields:
                        self.assertRegex(body[field], r"\.\d{6}Z$")
                    if name == "current":
                        self.assertEqual(body["readback_verified_at"], row["copied_verified_at"])
                        self.assertTrue(body["readback_verified_at"].endswith(".123Z"))

    def test_rfc3339_fraction_widths_offsets_and_expiry_never_round_up(self):
        for width in range(1, 10):
            digits = "123456789"[:width]
            expected_microseconds = int(digits[:6].ljust(6, "0"))
            for zone, offset in (("Z", 0), ("+05:30", 330), ("-07:45", -465)):
                with self.subTest(width=width, zone=zone):
                    raw = "2026-11-17T10:32:32." + digits + zone
                    actual = PARSER["parse_certification_timestamp"](raw, "cleanup expiry")
                    expected = datetime.datetime(2026, 11, 17, 10, 32, 32, expected_microseconds,
                        tzinfo=datetime.timezone(datetime.timedelta(minutes=offset))).astimezone(datetime.timezone.utc)
                    self.assertEqual(actual, expected)
                    self.assertEqual(actual.microsecond, expected_microseconds)
        self.assertEqual(PARSER["parse_certification_timestamp"]("2026-11-17T10:32:32.33494+00:00", "expiry").microsecond, 334940)
        for zone in ("Z", "+00:00", "-00:00"):
            self.assertEqual(PARSER["parse_certification_timestamp"]("2026-11-17T10:32:32.999999999"+zone, "expiry").microsecond, 999999)
            self.assertEqual(PARSER["parse_certification_timestamp"]("2026-11-17T10:32:32"+zone, "expiry").microsecond, 0)

    def test_rfc3339_malformed_timestamps_still_fail(self):
        for raw in ("", "2026-11-17", "2026-11-17T10:32:32", "2026-11-17T10:32:32.Z", "2026-11-17T10:32:32.-1Z",
                    "2026-11-17T10:32:32.1234567890Z", "2026-11-17T10:32:32.1.2Z", "2026-11-17T10:32:32.1+24:00",
                    "2026-11-17T10:32:32.1+00:60", "2026-11-17T10:32:32.1+0530", "2026-11-17T10:32:32.1Zextra",
                    "2026-02-30T10:32:32.1Z", "2026-11-17T25:32:32.1Z", "2026-11-17T10:32:32.١Z"):
            with self.subTest(raw=raw), self.assertRaises(PARSER["MediaCertificationError"]):
                PARSER["parse_certification_timestamp"](raw, "expiry")

    @unittest.skipUnless(sys.version_info[:2] < (3, 11), "strict fractional-width regression targets supported Python 3.9/3.10")
    def test_prior_actual_go_encodings_reproduce_the_supported_runtime_failure(self):
        rejected = set()
        for row in FIXTURE["cases"]:
            stamp = row["legacy_nano_expiry"]
            width = len(stamp.split(".")[1][:-1]) if "." in stamp else 0
            if width in {1, 2, 4, 5}:
                with self.subTest(case=row["name"]), self.assertRaises(ValueError):
                    datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
                rejected.add(width)
        self.assertEqual(rejected, {1, 2, 4, 5})


if __name__ == "__main__":
    unittest.main()
