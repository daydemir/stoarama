"""Offline synthetic transport tests only; never real provider or writer proof."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("capture_invoice_gaps", HERE / "capture_invoice_gaps.py")
gaps = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gaps)
BASELINE = PINS = None


class GapsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global BASELINE, PINS
        from synthetic_fixtures import baseline
        cls.fixture = tempfile.TemporaryDirectory(prefix='invoice-SYNTHETIC-baseline-')
        cls.addClassCleanup(cls.fixture.cleanup)
        BASELINE, PINS = baseline(Path(cls.fixture.name), gaps)
        gaps.PINS_SHA = gaps.digest(PINS.read_bytes())
        cls.baseline = gaps.load_baseline(BASELINE, PINS)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="capture-invoice-SYNTHETIC-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        for index, (name, path) in enumerate(gaps.requests()):
            if index in (0, 6):
                body = copy.deepcopy(self.baseline["account.json"]["body"])
            else:
                month, invoice, number, count, total = gaps.PAGES[index - 1]
                rows = copy.deepcopy(self.baseline["invoice-full-2026-%s.json" % month]["invoice_items"][(number - 1) * 200:])
                body = {"invoice_items": rows, "meta": {"total": total}, "links": {}}
                if number == 2:
                    body["links"] = {"pages": {"prev": "https://api.digitalocean.com/v2/customers/%s/invoices/%s?page=1&per_page=200" % (gaps.TEAM, invoice)}}
            raw = gaps.canonical(body)
            (self.root / (name + ".body.json")).write_bytes(raw)
            envelope = {"schema": "stoarama.capture-invoice-response.v1", "request_name": name,
                        "started_at": "2000-01-01T00:00:%02dZ" % (index * 2),
                        "completed_at": "2000-01-01T00:00:%02dZ" % (index * 2 + 1),
                        "method": "GET", "path": path, "status": 200,
                        "request_id": "SYNTHETIC-%d" % index, "date": "Sat, 01 Jan 2000 00:00:00 GMT",
                        "body": {"file": name + ".body.json", "sha256": gaps.digest(raw), "size_bytes": len(raw)},
                        "parsed_body": body}
            self.write(name, envelope)

    def write(self, name, value):
        (self.root / (name + ".json")).write_bytes(gaps.canonical(value))

    def change(self, name, change):
        value = json.loads((self.root / (name + ".json")).read_bytes())
        change(value)
        self.write(name, value)

    def body_change(self, name, change):
        def update(envelope):
            change(envelope["parsed_body"])
            raw = gaps.canonical(envelope["parsed_body"])
            (self.root / (name + ".body.json")).write_bytes(raw)
            envelope["body"].update(sha256=gaps.digest(raw), size_bytes=len(raw))
        self.change(name, update)

    def validate(self):
        return gaps.validate_capture(self.root, self.baseline)

    def test_synthetic_seven_response_mechanics_never_grant_writer_authority(self):
        path = self.root / "account-before.json"
        path.write_text(json.dumps(json.loads(path.read_bytes()), indent=2))
        result = self.validate()
        self.assertEqual((result["response_count"], result["historical_page_rows"]), (7, 182))
        self.assertFalse(result["owner_death_proven"])
        self.assertFalse(result["original_placement_proven"])
        self.assertFalse(result["apply_authority"])
        self.assertEqual(result["responses"][0]["envelope_sha256"], gaps.digest(path.read_bytes()))
        self.assertEqual(result["responses"][0]["envelope_size_bytes"], path.stat().st_size)

    def test_baseline_pin_drift_and_symlink_refuse(self):
        fake = self.root / "pins.json"
        fake.write_bytes(PINS.read_bytes() + b" ")
        with self.assertRaises(gaps.EvidenceError):
            gaps.load_baseline(BASELINE, fake)
        link = self.root / "pins-link.json"
        link.symlink_to(PINS)
        with self.assertRaises(OSError):
            gaps.load_baseline(BASELINE, link)

    def test_missing_or_extra_response_refuses(self):
        path = self.root / "unexpected-eighth-response.json"
        path.write_text("{}")
        with self.assertRaises(gaps.EvidenceError):
            self.validate()
        path.unlink()
        (self.root / "account-after.json").unlink()
        with self.assertRaises(gaps.EvidenceError):
            self.validate()

    def test_metadata_scope_status_types_order_and_raw_binding_refuse(self):
        name = "account-before"
        original = (self.root / (name + ".json")).read_bytes()
        changes = [lambda e:e.update(path="/v2/droplets"), lambda e:e.update(method="POST"),
                   lambda e:e.update(request_id=""), lambda e:e.update(status=True), lambda e:e.update(status=429),
                   lambda e:e.update(request_name="account-after"), lambda e:e.update(extra="unknown"),
                   lambda e:e.update(completed_at="1999-01-01T00:00:00Z"),
                   lambda e:e["body"].update(file="../account.json"), lambda e:e["body"].update(sha256="0"*64),
                   lambda e:e["body"].update(size_bytes=True), lambda e:e["parsed_body"].update(extra="different")]
        for change in changes:
            with self.subTest(change=changes.index(change)):
                (self.root / (name + ".json")).write_bytes(original)
                self.change(name, change)
                with self.assertRaises(gaps.EvidenceError):
                    self.validate()
        (self.root / (name + ".json")).write_bytes(original)
        self.change("account-after", lambda e:e.update(started_at="2000-01-01T00:00:00Z"))
        with self.assertRaises(gaps.EvidenceError):
            self.validate()

    def test_team_field_cannot_fall_back_to_matching_account_uuid(self):
        self.body_change("account-after", lambda b:b["account"].update(team={"uuid":"other", "name":"AY"}, uuid=gaps.TEAM))
        with self.assertRaises(gaps.EvidenceError):
            self.validate()

    def test_invoice_row_offset_count_and_link_refuse_after_rehash(self):
        name = "invoice-2026-08-page2"
        raw_original = (self.root / (name + ".body.json")).read_bytes()
        metadata_original = (self.root / (name + ".json")).read_bytes()
        changes = [lambda b:b["meta"].update(total=279), lambda b:b["meta"].update(extra="unknown"), lambda b:b["invoice_items"].pop(),
                   lambda b:b["invoice_items"][0].update(amount="changed"),
                   lambda b:b["invoice_items"].reverse(),
                   lambda b:b["links"]["pages"].update(next="https://api.digitalocean.com/unexpected"),
                   lambda b:b["links"]["pages"].update(prev="https://elsewhere.invalid/path"),
                   lambda b:b["links"]["pages"].update(prev="https://api.digitalocean.com/v2/customers/other/invoices/wrong?page=1&per_page=200"),
                   lambda b:b["links"]["pages"].update(prev=b["links"]["pages"]["prev"]+"&page=1")]
        for change in changes:
            with self.subTest(change=changes.index(change)):
                (self.root / (name + ".body.json")).write_bytes(raw_original)
                (self.root / (name + ".json")).write_bytes(metadata_original)
                self.body_change(name, change)
                with self.assertRaises(gaps.EvidenceError):
                    self.validate()

    def test_duplicate_nonfinite_and_oversize_raw_fail(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}'):
            with self.assertRaises(gaps.EvidenceError):
                gaps.strict_json(raw)
        path = self.root / "account-after.body.json"
        path.write_bytes(b"x" * (gaps.BODY_LIMIT + 1))
        with self.assertRaises(gaps.EvidenceError):
            self.validate()


if __name__ == "__main__":
    unittest.main(verbosity=2)
