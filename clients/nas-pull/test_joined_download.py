import errno
import hashlib
import http.server
import importlib.util
import io
import json
import os
import tempfile
import threading
import unittest
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

MODULE_PATH = Path(__file__).with_name("stoarama_pull.py")
PROTOCOL_GOLDEN = Path(__file__).with_name("testdata") / "joined_protocol_v1.golden.json"
CLOUD_GOLDENS = Path(__file__).resolve().parents[2] / "backend" / "internal" / "joinedrecording" / "testdata"
SPEC = importlib.util.spec_from_file_location("stoarama_pull_joined", MODULE_PATH)
pull = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pull)


class RangeResponse:
    def __init__(self, body, start, end, total, etag="etag-1", version=""):
        self.status = 206
        self.body = io.BytesIO(body)
        self.headers = {
            "ETag": '"%s"' % etag,
            "Content-Range": "bytes %d-%d/%d" % (start, end, total),
            "Content-Length": str(len(body)),
        }
        if version:
            self.headers["x-amz-version-id"] = version

    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def getcode(self): return self.status
    def read(self, size=-1): return self.body.read(size)


class JoinedDownloadTests(unittest.TestCase):
    def config(self, root):
        state, clips = root / "state", root / "clips"
        state.mkdir(); clips.mkdir()
        return SimpleNamespace(
            api_base="https://stoarama.test/api/v1", api_key="sir_test", origin="https://stoarama.test",
            output_dir=clips, state_dir=state, progress_file=state / "progress.json",
            legacy_progress_file=state / "cursor.json", runtime_file=state / "runtime.json",
            outage_file=state / "outage.json", capacity_file=state / "capacity.json",
            inventory_file=state / "inventory.sqlite3", download_workers=12, min_free_bytes=100, dry_run=False,
            joined_protocol_version=1,
        )

    def media_item(self, content=b"abcdef", **changes):
        manifest = self.manifest_payload()
        media = manifest["media"][0]
        media.update({
            "artifact_id": 41, "relative_path": "MIT_North-America_US_Cambridge_Kendall/May/Monday/stream_hour_01_080000-090000.mp4",
            "object_key": "joined/goodplus-20260821-generation-1/objects/%s.mp4" % hashlib.sha256(content).hexdigest(),
            "content_id": hashlib.sha256(content).hexdigest(), "size_bytes": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        })
        manifest["source_dispositions"][0].update({"media_artifact_id": 41, "media_ordinal": 1})
        item = {
            "artifact_id": 41, "batch_id": manifest["batch_id"],
            "hour_id": manifest["hour_id"], "kind": "media",
            "content_type": "video/mp4",
            "relative_path": media["relative_path"],
            "size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
            "download_path": "/api/v1/account/joined/41/download",
            "ledger_artifact_id": None, "ledger_relative_path": None, "ledger_sha256": None,
            "hour_manifest_id": 40,
            "hour_manifest_relative_path": "coverage/hours/%s.json" % manifest["hour_id"],
            "hour_manifest_sha256": None,
        }
        item.update(changes)
        if item["hour_manifest_sha256"] is None:
            item["hour_manifest_sha256"] = hashlib.sha256(self.manifest_bytes(item, payload=manifest)).hexdigest()
        return item

    def golden(self, name):
        return json.loads((CLOUD_GOLDENS / name).read_text())

    def ledger_payload(self, gap=False):
        ledger = self.golden("allocation_ledger_v1.golden.json")
        if not gap:
            return ledger
        empty_sha = pull.source_claim_sha([])
        ledger.update({
            "source_claim_sha256": empty_sha, "source_clip_count": 0, "source_bytes": 0,
            "first_clip_id": None, "last_clip_id": None, "consecutive_pairs": [], "sources": [],
        })
        for hour in ledger["hours"]:
            hour["source_clip_ids"] = []
        ledger["hour_source_claim_sha256"] = [empty_sha] * 12
        gap_manifest = self.golden("hour_manifest_gap_only_v1.golden.json")
        ledger["cross_hour_boundaries"][0] = gap_manifest["allocation"]["boundaries"][0]
        ledger["cross_day_boundaries"][0] = gap_manifest["allocation"]["cross_day_boundaries"][0]
        ledger["ledger_sha256"] = ""
        ledger["ledger_sha256"] = pull.joined_canonical_sha(ledger)
        return ledger

    def manifest_payload(self, media=None, gap=False):
        manifest = self.golden("hour_manifest_gap_only_v1.golden.json" if gap else "hour_manifest_mixed_v1.golden.json")
        if media is not None and not gap:
            artifact_id = media.get("id", media.get("artifact_id"))
            entry = manifest["media"][0]
            entry.update({
                "artifact_id": artifact_id, "relative_path": media["relative_path"],
                "object_key": "joined/%s/objects/%s.mp4" % (manifest["batch_id"], media["sha256"]),
                "content_id": media["sha256"], "size_bytes": media["size_bytes"], "sha256": media["sha256"],
            })
            manifest["source_dispositions"][0].update({"media_artifact_id": artifact_id, "media_ordinal": 1})
        if gap:
            ledger = self.ledger_payload(gap=True)
            ledger_bytes = pull.joined_canonical_bytes(ledger)
            manifest["allocation"].update({
                "artifact_id": 39, "size_bytes": len(ledger_bytes), "sha256": hashlib.sha256(ledger_bytes).hexdigest(),
                "ledger_sha256": ledger["ledger_sha256"], "hour_source_claim_sha256": ledger["hour_source_claim_sha256"][0],
                "boundaries": [ledger["cross_hour_boundaries"][0]],
                "cross_day_boundaries": [ledger["cross_day_boundaries"][0]],
            })
            manifest["source_claim_sha256"] = ledger["hour_source_claim_sha256"][0]
        return manifest

    def manifest_bytes(self, media=None, gap=False, payload=None):
        return pull.joined_canonical_bytes(payload if payload is not None else self.manifest_payload(media, gap))

    def manifest_item(self, gap=True):
        content = self.manifest_bytes(None, gap)
        manifest = self.manifest_payload(None, gap)
        return {
            "artifact_id": 40, "batch_id": manifest["batch_id"],
            "hour_id": manifest["hour_id"], "kind": "hour_manifest",
            "content_type": "application/json",
            "relative_path": "coverage/hours/%s.json" % manifest["hour_id"],
            "size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
            "download_path": "/api/v1/account/joined/40/download",
            "ledger_artifact_id": manifest["allocation"]["artifact_id"],
            "ledger_relative_path": manifest["allocation"]["relative_path"], "ledger_sha256": manifest["allocation"]["sha256"],
            "hour_manifest_id": None,
            "hour_manifest_relative_path": None, "hour_manifest_sha256": None,
        }, content

    def install_manifest(self, cfg, item):
        path = cfg.output_dir / "joined" / item["batch_id"] / item["hour_manifest_relative_path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.manifest_bytes(item))
        return path

    def install_ledger(self, cfg, gap=False):
        ledger = self.ledger_payload(gap)
        path = cfg.output_dir / "joined" / ledger["batch_id"] / "coverage" / "ledgers" / str(ledger["recording_id"]) / (ledger["local_date"] + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(pull.joined_canonical_bytes(ledger))
        return path

    def prepared(self, item):
        return {
            "url": "https://r2.test/exact-object", "etag": '"etag-1"', "if_match": '"etag-1"',
            "version_id": "", "size_bytes": item["size_bytes"], "sha256": item["sha256"],
            "content_type": item["content_type"], "expires_in_sec": 900,
        }

    def marker(self, item):
        return pull.joined_transfer_marker_bytes(item, {
            "etag": "etag-1", "version_id": "", "url": "https://r2.test/exact-object", "if_match": '"etag-1"',
            "url_scheme": "https", "url_authority": "r2.test", "url_path": "/exact-object",
        })

    def storage(self): return {"available": True, "total_bytes": 10**13, "free_bytes": 10**13}

    def names(self, cfg, item):
        directory_fd = pull.open_joined_output_dir(cfg, item)
        final = Path(item["relative_path"]).name
        part = ".%s.joined-%d.part" % (final, item["id"])
        return directory_fd, final, part, part + ".stoarama.json"

    def write_entry(self, directory_fd, name, content):
        descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=directory_fd)
        try:
            os.write(descriptor, content); os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.fsync(directory_fd)

    def test_protocol_artifact_shape_and_kind_validation(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); self.assertEqual(pull.Runtime(cfg).heartbeat_payload(None)["joined_protocol_version"], 1)
        good = self.media_item(); self.assertEqual(pull.valid_joined_item(good)["kind"], "media")
        for change in (
            {"artifact_id": 0}, {"batch_id": "../escape"}, {"batch_id": True}, {"batch_id": 7},
            {"relative_path": "../escape.mp4"}, {"relative_path": True},
            {"relative_path": "/absolute.mp4"}, {"relative_path": "joined/nested.mp4"}, {"sha256": "bad"},
            {"sha256": "A" * 64}, {"sha256": True}, {"download_path": True},
            {"download_path": "/api/v1/account/joined/42/download"}, {"size_bytes": pull.JOINED_MAX_BYTES + 1},
            {"content_type": "application/json"}, {"hour_manifest_id": None}, {"surprise": True},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                pull.valid_joined_item({**good, **change})
        batch = {**good, "kind": "batch_index", "content_type": "application/json", "hour_id": None,
                 "relative_path": "coverage/batch.json", "ledger_artifact_id": None, "ledger_relative_path": None,
                 "ledger_sha256": None, "hour_manifest_id": None,
                 "hour_manifest_relative_path": None, "hour_manifest_sha256": None}
        self.assertEqual(pull.valid_joined_item(batch)["kind"], "batch_index")

    def test_protocol_v1_shared_golden(self):
        fixture = json.loads(PROTOCOL_GOLDEN.read_text())
        self.assertEqual(set(fixture), {"feed_responses", "prepare_response"})
        self.assertEqual(set(fixture["feed_responses"]), {
            "allocation_ledger", "hour_manifest", "media", "batch_index",
        })
        validated = {
            kind: pull.valid_joined_item(response["item"])
            for kind, response in fixture["feed_responses"].items()
        }
        self.assertEqual({kind: item["kind"] for kind, item in validated.items()}, {
            kind: kind for kind in fixture["feed_responses"]
        })
        media = validated["media"]
        with mock.patch.object(pull, "request_json", return_value=fixture["prepare_response"]):
            prepared = pull.prepare_joined_download(SimpleNamespace(origin="https://stoarama.test"), media)
        self.assertEqual(prepared["url_authority"], "joined.example.test")
        self.assertEqual(prepared["url_path"], "/objects/%s.mp4" % media["sha256"])

    def test_cloud_canonical_goldens_and_strict_nested_decoders(self):
        expected = {
            "allocation_ledger_v1.golden.json": "255e2958738e5f87629224c4e537256b3638fb6abaeaa77a2054c559f5c4ef82",
            "batch_index_v1.golden.json": "834feb5fb3356f6ee964158b25249873138d9d04783d281c5cd5ccc6d72bfcf7",
            "hour_manifest_gap_only_v1.golden.json": "8ec7e39c4cba19cbb5b8e11a3f04952656d53c0b77b879ef764767353af951a9",
            "hour_manifest_mixed_v1.golden.json": "5af2dd888ed7db0a66f3d3d3c36223def703c14732b5515446555e9288a2cf22",
            "hour_manifest_quarantine_only_v1.golden.json": "addc432f0b6aec344e3152b233691035c491e5ed7f186329f8ab9db55a3a4593",
            "hour_manifest_v1.golden.json": "9e498dcde096e42295d9c05e021be45c57b315d6f9a9e050f05d55e98a70e7f8",
        }
        for name, digest in expected.items():
            path = CLOUD_GOLDENS / name
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), digest)
            payload = json.loads(path.read_text())
            validator = pull.valid_allocation_ledger if name.startswith("allocation") else pull.valid_batch_index if name.startswith("batch") else pull.valid_hour_manifest
            validator(payload)
        for name, descend in (
            ("allocation_ledger_v1.golden.json", ("sources", 0, "object")),
            ("hour_manifest_v1.golden.json", ("media", 0, "verification", "source_fingerprint", "tracks", "video")),
            ("batch_index_v1.golden.json", ("allocation_ledgers", 0)),
        ):
            payload = self.golden(name)
            target = payload
            for key in descend:
                target = target[key]
            target["unknown"] = True
            validator = pull.valid_allocation_ledger if name.startswith("allocation") else pull.valid_batch_index if name.startswith("batch") else pull.valid_hour_manifest
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "invalid fields"):
                validator(payload)
        self.assertEqual(
            pull.joined_timestamp_nanoseconds("2026-05-04T08:00:00.000000009Z", "fixture")
            - pull.joined_timestamp_nanoseconds("2026-05-04T08:00:00.000000001Z", "fixture"), 8,
        )
        with self.assertRaisesRegex(ValueError, "duplicate field"):
            pull.decode_joined_json(b'{"schema_version":1,"schema_version":1}')

    def test_protocol_zero_is_dormant_without_joined_api_or_storage_access(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(pull.Config().joined_protocol_version, 0)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); cfg.joined_protocol_version = 0; runtime = pull.Runtime(cfg)
            self.assertEqual(runtime.heartbeat_payload(None)["joined_protocol_version"], 0)
            with mock.patch.object(pull, "request_json") as request, mock.patch.object(pull, "open_joined_output_dir") as storage:
                self.assertFalse(pull.drain_joined(cfg, runtime, threading.Event()))
            request.assert_not_called(); storage.assert_not_called()

    def test_gap_only_hour_manifest_downloads_and_exact_acks(self):
        raw_item, content = self.manifest_item(gap=True); requests, acks = [], []
        def api(_cfg, _method, path, body=None, **_kwargs):
            if path == "/account/joined": return {"item": raw_item}
            if path.startswith("/account/clips?"): return {"clips": []}
            if path == raw_item["download_path"]: return self.prepared(raw_item)
            if path == "/account/joined/ack": acks.append(body); return {"ok": True}
            raise AssertionError(path)
        def open_range(request, **_kwargs):
            headers = dict(request.header_items()); requests.append(headers)
            start, end = map(int, headers["Range"].removeprefix("bytes=").split("-"))
            return RangeResponse(content[start:end + 1], start, end, len(content))
        with tempfile.TemporaryDirectory() as raw:
            cfg, runtime = self.config(Path(raw)), None; runtime = pull.Runtime(cfg); self.install_ledger(cfg, gap=True)
            with mock.patch.object(pull, "JOINED_RANGE_BYTES", 64), mock.patch.object(pull, "storage_status", return_value=self.storage()), \
                 mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull, "open_joined_url", side_effect=open_range):
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            item = pull.valid_joined_item(raw_item); final = pull.joined_output_path(cfg, item)
            self.assertEqual(final.read_bytes(), content); self.assertFalse(list(final.parent.glob("*.mp4")))
            self.assertEqual(acks, [{"artifact_id": 40, "relative_path": raw_item["relative_path"],
                                     "size_bytes": len(content), "sha256": raw_item["sha256"]}])
            self.assertTrue(requests); self.assertEqual(list(final.parent.glob(".*.part*")), [])

    def test_allocation_ledger_downloads_and_exact_acks(self):
        ledger = self.ledger_payload()
        content = pull.joined_canonical_bytes(ledger)
        raw_item = {
            "artifact_id": 55, "batch_id": ledger["batch_id"], "hour_id": None, "kind": "allocation_ledger",
            "content_type": "application/json", "relative_path": "coverage/ledgers/377/2026-05-04.json",
            "size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
            "download_path": "/api/v1/account/joined/55/download", "ledger_artifact_id": None,
            "ledger_relative_path": None, "ledger_sha256": None, "hour_manifest_id": None,
            "hour_manifest_relative_path": None, "hour_manifest_sha256": None,
        }
        acks = []
        def api(_cfg, _method, path, body=None, **_kwargs):
            if path == "/account/joined": return {"item": raw_item}
            if path.startswith("/account/clips?"): return {"clips": []}
            if path == raw_item["download_path"]: return self.prepared(raw_item)
            if path == "/account/joined/ack": acks.append(body); return {"ok": True}
            raise AssertionError(path)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg)
            with mock.patch.object(pull, "storage_status", return_value=self.storage()), mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull, "open_joined_url", return_value=RangeResponse(content, 0, len(content) - 1, len(content))):
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            final = pull.joined_output_path(cfg, pull.valid_joined_item(raw_item))
            self.assertEqual(final.read_bytes(), content)
            self.assertEqual(acks, [{"artifact_id": 55, "relative_path": raw_item["relative_path"], "size_bytes": len(content), "sha256": raw_item["sha256"]}])

    def test_hour_manifest_requires_exact_installed_ledger(self):
        raw_item, _content = self.manifest_item(gap=True)
        item = pull.valid_joined_item(raw_item)
        manifest = self.manifest_payload(gap=True)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg)
            with self.assertRaises(pull.ExistingFileMismatch):
                pull.validate_hour_ledger_binding(cfg, runtime, item, manifest, threading.Event())
            ledger_path = self.install_ledger(cfg, gap=True)
            ledger_path.write_bytes(ledger_path.read_bytes() + b"x")
            with mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaises(pull.ExistingFileMismatch):
                pull.validate_hour_ledger_binding(cfg, runtime, item, manifest, threading.Event())

    def test_final_index_proves_each_day_union_and_exact_media_without_scanning(self):
        ledger = self.ledger_payload()
        source = ledger["sources"][0]
        ledger["sources"] = [source]
        ledger["source_clip_count"] = 1
        ledger["source_bytes"] = source["object"]["size_bytes"]
        ledger["hours"][0]["source_clip_ids"] = [source["clip_id"]]
        for hour in ledger["hours"][1:]: hour["source_clip_ids"] = []
        ledger_ref = {
            "artifact_id": 55, "relative_path": "coverage/ledgers/377/2026-05-04.json", "size_bytes": 10,
            "sha256": "a" * 64, "recording_id": 377, "local_date": "2026-05-04",
            "qualification_sha256": ledger["qualification_sha256"], "source_claim_sha256": ledger["source_claim_sha256"],
            "ledger_sha256": ledger["ledger_sha256"], "source_count": 1, "source_bytes": source["object"]["size_bytes"],
        }
        manifests, hour_refs = {}, []
        for delivery_hour in range(1, 13):
            hour_id = "goodplus-20260821-generation-1__recording-377__date-2026-05-04__hour-%02d__generation-1" % delivery_hour
            path = "coverage/hours/%s.json" % hour_id
            ids = [source["clip_id"]] if delivery_hour == 1 else []
            allocation = {
                "artifact_id": 55, "relative_path": ledger_ref["relative_path"], "size_bytes": 10,
                "sha256": ledger_ref["sha256"], "ledger_sha256": ledger["ledger_sha256"],
                "hour_source_claim_sha256": ledger["hour_source_claim_sha256"][delivery_hour - 1],
            }
            allocation["boundaries"], allocation["cross_day_boundaries"] = pull.expected_hour_boundaries(ledger, delivery_hour)
            media = [{"artifact_id": 88, "relative_path": "delivery/hour-01.mp4", "size_bytes": 6, "sha256": hashlib.sha256(b"media!").hexdigest()}] if delivery_hour == 1 else []
            manifests[path] = {
                "batch_id": ledger["batch_id"], "hour_id": hour_id, "recording_id": 377, "local_date": "2026-05-04",
                "delivery_hour": delivery_hour, "status": "media" if media else "gap_only", "source_count": len(ids),
                "qualification_sha256": ledger["qualification_sha256"],
                "qualification_day": ledger["qualification_day"],
                "sources": [source] if ids else [], "source_claim_sha256": ledger["hour_source_claim_sha256"][delivery_hour - 1],
                "source_dispositions": [{"clip_id": source["clip_id"]}] if ids else [], "allocation": allocation, "media": media,
            }
            hour_refs.append({
                "hour_manifest_artifact_id": 100 + delivery_hour, "hour_id": hour_id, "recording_id": 377, "local_date": "2026-05-04", "delivery_hour": delivery_hour,
                "status": manifests[path]["status"], "relative_path": path, "size_bytes": 10, "sha256": str(delivery_hour) * 64,
                "source_count": len(ids), "source_bytes": source["object"]["size_bytes"] if ids else 0,
                "media_artifact_count": len(media),
            })
        index = {
            "batch_id": ledger["batch_id"], "allocation_ledgers": [ledger_ref], "hours": hour_refs,
            "source_clip_count": 1, "source_bytes": source["object"]["size_bytes"], "final_media_artifact_count": 1,
            "frozen_denominator_sha256": pull.frozen_denominator_sha([ledger]),
        }
        def read(_cfg, _runtime, _batch, path, *_args):
            return ledger if path == ledger_ref["relative_path"] else manifests[path]
        with mock.patch.object(pull, "read_joined_json_path", side_effect=read), mock.patch.object(pull, "valid_allocation_ledger"), mock.patch.object(pull, "valid_hour_manifest"), mock.patch.object(pull, "verify_joined_relative_file") as verify:
            pull.validate_batch_index_proof(None, None, index, threading.Event())
            verify.assert_called_once_with(None, None, ledger["batch_id"], "delivery/hour-01.mp4", 6, hashlib.sha256(b"media!").hexdigest(), mock.ANY)
            manifests[hour_refs[0]["relative_path"]]["source_dispositions"][0]["clip_id"] = 999
            with self.assertRaisesRegex(pull.ExistingFileMismatch, "disposition union"):
                pull.validate_batch_index_proof(None, None, index, threading.Event())
            manifests[hour_refs[0]["relative_path"]]["source_dispositions"][0]["clip_id"] = source["clip_id"]
            ledger_ref["source_claim_sha256"] = "f" * 64
            index["frozen_denominator_sha256"] = pull.frozen_denominator_sha([ledger_ref])
            with self.assertRaisesRegex(pull.ExistingFileMismatch, "ledger reference conflicts"):
                pull.validate_batch_index_proof(None, None, index, threading.Event())

    def test_media_requires_and_matches_local_sealed_hour_manifest(self):
        content, raw_item, requests, acks = b"abcdef", self.media_item(), [], []
        def api(_cfg, _method, path, body=None, **_kwargs):
            if path == "/account/joined": return {"item": raw_item}
            if path.startswith("/account/clips?"): return {"clips": []}
            if path == raw_item["download_path"]: return self.prepared(raw_item)
            if path == "/account/joined/ack": acks.append(body); return {"ok": True}
            raise AssertionError(path)
        def open_range(request, **_kwargs):
            headers = dict(request.header_items()); requests.append(headers)
            start, end = map(int, headers["Range"].removeprefix("bytes=").split("-"))
            return RangeResponse(content[start:end + 1], start, end, len(content))
        with tempfile.TemporaryDirectory() as raw:
            cfg, runtime = self.config(Path(raw)), None; runtime = pull.Runtime(cfg); self.install_manifest(cfg, raw_item)
            with mock.patch.object(pull, "JOINED_RANGE_BYTES", 3), mock.patch.object(pull, "storage_status", return_value=self.storage()), \
                 mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull, "open_joined_url", side_effect=open_range):
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            final = pull.joined_output_path(cfg, pull.valid_joined_item(raw_item))
            self.assertEqual(final.read_bytes(), content); self.assertFalse(final.with_name(final.name + ".stoarama.json").exists())
            self.assertEqual([r["Range"] for r in requests], ["bytes=0-2", "bytes=3-5"])
            self.assertEqual([r["If-match"] for r in requests], ['"etag-1"', '"etag-1"'])
            self.assertEqual(acks[0]["artifact_id"], 41); self.assertEqual(list(final.parent.glob(".*.part*")), [])

    def test_media_manifest_mismatch_or_missing_manifest_fails_before_download(self):
        item = pull.valid_joined_item(self.media_item())
        with tempfile.TemporaryDirectory() as raw:
            cfg, runtime = self.config(Path(raw)), None; runtime = pull.Runtime(cfg)
            with self.assertRaises(pull.ExistingFileMismatch): pull.download_joined_item(cfg, runtime, item, threading.Event())
            path = self.install_manifest(cfg, item); path.write_bytes(self.manifest_bytes({**item, "size_bytes": 7}))
            altered = {**item, "hour_manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            with mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "sealed hour manifest"):
                pull.download_joined_item(cfg, runtime, altered, threading.Event())

    def test_hash_is_bounded_cancellable_and_yields_to_raw(self):
        content, item = b"abcdef", pull.valid_joined_item(self.media_item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, part, marker = self.names(cfg, item)
            try:
                with mock.patch.object(pull, "poll_raw_pending", return_value=False):
                    pull.ensure_owned_joined_partial(cfg, runtime, directory_fd, part, marker, self.marker(item), threading.Event())
                descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd); os.write(descriptor, content); os.close(descriptor)
                with mock.patch.object(pull, "JOINED_RANGE_BYTES", 3), mock.patch.object(pull, "poll_raw_pending", side_effect=[False, True]) as poll, self.assertRaises(pull.JoinedDownloadYield):
                    pull.hash_joined_entry(cfg, runtime, directory_fd, part, threading.Event())
                self.assertEqual(poll.call_count, 2); self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, 6)
            finally: os.close(directory_fd)

    def test_unknown_partial_and_hardlink_are_never_modified(self):
        item = pull.valid_joined_item(self.media_item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); self.install_manifest(cfg, item)
            directory_fd, _, part, marker = self.names(cfg, item)
            prepared = {
                "etag": "etag-1", "version_id": "", "url": "https://r2.test/exact-object", "if_match": '"etag-1"',
                "url_scheme": "https", "url_authority": "r2.test", "url_path": "/exact-object",
            }
            try:
                self.write_entry(directory_fd, part, b"unknown")
                with mock.patch.object(pull, "prepare_joined_download", return_value=prepared), mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "ownership marker"):
                    pull.download_joined_item(cfg, runtime, item, threading.Event())
                self.write_entry(directory_fd, marker, self.marker(item)); os.link(part, "external-hardlink", src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
                with mock.patch.object(pull, "prepare_joined_download", return_value=prepared), mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "unknown hardlink"):
                    pull.download_joined_item(cfg, runtime, item, threading.Event())
                with self.assertRaisesRegex(pull.ExistingFileMismatch, "exclusively owned"): pull.truncate_joined_part(directory_fd, part)
                self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, 7)
            finally: os.close(directory_fd)

    def test_private_stage_crash_enospc_restart_and_conflict_safety(self):
        item = pull.valid_joined_item(self.media_item()); expected = self.marker(item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _part, marker = self.names(cfg, item)
            try:
                orphan = marker + ".stage-" + "a" * 32; self.write_entry(directory_fd, orphan, expected)
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), mock.patch.object(pull.os, "urandom", return_value=b"\xbb" * 16):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                self.assertEqual(os.stat(marker, dir_fd=directory_fd).st_nlink, 1)
                descriptor = os.open(orphan, os.O_RDONLY, dir_fd=directory_fd)
                try: self.assertEqual(os.read(descriptor, len(expected) + 1), expected)
                finally: os.close(descriptor)
            finally: os.close(directory_fd)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _part, marker = self.names(cfg, item)
            try:
                real_write, calls = os.write, 0
                def fail_write(descriptor, content):
                    nonlocal calls
                    calls += 1
                    if calls == 1: return real_write(descriptor, content[:7])
                    raise OSError(errno.ENOSPC, "no space left")
                with mock.patch.object(pull.os, "urandom", return_value=b"\xaa" * 16), mock.patch.object(pull.os, "write", side_effect=fail_write), self.assertRaisesRegex(OSError, "no space left"):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                short = marker + ".stage-" + "a" * 32
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), mock.patch.object(pull.os, "urandom", return_value=b"\xbb" * 16):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                self.assertEqual(os.stat(short, dir_fd=directory_fd).st_size, 7)
            finally: os.close(directory_fd)

    def test_restart_after_stage_link_removes_only_proven_link(self):
        item = pull.valid_joined_item(self.media_item()); expected = self.marker(item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _part, marker = self.names(cfg, item)
            try:
                linked = marker + ".stage-" + "a" * 32; unknown = marker + ".stage-" + "b" * 32
                self.write_entry(directory_fd, linked, expected); self.write_entry(directory_fd, unknown, b"unknown")
                os.link(linked, marker, src_dir_fd=directory_fd, dst_dir_fd=directory_fd); os.fsync(directory_fd)
                with mock.patch.object(pull, "poll_raw_pending", return_value=False): pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                self.assertIsNone(pull.joined_entry_stat(directory_fd, linked)); self.assertEqual(os.stat(unknown, dir_fd=directory_fd).st_size, 7)
                self.assertEqual(os.stat(marker, dir_fd=directory_fd).st_nlink, 1)
            finally: os.close(directory_fd)

    def test_conflicting_marker_is_never_modified(self):
        item = pull.valid_joined_item(self.media_item()); expected = self.marker(item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _part, marker = self.names(cfg, item)
            try:
                self.write_entry(directory_fd, marker, b"conflicting-marker")
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "marker conflicts"):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                self.assertEqual(os.stat(marker, dir_fd=directory_fd).st_size, len(b"conflicting-marker"))
            finally: os.close(directory_fd)

    def test_crash_after_final_link_completes_without_final_sidecar(self):
        content, raw_item = b"abcdef", self.media_item(); item = pull.valid_joined_item(raw_item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); self.install_manifest(cfg, item)
            directory_fd, final, part, marker = self.names(cfg, item)
            try:
                with mock.patch.object(pull, "poll_raw_pending", return_value=False): pull.ensure_owned_joined_partial(cfg, runtime, directory_fd, part, marker, self.marker(item), threading.Event())
                descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd); os.write(descriptor, content); os.fsync(descriptor); os.close(descriptor)
                os.link(part, final, src_dir_fd=directory_fd, dst_dir_fd=directory_fd); os.fsync(directory_fd)
            finally: os.close(directory_fd)
            acks = []
            def api(_cfg, _method, path, body=None, **_kwargs):
                if path == "/account/joined": return {"item": raw_item}
                if path.startswith("/account/clips?"): return {"clips": []}
                if path == raw_item["download_path"]: return self.prepared(raw_item)
                if path == "/account/joined/ack": acks.append(body); return {"ok": True}
                raise AssertionError(path)
            with mock.patch.object(pull, "request_json", side_effect=api): self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            final_path = pull.joined_output_path(cfg, item)
            self.assertEqual(final_path.read_bytes(), content); self.assertFalse(final_path.with_name(final_path.name + ".stoarama.json").exists())
            self.assertEqual(len(acks), 1); self.assertEqual(list(final_path.parent.glob(".*.part*")), [])

    def test_http_restart_rules_truncate_only_owned_partial(self):
        item = pull.valid_joined_item(self.media_item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, part, marker = self.names(cfg, item)
            prepared = {"url": "https://r2.test/x", "if_match": '"etag-1"', "etag": "etag-1", "version_id": ""}
            try:
                with mock.patch.object(pull, "poll_raw_pending", return_value=False): pull.ensure_owned_joined_partial(cfg, runtime, directory_fd, part, marker, self.marker(item), threading.Event())
                response = RangeResponse(b"abcdef", 0, 5, 6); response.status = 200
                descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd); os.write(descriptor, b"abc"); os.close(descriptor)
                with mock.patch.object(pull, "open_joined_url", return_value=response), self.assertRaisesRegex(RuntimeError, "ignored range"):
                    pull.append_joined_range(prepared, directory_fd, part, item, 0, 2)
                self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, 0)
                descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd); os.write(descriptor, b"abc"); os.close(descriptor)
                changed = RangeResponse(b"def", 3, 5, 6, etag="etag-2")
                with mock.patch.object(pull, "open_joined_url", return_value=changed), self.assertRaisesRegex(pull.ExistingFileMismatch, "identity drifted"):
                    pull.append_joined_range(prepared, directory_fd, part, item, 3, 5)
                self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, 0)
            finally: os.close(directory_fd)

    def test_joined_redirect_is_rejected_without_hitting_target(self):
        target_hits = []

        class Redirect(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/source":
                    self.send_response(302); self.send_header("Location", "/should-not-be-hit")
                    self.send_header("Content-Length", "0"); self.end_headers()
                    return
                target_hits.append(self.path)
                self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers()
            def log_message(self, _format, *_args): pass

        redirect = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Redirect)
        thread = threading.Thread(target=redirect.serve_forever, daemon=True); thread.start()
        try:
            request = pull.urllib.request.Request(
                "http://127.0.0.1:%d/source" % redirect.server_port, method="GET",
            )
            with self.assertRaises(urllib.error.HTTPError) as raised:
                pull.open_joined_url(request)
            self.assertEqual(raised.exception.code, 302)
            self.assertEqual(target_hits, [])
        finally:
            redirect.shutdown(); redirect.server_close(); thread.join(timeout=2)

    def test_joined_redirect_policy_does_not_change_raw_account_transport(self):
        cfg = SimpleNamespace(api_base="https://stoarama.test/api/v1", api_key="sir_test")
        with mock.patch.object(pull.urllib.request, "urlopen", return_value=io.BytesIO(b'{"clips":[]}')) as raw_open, \
             mock.patch.object(pull.urllib.request, "build_opener") as joined_opener:
            self.assertEqual(pull.request_json(cfg, "GET", "/account/clips?after_id=0&limit=1"), {"clips": []})
        raw_open.assert_called_once(); joined_opener.assert_not_called()

    def test_joined_prepare_pins_authority_port_and_escaped_path_but_not_query(self):
        item = pull.valid_joined_item(self.media_item())
        cfg = SimpleNamespace(origin="https://stoarama.test")

        def prepared(url):
            response = self.prepared(item); response["url"] = url
            with mock.patch.object(pull, "request_json", return_value=response):
                return pull.prepare_joined_download(cfg, item)

        first = prepared("https://r2.test:443/exact%2Fobject?signature=one")
        pull.validate_joined_download_renewal(
            first, prepared("https://r2.test:443/exact%2Fobject?signature=two"),
        )
        for changed in (
            "https://other.test:443/exact%2Fobject?signature=two",
            "https://r2.test:444/exact%2Fobject?signature=two",
            "https://r2.test:443/exact%2fobject?signature=two",
            "https://r2.test:443/other?signature=two",
        ):
            with self.subTest(changed=changed), self.assertRaises(pull.ExistingFileMismatch):
                pull.validate_joined_download_renewal(first, prepared(changed))
        for invalid in (
            "http://r2.test:443/exact%2Fobject?signature=two",
            "https://user@r2.test:443/exact%2Fobject?signature=two",
            "https://r2.test:443/exact%2Fobject?signature=two#fragment",
        ):
            with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                prepared(invalid)
        for invalid_expiry in (True, 0, 3601, "900"):
            response = self.prepared(item); response["expires_in_sec"] = invalid_expiry
            with self.subTest(expiry=invalid_expiry), mock.patch.object(pull, "request_json", return_value=response), self.assertRaisesRegex(RuntimeError, "expiry"):
                pull.prepare_joined_download(cfg, item)
        for field, invalid_values in {
            "url": (True, 7),
            "etag": (True, '"embedded"quote"', " padded ", '""'),
            "if_match": (True, 7),
            "version_id": (True, 7, " padded ", "line\nbreak", "x" * 1025),
            "sha256": (True, "C" * 64),
        }.items():
            for invalid in invalid_values:
                response = self.prepared(item); response[field] = invalid
                with self.subTest(field=field, invalid=invalid), mock.patch.object(pull, "request_json", return_value=response), self.assertRaises((RuntimeError, ValueError)):
                    pull.prepare_joined_download(cfg, item)

    def test_resumable_marker_binds_download_target_but_not_renewable_query(self):
        item = pull.valid_joined_item(self.media_item())
        first = {
            "url": "https://r2.test/exact?signature=one", "if_match": '"etag-1"',
            "etag": "etag-1", "version_id": "version-1", "url_scheme": "https",
            "url_authority": "r2.test", "url_path": "/exact",
        }
        renewed = {**first, "url": "https://r2.test/exact?signature=two"}
        self.assertEqual(pull.joined_transfer_marker_bytes(item, first), pull.joined_transfer_marker_bytes(item, renewed))
        changed = {**first, "url_authority": "other.test"}
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg)
            directory_fd, _final, part, marker = self.names(cfg, item)
            try:
                with mock.patch.object(pull, "poll_raw_pending", return_value=False):
                    pull.ensure_owned_joined_partial(
                        cfg, runtime, directory_fd, part, marker,
                        pull.joined_transfer_marker_bytes(item, first), threading.Event(),
                    )
                descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd)
                try: os.write(descriptor, b"resume")
                finally: os.close(descriptor)
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "marker conflicts"):
                    pull.ensure_owned_joined_partial(
                        cfg, runtime, directory_fd, part, marker,
                        pull.joined_transfer_marker_bytes(item, changed), threading.Event(),
                    )
                self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, len(b"resume"))
            finally:
                os.close(directory_fd)

    def test_ack_failure_replays_exact_final_without_download_or_sidecar(self):
        content, raw_item, fail_ack = b"abc", self.media_item(b"abc"), True
        def api(_cfg, _method, path, **_kwargs):
            nonlocal fail_ack
            if path == "/account/joined": return {"item": raw_item}
            if path.startswith("/account/clips?"): return {"clips": []}
            if path == raw_item["download_path"]: return self.prepared(raw_item)
            if path == "/account/joined/ack":
                if fail_ack: fail_ack = False; raise urllib.error.URLError("ack unavailable")
                return {"ok": True}
            raise AssertionError(path)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); self.install_manifest(cfg, raw_item)
            with mock.patch.object(pull, "storage_status", return_value=self.storage()), mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull, "open_joined_url", return_value=RangeResponse(content, 0, 2, 3)), self.assertRaisesRegex(urllib.error.URLError, "ack unavailable"):
                pull.drain_joined(cfg, runtime, threading.Event())
            final = pull.joined_output_path(cfg, pull.valid_joined_item(raw_item)); before = final.read_bytes()
            with mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull, "open_joined_url") as download:
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            download.assert_not_called(); self.assertEqual(final.read_bytes(), before); self.assertFalse(final.with_name(final.name + ".stoarama.json").exists())

    def test_exact_preexisting_final_is_acked_without_download(self):
        content, raw_item = b"abcdef", self.media_item(); item = pull.valid_joined_item(raw_item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); self.install_manifest(cfg, item)
            final = pull.joined_output_path(cfg, item); final.parent.mkdir(parents=True, exist_ok=True); final.write_bytes(content)
            def api(_cfg, _method, path, **_kwargs):
                if path == "/account/joined": return {"item": raw_item}
                if path.startswith("/account/clips?"): return {"clips": []}
                if path == "/account/joined/ack": return {"ok": True}
                raise AssertionError(path)
            with mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull, "open_joined_url") as download:
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            download.assert_not_called(); self.assertEqual(final.read_bytes(), content)

    def test_unknown_third_final_link_is_rejected(self):
        content, item = b"abcdef", pull.valid_joined_item(self.media_item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, final, part, marker = self.names(cfg, item)
            try:
                self.write_entry(directory_fd, part, content); self.write_entry(directory_fd, marker, self.marker(item))
                os.link(part, final, src_dir_fd=directory_fd, dst_dir_fd=directory_fd); os.link(final, "unknown-third-link", src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "partial conflicts"):
                    pull.complete_existing_joined(cfg, runtime, directory_fd, item, (final, part, marker), self.marker(item), threading.Event())
                self.assertEqual(os.stat(final, dir_fd=directory_fd).st_nlink, 3)
            finally: os.close(directory_fd)

    def test_unknown_joined_feed_fields_fail_closed(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            with mock.patch.object(pull, "request_json", return_value={"item": None, "surprise": True}), self.assertRaisesRegex(RuntimeError, "unknown fields"):
                pull.drain_joined(cfg, pull.Runtime(cfg), threading.Event())


if __name__ == "__main__": unittest.main()
