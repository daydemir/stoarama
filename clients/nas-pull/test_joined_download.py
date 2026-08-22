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
        )

    def protocol_response(self, version, generation, **changes):
        response = {
            "ok": True, "joined_delivery_accepted": True,
            "joined_protocol_version": version, "joined_protocol_generation": generation,
        }
        response.update(changes)
        return response

    def runtime(self, cfg):
        runtime = pull.Runtime(cfg)
        runtime.apply_joined_protocol_response(self.protocol_response(1, 1))
        return runtime

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
            "artifact_id": 41, "connection_id": 77, "batch_id": manifest["batch_id"],
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
            "frozen_source_sha256": pull.frozen_source_sha([], ledger["qualification_day"], ledger["recording_id"]),
            "source_claim_sha256": empty_sha, "source_clip_count": 0, "source_bytes": 0,
            "first_clip_id": None, "last_clip_id": None, "consecutive_pairs": [], "sources": [],
        })
        for hour in ledger["hours"]:
            hour["source_clip_ids"] = []
        ledger["hour_source_claim_sha256"] = [empty_sha] * 12
        gap_manifest = self.golden("hour_manifest_gap_only_v1.golden.json")
        first = gap_manifest["allocation"]["boundaries"][0]
        ledger["cross_hour_boundaries"] = [{
            **first, "previous_delivery_hour": hour, "next_delivery_hour": hour + 1,
            "scheduled_utc": "2026-05-04T%02d:00:00Z" % (hour + 8),
        } for hour in range(1, 12)]
        first_day = gap_manifest["allocation"]["cross_day_boundaries"][0]
        ledger["cross_day_boundaries"] = [first_day, {
            **first_day,
            "scheduled_previous_end_utc": "2026-05-04T20:00:00Z",
            "scheduled_next_start_utc": "2026-05-05T08:00:00Z",
            "allocation_decision": "no_next_day_source", "reason": "next_source_absent",
        }]
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
            "artifact_id": 40, "connection_id": 77, "batch_id": manifest["batch_id"],
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

    def install_manifest(self, cfg, item, acknowledged=True):
        path = cfg.output_dir / "joined" / item["batch_id"] / item["hour_manifest_relative_path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.manifest_bytes(item))
        if acknowledged:
            pull.persist_joined_ack_receipt(cfg, item["connection_id"], {
                "artifact_id": item["hour_manifest_id"], "relative_path": item["hour_manifest_relative_path"],
                "size_bytes": path.stat().st_size, "sha256": item["hour_manifest_sha256"],
            })
        return path

    def install_ledger(self, cfg, gap=False, acknowledged=True):
        ledger = self.ledger_payload(gap)
        path = cfg.output_dir / "joined" / ledger["batch_id"] / "coverage" / "ledgers" / str(ledger["recording_id"]) / (ledger["local_date"] + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(pull.joined_canonical_bytes(ledger))
        if acknowledged:
            pull.persist_joined_ack_receipt(cfg, 77, {
                "artifact_id": 39, "relative_path": "coverage/ledgers/%d/%s.json" % (ledger["recording_id"], ledger["local_date"]),
                "size_bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            })
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
            cfg = self.config(Path(raw))
            self.assertEqual(pull.Runtime(cfg).heartbeat_payload(None)["joined_protocol_version"], 0)
        good = self.media_item(); self.assertEqual(pull.valid_joined_item(good)["kind"], "media")
        for change in (
            {"artifact_id": 0}, {"connection_id": 0}, {"connection_id": True}, {"batch_id": "../escape"}, {"batch_id": True}, {"batch_id": 7},
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
        self.assertEqual(fixture["prepare_response"]["if_match"], '"%s"' % fixture["prepare_response"]["etag"])
        media = validated["media"]
        with mock.patch.object(pull, "request_json", return_value=fixture["prepare_response"]):
            prepared = pull.prepare_joined_download(SimpleNamespace(origin="https://stoarama.test"), media)
        self.assertEqual(prepared["url_authority"], "joined.example.test")
        self.assertEqual(prepared["url_path"], "/objects/%s.mp4" % media["sha256"])

    def test_cloud_canonical_goldens_and_strict_nested_decoders(self):
        expected = {
            "allocation_ledger_v1.golden.json": "aa5ea80fffb3d0396d7d10bdb130723d19fee3a5d7ac467f81fc9e00a2539902",
            "batch_index_v1.golden.json": "cf74f099e40382ba183dffa6a4808439b2f0e4e43a22c98b2cbdb63779f28a93",
            "hour_manifest_gap_only_v1.golden.json": "20de6f9405b19edfa9c80ea9b6fa0f505594b98a1ccad3f47809b878ceca1f53",
            "hour_manifest_mixed_v1.golden.json": "887aadb50a3e9341038e5a9bf8ee583a3e59b65ac0f9a1df6b1a131d6eddb28c",
            "hour_manifest_quarantine_only_v1.golden.json": "e87c781f0d7a9573691c29adadfa80c4e320430b1e16dda8ec392ae5296ef44c",
            "hour_manifest_v1.golden.json": "ac60926bc9b3b7f9abb0853e69a47785fecd1ddb737c966f9f0d4a7e89870416",
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
        with self.assertRaisesRegex(ValueError, "not canonical"):
            pull.decode_joined_json(b'{ "schema_version":1}')
        with self.assertRaisesRegex(ValueError, "field order"):
            pull.decode_joined_json(b'{"b":1,"a":2}')
        manifest = self.golden("hour_manifest_v1.golden.json")
        reordered = {"policy_version": manifest["policy_version"], "schema_version": manifest["schema_version"]}
        reordered.update({key: value for key, value in manifest.items() if key not in reordered})
        with self.assertRaisesRegex(ValueError, "field order"):
            pull.decode_joined_json(pull.joined_canonical_bytes(reordered))
        with self.assertRaisesRegex(ValueError, "non-finite"):
            pull.decode_joined_json(b'{"duration_seconds":1e309}')
        escaped = pull.joined_canonical_bytes({"category": "a&b<c>d\u2028e\u2029"})
        self.assertIn(b"a\\u0026b\\u003cc\\u003ed\\u2028e\\u2029", escaped)
        self.assertEqual(pull.decode_joined_json(escaped)["category"], "a&b<c>d\u2028e\u2029")
        batch = self.golden("batch_index_v1.golden.json")
        batch["frozen_recordings"][0]["naming_metadata"]["plaza_name"] = "Piazza & Silvestri"
        batch["batch_generation_sha256"] = pull.batch_generation_sha(batch)
        batch_bytes = pull.joined_canonical_bytes(batch)
        self.assertIn(b"Piazza \\u0026 Silvestri", batch_bytes)
        pull.valid_batch_index(pull.decode_joined_json(batch_bytes))
        manifest = self.golden("hour_manifest_v1.golden.json")
        manifest["media"][0]["verification"]["output_fingerprint"]["duration_seconds"] = float("inf")
        with self.assertRaisesRegex(ValueError, "invalid duration"):
            pull.valid_hour_manifest(manifest)
        manifest = self.golden("hour_manifest_v1.golden.json")
        manifest["media"][0]["utc_offset_seconds"] = 1
        with self.assertRaisesRegex(ValueError, "timing conflicts"):
            pull.valid_hour_manifest(manifest)

    def test_self_rehashed_boundary_and_media_run_mutations_fail(self):
        ledger = self.ledger_payload()
        ledger["cross_hour_boundaries"][0]["allocation_decision"] = "no_sources"
        ledger["ledger_sha256"] = ""
        ledger["ledger_sha256"] = pull.joined_canonical_sha(ledger)
        with self.assertRaisesRegex(ValueError, "absent boundary"):
            pull.valid_allocation_ledger(pull.decode_joined_json(pull.joined_canonical_bytes(ledger)))

        ledger = self.ledger_payload()
        ledger["hours"][0]["source_clip_ids"] = [1]
        ledger["hours"][1]["source_clip_ids"] = [2]
        ledger["hour_source_claim_sha256"][0] = pull.source_claim_sha(ledger["sources"][:1])
        ledger["hour_source_claim_sha256"][1] = pull.source_claim_sha(ledger["sources"][1:])
        ledger["ledger_sha256"] = ""
        ledger["ledger_sha256"] = pull.joined_canonical_sha(ledger)
        with self.assertRaisesRegex(ValueError, "closest frozen seam"):
            pull.valid_allocation_ledger(pull.decode_joined_json(pull.joined_canonical_bytes(ledger)))

        ledger = self.ledger_payload()
        ledger["cross_day_boundaries"][0]["allocation_decision"] = "no_next_day_source"
        ledger["ledger_sha256"] = ""
        ledger["ledger_sha256"] = pull.joined_canonical_sha(ledger)
        with self.assertRaisesRegex(ValueError, "absence reason"):
            pull.valid_allocation_ledger(pull.decode_joined_json(pull.joined_canonical_bytes(ledger)))

        manifest = self.golden("hour_manifest_mixed_v1.golden.json")
        manifest["source_dispositions"][1].update({
            "disposition": "included", "media_artifact_id": 88, "media_ordinal": 1, "reason_code": "",
        })
        manifest["quarantine_evidence"] = []
        manifest["media"][0]["source_clip_ids"] = [1, 2]
        manifest["media"][0]["actual_end_utc"] = manifest["sources"][1]["end_utc"]
        with self.assertRaisesRegex(ValueError, "gap evidence conflicts"):
            pull.valid_hour_manifest(pull.decode_joined_json(pull.joined_canonical_bytes(manifest)))

    def test_corrected_cloud_hour_contract_rejects_self_rehashed_mutations(self):
        quarantine = self.golden("hour_manifest_quarantine_only_v1.golden.json")
        pull.valid_hour_manifest(quarantine)
        quarantine["gaps"][0], quarantine["gaps"][1] = quarantine["gaps"][1], quarantine["gaps"][0]
        with self.assertRaisesRegex(ValueError, "source order"):
            pull.valid_hour_manifest(pull.decode_joined_json(pull.joined_canonical_bytes(quarantine)))

        manifest = self.golden("hour_manifest_v1.golden.json")
        manifest["sources"][0]["audio_sequence_contract"] = {
            "codec_name": "aac", "sample_rate": 48000, "channels": 2, "channel_layout": "stereo",
            "initial_padding": 0, "skip_samples": 0, "discard_padding": 0, "codec_delay": 0,
            "trailing_padding": 0,
        }
        with self.assertRaisesRegex(ValueError, "audio contracts"):
            pull.valid_hour_manifest(manifest)

        manifest = self.golden("hour_manifest_v1.golden.json")
        manifest["media"][0]["size_bytes"] = pull.JOINED_MAX_BYTES + 1
        with self.assertRaisesRegex(ValueError, "size cap"):
            pull.valid_hour_manifest(manifest)

        manifest = self.golden("hour_manifest_v1.golden.json")
        fingerprint = manifest["media"][0]["verification"]["source_fingerprint"]
        fingerprint.update({
            "audio_sequence_contracts": [], "effective_audio_bytes": 0,
            "effective_audio_sample_frames": 0, "effective_audio_sha256": "",
        })
        with self.assertRaisesRegex(ValueError, "audio evidence"):
            pull.valid_hour_manifest(manifest)

    def test_source_identity_and_nanosecond_parity_with_cloud(self):
        source = self.golden("allocation_ledger_v1.golden.json")["sources"][0]
        for label, mutate in (
            ("canonical HTTPS", lambda value: value.update(endpoint="https://cap.test/?secret=value")),
            ("unsafe", lambda value: value["object"].update(key="raw/../escape.mp4")),
            ("ETag", lambda value: value["object"].update(etag='W/"weak"')),
        ):
            changed = json.loads(json.dumps(source))
            mutate(changed)
            with self.subTest(label=label), self.assertRaisesRegex(ValueError, label):
                pull.valid_source(changed, changed["recording_id"], source_only=True)
        changed = json.loads(json.dumps(source))
        changed["end_utc"] = "2026-05-04T08:15:00.000000001Z"
        with self.assertRaisesRegex(ValueError, "range"):
            pull.valid_source(changed, changed["recording_id"], source_only=True)
        changed = json.loads(json.dumps(source))
        changed["start_utc"], changed["end_utc"] = "2026-05-05T08:00:00Z", "2026-05-05T08:01:00Z"
        with self.assertRaisesRegex(ValueError, "local date"):
            pull.valid_source(changed, changed["recording_id"], source_only=True, location=pull.ZoneInfo("UTC"), local_date="2026-05-04")

        qualification = self.golden("allocation_ledger_v1.golden.json")["qualification_day"]
        changed = json.loads(json.dumps(qualification))
        changed["window_end"] = "2026-05-04T20:00:00.000000001Z"
        with self.assertRaisesRegex(ValueError, "qualification day"):
            pull.valid_qualification_day(changed, 377, "UTC", "2026-05-04")
        equivalent = json.loads(json.dumps(qualification))
        equivalent.update({
            "window_start": "2026-05-04T09:00:00+01:00",
            "window_end": "2026-05-04T21:00:00+01:00",
            "completed_at": "2026-05-04T21:00:00+01:00",
        })
        pull.valid_qualification_day(equivalent, 377, "UTC", "2026-05-04")

        manifest = self.golden("hour_manifest_v1.golden.json")
        manifest["scheduled_end_utc"] = "2026-05-04T09:00:00.000000001Z"
        with self.assertRaisesRegex(ValueError, "schedule"):
            pull.valid_hour_manifest(pull.decode_joined_json(pull.joined_canonical_bytes(manifest)))

    def test_frozen_selection_window_and_storage_evidence(self):
        ledger = self.golden("allocation_ledger_v1.golden.json")
        source = ledger["sources"][0]
        changed = json.loads(json.dumps(source)); changed["storage_destination_id"] = 0
        with self.assertRaisesRegex(ValueError, "storage_destination_id"):
            pull.valid_source(changed, ledger["recording_id"], source_only=True)
        changed_ledger = json.loads(json.dumps(ledger)); changed_ledger["sources"][0]["released_at"] = "0001-01-01T01:00:00+01:00"
        changed_ledger["source_claim_sha256"] = pull.source_claim_sha(changed_ledger["sources"])
        by_id = {item["clip_id"]: item for item in changed_ledger["sources"]}
        changed_ledger["hour_source_claim_sha256"] = [
            pull.source_claim_sha([by_id[clip_id] for clip_id in hour["source_clip_ids"]])
            for hour in changed_ledger["hours"]
        ]
        changed_ledger["frozen_source_sha256"] = pull.joined_canonical_sha([{
            "clip_id": item["clip_id"], "recording_id": item["recording_id"],
            "recording_job_id": item["recording_job_id"], "storage_destination_id": item["storage_destination_id"],
            "provider": item["provider"], "endpoint": item["endpoint"], "region": item["region"],
            "bucket": item["bucket"], "object_key": item["object"]["key"], "start_utc": item["start_utc"],
            "end_utc": item["end_utc"], "size_bytes": item["object"]["size_bytes"],
            "ingest_sha256": item["object"]["sha256"], "released_at": item["released_at"],
        } for item in changed_ledger["sources"]])
        changed_ledger["ledger_sha256"] = ""
        changed_ledger["ledger_sha256"] = pull.joined_canonical_sha(changed_ledger)
        with self.assertRaisesRegex(ValueError, "released_at is zero"):
            pull.valid_allocation_ledger(pull.decode_joined_json(pull.joined_canonical_bytes(changed_ledger)))
        changed = json.loads(json.dumps(ledger)); changed["qualification_day"]["quality_tier"] = "good+"
        with self.assertRaisesRegex(ValueError, "invalid fields"):
            pull.valid_allocation_ledger(changed)

        batch = self.golden("batch_index_v1.golden.json")
        changed = json.loads(json.dumps(batch)); changed["frozen_recordings"][0]["selection_tier"] = "fine+"
        with self.assertRaisesRegex(ValueError, "recording order"):
            pull.valid_batch_index(changed)
        changed = json.loads(json.dumps(batch)); changed["frozen_recordings"][0]["completed_at"] = "0001-01-01T01:00:00+01:00"
        changed["frozen_denominator_sha256"] = pull.frozen_denominator_sha(changed["selection_authority"], changed["frozen_recordings"], changed["allocation_ledgers"])
        changed["batch_generation_sha256"] = pull.batch_generation_sha(changed)
        with self.assertRaisesRegex(ValueError, "completion is zero"):
            pull.valid_batch_index(changed)
        changed = json.loads(json.dumps(batch)); changed["selection_authority"]["cutoff"] = "2026-08-21T06:59:07+00:00"
        changed["frozen_denominator_sha256"] = pull.frozen_denominator_sha(changed["selection_authority"], changed["frozen_recordings"], changed["allocation_ledgers"])
        changed["batch_generation_sha256"] = pull.batch_generation_sha(changed)
        with self.assertRaisesRegex(ValueError, "selection authority"):
            pull.valid_batch_index(changed)
        for cutoff in ("2026-08-21T06:59:07.000Z", "2026-08-21T06:59:07.1200Z"):
            changed = json.loads(json.dumps(batch)); changed["selection_authority"]["cutoff"] = cutoff
            changed["frozen_denominator_sha256"] = pull.frozen_denominator_sha(changed["selection_authority"], changed["frozen_recordings"], changed["allocation_ledgers"])
            changed["batch_generation_sha256"] = pull.batch_generation_sha(changed)
            with self.subTest(cutoff=cutoff), self.assertRaisesRegex(ValueError, "noncanonical selection cutoff"):
                pull.valid_batch_index(changed)
        changed = json.loads(json.dumps(batch)); changed["selection_authority"]["qualification_run_frozen_at"] = "0001-01-01T01:00:00+01:00"
        changed["frozen_denominator_sha256"] = pull.frozen_denominator_sha(changed["selection_authority"], changed["frozen_recordings"], changed["allocation_ledgers"])
        changed["batch_generation_sha256"] = pull.batch_generation_sha(changed)
        with self.assertRaisesRegex(ValueError, "selection authority"):
            pull.valid_batch_index(changed)

        first = pull.datetime.date(2026, 5, 4)
        days = []
        for ordinal in range(1, 15):
            date = first + pull.datetime.timedelta(days=ordinal - 1)
            days.append({
                "local_date": date.isoformat(), "qualification_window_ordinal": ordinal, "job_id": 100 + ordinal,
                "window_start": "%sT08:00:00Z" % date, "window_end": "%sT20:00:00Z" % date,
                "completed_at": "%sT20:00:00Z" % date,
            })
        recording = {
            "recording_id": 377, "timezone": "UTC", "completed_at": days[-1]["completed_at"],
        }
        cutoff, run_frozen = days[-1]["completed_at"], "2026-04-01T00:00:00Z"
        expected = pull.joined_canonical_sha({
            "recording_id": 377, "timezone": "UTC", "days": days,
            "frozen_at": cutoff, "evidence_sha256": "",
        })
        self.assertEqual(pull.qualification_window_sha(recording, days, cutoff, run_frozen), expected)
        changed = json.loads(json.dumps(days)); changed[7]["qualification_window_ordinal"] = 7
        with self.assertRaisesRegex(ValueError, "window conflicts"):
            pull.qualification_window_sha(recording, changed, cutoff, run_frozen)
        changed_recording = dict(recording); changed_recording["completed_at"] = days[-2]["completed_at"]
        with self.assertRaisesRegex(ValueError, "completion conflicts"):
            pull.qualification_window_sha(changed_recording, days, cutoff, run_frozen)

    def test_malformed_source_authorities_blank_fields_and_manifest_order_fail(self):
        source = self.golden("allocation_ledger_v1.golden.json")["sources"][0]
        vectors_path = CLOUD_GOLDENS / "source_endpoint_v1_vectors.json"
        self.assertEqual(hashlib.sha256(vectors_path.read_bytes()).hexdigest(), "bfadc65966b2793bb199c673a6cf37e735d0edf499f2dc6ee4335e62b2c41aa4")
        vectors = json.loads(vectors_path.read_text())
        self.assertEqual(len(vectors["valid"]), 1)
        for vector in vectors["valid"]:
            changed = json.loads(json.dumps(source)); changed["endpoint"] = vector["endpoint"]
            pull.valid_source(changed, 377, source_only=True)
        for endpoint in vectors["invalid"]:
            changed = json.loads(json.dumps(source)); changed["endpoint"] = endpoint
            with self.subTest(endpoint=endpoint), self.assertRaisesRegex(ValueError, "canonical HTTPS"):
                pull.valid_source(changed, 377, source_only=True)
        for field in ("provider", "region", "bucket"):
            changed = json.loads(json.dumps(source)); changed[field] = "   "
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "storage identity"):
                pull.valid_source(changed, 377, source_only=True)

        contract = {
            "codec_name": "aac", "sample_rate": 48000, "channels": 2, "channel_layout": "stereo",
            "initial_padding": 0, "skip_samples": 0, "discard_padding": 0, "codec_delay": 0,
            "trailing_padding": 0,
        }
        for field in ("codec_name", "channel_layout"):
            changed = dict(contract); changed[field] = "  "
            with self.subTest(audio_field=field), self.assertRaisesRegex(ValueError, "blank format"):
                pull.valid_audio_contract(changed)

        manifest = self.golden("hour_manifest_quarantine_only_v1.golden.json")
        manifest["sources"][0], manifest["sources"][1] = manifest["sources"][1], manifest["sources"][0]
        manifest["source_dispositions"][0], manifest["source_dispositions"][1] = manifest["source_dispositions"][1], manifest["source_dispositions"][0]
        for index, source in enumerate(manifest["sources"]):
            if index == 0:
                source["seam_to_previous"] = {"verdict": "", "reason": "", "signed_gap_nanoseconds": 0}
                continue
            previous = manifest["sources"][index - 1]
            signed_gap = pull.joined_timestamp_nanoseconds(source["start_utc"], "attack start") - pull.joined_timestamp_nanoseconds(previous["end_utc"], "attack end")
            source["seam_to_previous"] = {
                "verdict": "gap" if signed_gap > 0 else "overlap" if signed_gap < 0 else "continuous",
                "reason": "signed_presentation_gap" if signed_gap else "timestamp_adjacent_preflight_candidate",
                "signed_gap_nanoseconds": signed_gap,
            }
        manifest["gaps"] = [{
            "previous_clip_id": previous["clip_id"], "next_clip_id": following["clip_id"],
            "at_utc": previous["end_utc"],
            "signed_gap_nanoseconds": following["seam_to_previous"]["signed_gap_nanoseconds"],
            "reason": "source_quarantined",
        } for previous, following in zip(manifest["sources"], manifest["sources"][1:])]
        source_sha = pull.source_claim_sha(manifest["sources"])
        manifest["source_claim_sha256"] = source_sha
        manifest["allocation"]["hour_source_claim_sha256"] = source_sha
        evidence = manifest["quarantine_evidence"][0]
        evidence["source_clip_ids"] = [source["clip_id"] for source in manifest["sources"]]
        evidence["source_claim_sha256"] = pull.candidate_source_claim_sha(manifest["sources"])
        evidence["evidence_sha256"] = pull.joined_canonical_sha({
            "source_claim_sha256": evidence["source_claim_sha256"], "reason_code": evidence["reason_code"],
            "failure_sha256": evidence["failure_sha256"], "policy_version": evidence["policy_version"],
            "media_tool_identity": evidence["media_tool_identity"], "repeat_count": evidence["isolated_attempt_count"],
        })
        with self.assertRaisesRegex(ValueError, "source order"):
            pull.valid_hour_manifest(pull.decode_joined_json(pull.joined_canonical_bytes(manifest)))

    def test_audio_codec_and_quarantine_reason_are_exactly_bound(self):
        verification = self.golden("hour_manifest_v1.golden.json")["media"][0]["verification"]
        def audio_track(video):
            track = {}
            for key, value in video.items():
                if key == "first_timestamp":
                    track["decoded_samples"] = 1
                track[key] = "audio" if key == "media_type" else value
            return track
        source_track = audio_track(verification["source_fingerprint"]["tracks"]["video"])
        output_track = audio_track(verification["output_fingerprint"]["tracks"]["video"])
        aac = {
            "codec_name": "aac", "sample_rate": 48000, "channels": 2, "channel_layout": "stereo",
            "initial_padding": 0, "skip_samples": 0, "discard_padding": 0, "codec_delay": 0,
            "trailing_padding": 0,
        }
        opus = {**aac, "codec_name": "opus"}
        for fingerprint, track, contract in (
            (verification["source_fingerprint"], source_track, aac),
            (verification["output_fingerprint"], output_track, opus),
        ):
            fingerprint["tracks"]["audio"] = track
            fingerprint.update({
                "audio_sequence_contracts": [contract], "effective_audio_bytes": 1,
                "effective_audio_sample_frames": 1, "effective_audio_sha256": "a" * 64,
            })
        with self.assertRaisesRegex(ValueError, "audio format"):
            pull.valid_verification(verification)

        for fixture in ("hour_manifest_mixed_v1.golden.json", "hour_manifest_quarantine_only_v1.golden.json"):
            manifest = self.golden(fixture)
            manifest["source_dispositions"][-1]["reason_code"] = "different_failure"
            with self.subTest(fixture=fixture), self.assertRaisesRegex(ValueError, "exactly cover"):
                pull.valid_hour_manifest(pull.decode_joined_json(pull.joined_canonical_bytes(manifest)))

    def test_consecutive_ledgers_must_share_exact_cross_day_fact(self):
        previous = self.ledger_payload()
        following = self.ledger_payload()
        following["cross_day_boundaries"][0] = dict(previous["cross_day_boundaries"][1])
        pull.validate_cross_day_ledger_link(previous, following)
        following["cross_day_boundaries"][0]["verdict"] = "gap"
        with self.assertRaisesRegex(pull.ExistingFileMismatch, "shared day boundary"):
            pull.validate_cross_day_ledger_link(previous, following)

    def test_protocol_zero_is_dormant_without_joined_api_or_storage_access(self):
        with mock.patch.dict(os.environ, {"STOARAMA_JOINED_PROTOCOL_VERSION": "invalid-dead-setting"}):
            self.assertFalse(hasattr(pull.Config(), "joined_protocol_version"))
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            self.assertEqual(runtime.heartbeat_payload(None)["joined_protocol_version"], 0)
            with mock.patch.object(pull, "request_json") as request, mock.patch.object(pull, "open_joined_output_dir") as storage:
                self.assertFalse(pull.drain_joined(cfg, runtime, threading.Event()))
            request.assert_not_called(); storage.assert_not_called()

    def test_remote_protocol_generation_is_monotonic_and_fail_closed(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            self.assertFalse(runtime.joined_protocol_enabled())

            runtime.apply_joined_protocol_response(self.protocol_response(1, 4))
            self.assertTrue(runtime.joined_protocol_enabled())
            self.assertEqual(runtime.heartbeat_payload(None)["joined_protocol_version"], 1)
            runtime.apply_joined_protocol_response(self.protocol_response(1, 4))
            self.assertTrue(runtime.joined_protocol_enabled())

            runtime.apply_joined_protocol_response(self.protocol_response(1, 3))
            self.assertFalse(runtime.joined_protocol_enabled())
            runtime.apply_joined_protocol_response(self.protocol_response(1, 4))
            self.assertFalse(runtime.joined_protocol_enabled())

            runtime.apply_joined_protocol_response(self.protocol_response(0, 5))
            self.assertFalse(runtime.joined_protocol_enabled())
            runtime.apply_joined_protocol_response(self.protocol_response(1, 6))
            self.assertTrue(runtime.joined_protocol_enabled())
            runtime.apply_joined_protocol_response(self.protocol_response(1, 7, joined_delivery_accepted=False))
            self.assertFalse(runtime.joined_protocol_enabled())
            for malformed in ({}, None, self.protocol_response(True, 7),
                              self.protocol_response(1, 0), self.protocol_response(1, 7, extra=True),
                              self.protocol_response(1, 7, ok=False),
                              self.protocol_response(1, 7, joined_delivery_accepted=1)):
                runtime.apply_joined_protocol_response(malformed)
                self.assertFalse(runtime.joined_protocol_enabled())

    def test_downgrade_after_feed_fetch_stops_before_joined_item(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            def feed(*_args, **_kwargs):
                runtime.apply_joined_protocol_response(self.protocol_response(0, 2))
                return {"item": self.media_item()}
            with mock.patch.object(pull, "request_json", side_effect=feed), \
                 mock.patch.object(pull, "download_joined_item") as download:
                self.assertFalse(pull.drain_joined(cfg, runtime, threading.Event()))
            download.assert_not_called()

    def test_active_download_downgrade_yields_at_next_range_boundary(self):
        content = b"abcdef"
        raw_item = self.media_item(content)
        item = pull.valid_joined_item(raw_item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            self.install_manifest(cfg, item)
            ranges = []
            def open_range(request, **_kwargs):
                start, end = map(int, dict(request.header_items())["Range"].removeprefix("bytes=").split("-"))
                ranges.append((start, end))
                runtime.apply_joined_protocol_response(self.protocol_response(0, 2))
                return RangeResponse(content[start:end + 1], start, end, len(content))
            with mock.patch.object(pull, "JOINED_RANGE_BYTES", 3), \
                 mock.patch.object(pull, "poll_raw_pending", return_value=False), \
                 mock.patch.object(pull, "storage_status", return_value=self.storage()), \
                 mock.patch.object(pull, "request_json", return_value=self.prepared(raw_item)), \
                 mock.patch.object(pull, "open_joined_url", side_effect=open_range), \
                 self.assertRaises(pull.JoinedDownloadYield):
                pull.download_joined_item(cfg, runtime, item, threading.Event())
            self.assertEqual(ranges, [(0, 2)])
            final = pull.joined_output_path(cfg, item)
            self.assertFalse(final.exists())
            self.assertEqual(final.parent.joinpath(".%s.joined-%d.part" % (final.name, item["id"])).stat().st_size, 3)

    def test_downgrade_after_download_stops_before_ack(self):
        raw_item = self.media_item()
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            paths = []
            def api(_cfg, _method, path, **_kwargs):
                paths.append(path)
                if path == "/account/joined":
                    return {"item": raw_item}
                raise AssertionError("ACK attempted after downgrade")
            def download(*_args):
                runtime.apply_joined_protocol_response(self.protocol_response(0, 2))
                return True
            with mock.patch.object(pull, "request_json", side_effect=api), \
                 mock.patch.object(pull, "download_joined_item", side_effect=download):
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            self.assertEqual(paths, ["/account/joined"])

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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            self.install_ledger(cfg, gap=True)
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
            "artifact_id": 55, "connection_id": 77, "batch_id": ledger["batch_id"], "hour_id": None, "kind": "allocation_ledger",
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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
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
            "qualification_sha256": ledger["qualification_sha256"], "frozen_source_sha256": ledger["frozen_source_sha256"],
            "source_claim_sha256": ledger["source_claim_sha256"],
            "ledger_sha256": ledger["ledger_sha256"], "source_count": 1, "source_bytes": source["object"]["size_bytes"],
        }
        manifests, hour_refs = {}, []
        batch_golden = self.golden("batch_index_v1.golden.json")
        frozen = json.loads(json.dumps(batch_golden["frozen_recordings"][0]))
        frozen["qualification_sha256"] = ledger["qualification_sha256"]
        frozen["completed_at"] = ledger["qualification_day"]["completed_at"]
        selection_authority = json.loads(json.dumps(batch_golden["selection_authority"]))
        selection_authority["selected_qualification_windows_sha256"] = pull.selected_qualification_windows_sha([frozen])
        media_tool = batch_golden["media_tool"]
        media_path = "01_Europe_Italy_Bevagna_Piazza_Silvestri/May/Monday/01_Piazza_Silvestri_2026_May_W1_Monday_hour_01_080000-080100.mp4"
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
            media = [{
                "artifact_id": 88, "relative_path": media_path,
                "size_bytes": 6, "sha256": hashlib.sha256(b"media!").hexdigest(), "part": 1, "parts": 1,
                "actual_start_utc": source["start_utc"], "actual_end_utc": source["end_utc"],
            }] if delivery_hour == 1 else []
            manifests[path] = {
                "batch_id": ledger["batch_id"], "hour_id": hour_id, "recording_id": 377, "local_date": "2026-05-04",
                "delivery_hour": delivery_hour, "status": "media" if media else "gap_only", "source_count": len(ids),
                "qualification_sha256": ledger["qualification_sha256"],
                "qualification_day": ledger["qualification_day"],
                "timezone": ledger["timezone"], "media_tool": media_tool,
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
            "generation": ledger["generation"], "frozen_recordings": [frozen], "media_tool": media_tool,
            "selection_authority": selection_authority,
            "source_clip_count": 1, "source_bytes": source["object"]["size_bytes"], "final_media_artifact_count": 1,
            "frozen_denominator_sha256": pull.frozen_denominator_sha(selection_authority, [frozen], [ledger]),
        }
        def read(_cfg, _runtime, _batch, path, *_args):
            return ledger if path == ledger_ref["relative_path"] else manifests[path]
        with mock.patch.object(pull, "read_joined_json_path", side_effect=read), mock.patch.object(pull, "valid_allocation_ledger"), mock.patch.object(pull, "valid_hour_manifest"), mock.patch.object(pull, "verify_joined_relative_file") as verify:
            pull.validate_batch_index_proof(None, None, index, threading.Event())
            verify.assert_called_once_with(None, None, ledger["batch_id"], media_path, 6, hashlib.sha256(b"media!").hexdigest(), mock.ANY)
            manifests[hour_refs[0]["relative_path"]]["media"][0]["artifact_id"] = hour_refs[0]["hour_manifest_artifact_id"]
            with self.assertRaisesRegex(pull.ExistingFileMismatch, "duplicated"):
                pull.validate_batch_index_proof(None, None, index, threading.Event())
            manifests[hour_refs[0]["relative_path"]]["media"][0]["artifact_id"] = 88
            ledger["generation"] += 1
            with self.assertRaisesRegex(pull.ExistingFileMismatch, "ledger reference"):
                pull.validate_batch_index_proof(None, None, index, threading.Event())
            ledger["generation"] -= 1
            manifests[hour_refs[0]["relative_path"]]["media_tool"] = {**media_tool, "ffmpeg_version": "mutated"}
            with self.assertRaisesRegex(pull.ExistingFileMismatch, "hour reference"):
                pull.validate_batch_index_proof(None, None, index, threading.Event())
            manifests[hour_refs[0]["relative_path"]]["media_tool"] = media_tool
            manifests[hour_refs[0]["relative_path"]]["media"][0]["relative_path"] = "wrong/May/Monday/wrong.mp4"
            with self.assertRaisesRegex(pull.ExistingFileMismatch, "delivery path"):
                pull.validate_batch_index_proof(None, None, index, threading.Event())
            manifests[hour_refs[0]["relative_path"]]["media"][0]["relative_path"] = media_path
            manifests[hour_refs[0]["relative_path"]]["source_dispositions"][0]["clip_id"] = 999
            with self.assertRaisesRegex(pull.ExistingFileMismatch, "disposition union"):
                pull.validate_batch_index_proof(None, None, index, threading.Event())
            manifests[hour_refs[0]["relative_path"]]["source_dispositions"][0]["clip_id"] = source["clip_id"]
            ledger_ref["source_claim_sha256"] = "f" * 64
            index["frozen_denominator_sha256"] = pull.frozen_denominator_sha(index["selection_authority"], index["frozen_recordings"], [ledger_ref])
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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            self.install_manifest(cfg, raw_item)
            with mock.patch.object(pull, "JOINED_RANGE_BYTES", 3), mock.patch.object(pull, "storage_status", return_value=self.storage()), \
                 mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull, "open_joined_url", side_effect=open_range):
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            final = pull.joined_output_path(cfg, pull.valid_joined_item(raw_item))
            self.assertEqual(final.read_bytes(), content); self.assertFalse(final.with_name(final.name + ".stoarama.json").exists())
            self.assertEqual([r["Range"] for r in requests], ["bytes=0-2", "bytes=3-5"])
            self.assertEqual([r["If-match"] for r in requests], ['"etag-1"', '"etag-1"'])
            self.assertEqual(acks[0]["artifact_id"], 41); self.assertEqual(list(final.parent.glob(".*.part*")), [])

    def test_installed_dependency_reacks_before_child_and_persists_receipt(self):
        raw_item, _ = self.manifest_item(gap=True)
        item = pull.valid_joined_item(raw_item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            self.install_ledger(cfg, gap=True, acknowledged=False)
            with mock.patch.object(pull, "request_json", side_effect=urllib.error.URLError("ACK unavailable")), \
                 mock.patch.object(pull, "open_joined_url") as download, self.assertRaisesRegex(urllib.error.URLError, "ACK unavailable"):
                pull.download_joined_item(cfg, runtime, item, threading.Event())
            download.assert_not_called()
            self.assertFalse(pull.joined_output_path(cfg, item).exists())
            acks = []
            def api(_cfg, _method, path, body=None, **_kwargs):
                if path.startswith("/account/clips?"): return {"clips": []}
                acks.append(body); return {"ok": True}
            with mock.patch.object(pull, "request_json", side_effect=api):
                pull.ensure_joined_dependency_ack(cfg, runtime, item, threading.Event())
            self.assertEqual(acks[0]["artifact_id"], item["ledger_artifact_id"])
            acks.clear()
            with mock.patch.object(pull, "request_json", side_effect=api):
                pull.ensure_joined_dependency_ack(cfg, runtime, item, threading.Event())
            self.assertEqual(acks, [])

    def test_wrong_manifest_artifact_id_is_denied_before_media_work(self):
        item = pull.valid_joined_item(self.media_item())
        wrong = {**item, "hour_manifest_id": item["hour_manifest_id"] + 1}
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            self.install_manifest(cfg, item, acknowledged=False)
            def reject(_cfg, _method, path, body=None, **_kwargs):
                if path.startswith("/account/clips?"): return {"clips": []}
                self.assertEqual(body["artifact_id"], wrong["hour_manifest_id"])
                raise pull.ExistingFileMismatch("foreign manifest artifact ID")
            with mock.patch.object(pull, "request_json", side_effect=reject), mock.patch.object(pull, "open_joined_url") as download, \
                 self.assertRaisesRegex(pull.ExistingFileMismatch, "foreign manifest artifact ID"):
                pull.download_joined_item(cfg, runtime, wrong, threading.Event())
            download.assert_not_called()

    def test_media_manifest_mismatch_or_missing_manifest_fails_before_download(self):
        item = pull.valid_joined_item(self.media_item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            with self.assertRaises(pull.ExistingFileMismatch): pull.download_joined_item(cfg, runtime, item, threading.Event())
            path = self.install_manifest(cfg, item); path.write_bytes(self.manifest_bytes({**item, "size_bytes": 7}))
            altered = {**item, "hour_manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            with mock.patch.object(pull, "poll_raw_pending", return_value=False), mock.patch.object(
                pull, "request_json", side_effect=pull.ExistingFileMismatch("wrong dependency ACK identity")
            ), self.assertRaisesRegex(pull.ExistingFileMismatch, "wrong dependency ACK identity"):
                pull.download_joined_item(cfg, runtime, altered, threading.Event())

    def test_hash_is_bounded_cancellable_and_yields_to_raw(self):
        content, item = b"abcdef", pull.valid_joined_item(self.media_item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            directory_fd, _, part, marker = self.names(cfg, item)
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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            self.install_manifest(cfg, item)
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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            directory_fd, _, _part, marker = self.names(cfg, item)
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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            directory_fd, _, _part, marker = self.names(cfg, item)
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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            directory_fd, _, _part, marker = self.names(cfg, item)
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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            directory_fd, _, _part, marker = self.names(cfg, item)
            try:
                self.write_entry(directory_fd, marker, b"conflicting-marker")
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "marker conflicts"):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                self.assertEqual(os.stat(marker, dir_fd=directory_fd).st_size, len(b"conflicting-marker"))
            finally: os.close(directory_fd)

    def test_crash_after_final_link_completes_without_final_sidecar(self):
        content, raw_item = b"abcdef", self.media_item(); item = pull.valid_joined_item(raw_item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            self.install_manifest(cfg, item)
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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            directory_fd, _, part, marker = self.names(cfg, item)
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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            self.install_manifest(cfg, raw_item)
            with mock.patch.object(pull, "storage_status", return_value=self.storage()), mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull, "open_joined_url", return_value=RangeResponse(content, 0, 2, 3)), self.assertRaisesRegex(urllib.error.URLError, "ack unavailable"):
                pull.drain_joined(cfg, runtime, threading.Event())
            final = pull.joined_output_path(cfg, pull.valid_joined_item(raw_item)); before = final.read_bytes()
            with mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull, "open_joined_url") as download:
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            download.assert_not_called(); self.assertEqual(final.read_bytes(), before); self.assertFalse(final.with_name(final.name + ".stoarama.json").exists())

    def test_exact_preexisting_final_is_acked_without_download(self):
        content, raw_item = b"abcdef", self.media_item(); item = pull.valid_joined_item(raw_item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            self.install_manifest(cfg, item)
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
            cfg = self.config(Path(raw))
            runtime = self.runtime(cfg)
            directory_fd, final, part, marker = self.names(cfg, item)
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
                pull.drain_joined(cfg, self.runtime(cfg), threading.Event())


if __name__ == "__main__": unittest.main()
