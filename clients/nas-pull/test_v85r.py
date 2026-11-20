"""Pins, recovered classifier replay, conservative fallback and actual Linux producer."""
import contextlib
import copy
import gzip
import hashlib
import http.server
import io
import threading
import importlib.util
import json
import os
from pathlib import Path
import platform
import py_compile
import subprocess
import sqlite3
import sys
import tarfile
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest import mock

import durham_stitch

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('v85r_pull', ROOT/'stoarama_pull.py')
pull = importlib.util.module_from_spec(spec); spec.loader.exec_module(pull)
spec = importlib.util.spec_from_file_location('v85r_runner', ROOT/'v85r/runner.py')
runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
spec = importlib.util.spec_from_file_location('v85r_build_durham', ROOT/'v85r/build_durham.py')
build_durham = importlib.util.module_from_spec(spec); spec.loader.exec_module(build_durham)


class V85rTests(unittest.TestCase):
    def test_corpus_download_identifies_client_and_rejects_unapproved_bytes(self):
        from types import SimpleNamespace
        requests = []
        body = json.dumps(dict(source_key=[[1, 'a'*64, 10], [2, 'b'*64, 11]],
                               nomination=False, status='UNKNOWN_fixture')).encode() + b'\n'

        class Edge(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append((self.path, self.headers.get('Authorization')))
                if self.headers.get('User-Agent') != pull.USER_AGENT:
                    self.send_error(403, 'Forbidden')
                    return
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        with http.server.ThreadingHTTPServer(('127.0.0.1', 0), Edge) as server:
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                with tempfile.TemporaryDirectory() as state:
                    filename = str(Path(state)/'inventory.sqlite3')
                    with contextlib.closing(sqlite3.connect(filename)) as db:
                        inventory = SimpleNamespace(lock=threading.RLock(), db=db)
                        cfg = SimpleNamespace(inventory_file=filename, api_key='fixture',
                                              api_base='http://127.0.0.1:%d/api/v1' % server.server_port)
                        with self.assertRaisesRegex(pull.StitchHold, 'frozen corpus hash/census differs'):
                            pull.stitch_corpus(cfg)
                        self.assertEqual(db.execute('SELECT count(*) FROM v85r_stitch_corpus_meta').fetchone()[0], 0)
                        self.assertEqual(db.execute('SELECT count(*) FROM v85r_stitch_edges').fetchone()[0], 0)
                self.assertEqual(requests, [('/api/v1/account/connections/stitch/corpus', 'Bearer fixture')])
            finally:
                server.shutdown()
                worker.join()

    def test_all_packaged_files_match_reviewed_pins_and_installed_bytes(self):
        raw=(ROOT/'v85r/manifest.json').read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(),pull.V85R_MANIFEST_SHA)
        manifest=json.loads(raw)
        self.assertEqual(manifest['policy'],'v85r')
        with tempfile.TemporaryDirectory() as state:
            installed=pull.stitch_v85r_bundle(state, threading.Event())
            for name,pin in manifest['files'].items():
                self.assertEqual(hashlib.sha256((installed/name).read_bytes()).hexdigest(),pin,name)
                if name!='remeasure-linux-amd64':
                    self.assertEqual(hashlib.sha256((ROOT/'v85r'/name).read_bytes()).hexdigest(),pin,name)
            (installed/'runner.py').write_text('tampered')
            with self.assertRaisesRegex(pull.StitchHold,'installed V85r member hash'):
                pull.stitch_v85r_bundle(state, threading.Event())

    def test_bundle_tamper_and_wrong_ffmpeg_refused(self):
        with tempfile.TemporaryDirectory() as state, mock.patch.object(pull,'V85R_BUNDLE',('0' if pull.V85R_BUNDLE[0]!='0' else '1')+pull.V85R_BUNDLE[1:]):
            with self.assertRaises((pull.StitchHold,ValueError)):
                pull.stitch_v85r_bundle(state, threading.Event())
        with self.assertRaises(pull.StitchHold):
            pull.stitch_v85r_provenance(dict(ffmpeg=__file__,ffprobe=__file__), threading.Event())

    def test_tool_download_installs_only_hash_verified_bytes(self):
        from types import SimpleNamespace
        body=b'synthetic pinned executable'
        sha=hashlib.sha256(body).hexdigest()
        capability=dict(sha256=sha,size_bytes=len(body),url='https://tools.invalid/artifact',if_match='"fixture"')
        with tempfile.TemporaryDirectory() as state, mock.patch.object(pull.sys,'platform','linux'), \
             mock.patch('platform.machine',return_value='x86_64'), mock.patch.object(pull,'V85R_TOOLS',{'ffmpeg':(sha,len(body))}), \
             mock.patch.object(pull,'request_json',return_value=capability), mock.patch.object(pull.urllib.request,'urlopen',return_value=io.BytesIO(body)):
            tools=pull.stitch_v85r_tools(SimpleNamespace(state_dir=state),time.monotonic()+20,threading.Event())
            self.assertEqual(Path(tools['ffmpeg']).read_bytes(),body)
        with tempfile.TemporaryDirectory() as state, mock.patch.object(pull.sys,'platform','linux'), \
             mock.patch('platform.machine',return_value='x86_64'), mock.patch.object(pull,'V85R_TOOLS',{'ffmpeg':(sha,len(body))}), \
             mock.patch.object(pull,'request_json',return_value=capability), mock.patch.object(pull.urllib.request,'urlopen',return_value=io.BytesIO(b'tampered')):
            with self.assertRaises(pull.StitchHold):
                pull.stitch_v85r_tools(SimpleNamespace(state_dir=state),time.monotonic()+20,threading.Event())
            self.assertEqual(list(Path(state).rglob('ffmpeg')),[])

    def recorded_rows(self):
        raw = (ROOT/'testdata/v85r-october-evidence.json.gz').read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), 'e62739b52a73cdd10dd20ab8d023aab801f0efbb1ef165cf8bba23bf9ce374e2')
        rows = json.loads(gzip.decompress(raw))['rows']
        self.assertEqual(len(rows), 14)
        return rows

    def policy(self):
        c = runner.classifier()
        bad, human = c.C.load_vetoes(ROOT/'v85r/source71.json', ROOT/'v85r/human12.json')
        return c, bad, human

    def test_complete_october_recorded_evidence_replays_through_production_adapter(self):
        c, bad, human = self.policy()
        for case in self.recorded_rows():
            row = case['row']
            with self.subTest(case=case['case']):
                self.assertEqual(runner.decide(c, row, row['source_bindings'], bad, human, set()),
                                 tuple(case['expected']))

    def test_durham_rejects_unbound_lease_and_recovered_attempt(self):
        expires = "2099-01-01T00:00:00+00:00"
        for recovered in (False, True):
            with self.subTest(recovered=recovered), tempfile.TemporaryDirectory() as state:
                state = Path(state)
                task = {"id": 1, "expires_at": expires}
                saved = state / "worker-1" / "attempt.json"
                if recovered:
                    saved.parent.mkdir(mode=0o700)
                    saved.write_text(json.dumps(task))
                http = SimpleNamespace(
                    stop=SimpleNamespace(wait=mock.Mock(side_effect=[False, True])),
                    post=mock.Mock(return_value={"source": "r2", "task": task}),
                    v85r=SimpleNamespace(manifest="native"),
                )
                with mock.patch.object(durham_stitch.pull, "stitch_priority"), mock.patch.object(
                    durham_stitch, "process_task"
                ) as process:
                    durham_stitch.run_worker(http, 1, state, {})
                    process.assert_not_called()
                self.assertEqual(saved.exists(), recovered)

    def test_durham_resume_checks_the_serving_api_before_io(self):
        http = SimpleNamespace(
            post=mock.Mock(return_value={"state": "leased"}),
            v85r=SimpleNamespace(manifest="native"),
        )
        task = dict(id=1, attempt=1, lease_token="lease", plan_sha256="plan")
        with self.assertRaisesRegex(durham_stitch.DurhamError, "API toolchain differs"):
            durham_stitch.process_task(http, task, None, None, None, None)
        self.assertEqual(http.post.call_count, 1)

    def durham(self):
        worker = object.__new__(durham_stitch.DurhamV85r)
        worker.manifest = "a" * 64
        worker.pins = json.loads((ROOT / "v85r/durham-arm64.json").read_bytes())
        return worker

    def pinned_bundle(self, base):
        """A synthetic bundle whose media member names the copied worker bytes."""
        sources = build_durham.worker_source_pins(base)
        media = build_durham.media_toolchain(json.loads(
            (ROOT / "v85r/durham-arm64.json").read_bytes())["tools"], {"lib": "1" * 64})
        media["sources"] = sources
        files = {"media-toolchain.json": (json.dumps(media, indent=2, sort_keys=True) + "\n").encode()}
        manifest = {"policy": "v85r", "files": {name: hashlib.sha256(body).hexdigest()
                                                for name, body in files.items()}}
        files["manifest.json"] = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as packed:
            for name, body in sorted(files.items()):
                info = tarfile.TarInfo(name)
                info.size, info.mode = len(body), 0o600
                packed.addfile(info, io.BytesIO(body))
        compressed = io.BytesIO()
        with gzip.GzipFile(filename="", fileobj=compressed, mode="wb", mtime=0) as output:
            output.write(archive.getbuffer())
        blob = compressed.getvalue()
        profile = Path(base) / "v85r"
        profile.mkdir(mode=0o700)
        (profile / "durham-arm64.tar.gz").write_bytes(blob)
        (profile / "durham-arm64.json").write_bytes(json.dumps(dict(
            json.loads((ROOT / "v85r/durham-arm64.json").read_bytes()),
            bundle_sha256=hashlib.sha256(blob).hexdigest(), bundle_size_bytes=len(blob),
            manifest_sha256=hashlib.sha256(files["manifest.json"]).hexdigest()), sort_keys=True).encode())
        return sources

    def child_env(self):
        """Service-parity child environment: no state, no loader overrides.

        The unit scrubs QEMU_*/LD_* with UnsetEnvironment; CI runners inherit
        them, and main refuses them before the intentional missing state dir.
        """
        return {key: value for key, value in os.environ.items()
                if key != "STOARAMA_STATE_DIR" and not key.startswith(("LD_", "QEMU_"))}

    def run_direct(self, base, env):
        """Run the real direct-__main__ entry exactly as the unit does."""
        return subprocess.run([sys.executable, "-E", "-s", "-B", str(base / "durham_stitch.py")],
                              cwd=base, capture_output=True, text=True, timeout=300, env=env)

    def test_preloaded_helpers_are_refused_by_the_verified_entry(self):
        with tempfile.TemporaryDirectory() as scratch:
            base = Path(scratch).resolve()
            self.copy_worker_sources(base)
            self.pinned_bundle(base)
            # This module imported durham_stitch by name, so the canonical
            # helper is already loaded and the verified entry must refuse.
            self.assertIn("stoarama_pull", sys.modules)
            with self.assertRaisesRegex(durham_stitch.DurhamError, "already loaded"):
                durham_stitch.verified_startup(base)

    def test_direct_main_ignores_an_unchecked_stale_pyc_for_a_verified_source(self):
        with tempfile.TemporaryDirectory() as scratch:
            base = Path(scratch).resolve()
            self.copy_worker_sources(base)
            self.pinned_bundle(base)
            # A .pyc compiled UNCHECKED_HASH is never validated against its
            # source, so -B alone would execute it even after a verified digest.
            sentinel = base / "sentinel.py"
            sentinel.write_bytes(b"raise SystemExit('cached fixture executed')\n")
            py_compile.compile(str(sentinel), doraise=True, cfile=importlib.util.cache_from_source(
                str(base / "stoarama_pull.py")), invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH)
            env = self.child_env()
            # -E -s -B still admits an UNCHECKED_HASH cache written for the
            # pinned source, so the control proves the stale bytecode is live.
            control = subprocess.run([sys.executable, "-E", "-s", "-B", "-c", "import stoarama_pull"],
                                     cwd=base, capture_output=True, text=True, timeout=120, env=env)
            self.assertEqual(control.returncode, 1)
            self.assertIn("cached fixture executed", control.stderr)
            direct = self.run_direct(base, env)
            # The verified entry runs the checked source and reaches main's own
            # state lookup, never the cached sentinel and never a NameError.
            self.assertNotEqual(direct.returncode, 0)
            self.assertNotIn("cached fixture executed", direct.stderr)
            self.assertNotIn("NameError", direct.stderr)
            self.assertIn("STOARAMA_STATE_DIR", direct.stderr)
            (base / "delivery_coverage.py").write_bytes(b"# changed\n")
            tampered = self.run_direct(base, env)
            self.assertNotEqual(tampered.returncode, 0)
            self.assertIn("Durham worker source identity differs", tampered.stderr)
            self.assertNotIn("cached fixture executed", tampered.stderr)

    def test_direct_main_refuses_unbounded_pins_and_special_files(self):
        with tempfile.TemporaryDirectory() as scratch:
            base = Path(scratch).resolve()
            self.copy_worker_sources(base)
            self.pinned_bundle(base)
            pins = json.loads((base / "v85r/durham-arm64.json").read_bytes())
            env = self.child_env()
            for size, reason in ((0, "pins differ"), (1024 ** 3, "pins differ")):
                (base / "v85r/durham-arm64.json").write_bytes(
                    json.dumps(dict(pins, bundle_size_bytes=size)).encode())
                ran = self.run_direct(base, env)
                self.assertNotEqual(ran.returncode, 0)
                self.assertIn("Durham toolchain " + reason, ran.stderr)
            (base / "v85r/durham-arm64.json").write_bytes(json.dumps(pins).encode())
            # A FIFO swapped in for the bundle fails promptly, not by blocking.
            bundle = base / "v85r/durham-arm64.tar.gz"
            bundle.unlink()
            os.mkfifo(bundle)
            ran = self.run_direct(base, env)
            self.assertNotEqual(ran.returncode, 0)
            self.assertIn("resolved regular file", ran.stderr)

    def test_imported_main_refuses_to_lease_without_verified_startup(self):
        self.assertIsNone(durham_stitch._VERIFIED)
        with mock.patch.object(durham_stitch, "run_worker") as worker:
            with self.assertRaisesRegex(durham_stitch.DurhamError, "not loaded from verified"):
                durham_stitch.main()
            worker.assert_not_called()

    def copy_worker_sources(self, base):
        for name in durham_stitch.WORKER_SOURCES:
            (base / name).write_bytes((ROOT / name).read_bytes())

    def test_worker_source_pins_are_the_same_four_exact_files_on_both_sides(self):
        self.assertEqual(durham_stitch.AUDIO_PROOF, "aac-lc-primed-pcm-v1")
        self.assertEqual(durham_stitch.AUDIO_PROOF, pull.STITCH_AUDIO_PROOF)
        self.assertEqual(durham_stitch.AUDIO_PROOF, build_durham.AUDIO_PROOF)
        self.assertEqual(sorted(durham_stitch.WORKER_SOURCES),
                         sorted(("delivery_coverage.py", "durham_aac.py", "durham_stitch.py", "stoarama_pull.py")))
        self.assertEqual(sorted(build_durham.WORKER_SOURCES), sorted(durham_stitch.WORKER_SOURCES))
        pins = build_durham.worker_source_pins()
        self.assertEqual(sorted(pins), sorted(durham_stitch.WORKER_SOURCES))
        for name, sha in pins.items():
            self.assertRegex(sha, "^[0-9a-f]{64}$")
            self.assertEqual(sha, hashlib.sha256((ROOT / name).read_bytes()).hexdigest(), name)
        # Deterministic across calls, and the four reviewed scripts only.
        self.assertEqual(pins, build_durham.worker_source_pins())
        with tempfile.TemporaryDirectory() as scratch:
            base = Path(scratch).resolve()
            self.copy_worker_sources(base)
            self.assertEqual(build_durham.worker_source_pins(base), pins)
            (base / durham_stitch.WORKER_SOURCES[0]).write_bytes(b"# changed\n")
            changed = build_durham.worker_source_pins(base)
            self.assertEqual(sorted(changed), sorted(durham_stitch.WORKER_SOURCES))
            self.assertNotEqual(changed[durham_stitch.WORKER_SOURCES[0]],
                                pins[durham_stitch.WORKER_SOURCES[0]])
            for name in durham_stitch.WORKER_SOURCES[1:]:
                self.assertEqual(changed[name], pins[name])

    def test_media_toolchain_binds_audio_mode_and_worker_sources(self):
        pins = json.loads((ROOT / "v85r/durham-arm64.json").read_bytes())
        media = build_durham.media_toolchain(pins["tools"], {"lib": "1" * 64})
        self.assertEqual(media["audio_proof"], "aac-lc-primed-pcm-v1")
        self.assertEqual(media["sources"], build_durham.worker_source_pins())
        self.assertEqual(sorted(media["sources"]), sorted(durham_stitch.WORKER_SOURCES))
        self.assertEqual(media["tools"], pins["tools"])
        self.assertEqual(media["version"], pins["media_tool_version"])
        # The media identity cannot be handed caller-supplied worker digests,
        # and changed worker bytes change the computed map it would bundle.
        with self.assertRaises(TypeError):
            build_durham.media_toolchain(pins["tools"], {"lib": "1" * 64},
                                         {"durham_aac.py": "0" * 64})
        with tempfile.TemporaryDirectory() as scratch:
            base = Path(scratch).resolve()
            self.copy_worker_sources(base)
            (base / "durham_aac.py").write_bytes(b"# changed\n")
            changed = build_durham.worker_source_pins(base)
            self.assertNotEqual(changed["durham_aac.py"], media["sources"]["durham_aac.py"])
            self.assertNotEqual(changed, media["sources"])

    def test_verify_media_requires_the_reviewed_audio_mode_and_exact_sources(self):
        worker = self.durham()
        media = build_durham.media_toolchain(worker.pins["tools"], {"lib": "1" * 64})
        sources = worker.verify_media(media, threading.Event())
        self.assertEqual(sources, build_durham.worker_source_pins())
        self.assertEqual(worker.recheck_sources(threading.Event()), sources)
        for mutate in (lambda m: m.update(audio_proof="aac-lc-primed-pcm-v2"),
                       lambda m: m.pop("audio_proof"),
                       lambda m: m.update(sources={})):
            broken = dict(copy.deepcopy(media))
            mutate(broken)
            with self.assertRaises(durham_stitch.DurhamError):
                worker.verify_media(broken, threading.Event())

    def test_changed_missing_extra_or_misbound_worker_source_fails_verification(self):
        pins = build_durham.worker_source_pins()
        with tempfile.TemporaryDirectory() as scratch:
            base = Path(scratch).resolve()
            self.copy_worker_sources(base)
            paths = {name: base / name for name in durham_stitch.WORKER_SOURCES}
            worker = self.durham()
            with mock.patch.object(durham_stitch.DurhamV85r, "source_files", return_value=paths):
                self.assertEqual(worker.verify_sources(pins, threading.Event()), pins)

                def broken(mutate, pins, reason):
                    mutate()
                    try:
                        with self.assertRaisesRegex(durham_stitch.DurhamError, reason):
                            worker.verify_sources(pins, threading.Event())
                    finally:
                        for name in durham_stitch.WORKER_SOURCES:
                            path = base / name
                            if path.is_symlink():
                                path.unlink()
                        for name in ("extra_worker.py", "link.py"):
                            path = base / name
                            if path.is_symlink() or path.exists():
                                path.unlink()
                        self.copy_worker_sources(base)

                broken(lambda: (base / "stoarama_pull.py").write_bytes(b"# changed\n"),
                       pins, "worker source identity differs")
                broken(lambda: (base / "durham_stitch.py").unlink(),
                       pins, "resolved regular file")
                broken(lambda: (base / "extra_worker.py").write_text("x\n"),
                       dict(pins, extra_worker=hashlib.sha256(b"x\n").hexdigest()),
                       "worker source set differs")
                swapped = dict(pins)
                swapped["durham_stitch.py"], swapped["stoarama_pull.py"] = (
                    pins["stoarama_pull.py"], pins["durham_stitch.py"])
                broken(lambda: None, swapped, "worker source identity differs")
                broken(lambda: os.symlink(base / "durham_stitch.py", base / "link.py"),
                       dict(pins, **{"link.py": pins["durham_stitch.py"]}), "worker source set differs")
                broken(lambda: (base / "durham_aac.py").unlink() or os.symlink(
                           base / "durham_stitch.py", base / "durham_aac.py"),
                       pins, "resolved regular file")
            with self.assertRaisesRegex(durham_stitch.DurhamError, "not verified"):
                self.durham().recheck_sources(threading.Event())

    def test_durham_rechecks_pinned_sources_before_stitch_build(self):
        for reason in ("worker source identity differs", None):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as state:
                root = Path(state).resolve()
                http = SimpleNamespace(
                    post=mock.Mock(return_value={"toolchain": "a" * 64, "state": "leased"}),
                    stop=threading.Event(),
                    v85r=SimpleNamespace(manifest="a" * 64,
                                         recheck_sources=mock.Mock(side_effect=(
                                             None if reason is None else durham_stitch.DurhamError(reason)))),
                )
                task = dict(id=1, attempt=1, lease_token="lease", plan_sha256="plan",
                            expires_at="2099-01-01T00:00:00+00:00",
                            plan=dict(clips=[dict(size_bytes=1)]))
                with mock.patch.object(durham_stitch.pull, "stitch_resources"), \
                        mock.patch.object(durham_stitch, "fetch_clip"), \
                        mock.patch.object(durham_stitch.pull, "stitch_build") as build:
                    if reason is None:
                        build.side_effect = durham_stitch.DurhamError("stop here")
                        with self.assertRaisesRegex(durham_stitch.DurhamError, "stop here"):
                            durham_stitch.process_task(http, task, root, root, None, {})
                        self.assertEqual(build.call_count, 1)
                    else:
                        with self.assertRaisesRegex(durham_stitch.DurhamError, reason):
                            durham_stitch.process_task(http, task, root, root, None, {})
                        build.assert_not_called()
                self.assertEqual(http.v85r.recheck_sources.call_count, 1)
                self.assertFalse((root / "result.json").exists())

    def test_durham_http_emits_r2_v3_source_and_toolchain_headers(self):
        seen = {}

        class Response:
            status = 200
            def read(self, limit):
                return b'{"task": null}'
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False

        class Connection:
            def __init__(self, host, port, timeout):
                seen["origin"] = (host, port, timeout)
            def request(self, method, path, headers=None, body=None):
                seen.update(method=method, path=path, headers=headers, body=body)
            def getresponse(self):
                return Response()
            def close(self):
                pass

        manifest = "b" * 64
        http = durham_stitch.DurhamHTTP("https://api.invalid", "https://r2.invalid", "token",
                                        threading.Event(), SimpleNamespace(manifest=manifest))
        with mock.patch.object(durham_stitch, "HTTPSConnection", Connection):
            self.assertEqual(http.post("lease", {}), {"task": None})
        self.assertEqual(seen["path"], "/account/connections/stitch/lease")
        self.assertEqual(seen["headers"]["X-Stoarama-Stitch-Source"], "r2-v3")
        self.assertEqual(seen["headers"]["X-Stoarama-Stitch-Toolchain"], manifest)
        self.assertEqual(seen["headers"]["Authorization"], "Bearer token")
        self.assertEqual(seen["origin"][:2], ("api.invalid", 443))

    def test_durham_provenance_emits_reviewed_audio_proof(self):
        worker = self.durham()
        provenance = worker.provenance()
        self.assertEqual(provenance["audio_proof"], "aac-lc-primed-pcm-v1")
        self.assertEqual(provenance["architecture"], "linux-arm64")
        self.assertEqual(provenance["manifest_sha256"], "a" * 64)
        self.assertEqual(provenance["ffmpeg_sha256"], worker.pins["tools"]["ffmpeg"]["sha256"])
        self.assertEqual(provenance["ffprobe_sha256"], worker.pins["tools"]["ffprobe"]["sha256"])
        self.assertFalse(provenance["october_ffmpeg_recovered"])
        self.assertEqual(provenance["policy"], pull.STITCH_POLICY)

    def test_service_excludes_external_python_environment(self):
        unit = (ROOT / "stoarama-durham-stitch.service").read_text()
        self.assertIn("ExecStart=/usr/bin/python3 -E -s -B /opt/stoarama-stitch/durham_stitch.py\n", unit)
        self.assertNotIn("python3 -B ", unit)

    def test_only_the_durham_measure_child_disables_its_bytecode_cache(self):
        tail = [str(ROOT / "v85r/runner.py"), str(ROOT / "request.json")]
        for kind, expected in ((pull.StitchRunKind.DURHAM_R2,
                                [sys.executable, "-X", "pycache_prefix=/dev/null", "-I", "-B", *tail]),
                               (pull.StitchRunKind.PRODUCTION, [sys.executable, "-I", "-B", *tail])):
            stop = SimpleNamespace(kind=kind)
            self.assertEqual(pull.stitch_measure_argv(ROOT / "v85r", ROOT / "request.json", stop), expected)

    @unittest.skipUnless(os.environ.get("V85R_DURHAM_TEST_BUNDLE"), "generated Durham bundle required")
    def test_native_durham_bundle_replays_frozen_october_evidence(self):
        pins = json.loads((ROOT / "v85r/durham-arm64.json").read_bytes())
        blob = Path(os.environ["V85R_DURHAM_TEST_BUNDLE"]).read_bytes()
        self.assertEqual(len(blob), pins["bundle_size_bytes"])
        with tempfile.TemporaryDirectory() as state:
            bundle = pull.stitch_unpack_v85r(blob, pins["bundle_sha256"], pins["manifest_sha256"], state, threading.Event())
            native_source = (bundle / "runner.py").read_bytes()
            legacy_source = (ROOT / "v85r/runner.py").read_bytes()
            self.assertEqual(native_source.count(b"remeasure-linux-arm64"), 2)
            self.assertEqual(native_source.replace(b"remeasure-linux-arm64", b"remeasure-linux-amd64"), legacy_source)
            self.assertFalse((bundle / "remeasure-linux-amd64").exists())
            evaluator = (bundle / "remeasure-linux-arm64").read_bytes()
            self.assertEqual(hashlib.sha256(evaluator).hexdigest(), pins["evaluator_sha256"])
            self.assertEqual(int.from_bytes(evaluator[18:20], "little"), 183)
            media = json.loads((bundle / "media-toolchain.json").read_bytes())
            self.assertEqual(media["audio_proof"], durham_stitch.AUDIO_PROOF)
            self.assertEqual(sorted(media["sources"]), sorted(durham_stitch.WORKER_SOURCES))
            self.assertTrue(all(len(sha) == 64 for sha in media["sources"].values()))
            self.assertEqual(media["tools"], pins["tools"])

            spec = importlib.util.spec_from_file_location("v85r_durham_runner", bundle / "runner.py")
            native = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(native)
            classifier = native.classifier()
            bad, human = classifier.C.load_vetoes(bundle / "source71.json", bundle / "human12.json")
            for case in self.recorded_rows():
                row = case["row"]
                with self.subTest(case=case["case"]):
                    actual = native.decide(classifier, row, row["source_bindings"], bad, human, set())
                    self.assertEqual(actual, tuple(case["expected"]))

    def stationary_row(self):
        row = copy.deepcopy(self.recorded_rows()[0]['row'])
        spatial = row['spatial_temporal_diagnostic']
        spatial.update(tail_dx_pixels_per_frame=0, tail_dy_pixels_per_frame=0,
                       head_dx_pixels_per_frame=0, head_dy_pixels_per_frame=0,
                       motion_observable=False, ambiguities=['no_observable_motion'],
                       texture_gradient_mad=3, minimum_spatial_novelty_margin=2, heldout_pixels=1000,
                       forward_heldout_residual_mad=10, backward_heldout_residual_mad=10,
                       internal_heldout_residual_mad=.1, direction_speed_disagreement=0,
                       max_acceleration_pixels_per_frame_squared=0, boundary_unwarped_mad=10)
        row['spatial_provenance']['diagnostic_sha256'] = hashlib.sha256(json.dumps(
            spatial, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        return row

    def test_stationary_successor_veto_reaches_adapter_and_runtime(self):
        c, bad, human = self.policy()
        row = self.stationary_row()
        result = c.decision(row, bad, human)
        self.assertTrue(result['baselineK_nomination'])
        self.assertEqual(result['status'], 'REJECT_stationary_severe_seam_forecast')
        self.assertEqual(runner.decide(c, row, row['source_bindings'], bad, human, set()),
                         (False, result['status']))
        request = dict(prev=row['source_bindings'][0], next=row['source_bindings'][1],
                       tools=dict(ffmpeg='fixture', ffprobe='fixture'))
        # The live producer projects bindings to these three identity fields.
        row['source_bindings'] = [dict(clip_id=b['clip_id'], sha256=b['sha256'], size_bytes=b['size_bytes'])
                                  for b in row['source_bindings']]
        row['spatial_provenance']['source_bindings'] = row['source_bindings']
        native = dict(evidence_row=row, diagnostic_window20=row['diagnostic_window20'])
        with mock.patch.object(runner.subprocess, 'run', return_value=mock.Mock(stdout=json.dumps(native))):
            decision = runner.run(request)
        self.assertEqual(decision['status'], 'REJECT_stationary_severe_seam_forecast')
        self.assertEqual(decision['decision'], 'split')
        self.assertFalse(decision['join'])

    def test_missing_or_unauthenticated_evidence_holds_a_baseline_positive(self):
        c, bad, human = self.policy()
        original = self.recorded_rows()[0]['row']
        for field in ('full_source_integrity', 'pair', 'spatial_temporal_diagnostic', 'spatial_provenance',
                      'research_packet_replay', 'research_audio_inventory'):
            row = copy.deepcopy(original)
            del row[field]
            self.assertEqual(runner.decide(c, row, row['source_bindings'], bad, human, set()),
                             (False, 'UNKNOWN_missing_authenticated_evidence'), field)
        for field in ('motion_observable', 'ambiguities', 'heldout_pixels', 'forward_heldout_residual_mad'):
            row = copy.deepcopy(original)
            del row['spatial_temporal_diagnostic'][field]
            row['spatial_provenance']['diagnostic_sha256'] = hashlib.sha256(json.dumps(
                row['spatial_temporal_diagnostic'], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            self.assertEqual(runner.decide(c, row, row['source_bindings'], bad, human, set()),
                             (False, 'UNKNOWN_missing_authenticated_evidence'), field)
        for mutate in (lambda r: r['full_source_integrity']['contexts'][0].update(VerifiedProofSHA='0'*64),
                       lambda r: r['spatial_provenance'].update(diagnostic_sha256='0'*64),
                       lambda r: r['spatial_provenance'].update(original_endpoint_frames_preserved=False),
                       lambda r: r['pair']['prev'].update(sha256='0'*64)):
            row = copy.deepcopy(original)
            mutate(row)
            self.assertFalse(runner.decide(c, row, row['source_bindings'], bad, human, set())[0])

    def test_spatial_positive_research_route_never_promotes(self):
        c, bad, human = self.policy()
        row = self.stationary_row()
        spatial = row['spatial_temporal_diagnostic']
        spatial.update(tail_dx_pixels_per_frame=1, head_dx_pixels_per_frame=1,
                       motion_observable=True, ambiguities=[],
                       forward_heldout_residual_mad=.1, backward_heldout_residual_mad=.1)
        row['spatial_provenance']['diagnostic_sha256'] = hashlib.sha256(json.dumps(
            spatial, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        row['diagnostic_window20']['frame_match'].update(tail_frames=99, head_frames=99)
        for context in row['full_source_integrity']['contexts']:
            context.update(FramePTS=[i*.04 for i in range(1500)], LastFrameDuration=.04,
                           RegisteredCaptureSpan=60)
        # Supply complete matching capture identity, required by that research lane.
        for n, side in enumerate(('prev', 'next')):
            row['pair'][side].update(recording_id=1, recording_job_id=1,
                                     capture_lease_token='fixture', capture_sequence=n+1)
        raw = c.decision(row, bad, human)
        self.assertTrue(raw['spatial_nomination'])
        self.assertFalse(raw['baselineK_nomination'])
        self.assertEqual(runner.decide(c, row, row['source_bindings'], bad, human, set()),
                         (False, 'SPLIT_frozen_score_reject'))

    def test_conservative_fallback(self):
        keys=[[1,'a'*64,10],[2,'b'*64,11]]
        frozen=dict(source_key=keys,nomination=True,status=runner.APPROVED)
        def decide(**kw):
            return runner.resolve(True,runner.APPROVED,keys,**kw)
        self.assertEqual(decide(frozen=frozen)['reason'],'exact_october')
        self.assertTrue(decide()['join'])
        for change in [dict(source_key=[[9,'a'*64,10],keys[1]]),dict(status='UNKNOWN_october'),dict(nomination=False)]:
            self.assertEqual(decide(frozen=dict(frozen,**change))['decision'],'hold')
        for kw in [dict(missing=True),dict(decoder_errors=True)]:
            self.assertEqual(decide(frozen=frozen,**kw)['decision'],'hold')
        for status in ['UNKNOWN_missing','HOLD_native','RESEARCH_nomination','unrecognized']:
            self.assertEqual(runner.resolve(True,status,keys)['decision'],'hold')
        self.assertEqual(runner.resolve(False,'SPLIT_frozen_score_reject',keys)['decision'],'split')
        self.assertTrue(runner.has_decode_error(dict(native=dict(decode_error='failed'))))
        self.assertTrue(runner.has_decode_error(dict(decode_errors=1)))

    @unittest.skipUnless(os.environ.get('V85R_FROZEN_CORPUS'),'exact recovered JSONL required')
    def test_client_indexes_entire_exact_corpus_including_unknown_rows(self):
        import threading
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as state:
            filename=str(Path(state)/'inventory.sqlite3')
            db=sqlite3.connect(filename)
            self.addCleanup(db.close)
            inventory=SimpleNamespace(lock=threading.RLock(),db=db)
            cfg=SimpleNamespace(inventory_file=filename,api_base='https://api.invalid',api_key='fixture')
            with mock.patch.object(pull.urllib.request,'urlopen',side_effect=lambda *a,**kw:open(os.environ['V85R_FROZEN_CORPUS'],'rb')):
                pull.stitch_corpus(cfg)
            self.assertEqual(db.execute('SELECT count(*) FROM v85r_stitch_edges').fetchone()[0],73843)
            self.assertEqual(db.execute('SELECT count(*) FROM v85r_stitch_edges WHERE nomination=1').fetchone()[0],52462)

    @unittest.skipUnless(platform.system()=='Linux' and platform.machine() in ('x86_64','amd64') and os.environ.get('V85R_TEST_TOOLS'),
                         'Linux amd64 with pinned V85R_TEST_TOOLS required')
    def test_actual_packaged_linux_producer_and_frozen_status_hold(self):
        tools={name:str(Path(os.environ['V85R_TEST_TOOLS'])/name) for name in ('ffmpeg','ffprobe')}
        provenance=pull.stitch_v85r_provenance(tools, threading.Event())
        self.assertEqual(provenance['ffmpeg_version'],'N-126826-gc0e8b139fd-20260924')
        with tempfile.TemporaryDirectory() as tmp:
            work=Path(tmp);bundle=pull.stitch_v85r_bundle(work, threading.Event())
            media=work/'source.mp4'
            subprocess.run([tools['ffmpeg'],'-nostdin','-v','error','-f','lavfi','-i','testsrc2=size=128x96:rate=25:duration=60',
                            '-c:v','libx264','-preset','ultrafast','-threads','1','-bf','0','-g','25','-pix_fmt','yuv420p',str(media)],check=True)
            sha=hashlib.sha256(media.read_bytes()).hexdigest()
            a=dict(clip_id=1,recording_id=1,recording_job_id=1,sha256=sha,size_bytes=media.stat().st_size,
                   nominal_seconds=60,start_utc='2026-09-01T00:00:00Z',end_utc='2026-09-01T00:01:00Z')
            b=dict(a,clip_id=2,start_utc=a['end_utc'],end_utc='2026-09-01T00:02:00Z')
            request=dict(prev=a,next=b,prev_path=str(media),next_path=str(media),tools=tools,actual_span=False)
            def measure():
                (work/'request.json').write_text(json.dumps(request))
                p=subprocess.run(['python3','-I','-B',str(bundle/'runner.py'),str(work/'request.json')],capture_output=True,text=True,check=True,timeout=400)
                return json.loads(p.stdout)
            produced = subprocess.run([str(bundle/'remeasure-linux-amd64')], input=json.dumps(request),
                                      capture_output=True, text=True, check=True, timeout=200,
                                      env=dict(os.environ, V85R_FFMPEG=tools['ffmpeg'], V85R_FFPROBE=tools['ffprobe']))
            evidence = json.loads(produced.stdout)['evidence_row']
            self.assertEqual(evidence['source_bindings'], [dict(clip_id=z['clip_id'], sha256=z['sha256'],
                             size_bytes=z['size_bytes']) for z in (a, b)])
            self.assertEqual(evidence['spatial_provenance']['within_side_frame_counts'], [6, 6])
            self.assertTrue(evidence['spatial_provenance']['presentation_order_verified'])
            self.assertEqual(evidence['spatial_temporal_diagnostic']['status'], 'diagnostic_only')
            for context in evidence['full_source_integrity']['contexts']:
                self.assertTrue(context['StrictDecodePassed'])
                self.assertEqual(len(context['FramePTS']), 1500)
                self.assertEqual(context['OwnerPinnedProofSHA'], context['VerifiedProofSHA'])
            actual=measure()
            self.assertIn('diagnostic_sha256',actual) # cannot pass through an import/measurement failure
            self.assertNotEqual(actual['reason'],'measurement_failed')
            request['frozen']=dict(source_key=[[z['clip_id'],sha,z['size_bytes']] for z in (a,b)],
                                   nomination=actual['nomination'],status='UNKNOWN_october_status_difference')
            self.assertEqual(measure()['decision'],'hold')


if __name__=='__main__':unittest.main()
