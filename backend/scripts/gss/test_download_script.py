"""download_script.py against a local fake access server whose generation is
retired mid-download (HTTP 410): it continues on the current generation when
that lists the same files, and stops without downloading from it otherwise."""
import contextlib
import hashlib
import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
API = '/api/v1/global-street-scores-delivery'
STREAM = 379
OLD, NEW, THIRD = 'a' * 32, 'b' * 32, 'e' * 32
HOST = 'c' * 32 + '.r2.cloudflarestorage.com'
MANIFEST = f'/api/coverage-delivery/manifest?stream={STREAM}&scope=available'
WEEK1, WEEK2 = '2026-07-27', '2026-08-03'
FOLDER = f'{STREAM}_Europe_Italy_Assisi_X/July/Monday/'


def body(i, version=0):
    return f'video {i} v{version} '.encode() * 1000


def output(i, version=0, date=WEEK1, data=None):
    data = data or body(i, version)
    return {'id': 1000 + i, 'recording_id': STREAM, 'local_date': date, 'available': True,
            'path': f'{FOLDER}{STREAM}_X_2026_July_W4_Monday_hour_01_part_{i:02d}.mp4',
            'file_url': f'{API}/file/{1000 + i}', 'ticket_presentation_path': True,
            'sha256': hashlib.sha256(data).hexdigest(), 'size_bytes': len(data), '_body': data}


def outputs(*changed, n=5):
    """Outputs 1..n; those in changed carry different bytes."""
    return [output(i, 1 if i in changed else 0) for i in range(1, n + 1)]


def name(o):
    return o['path'].rsplit('/', 1)[1]


def partial_name(o):
    return '.output_' + o['sha256'] + '.' + hashlib.sha256(name(o).encode()).hexdigest()[:16] + '.partial'


class FakeDelivery:
    """The public access API with manifests and tickets per generation, and R2's ranged GET.

    on_ticket(n, gen) runs after the n-th issued ticket (a republish: move
    current, retire generations); current_gen(k) picks the k-th current-manifest
    answer; truncate cuts the first that many current-manifest bodies short."""

    def __init__(self, gens, on_ticket=lambda n, gen: None, current_gen=None, truncate=0, hold=None, on_event=lambda e: None):
        self.gens = gens
        self.bodies = {o['sha256']: o['_body'] for outs in gens.values() for o in outs}
        self.current, self.retired = OLD, set()
        self.on_ticket, self.current_gen, self.truncate, self.hold, self.on_event = on_ticket, current_gen, truncate, hold, on_event
        self.tickets, self.events, self.current_fetches = [], [], 0
        self.lock = threading.Lock()
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                u = urllib.parse.urlsplit(self.path)
                q = dict(urllib.parse.parse_qsl(u.query))
                gen = q.get('generation')
                if gen in fake.retired:
                    fake.event(('410', u.path))
                    return self.reply(410, {'error': 'This download link is outdated; copy a fresh command from the page.'})
                if u.path == API + '/asset' and q['path'] == MANIFEST:
                    cut = False
                    if gen is None:
                        with fake.lock:
                            k = fake.current_fetches = fake.current_fetches + 1
                        gen = fake.current_gen(k) if fake.current_gen else fake.current
                        fake.event(('current', gen))
                        cut = k <= fake.truncate
                    return self.reply(200, fake.manifest(gen), cut)
                if u.path.startswith(API + '/ticket/'):
                    return self.reply(200, fake.ticket(gen, int(u.path.rsplit('/', 1)[1]), q))
                self.reply(404, {'error': 'not found'})

            def reply(self, status, obj, cut=False):
                raw = json.dumps(obj).encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw[:len(raw) // 2] if cut else raw)

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.origin = 'http://127.0.0.1:%d' % self.server.server_address[1]

    def event(self, e):
        self.events.append(e)
        self.on_event(e)

    def manifest(self, gen):
        return {'schema': 2, 'collection': 'verified-assembled', 'recording_id': STREAM, 'period': [WEEK1, '2026-08-09'],
                'generation': gen, 'outputs': [dict({k: v for k, v in o.items() if k != '_body'}, delivery_generation=gen)
                                               for o in self.gens[gen]]}

    def ticket(self, gen, file_id, q):
        o = next(o for o in self.gens[gen] if o['id'] == file_id)
        assert q['sha256'] == o['sha256'] and q['path'] == o['path']
        with self.lock:
            self.tickets.append((gen, file_id))
            self.event(('ticket', gen, file_id))
            self.on_ticket(len(self.tickets), gen)
        path = '/stoarama/joined/' + o['sha256'] + '.mp4'
        md5 = hashlib.md5(o['_body']).hexdigest()
        return {'transport': 'r2-direct', 'storage_host': HOST, 'sha256': o['sha256'], 'size_bytes': o['size_bytes'],
                'filename': name(o), 'ttl_seconds': 300, 'expires_at': time.time() + 300,
                'object_path_sha256': hashlib.sha256(path.encode()).hexdigest(), 'etag': '"%s"' % md5,
                'required_headers': {'If-Match': '"%s"' % md5}, 'test_generation': gen, 'test_id': file_id,
                'url': 'https://%s%s?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=300&X-Amz-SignedHeaders=host%%3Bif-match'
                       '&X-Amz-Signature=%s' % (HOST, path, 'd' * 64)}

    def storage(self, t, o, start):
        """R2's ranged GET for a ticket, which stays valid after its generation is retired."""
        self.event(('storage', t['test_generation'], t['test_id'], start))
        if self.hold:
            self.hold(t['test_id'])
        data = self.bodies[t['sha256']][start:]
        response = io.BytesIO(data)
        response.status = 206
        response.headers = {'ETag': t['etag'], 'Content-Length': str(len(data)),
                            'Content-Range': 'bytes %d-%d/%d' % (start, o['size_bytes'] - 1, o['size_bytes'])}
        return response

    def republish(self, retire, current):
        self.retired.add(retire)
        self.current = current


class DownloadScriptTests(unittest.TestCase):
    def setUp(self):
        d = tempfile.TemporaryDirectory()
        self.addCleanup(d.cleanup)
        self.dest = d.name

    def run_script(self, fake, jobs=2, week=None):
        ns = {'__name__': 'gss_download_script'}
        with open(os.path.join(HERE, 'download_script.py'), 'rb') as f:
            exec(compile(f.read(), 'download_script.py', 'exec'), ns)
        ns['storage'] = fake.storage
        argv = ['download-script.py', '--stream', str(STREAM), '--scope', 'available', '--jobs', str(jobs),
                '--destination', self.dest, '--server', fake.origin, '--dataset-api', API, '--generation', OLD]
        argv += ['--week', str(week)] if week else []
        out, err = io.StringIO(), io.StringIO()
        old_argv, sys.argv = sys.argv, argv
        try:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = ns['main']()
        finally:
            sys.argv = old_argv
            urllib.request.install_opener(None)  # main() installs a no-redirect opener
            fake.server.shutdown()
            fake.server.server_close()
        return code, out.getvalue(), err.getvalue()

    def files(self):
        return {p.name: p.read_bytes() for p in Path(self.dest).rglob('*.mp4')}

    def expect_files(self, outs):
        self.assertEqual(self.files(), {name(o): o['_body'] for o in outs})
        self.assertEqual(list(Path(self.dest).rglob('.output_*')), [])

    def receipts(self):
        return {r['generation']: r for r in (json.loads(p.read_text()) for p in Path(self.dest).glob('*.manifest*.json'))}

    def expect_receipts(self, fake, gens, week=None):
        receipts = self.receipts()
        self.assertEqual(sorted(receipts), sorted(gens))
        for gen in gens:
            m = fake.manifest(gen)
            if week:
                m = dict(m, outputs=[o for o in m['outputs'] if o['local_date'] == WEEK1], selected_week=week)
            self.assertEqual(receipts[gen], m)

    def test_unchanged_files_continue_on_the_new_generation(self):
        fake = FakeDelivery({OLD: outputs(), NEW: outputs()},
                            on_ticket=lambda n, gen: n == 3 and fake.republish(OLD, NEW))
        code, out, err = self.run_script(fake)
        self.assertFalse(code, err)
        self.assertIn(f'Dataset generation {OLD} was retired; generation {NEW} lists the same 5 files', out)
        self.expect_files(outputs())
        self.expect_receipts(fake, (OLD, NEW))
        gens = [g for g, _ in fake.tickets]
        self.assertEqual((gens.count(OLD), gens.count(NEW)), (3, 2))
        self.assertEqual(sorted(i for _, i in fake.tickets), [1001, 1002, 1003, 1004, 1005])

    def test_changed_files_stop_without_mixing_generations(self):
        fake = FakeDelivery({OLD: outputs(), NEW: outputs(5)},
                            on_ticket=lambda n, gen: n == 2 and fake.republish(OLD, NEW))
        code, out, err = self.run_script(fake)
        self.assertTrue(code)
        self.assertIn(f'Stopped: dataset generation {OLD} was retired, and the current generation {NEW} changes this download: '
                      '0 files added, 0 removed, 1 changed.', err)
        self.assertIn('  changed: ' + output(5)['path'], err)
        self.assertIn(f'No files were downloaded from {NEW}. Completed outputs and any partial downloads have been kept. '
                      'Copy a fresh command from the page to continue.', err)
        self.assertIn('2 outputs complete; 0 unavailable or failed; 3 not started.', out)
        self.assertEqual([g for g, _ in fake.tickets], [OLD, OLD])
        self.expect_files([o for o in outputs() if (OLD, o['id']) in fake.tickets])
        self.expect_receipts(fake, (OLD,))

    def test_transfer_in_flight_finishes_before_the_switch(self):
        refused = threading.Event()
        first = []

        def on_ticket(n, gen):
            if n == 1:
                first.append(fake.tickets[0][1])
                fake.republish(OLD, NEW)

        def hold(file_id):
            if file_id == first[0] and not refused.wait(10):
                raise AssertionError('no 410 while the first transfer was open')
        fake = FakeDelivery({OLD: outputs(), NEW: outputs()}, on_ticket=on_ticket, hold=hold,
                            on_event=lambda e: e[0] == '410' and refused.set())
        code, out, err = self.run_script(fake, jobs=2)
        self.assertFalse(code, err)
        kinds = [e[0] for e in fake.events]
        self.assertLess(kinds.index('410'), kinds.index('current'))
        self.assertEqual(fake.tickets[0], (OLD, first[0]))
        self.assertEqual([g for g, _ in fake.tickets].count(OLD), 1)
        self.expect_files(outputs())
        self.expect_receipts(fake, (OLD, NEW))

    def test_seeded_partial_resumes_across_the_switch(self):
        o4 = output(4)
        folder = Path(self.dest, FOLDER)
        folder.mkdir(parents=True)
        half = o4['size_bytes'] // 2
        (folder / partial_name(o4)).write_bytes(o4['_body'][:half])
        fake = FakeDelivery({OLD: outputs(), NEW: outputs()},
                            on_ticket=lambda n, gen: n == 3 and fake.republish(OLD, NEW))
        code, out, err = self.run_script(fake, jobs=1)
        self.assertFalse(code, err)
        self.assertEqual(fake.tickets, [(OLD, 1001), (OLD, 1002), (OLD, 1003), (NEW, 1004), (NEW, 1005)])
        self.assertIn(('storage', NEW, 1004, half), fake.events)
        self.expect_files(outputs())

    def test_week_selection_decides_what_counts_as_changed(self):
        def week_outputs(changed=()):
            return [output(i, 1 if i in changed else 0, WEEK1 if i <= 3 else WEEK2) for i in range(1, 6)]
        for changed, stops in ((5, False), (2, True)):
            with self.subTest(changed=changed):
                self.setUp()
                fake = FakeDelivery({OLD: week_outputs(), NEW: week_outputs((changed,))},
                                    on_ticket=lambda n, gen: n == 1 and fake.republish(OLD, NEW))
                code, out, err = self.run_script(fake, jobs=1, week=1)
                if stops:
                    self.assertTrue(code)
                    self.assertIn('0 files added, 0 removed, 1 changed.', err)
                    self.assertIn('  changed: ' + output(2)['path'], err)
                    self.expect_files(week_outputs()[:1])
                    self.expect_receipts(fake, (OLD,), week=1)
                else:
                    self.assertFalse(code, err)
                    self.assertIn(f'generation {NEW} lists the same 3 files', out)
                    self.expect_files(week_outputs()[:3])
                    self.expect_receipts(fake, (OLD, NEW), week=1)

    def test_second_retirement_after_switching(self):
        def on_ticket(n, gen):
            if n == 2:
                fake.republish(OLD, NEW)
            if n == 4:
                fake.republish(NEW, THIRD)
        fake = FakeDelivery({OLD: outputs(), NEW: outputs(), THIRD: outputs()}, on_ticket=on_ticket)
        code, out, err = self.run_script(fake, jobs=1)
        self.assertFalse(code, err)
        self.assertEqual([g for g, _ in fake.tickets], [OLD, OLD, NEW, NEW, THIRD])
        self.assertIn(f'Dataset generation {OLD} was retired; generation {NEW}', out)
        self.assertIn(f'Dataset generation {NEW} was retired; generation {THIRD}', out)
        self.expect_files(outputs())
        self.expect_receipts(fake, (OLD, NEW, THIRD))

    def test_switches_without_progress_are_bounded(self):
        fresh = ['%032x' % k for k in range(1, 9)]
        cases = {
            # Every current generation is retired as soon as it is served.
            'immediately retired': lambda k: fresh[k - 1],
            # A stale cache alternates A -> B -> A, both retired.
            'A B A': lambda k: (NEW, OLD)[(k - 1) % 2],
        }
        for case, current_gen in cases.items():
            with self.subTest(case):
                self.setUp()
                gens = {g: outputs() for g in [OLD, NEW] + fresh}
                fake = FakeDelivery(gens, current_gen=current_gen,
                                    on_ticket=lambda n, gen: n == 1 and fake.retired.update(gens))
                code, out, err = self.run_script(fake, jobs=2)
                self.assertTrue(code)
                self.assertIn('were each retired before another file finished. Copy a fresh command from the page to continue.', err)
                self.assertEqual(fake.current_fetches, 3)
                self.assertEqual(len(fake.tickets), 1)
                self.expect_files([o for o in outputs() if o['id'] == fake.tickets[0][1]])

    def test_truncated_current_manifest_is_retried(self):
        fake = FakeDelivery({OLD: outputs(), NEW: outputs()}, truncate=1,
                            on_ticket=lambda n, gen: n == 2 and fake.republish(OLD, NEW))
        code, out, err = self.run_script(fake, jobs=2)
        self.assertFalse(code, err)
        self.assertNotIn('Traceback', err)
        self.assertEqual(fake.current_fetches, 2)
        self.expect_files(outputs())
        self.expect_receipts(fake, (OLD, NEW))

    def test_same_bytes_in_one_folder_never_share_a_partial(self):
        same = b'identical video ' * 4000
        outs = [output(1, data=same), output(2, data=same)]
        both = threading.Barrier(2, timeout=10)
        fake = FakeDelivery({OLD: outs}, hold=lambda file_id: both.wait())
        code, out, err = self.run_script(fake, jobs=2)
        self.assertFalse(code, err)
        self.expect_files(outs)

    def test_retired_before_start(self):
        fake = FakeDelivery({OLD: outputs()})
        fake.retired.add(OLD)
        with self.assertRaisesRegex(SystemExit, f'Dataset generation {OLD} is retired. Copy a fresh command'):
            self.run_script(fake)


if __name__ == '__main__':
    unittest.main()
