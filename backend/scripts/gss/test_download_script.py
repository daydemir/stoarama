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
OLD, NEW = 'a' * 32, 'b' * 32
HOST = 'c' * 32 + '.r2.cloudflarestorage.com'
MANIFEST = f'/api/coverage-delivery/manifest?stream={STREAM}&scope=available'


def body(i, version=0):
    return f'video {i} v{version} '.encode() * 1000


def output(i, version=0):
    data = body(i, version)
    return {'id': 1000 + i, 'recording_id': STREAM, 'local_date': '2026-07-27', 'available': True,
            'path': f'{STREAM}_Europe_Italy_Assisi_X/July/Monday/{STREAM}_X_2026_July_W4_Monday_hour_01_part_{i:02d}.mp4',
            'file_url': f'{API}/file/{1000 + i}', 'ticket_presentation_path': True,
            'sha256': hashlib.sha256(data).hexdigest(), 'size_bytes': len(data)}


def manifest(gen, outputs):
    return {'schema': 2, 'collection': 'verified-assembled', 'recording_id': STREAM, 'period': ['2026-07-27', '2026-08-09'],
            'generation': gen, 'outputs': [dict(o, delivery_generation=gen) for o in outputs]}


class FakeDelivery:
    """The public access API: manifests and tickets per generation, retiring OLD after retire_after tickets."""

    def __init__(self, new_outputs, retire_after):
        self.manifests = {OLD: manifest(OLD, [output(i) for i in range(1, 6)]), NEW: manifest(NEW, new_outputs)}
        self.bodies = {hashlib.sha256(body(i, v)).hexdigest(): body(i, v) for i in range(1, 6) for v in (0, 1)}
        self.current, self.retired, self.retire_after = OLD, set(), retire_after
        self.tickets = []  # (generation, file id) of every ticket issued
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                u = urllib.parse.urlsplit(self.path)
                q = dict(urllib.parse.parse_qsl(u.query))
                gen = q.get('generation')
                if gen in fake.retired:
                    return self.reply(410, {'error': 'This download link is outdated; copy a fresh command from the page.'})
                if u.path == API + '/asset' and q['path'] == MANIFEST:
                    return self.reply(200, fake.manifests[gen or fake.current])
                if u.path.startswith(API + '/ticket/'):
                    return self.reply(200, fake.ticket(gen, int(u.path.rsplit('/', 1)[1]), q))
                self.reply(404, {'error': 'not found'})

            def reply(self, status, obj):
                raw = json.dumps(obj).encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.origin = 'http://127.0.0.1:%d' % self.server.server_address[1]

    def ticket(self, gen, file_id, q):
        o = next(o for o in self.manifests[gen]['outputs'] if o['id'] == file_id)
        assert q['sha256'] == o['sha256'] and q['path'] == o['path']
        self.tickets.append((gen, file_id))
        if len(self.tickets) == self.retire_after:  # the republish: NEW is pinned, OLD retired
            self.current = NEW
            self.retired.add(OLD)
        path = '/stoarama/joined/' + o['sha256'] + '.mp4'
        md5 = hashlib.md5(self.bodies[o['sha256']]).hexdigest()
        return {'transport': 'r2-direct', 'storage_host': HOST, 'sha256': o['sha256'], 'size_bytes': o['size_bytes'],
                'filename': o['path'].rsplit('/', 1)[1], 'ttl_seconds': 300, 'expires_at': time.time() + 300,
                'object_path_sha256': hashlib.sha256(path.encode()).hexdigest(), 'etag': '"%s"' % md5,
                'required_headers': {'If-Match': '"%s"' % md5},
                'url': 'https://%s%s?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=300&X-Amz-SignedHeaders=host%%3Bif-match'
                       '&X-Amz-Signature=%s' % (HOST, path, 'd' * 64)}

    def storage(self, t, o, start):
        """R2's ranged GET for a ticket, which stays valid after its generation is retired."""
        data = self.bodies[t['sha256']][start:]
        response = io.BytesIO(data)
        response.status = 206
        response.headers = {'ETag': t['etag'], 'Content-Length': str(len(data)),
                            'Content-Range': 'bytes %d-%d/%d' % (start, o['size_bytes'] - 1, o['size_bytes'])}
        return response


class RetiredGenerationTests(unittest.TestCase):
    def run_script(self, fake, dest):
        ns = {'__name__': 'gss_download_script'}
        with open(os.path.join(HERE, 'download_script.py'), 'rb') as f:
            exec(compile(f.read(), 'download_script.py', 'exec'), ns)
        ns['storage'] = fake.storage
        argv = ['download-script.py', '--stream', str(STREAM), '--scope', 'available', '--jobs', '1', '--destination', dest,
                '--server', fake.origin, '--dataset-api', API, '--generation', OLD]
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

    def files(self, dest):
        return {p.name: p.read_bytes() for p in Path(dest).rglob('*.mp4')}

    def test_unchanged_files_continue_on_the_new_generation(self):
        fake = FakeDelivery([output(i) for i in range(1, 6)], retire_after=3)
        with tempfile.TemporaryDirectory() as dest:
            code, out, err = self.run_script(fake, dest)
            self.assertFalse(code, err)
            self.assertIn(f'Dataset generation {OLD} was retired; generation {NEW} lists the same 5 files', out)
            self.assertEqual(self.files(dest), {output(i)['path'].rsplit('/', 1)[1]: body(i) for i in range(1, 6)})
            self.assertEqual(len(list(Path(dest).glob('*.manifest*.json'))), 2)  # a receipt per generation
            self.assertEqual(list(Path(dest).rglob('.output_*')), [])
        # Tickets 1-3 under OLD (the third finishes: R2 capability), 4 is refused, then 4-5 under NEW.
        self.assertEqual(fake.tickets, [(OLD, 1001), (OLD, 1002), (OLD, 1003), (NEW, 1004), (NEW, 1005)])

    def test_changed_files_stop_without_mixing_generations(self):
        fake = FakeDelivery([output(i, 1 if i == 5 else 0) for i in range(1, 6)], retire_after=2)
        with tempfile.TemporaryDirectory() as dest:
            code, out, err = self.run_script(fake, dest)
            self.assertTrue(code)
            self.assertIn(f'Stopped: dataset generation {OLD} was retired, and the current generation {NEW} changes this download: '
                          '0 files added, 0 removed, 1 changed.', err)
            self.assertIn('  changed: ' + output(5)['path'], err)
            self.assertIn('Copy a fresh command from the page to continue.', err)
            self.assertIn('2 outputs complete; 0 unavailable or failed; 3 not started.', out)
            self.assertEqual(self.files(dest), {output(i)['path'].rsplit('/', 1)[1]: body(i) for i in (1, 2)})
        self.assertEqual(fake.tickets, [(OLD, 1001), (OLD, 1002)])

    def test_retired_before_start(self):
        fake = FakeDelivery([], retire_after=0)
        fake.retired.add(OLD)
        with tempfile.TemporaryDirectory() as dest, self.assertRaisesRegex(SystemExit, f'Dataset generation {OLD} is retired. Copy a fresh command'):
            self.run_script(fake, dest)


if __name__ == '__main__':
    unittest.main()
