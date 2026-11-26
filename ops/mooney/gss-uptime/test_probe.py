import http.server
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

PROBE = Path(__file__).parent / 'probe.py'


class Handler(http.server.BaseHTTPRequestHandler):
    status = 200
    body = b'{"streams": []}'

    def do_GET(self):
        self.send_response(Handler.status)
        self.end_headers()
        self.wfile.write(Handler.body)

    def log_message(self, *args):
        pass


class ProbeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        root = Path(self.dir.name)
        self.state, self.sent, self.fail = root / 'state', root / 'sent', root / 'fail'
        self.state.mkdir()
        (root / 'bin').mkdir()
        darma = root / 'bin' / 'darma'
        darma.write_text('#!/bin/sh\nif [ -s "%s" ] && [ "$(cat "%s")" -gt 0 ]; then n=$(cat "%s"); echo $((n - 1)) > "%s"; echo "boom token=abc123 relay down" >&2; exit 7; fi\nprintf "%%s|%%s\\n" "$3" "$4" >> "%s"\n' % (self.fail, self.fail, self.fail, self.fail, self.sent))
        darma.chmod(0o755)
        self.env = dict(os.environ, PATH=str(root / 'bin') + os.pathsep + os.environ['PATH'])
        Handler.status, Handler.body = 200, b'{"streams": []}'

    def tearDown(self):
        self.dir.cleanup()

    def run_probe(self):
        origin = 'http://127.0.0.1:%d' % self.server.server_address[1]
        return subprocess.run([sys.executable, '-I', str(PROBE), '--state', str(self.state), '--notify-agent', 'mooney', '--origin', origin, '--retry-seconds', '0'],
                              env=self.env, capture_output=True, text=True)

    def messages(self):
        return self.sent.read_text().splitlines() if self.sent.exists() else []

    def test_alerts_on_second_failure_then_recovers_once(self):
        self.assertEqual(self.run_probe().returncode, 0)
        Handler.status = 502
        first = self.run_probe()
        self.assertEqual((first.returncode, first.stdout.strip(), self.messages()), (1, 'probe metadata=http_502 page=http_502', []))
        self.run_probe()
        self.assertEqual(len(self.messages()), 1)
        self.assertIn('DOWN', self.messages()[0])
        self.run_probe()
        self.assertEqual(len(self.messages()), 1, 'unchanged outage re-alerted before the reminder interval')
        Handler.status = 200
        self.assertEqual(self.run_probe().returncode, 0)
        self.run_probe()
        self.assertEqual(len(self.messages()), 2)
        self.assertIn('recovered', self.messages()[1])

    def test_truncated_metadata_is_down(self):
        Handler.body = b'{"streams": [1, 2'
        self.assertEqual(self.run_probe().stdout.strip(), 'probe metadata=bad_json')

    def test_failed_notification_retries(self):
        Handler.status = 503
        self.run_probe()
        self.fail.write_text('99')
        self.assertEqual(self.run_probe().returncode, 2)
        self.fail.unlink()
        self.run_probe()
        self.assertEqual(len(self.messages()), 1)
        self.assertEqual(json.loads((self.state / 'probe.json').read_text())['alerted']['condition'], 'metadata=http_503 page=http_503')

    def test_retry_then_success(self):
        Handler.status = 503
        self.run_probe()
        self.fail.write_text('2')
        result = self.run_probe()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(self.messages()), 1)
        self.assertEqual(result.stdout.count('notify attempt'), 2)

    def test_failed_notification_logs_reason(self):
        Handler.status = 503
        self.run_probe()
        self.fail.write_text('99')
        result = self.run_probe()
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout.count('notify attempt'), 3)
        self.assertIn('rc=7 stderr=boom token=<redacted> relay down', result.stdout)
        self.assertNotIn('abc123', result.stdout)
        self.assertTrue(result.stdout.rstrip().endswith('notification_failed'))


if __name__ == '__main__':
    unittest.main()
