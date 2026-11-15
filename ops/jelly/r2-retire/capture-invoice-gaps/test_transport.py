"""Actual local subprocess/exec/signal tests with fabricated files and no network."""
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import capture_invoice_gaps as gaps
import transport
from synthetic_fixtures import baseline


TOKEN = 'SYNTHETIC-NOT-A-PROVIDER-CREDENTIAL'
HERE = Path(__file__).resolve().parent


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='invoice-launch-SYNTHETIC-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.base, self.pins = baseline(self.root, gaps)
        self.output = self.root / 'evidence'
        self.config = self.root / 'synthetic.env'
        self.config.write_text('export DIGITALOCEAN_TOKEN="%s"\n' % TOKEN)
        self.config.chmod(0o600)

    def source(self, seconds=3, hook=''):
        raw = (HERE / 'capture_invoice_gaps.py').read_text()
        values = {'REMOTE_BASE': 'Path(%r)' % str(self.base), 'REMOTE_PINS': 'Path(%r)' % str(self.pins),
                  'REMOTE_OUT': 'Path(%r)' % str(self.output), 'PINS_SHA': repr(gaps.digest(self.pins.read_bytes())),
                  'RUN_SECONDS': repr(seconds)}
        lines = [key + ' = ' + values[key] if (key := line.split(' = ', 1)[0]) in values else line for line in raw.splitlines()]
        fake_transport = '''
def request_http(path, token):
    index = [p for _, p in requests()].index(path)
    body = load_baseline(REMOTE_BASE, REMOTE_PINS)
    if index in (0, 6):
        value = body['account.json']['body']
    else:
        month, _, number, _, total = PAGES[index - 1]
        value = {'invoice_items': body['invoice-full-2026-%s.json' % month]['invoice_items'][(number-1)*200:], 'meta': {'total': total}, 'links': {}}
    return 200, 'SYNTHETIC-%d' % index, '', canonical(value)
'''
        text = '\n'.join(lines).replace("if __name__ == \"__main__\":", fake_transport + '\n' + hook + '\nif __name__ == "__main__":')
        path = self.root / 'synthetic_core.py'
        path.write_text(text)
        return path, gaps.digest(path.read_bytes())

    def bootstrap(self, source, sha, hook=''):
        # The tested launcher functions are unchanged. Only fixed host/file
        # bindings and the collector's HTTP adapter are synthetic; no production
        # source or credential file is opened by this harness.
        return '''
import importlib.util,sys,time
from pathlib import Path
spec=importlib.util.spec_from_file_location('launch',%r)
t=importlib.util.module_from_spec(spec);spec.loader.exec_module(t)
t.REMOTE_HOME=Path(%r);t.REMOTE_SOURCE=Path(%r);t.REMOTE_CREDENTIALS=Path(%r);t.PYTHON=sys.executable
%s
try:t.remote(%r)
except BaseException:sys.exit('SYNTHETIC remote refused; no retry')
''' % (str(HERE / 'transport.py'), str(self.root), str(source), str(self.config), hook, sha)

    def launch(self, seconds=3, core_hook='', launcher_hook='', close_input=False):
        source, sha = self.source(seconds, core_hook)
        stdout = (self.root / 'stdout').open('wb')
        stderr = (self.root / 'stderr').open('wb')
        env = {'HOME': str(self.root), 'PATH': os.environ.get('PATH', '')}
        child = subprocess.Popen([sys.executable, '-I', '-S', '-B', '-c', self.bootstrap(source, sha, launcher_hook)],
                                 stdin=subprocess.PIPE, stdout=stdout, stderr=stderr, env=env)
        try:
            if close_input:
                child.stdin.close()
            code = child.wait(timeout=6)
            self.assertEqual(child.wait(timeout=0), code)  # Owned parent reaped.
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=5)
            if not child.stdin.closed:
                child.stdin.close()
            stdout.close()
            stderr.close()
        for path in self.output.glob('*'):
            self.assertNotIn(TOKEN.encode(), path.read_bytes())
        self.assertNotIn(TOKEN.encode(), (self.root / 'stdout').read_bytes())
        self.assertNotIn(TOKEN.encode(), (self.root / 'stderr').read_bytes())
        return child.pid, code

    def test_real_preparer_child_then_same_pid_exec_and_seven_responses(self):
        pid, code = self.launch()
        self.assertEqual(code, 0, (self.root / 'stderr').read_text())
        attempt = json.loads((self.output / 'attempt.json').read_bytes())
        started = json.loads((self.output / 'started.json').read_bytes())
        self.assertEqual((attempt['shell_pid'], started['pid']), (pid, pid))
        manifest = json.loads((self.output / 'manifest.json').read_bytes())
        self.assertEqual(manifest['validation']['response_count'], 7)
        self.assertLess(manifest['elapsed_seconds'], attempt['deadline'] - attempt['monotonic_start'])
        self.assertFalse(manifest['validation']['apply_authority'])

    def test_existing_remote_attempt_refuses_before_auth_open(self):
        self.output.mkdir()
        marker = self.root / 'auth-opened'
        _, code = self.launch(launcher_hook="t.token_file=lambda: Path(%r).write_text('FORBIDDEN')" % str(marker))
        self.assertNotEqual(code, 0)
        self.assertFalse(marker.exists())
        self.assertEqual(list(self.output.iterdir()), [])

    def test_verified_bytes_survive_replacement_of_original_source_path(self):
        hook = "original=t.core\ndef changed(raw):\n t.REMOTE_SOURCE.write_text('INVALID SOURCE')\n return original(raw)\nt.core=changed"
        _, code = self.launch(launcher_hook=hook)
        self.assertEqual(code, 0, (self.root / 'stderr').read_text())
        self.assertEqual((self.root / 'synthetic_core.py').read_text(), 'INVALID SOURCE')

    def test_source_drift_refuses_before_preparation_and_auth(self):
        marker = self.root / 'auth-opened'
        _, code = self.launch(launcher_hook="t.REMOTE_SOURCE.write_text('DRIFT')\nt.token_file=lambda:Path(%r).write_text('FORBIDDEN')" % str(marker))
        self.assertNotEqual(code, 0)
        self.assertFalse(self.output.exists())
        self.assertFalse(marker.exists())

    def test_preparation_delay_is_inside_the_same_absolute_budget(self):
        marker = self.root / 'prepare-reached'
        hook = "original=load_baseline\ndef load_baseline(*args):\n    if 'prepare' in __import__('sys').argv:\n        Path(%r).write_text('reached');time.sleep(5)\n    return original(*args)" % str(marker)
        started = time.monotonic()
        _, code = self.launch(seconds=1.5, core_hook=hook)
        self.assertNotEqual(code, 0)
        self.assertLess(time.monotonic() - started, 4)
        self.assertTrue(marker.exists())
        self.assertFalse((self.output / 'started.json').exists())

    def test_credential_delay_cannot_reset_collection_deadline(self):
        marker = self.root / 'credential-reached'
        _, code = self.launch(seconds=1.5, launcher_hook="original=t.token_file\nt.token_file=lambda:(Path(%r).write_text('reached'),time.sleep(5),original())[2]" % str(marker))
        self.assertNotEqual(code, 0)
        self.assertTrue(marker.exists())
        self.assertFalse((self.output / 'started.json').exists())
        self.assertEqual(json.loads((self.output / 'failure.json').read_bytes())['stage'], 'credential')

    def test_durable_started_write_is_already_under_the_armed_deadline(self):
        marker = self.root / 'started-write-reached'
        hook = "original_write=write_new\ndef write_new(path,raw):\n    if path.name=='started.json':\n        Path(%r).write_text('reached');time.sleep(5)\n    return original_write(path,raw)" % str(marker)
        started = time.monotonic()
        _, code = self.launch(seconds=1.5, core_hook=hook)
        self.assertNotEqual(code, 0)
        self.assertLess(time.monotonic() - started, 4)
        self.assertTrue(marker.exists())
        self.assertFalse((self.output / 'manifest.json').exists())

    def test_finalization_cannot_publish_a_late_success(self):
        marker = self.root / 'finalization-reached'
        hook = "original_write=write_new\ndef write_new(path,raw):\n    if path.name=='SHA256SUMS':\n        Path(%r).write_text('reached');time.sleep(5)\n    return original_write(path,raw)" % str(marker)
        _, code = self.launch(seconds=1.5, core_hook=hook)
        self.assertNotEqual(code, 0)
        self.assertTrue(marker.exists())
        self.assertFalse((self.output / 'SHA256SUMS').exists())
        self.assertFalse((self.root / 'stdout').read_bytes())

    def test_real_input_disconnect_terminates_without_success(self):
        _, code = self.launch(close_input=True)
        self.assertNotEqual(code, 0)
        self.assertFalse((self.output / 'manifest.json').exists())

    def test_literal_credentials_duplicate_and_mode_refusals(self):
        with patch.object(transport, 'REMOTE_CREDENTIALS', self.config):
            self.assertEqual(transport.token_file(), TOKEN)
            for body in ['DIGITALOCEAN_TOKEN=x\nDIGITALOCEAN_TOKEN=y\n', 'DIGITALOCEAN_TOKEN="$(secret)"\n', 'DIGITALOCEAN_TOKEN=\n']:
                self.config.write_text(body)
                with self.assertRaises(transport.Refused):
                    transport.token_file()
            self.config.write_text('DIGITALOCEAN_TOKEN=x\n')
            self.config.chmod(0o644)
            with self.assertRaises(transport.Refused):
                transport.token_file()

    def test_exact_argv_and_source_checksum_refusal(self):
        source, sha = self.source()
        path = HERE / 'transport.py'
        argv = transport.command(source, sha, path, gaps.digest(path.read_bytes()))
        remote = shlex.split(argv[-1])
        self.assertEqual(argv[:-1], transport.SSH)
        self.assertEqual(remote[:5], [transport.PYTHON, '-I', '-S', '-B', '-c'])
        self.assertEqual(remote[5], path.read_text())
        self.assertEqual(remote[6:], ['remote', '--source-sha', sha])
        with self.assertRaises(transport.Refused):
            transport.command(source, '0' * 64, path, gaps.digest(path.read_bytes()))

    def test_local_exclusive_capture_and_terminal_reaping(self):
        directory = self.root / 'local'
        result = {'outcome': 'complete', 'requests': 7, 'manifest_sha256': '1' * 64, 'apply_authority': False}
        argv = [sys.executable, '-I', '-S', '-B', '-c', 'print(%r)' % json.dumps(result)]
        self.assertTrue(transport.run(argv, directory)['reaped'])
        for p in directory.iterdir():
            self.assertEqual(p.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError):
            transport.run(argv, directory)

    def test_local_timeout_closes_input_and_reaps_only_owned_process(self):
        directory = self.root / 'local-timeout'
        with patch.object(transport, 'LOCAL_SECONDS', 0.1):
            with self.assertRaises(transport.Refused):
                transport.run([sys.executable, '-I', '-S', '-B', '-c', 'import time;time.sleep(30)'], directory)
        result = json.loads((directory / 'transport-result.json').read_bytes())
        self.assertEqual(result['outcome'], 'HOLD')
        self.assertTrue(result['reaped'])

    def test_local_signal_interruptions_reap_owned_child(self):
        for sig in [signal.SIGTERM, signal.SIGINT]:
            with self.subTest(signal=sig):
                directory = self.root / ('signal-%d' % sig)
                program = 'import os,signal,time;os.kill(os.getppid(),%d);time.sleep(30)' % sig
                with self.assertRaises(transport.Refused):
                    transport.run([sys.executable, '-I', '-S', '-B', '-c', program], directory)
                result = json.loads((directory / 'transport-result.json').read_bytes())
                self.assertEqual(result['outcome'], 'HOLD')
                self.assertTrue(result['reaped'])

    def test_unobserved_child_exit_remains_explicit_hold(self):
        class Unreaped:
            returncode = None
            stdin = io.BytesIO()
            terminated = 0
            def wait(self, timeout):
                raise subprocess.TimeoutExpired('SYNTHETIC', timeout)
            def poll(self):
                return None
            def terminate(self):
                self.terminated += 1
        child = Unreaped()
        directory = self.root / 'unreaped'
        with patch.object(transport.subprocess, 'Popen', return_value=child):
            with self.assertRaises(transport.Refused):
                transport.run(['SYNTHETIC-NO-PROCESS-STARTED'], directory)
        result = json.loads((directory / 'transport-result.json').read_bytes())
        self.assertEqual(result['outcome'], 'HOLD')
        self.assertFalse(result['reaped'])
        self.assertIsNone(result['exit_code'])
        self.assertEqual(child.terminated, 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
