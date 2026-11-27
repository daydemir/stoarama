"""Offline tests: object storage and SQL are in-memory, shell commands are fakes."""
import contextlib
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('gss_gen', HERE / 'gss_gen.py')
gss = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gss)
PARENT, CANDIDATE, OTHER, ATTEMPT = ('a' * 32, 'b' * 32, 'c' * 32, 'd' * 32)


def body(gen, parent=None):
    return gzip.compress(json.dumps({'generation': gen, 'parent_generation': parent}).encode(), mtime=0)


class MockStore:
    def __init__(self):
        self.objects = {gss.P + '/generations/' + gen + '.json': body(gen, parent)
                        for gen, parent in [(PARENT, None), (CANDIDATE, PARENT), (OTHER, PARENT)]}
        self.objects[gss.CURRENT] = body(PARENT)
        self.pin = PARENT
        self.before_put = lambda: None
        self.after_put = lambda: None
        self.before_pin = lambda: None
        self.after_pin = lambda: None

    def state(self):
        return gss.registry(self.objects[gss.CURRENT])['generation'], self.pin

    def etag(self, key):
        return '"' + hashlib.sha256(self.objects[key]).hexdigest() + '"'

    def s3(self, command, *args, check=True):
        def arg(flag):
            return args[args.index(flag) + 1]
        key = arg('--key')
        if command == 'head-object':
            if key not in self.objects:
                return subprocess.CompletedProcess([], 1, '', 'not found')
        elif command == 'get-object':
            Path(args[-1]).write_bytes(self.objects[key])
        elif command == 'copy-object':
            assert arg('--copy-source-if-match') == self.etag(gss.CURRENT)
            assert arg('--if-none-match') == '*' and key not in self.objects
            self.objects[key] = self.objects[gss.CURRENT]
        elif command == 'put-object':
            self.before_put()
            if arg('--if-match') != self.etag(key):
                raise SystemExit('conditional PUT failed')
            self.objects[key] = Path(arg('--body')).read_bytes()
            self.after_put()
        else:
            raise AssertionError(command)
        return subprocess.CompletedProcess([], 0, json.dumps({'ETag': self.etag(key)}), '')

    def pg(self, sql, write=False):
        if not write:
            assert sql == 'SELECT generation FROM gss_public_generation_pin WHERE singleton'
            return self.pin
        match = re.fullmatch(r"UPDATE gss_public_generation_pin SET generation='([a-f0-9]{32})', pinned_at=clock_timestamp\(\) WHERE singleton AND generation='([a-f0-9]{32})' RETURNING generation", sql)
        assert match, sql
        new, old = match.groups()
        self.before_pin()
        if self.pin != old:
            return ''
        self.pin = new
        self.after_pin()
        return new


def fail():
    raise SystemExit('forced command failure')


class PointerTests(unittest.TestCase):
    def setUp(self):
        self.store = MockStore()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.addCleanup(patch.stopall)
        patch.multiple(gss, s3=self.store.s3, pg=self.store.pg, B='fake-bucket', H=directory.name).start()
        self.output = io.StringIO()
        for redirect in (contextlib.redirect_stdout, contextlib.redirect_stderr):
            context = redirect(self.output)
            context.__enter__()
            self.addCleanup(context.__exit__, None, None, None)

    def test_success_and_rollback(self):
        self.assertEqual(gss.swap(ATTEMPT, PARENT, CANDIDATE), 0)
        self.assertEqual(self.store.state(), (CANDIDATE, CANDIDATE))
        self.assertEqual(gss.rollback(ATTEMPT, CANDIDATE, PARENT), 0)
        self.assertEqual(self.store.state(), (PARENT, PARENT))

    def test_competing_parent(self):
        self.store.objects[gss.CURRENT] = body(OTHER, PARENT)
        self.store.pin = OTHER
        self.assertEqual(gss.swap(ATTEMPT, PARENT, CANDIDATE), 3)
        self.assertEqual(gss.rollback(ATTEMPT, CANDIDATE, PARENT), 8)
        self.assertEqual(self.store.state(), (OTHER, OTHER))

    def test_competing_object_at_cas(self):
        def competitor():
            self.store.objects[gss.CURRENT] = body(OTHER, PARENT)
        self.store.before_put = competitor
        self.assertEqual(gss.swap(ATTEMPT, PARENT, CANDIDATE), 8)
        self.assertEqual(self.store.state(), (OTHER, PARENT))

    def test_competing_pin_at_cas(self):
        self.store.before_pin = lambda: setattr(self.store, 'pin', OTHER)
        self.assertEqual(gss.swap(ATTEMPT, PARENT, CANDIDATE), 8)
        self.assertEqual(self.store.state(), (CANDIDATE, OTHER))

    def test_partial_swap(self):
        self.store.before_pin = fail
        self.assertEqual(gss.swap(ATTEMPT, PARENT, CANDIDATE), 4)
        self.assertEqual(self.store.state(), (PARENT, PARENT))

    def test_ambiguous_put(self):
        def once():
            self.store.after_put = lambda: None
            fail()
        self.store.after_put = once
        self.assertEqual(gss.swap(ATTEMPT, PARENT, CANDIDATE), 4)
        self.assertEqual(self.store.state(), (PARENT, PARENT))

    def test_ambiguous_pin(self):
        def once():
            self.store.after_pin = lambda: None
            fail()
        self.store.after_pin = once
        self.assertEqual(gss.swap(ATTEMPT, PARENT, CANDIDATE), 4)
        self.assertEqual(self.store.state(), (PARENT, PARENT))

    def test_failed_rollback(self):
        self.store.before_pin = fail
        self.store.after_put = lambda: setattr(self.store, 'before_put', fail)
        self.assertEqual(gss.swap(ATTEMPT, PARENT, CANDIDATE), 8)
        self.assertEqual(self.store.state(), (CANDIDATE, PARENT))
        self.assertIn('UNRESOLVED DIVERGENCE', self.output.getvalue())

    def test_pin_only_partial_rollback(self):
        self.store.pin = CANDIDATE
        self.assertEqual(gss.rollback(ATTEMPT, CANDIDATE, PARENT), 0)
        self.assertEqual(self.store.state(), (PARENT, PARENT))

    def test_ambiguous_rollback_completed(self):
        self.store.pin = CANDIDATE
        self.store.after_pin = fail
        self.assertEqual(gss.rollback(ATTEMPT, CANDIDATE, PARENT), 0)
        self.assertEqual(self.store.state(), (PARENT, PARENT))

    def test_current_body_must_match_staged(self):
        self.store.objects[gss.CURRENT] = body(PARENT, OTHER)
        self.assertEqual(gss.swap(ATTEMPT, PARENT, CANDIDATE), 3)
        self.assertEqual(self.store.objects[gss.CURRENT], body(PARENT, OTHER))

    def test_cancellation_failure_is_not_reported_as_rollback(self):
        with patch.object(gss, 'record_attempt', side_effect=OSError('cannot sync attempt')):
            self.assertEqual(gss.rollback(ATTEMPT, CANDIDATE, PARENT), 8)
        self.assertEqual(self.store.state(), (PARENT, PARENT))
        self.assertIn('attempt cancellation not durable', self.output.getvalue())

    def test_attempt_id_cannot_be_reused_or_rebound(self):
        self.assertEqual(gss.swap(ATTEMPT, PARENT, CANDIDATE), 0)
        self.assertEqual(gss.swap(ATTEMPT, PARENT, OTHER), 8)
        self.assertEqual(gss.rollback(ATTEMPT, OTHER, PARENT), 8)
        self.assertEqual(gss.swap(ATTEMPT, PARENT, CANDIDATE), 8)
        self.assertEqual(self.store.state(), (CANDIDATE, CANDIDATE))
        self.assertEqual(gss.rollback(ATTEMPT, CANDIDATE, PARENT), 0)
        self.assertEqual(gss.swap(ATTEMPT, PARENT, CANDIDATE), 3)
        self.assertEqual(self.store.state(), (PARENT, PARENT))


class CycleTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.home = Path(temp.name)
        self.records = self.home / 'Build/stoarama/records/finish-plan-20261117/gss-delivery'
        self.records.mkdir(parents=True)
        (self.records / 'cadence').mkdir()
        self.out = self.home / 'build'
        (self.out / 'generation').mkdir(parents=True)
        (self.out / 'ledger').mkdir()
        (self.out / 'generation/summary.json').write_text(json.dumps({
            'generation': CANDIDATE, 'parent_generation': PARENT,
            'availability': {'available': 1}, 'regressed_stream_days': 0}))
        (self.out / 'ledger/best14-ledger-summary.txt').write_text('new summary')
        (self.out / 'ledger/best14-ledger.tsv').write_text('new ledger')
        (self.records / 'best14-ledger.tsv').write_text('old ledger')
        (self.home / '.credentials').mkdir()
        (self.home / '.credentials/stoarama-r2-readonly.env').write_text(
            'R2_ACCESS_KEY_ID=fake\nR2_SECRET_ACCESS_KEY=fake\nR2_ACCOUNT_ID=fake\n')
        self.bin = self.home / 'bin'
        self.bin.mkdir()
        self.calls = self.home / 'calls'
        self.state = self.home / 'state'
        self.state.write_text(PARENT)
        # Every network command is replaced, and mock Python delegates only
        # the pure inline JSON parsing to the real interpreter.
        self.fake('ssh', '''
command=$*
case "$command" in
  *"gss_gen.py verify "*)
    if [ "$SCENARIO" = final_verify_failed ] && [ "$(grep -c 'gss_gen.py verify ' "$CALLS")" = 3 ]; then
      exit 1
    fi
    test "$(cat "$STATE")" = "${command##* }" ;;
  *"gss_gen.py stage "*) echo staged ;;
  *"gss_gen.py swap "*)
    [ "$SCENARIO" != refused ] || exit 3
    [ "$SCENARIO" != delayed ] || exit 255
    printf '%s' "${command##* }" > "$STATE"
    [ "$SCENARIO" != disconnected ] || exit 255 ;;
  *"gss_gen.py rollback "*)
    [ "$SCENARIO" != rollback_failed ] || exit 1
    printf '%s' "${command##* }" > "$STATE" ;;
  *) exit 99 ;;
esac
''')
        self.fake('curl', '''printf '%s' '{"streams":[{"availability":"available"}]}'\n''')
        self.fake('rsync', 'exit 0\n')
        self.fake('sleep', 'exit 0\n')
        self.fake('python3', f'''
case "$*" in
  *"gss_preview_check.py"*)
    case "$SCENARIO" in preview_failed|rollback_failed) echo 'failures 1'; exit 1;; esac
    echo 'failures 0' ;;
  *"viewer_smoke.py"*|*"download_smoke.py"*|*"best14_ledger.py"*) exit 0 ;;
  *) exec "{sys.executable}" "$@" ;;
esac
''')

    def fake(self, name, script):
        p = self.bin / name
        p.write_text('#!/bin/sh\nprintf "%s %s\\n" "${0##*/}" "$*" >> "$CALLS"\n' + script)
        p.chmod(0o755)

    def run_cycle(self, scenario):
        env = dict(HOME=str(self.home), PATH=str(self.bin) + ':/usr/bin:/bin',
                   STATE=str(self.state), CALLS=str(self.calls), SCENARIO=scenario)
        return subprocess.run(['sh', str(HERE / 'republish-cycle.sh'), str(self.home), str(self.out)],
                              env=env, capture_output=True, text=True)

    def test_preview_failure_missing_later_logs(self):
        p = self.run_cycle('preview_failed')
        self.assertEqual(p.returncode, 4, p.stderr)
        self.assertFalse(Path(str(self.out) + '.viewer-live.log').exists())
        self.assertIn('ROLLBACK VERIFIED', p.stderr)
        self.assertEqual(self.state.read_text(), PARENT)
        self.assertEqual((self.records / 'best14-ledger.tsv').read_text(), 'old ledger')

    def test_failed_swap_command_not_hidden_by_tail(self):
        p = self.run_cycle('disconnected')
        self.assertEqual(p.returncode, 4, p.stderr)
        self.assertNotIn('gss_preview_check.py', self.calls.read_text())
        self.assertEqual(self.state.read_text(), PARENT)

    def test_delayed_swap_after_recovery_keeps_parent_and_ledger(self):
        # The fake transport loses the swap request before its remote worker
        # acquires generation.lock. Capture the actual shell's commands, then
        # execute their ordering against the real pointer and attempt code with
        # in-memory storage and a file-backed lock/cancellation ledger.
        p = self.run_cycle('delayed')
        self.assertEqual(p.returncode, 4, p.stderr)
        self.assertIn('ROLLBACK VERIFIED', p.stderr)
        commands = self.calls.read_text().splitlines()
        swap = next(x.split('gss_gen.py ')[1].split() for x in commands if 'gss_gen.py swap ' in x)
        rollback = next(x.split('gss_gen.py ')[1].split() for x in commands if 'gss_gen.py rollback ' in x)
        self.assertEqual(swap[1], rollback[1])
        self.assertEqual(swap[2:], [PARENT, CANDIDATE])
        self.assertEqual(rollback[2:], [CANDIDATE, PARENT])
        self.assertEqual(json.loads((self.records / 'cadence/attempts' / (swap[1] + '.json')).read_text()),
                         {'parent': PARENT, 'candidate': CANDIDATE})
        store = MockStore()
        ready, release = threading.Event(), threading.Event()

        def delayed():
            ready.set()
            if not release.wait(5):
                raise RuntimeError('test did not release delayed swap')
            return gss.main(swap)

        with patch.multiple(gss, s3=store.s3, pg=store.pg, H=str(self.home),
                            r2env=lambda: {'R2_BUCKET': 'fake', 'R2_ACCOUNT_ID': 'fake'}):
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(delayed)
                try:
                    self.assertTrue(ready.wait(5))
                    self.assertEqual(gss.main(rollback), 0)
                    self.assertEqual(store.state(), (PARENT, PARENT))
                finally:
                    release.set()
                self.assertEqual(pending.result(timeout=5), 3)
        self.assertEqual(store.state(), (PARENT, PARENT))
        self.assertEqual((self.records / 'best14-ledger.tsv').read_text(), 'old ledger')
        self.assertEqual(json.loads((self.home / 'generation-attempts' / (swap[1] + '.json')).read_text()),
                         {'parent': PARENT, 'candidate': CANDIDATE, 'cancelled': True})

    def test_failed_rollback_is_unresolved(self):
        p = self.run_cycle('rollback_failed')
        self.assertEqual(p.returncode, 8, p.stderr)
        self.assertIn('UNRESOLVED DIVERGENCE', p.stderr)
        self.assertEqual(self.state.read_text(), CANDIDATE)

    def test_competing_swap_refusal_does_not_rollback(self):
        p = self.run_cycle('refused')
        self.assertEqual(p.returncode, 3, p.stderr)
        self.assertNotIn('gss_gen.py rollback', self.calls.read_text())

    def test_success_publishes_ledger(self):
        p = self.run_cycle('success')
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(self.state.read_text(), CANDIDATE)
        self.assertEqual((self.records / 'best14-ledger.tsv').read_text(), 'new ledger')
        self.assertIn('PUBLISHED', p.stdout)

    def test_final_verification_failure_restores_ledger(self):
        p = self.run_cycle('final_verify_failed')
        self.assertEqual(p.returncode, 4, p.stderr)
        self.assertEqual(self.state.read_text(), PARENT)
        self.assertEqual((self.records / 'best14-ledger.tsv').read_text(), 'old ledger')
        self.assertFalse((self.records / 'best14-ledger-STATUS.txt').exists())
        self.assertFalse((self.records / 'best14-ledger-summary.txt').exists())

    def test_lock_refuses_second_cycle(self):
        (self.home / '.stoarama-gss-cycle.lock').mkdir()
        p = self.run_cycle('success')
        self.assertEqual(p.returncode, 6, p.stderr)
        self.assertFalse(self.calls.exists())


if __name__ == '__main__':
    unittest.main()
