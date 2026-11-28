import copy
import hashlib
import json
import os
import signal
import sys
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import test_stoarama_pull as support
pull = support.pull


class HostPressureTests(unittest.TestCase):
    def test_exact_fields_units_missing_and_invalid_values(self):
        mem = '\n'.join('%s: %d kB' % (name, n) for n, name in enumerate(sorted(pull.PROC_MEMINFO_FIELDS)))
        values = pull.proc_pressure_diagnostics(mem, 'cpu 1 2 3\nprocs_running 10\nprocs_blocked 3',
            'pswpin 2\npswpout 4\npgmajfault 8\npgscan_kswapd 16\npgsteal_kswapd_dma32 32\npgscan_direct 99')
        self.assertEqual(set(values['meminfo_bytes']), pull.PROC_MEMINFO_FIELDS)
        self.assertEqual(values['meminfo_bytes'], {name: n*1024 for n, name in enumerate(sorted(pull.PROC_MEMINFO_FIELDS))})
        self.assertEqual(values['proc_stat'], dict(procs_running=10, procs_blocked=3))
        self.assertEqual(values['vmstat'], dict(pswpin=2, pswpout=4, pgmajfault=8, pgscan_kswapd=16, pgsteal_kswapd_dma32=32))
        bad = 'MemFree: -1 kB\nCached: 2 MB\nDirty: 9999999999999999999 kB\nBuffers: 1.5 kB\nSlab: ١ kB'
        self.assertEqual(pull.proc_pressure_diagnostics(bad, 'procs_running -1', 'pswpin nan\npgmajfault 9223372036854775808'), {})
        self.assertEqual(pull.proc_pressure_diagnostics(None, None, None), {})
        self.assertEqual(pull.meminfo_bytes('MemTotal: 7 kB\nMemAvailable: 3 kB\n'+mem), (7168, 3072))

    def test_bounded_input_vmstat_and_log_line(self):
        self.assertEqual(pull.proc_pressure_diagnostics('x'*pull.PROC_DIAGNOSTIC_MAX_BYTES+'\nMemFree: 7 kB', '', ''), {})
        vm = '\n'.join('pgscan_kswapd_%02d %d' % (n, (1<<63)-1) for n in range(100))
        values = pull.proc_pressure_diagnostics('\n'.join(name+': 1 kB' for name in pull.PROC_MEMINFO_FIELDS),
            'procs_running 1\nprocs_blocked 2', vm)
        self.assertEqual(len(values['vmstat']), pull.PROC_DIAGNOSTIC_MAX_VM_FIELDS)
        with mock.patch.object(pull, 'nas_pressure_snapshot', return_value=values):
            line = 'R2 copy held: copy memory headroom'+pull.memory_hold_diagnostics('copy memory headroom')
        self.assertLess(len(line), pull.CLIENT_LOG_LINE_MAX)
        self.assertEqual(json.loads(line.split(' memory_diagnostics=', 1)[1]), values)
        self.assertEqual(pull.memory_hold_diagnostics('copy free space reserve'), '')

    def test_host_facts_uses_host_proc_and_keeps_resource_fields(self):
        files = {'/proc/meminfo':'MemTotal: 100 kB\nMemAvailable: 25 kB\nCached: 10 kB',
                 '/proc/stat':'procs_running 8\nprocs_blocked 11', '/proc/vmstat':'pgmajfault 21'}
        facts = pull.HostFacts('/unused')
        facts.static = {'machine':'x86_64'}
        with mock.patch.object(pull, '_read_text', side_effect=lambda path, *args: files.get(path)), mock.patch.object(pull.os, 'getloadavg', return_value=(10.3, 0, 0)):
            host = facts()
        self.assertEqual((host['mem_total_bytes'], host['mem_available_bytes'], host['load1']), (102400, 25600, 10.3))
        self.assertEqual(host['memory_diagnostics'], dict(meminfo_bytes={'Cached':10240}, proc_stat={'procs_running':8, 'procs_blocked':11}, vmstat={'pgmajfault':21}))

    def test_memory_hold_snapshot_does_not_change_wait_or_counter(self):
        for reason in ('copy memory headroom', 'copy cgroup memory telemetry unavailable', 'NAS memory or load critical', 'cgroup memory usage unavailable'):
            with self.subTest(reason=reason):
                stop = SimpleNamespace(wait=mock.Mock(side_effect=[False, True]))
                telemetry = pull.R2CopyTelemetry()
                with mock.patch.object(pull, 'R2CopyControl'), mock.patch.object(pull, 'R2_COPY_TELEMETRY', telemetry), \
                     mock.patch.object(pull, 'r2_copy_resources', side_effect=pull.R2CopyHold(reason)), \
                     mock.patch.object(pull, 'nas_pressure_snapshot', return_value={'proc_stat':{'procs_blocked':7}}), mock.patch.object(pull, 'log') as log:
                    pull.r2_copy_worker(None, None, None, stop, 0)
                self.assertEqual(telemetry.holds, {reason:1})
                retry = 5 if reason == "copy memory headroom" else 60
                self.assertEqual(stop.wait.call_args_list, [mock.call(0), mock.call(retry)])
                self.assertEqual(json.loads(log.call_args.args[1].split(' memory_diagnostics=')[1]), {'proc_stat':{'procs_blocked':7}})


class StorageChallengeTests(unittest.TestCase):
    def test_one_shot_and_repeated_or_malformed_responses(self):
        challenge = pull.HeartbeatStorageChallenge()
        self.assertIsNone(challenge.take())
        for bad in (None, 42, "", "a"*63, "A"*64, "g"*64):
            challenge.receive(bad)
            self.assertIsNone(challenge.take())
        challenge.receive("a"*64)
        self.assertEqual(challenge.take(), "a"*64)
        self.assertIsNone(challenge.take())
        challenge.receive("a"*64)
        self.assertIsNone(challenge.take(), 'an echoed challenge cannot be restored')
        challenge.receive("b"*64)
        challenge.receive(None)
        self.assertIsNone(challenge.take(), 'missing challenge clears cached state')

    def test_concurrent_takes_spend_only_once(self):
        challenge = pull.HeartbeatStorageChallenge()
        challenge.receive("a"*64)
        with pull.concurrent.futures.ThreadPoolExecutor(8) as pool:
            values = list(pool.map(lambda _:challenge.take(), range(8)))
        self.assertEqual(values.count("a"*64), 1)
        self.assertEqual(values.count(None), 7)


class HeartbeatFreshStorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cfg = support.NASPullTests().config(Path(self.tmp.name))
        self.storage = dict(available=True, total_bytes=10000, free_bytes=9000)
        self.sent = []

    def transport(self, url, method, headers, **kwargs):
        self.sent.append(json.loads(kwargs['body']))
        self.assertEqual(method, 'POST')
        self.assertEqual(kwargs['timeout'], pull.HEARTBEAT_TIMEOUT_SEC)
        self.assertLessEqual(pull.heartbeat_monotonic(), kwargs['send_deadline'])
        self.assertEqual(headers['Authorization'], 'Bearer '+self.cfg.api_key)
        return 200, {}, b'{"ok":true}'

    def test_installed_artifact_receipt_reads_stable_current_bytes_only(self):
        self.cfg.is_candidate = False
        body = b"reviewed installed artifact fixture"
        self.cfg.current_file.write_bytes(body)
        runtime = pull.Runtime(self.cfg)
        runtime.host_facts = lambda: {"machine": "x86_64"}
        with mock.patch.object(pull, "CLIENT_VERSION", pull.NAS_INSTALLED_RECEIPT_VERSION), \
             mock.patch.object(pull, "executing_release_sha256", side_effect=AssertionError("manifest is not a receipt")):
            def release():
                return runtime.heartbeat_payload(None)["host"]["release"]
            self.assertNotIn("artifact_sha256", release())
            runtime.stable_marked = True
            self.assertEqual(release()["artifact_sha256"], hashlib.sha256(body).hexdigest())
            self.cfg.is_candidate = True
            self.assertNotIn("artifact_sha256", release())
            self.cfg.is_candidate = False
            self.cfg.current_file.write_bytes(body + b"changed")
            self.assertEqual(release()["artifact_sha256"], hashlib.sha256(body + b"changed").hexdigest())
            self.cfg.current_file.unlink()
            self.assertNotIn("artifact_sha256", release())
            target = self.cfg.state_dir / "external.py"
            target.write_bytes(body)
            self.cfg.current_file.symlink_to(target)
            self.assertNotIn("artifact_sha256", release())
        with mock.patch.object(pull, "CLIENT_VERSION", pull.FAST_COPY_CLIENT_VERSION):
            self.assertNotIn("artifact_sha256", runtime.heartbeat_payload(None)["host"]["release"])

    def test_receipt_release_keeps_fast_copy_and_no_local_stitch(self):
        with mock.patch.object(pull, "CLIENT_VERSION", pull.NAS_INSTALLED_RECEIPT_VERSION), \
             mock.patch.object(pull, "r2_fast_copy_worker") as fast, \
             mock.patch.object(pull, "r2_copy_worker") as slow:
            self.assertFalse(pull.StitchSwitches().local_enabled)
            pull.r2_copy_loop(None, None, None, None)
            fast.assert_called_once_with(None, None, None, None)
            slow.assert_not_called()

    def test_sample_follows_inventory_and_host_assembly(self):
        runtime = pull.Runtime(self.cfg)
        events = []
        runtime.set_storage(dict(available=True, total_bytes=10000, free_bytes=1))
        def host():
            events.append('host')
            runtime.storage_observed_monotonic -= 1000  # payload construction can stall beyond the cached age
            return {'machine':'x86_64'}
        runtime.host_facts = host
        def storage(cfg):
            events.append('storage')
            return self.storage
        with mock.patch.object(pull, 'storage_status', side_effect=storage), mock.patch.object(pull, 'r2_copy_http', side_effect=self.transport):
            self.assertEqual(pull.send_heartbeat(self.cfg, runtime, None), {'ok':True})
        self.assertEqual(events, ['host', 'storage'])
        self.assertEqual(self.sent[0]['storage'], self.storage)
        self.assertEqual(runtime.storage['free_bytes'], 1)  # transport did not use or mutate the cache

    def test_every_compatibility_retry_resamples_after_payload_changes(self):
        payload = dict(storage={'available':True,'free_bytes':1}, cleanup={'quarantined':1}, host={'machine':'x86_64','release':{'stable':True}})
        runtime = SimpleNamespace(heartbeat_payload=lambda outage:copy.deepcopy(payload), storage_challenge=pull.HeartbeatStorageChallenge())
        runtime.storage_challenge.receive('a'*64)
        samples = [dict(self.storage, free_bytes=n) for n in (300, 200, 100)]
        def transport(*args, **kwargs):
            self.transport(*args, **kwargs)
            return (400, {}, b'') if len(self.sent) < 3 else (200, {}, json.dumps(dict(ok=True, storage_challenge_nonce='b'*64)).encode())
        with mock.patch.object(pull, 'storage_status', side_effect=samples) as read, mock.patch.object(pull, 'r2_copy_http', side_effect=transport):
            self.assertEqual(pull.send_heartbeat(self.cfg, runtime, None), {'ok':True})
        self.assertEqual(read.call_count, 3)
        self.assertEqual([p['storage'].get('challenge_nonce') for p in self.sent], ['a'*64, None, None])
        self.assertEqual(runtime.storage_challenge.take(), 'b'*64)
        self.assertEqual([p['storage']['free_bytes'] for p in self.sent], [300, 200, 100])
        self.assertIn('cleanup', self.sent[0]); self.assertNotIn('cleanup', self.sent[1])
        self.assertIn('release', self.sent[1]['host']); self.assertNotIn('release', self.sent[2]['host'])
        self.assertEqual(payload['storage']['free_bytes'], 1)

    def test_unavailable_failed_and_aged_reads_skip_entire_heartbeat(self):
        for result in ({'available':False}, OSError('unmounted')):
            with self.subTest(result=result), mock.patch.object(pull, 'storage_status', **({'side_effect':result} if isinstance(result, Exception) else {'return_value':result})), mock.patch.object(pull, 'r2_copy_http') as transport:
                with self.assertRaises((pull.HeartbeatStorageUnavailable, OSError)):
                    pull.heartbeat_request_json(self.cfg, {'storage':self.storage})
                transport.assert_not_called()
        now = [100.0]
        def slow(cfg):
            now[0] += pull.STORAGE_TELEMETRY_MAX_AGE_SEC+1
            return self.storage
        with mock.patch.object(pull, 'heartbeat_monotonic', side_effect=lambda:now[0]), mock.patch.object(pull, 'storage_status', side_effect=slow), mock.patch.object(pull, 'r2_copy_http') as transport:
            with self.assertRaises(pull.HeartbeatStorageUnavailable):
                pull.heartbeat_request_json(self.cfg, {})
            transport.assert_not_called()

    def server(self, handler):
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda:(server.shutdown(), server.server_close(), thread.join(2)))
        self.cfg.api_base = 'http://127.0.0.1:%d' % server.server_port
        return server

    def test_real_boot_loss_and_recovery_never_reuse_nonce(self):
        runtime = pull.Runtime(self.cfg)
        requests, samples, received = [], [], {}
        nonces = ['a'*64, 'b'*64, 'c'*64, 'd'*64]
        real_receive = runtime.storage_challenge.receive
        def receive(nonce):
            received[nonce] = pull.heartbeat_monotonic()
            real_receive(nonce)
        runtime.storage_challenge.receive = receive
        def storage(cfg):
            samples.append(pull.heartbeat_monotonic())
            return dict(self.storage, free_bytes=9000+len(samples))
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                requests.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                if len(requests) == 2:
                    # The server accepted the echoed nonce and rotated, but the
                    # response was lost. The next attempt must be display-only.
                    self.close_connection = True
                    return
                self.send_response(200); self.end_headers()
                self.wfile.write(json.dumps(dict(ok=True, storage_challenge_nonce=nonces[len(requests)-1])).encode())
        self.server(Handler)
        with mock.patch.object(pull, 'storage_status', side_effect=storage):
            self.assertEqual(pull.send_heartbeat(self.cfg, runtime, None), {'ok':True})
            with self.assertRaises(pull.R2CopyHold):
                pull.send_heartbeat(self.cfg, runtime, None)
            self.assertEqual(pull.send_heartbeat(self.cfg, runtime, None), {'ok':True})
            self.assertEqual(pull.send_heartbeat(self.cfg, runtime, None), {'ok':True})
        self.assertEqual([p['storage'].get('challenge_nonce') for p in requests], [None, nonces[0], None, nonces[2]])
        self.assertEqual([p['storage']['free_bytes'] for p in requests], [9001,9002,9003,9004])
        self.assertGreater(samples[1], received[nonces[0]])
        self.assertGreater(samples[3], received[nonces[2]])

    def test_failed_fresh_read_burns_nonce_without_sending(self):
        challenge = pull.HeartbeatStorageChallenge()
        challenge.receive('a'*64)
        with mock.patch.object(pull, 'storage_status', return_value={'available':False}), mock.patch.object(pull, 'r2_copy_http') as transport:
            with self.assertRaises(pull.HeartbeatStorageUnavailable):
                pull.heartbeat_request_json(self.cfg, {}, challenge)
            transport.assert_not_called()
        with mock.patch.object(pull, 'storage_status', return_value=self.storage), mock.patch.object(pull, 'r2_copy_http', side_effect=self.transport):
            self.assertEqual(pull.heartbeat_request_json(self.cfg, {'storage':{'challenge_nonce':'a'*64}}, challenge), {'ok':True})
        self.assertNotIn('challenge_nonce', self.sent[0]['storage'])

    def test_real_send_uses_fresh_sample_and_total_watchdog_reaps_child(self):
        received = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                received.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                self.send_response(200); self.end_headers()
                try:
                    for _ in range(30):
                        self.wfile.write(b' '); self.wfile.flush(); time.sleep(.05)
                except (BrokenPipeError, ConnectionResetError): pass
        self.server(Handler)
        children = []
        real = pull.subprocess.Popen
        def spawn(*args, **kwargs):
            child = real(*args, **kwargs); children.append(child); return child
        started = time.monotonic()
        with mock.patch.object(pull, 'storage_status', return_value=self.storage), mock.patch.object(pull, 'HEARTBEAT_TIMEOUT_SEC', .4), mock.patch.object(pull.subprocess, 'Popen', side_effect=spawn):
            with self.assertRaises(pull.R2CopyHold):
                pull.heartbeat_request_json(self.cfg, {'storage':{'free_bytes':1}})
        elapsed = time.monotonic()-started
        self.assertLess(elapsed, 1.5)
        self.assertEqual(received[0]['storage'], self.storage)
        self.assertEqual(len(children), 1); self.assertIsNotNone(children[0].poll())
        print('v5 trickle: total=%.3fs budget=0.4s, child reaped' % elapsed)

    def test_real_https_send_uses_shared_age_check_and_validates_peer(self):
        root = Path(self.tmp.name)
        cert, key, config = root/'cert.pem', root/'key.pem', root/'ssl.conf'
        config.write_text('[req]\ndistinguished_name=dn\nx509_extensions=v3\nprompt=no\n[dn]\nCN=localhost\n[v3]\nsubjectAltName=DNS:localhost\nbasicConstraints=critical,CA:TRUE\n')
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(key), '-out', str(cert), '-days', '1', '-config', str(config)],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        received = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                received.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                self.send_response(200); self.end_headers(); self.wfile.write(b'{"ok":true}')
        server = self.server(Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(cert), str(key))
        server.socket = context.wrap_socket(server.socket, server_side=True)
        self.cfg.api_base = 'https://localhost:%d' % server.server_port
        child = pull.R2_COPY_HTTP_CHILD.replace('context=self._context',
            'context=__import__("ssl").create_default_context(cafile=%r)' % str(cert))
        with mock.patch.object(pull, 'R2_COPY_HTTP_CHILD', child), mock.patch.object(pull, 'storage_status', return_value=self.storage):
            self.assertEqual(pull.heartbeat_request_json(self.cfg, {}), {'ok':True})
        self.assertEqual(received[0]['storage'], self.storage)

    def test_child_deadline_survives_parent_exec_during_connect(self):
        root = Path(self.tmp.name)
        entered, child_pid, sampled = root/'connect', root/'pid', root/'sample'
        received_ages = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                received_ages.append(pull.heartbeat_monotonic()-float(sampled.read_text()))
                self.send_response(200); self.end_headers()
        self.server(Handler)
        # The replacement parent only reaps. No old Python thread/watchdog
        # survives exec; SIGALRM must terminate the child blocked in connect.
        code = r'''import importlib.util,json,os,sys,threading,time
from pathlib import Path
from types import SimpleNamespace
spec=importlib.util.spec_from_file_location("pull",sys.argv[1]);p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
entered,pidfile,sampled=map(Path,sys.argv[2:5])
p.HEARTBEAT_TIMEOUT_SEC=1.0;p.STORAGE_TELEMETRY_MAX_AGE_SEC=2.0
def storage(cfg):
 sampled.write_text(str(p.heartbeat_monotonic()));return dict(available=True,total_bytes=1000,free_bytes=500)
p.storage_status=storage
p.R2_COPY_HTTP_CHILD=p.R2_COPY_HTTP_CHILD.replace("class FreshHTTP(FreshSend,http.client.HTTPConnection):pass",
 "class FreshHTTP(FreshSend,http.client.HTTPConnection):\n    def connect(self):\n        super().connect();open(%r,'w').close();time.sleep(3)" % str(entered))
real=p.subprocess.Popen
def spawn(*a,**k):
 child=real(*a,**k);pidfile.write_text(str(child.pid));return child
p.subprocess.Popen=spawn
def replace():
 while not entered.exists():time.sleep(.005)
 os.execv(sys.executable,[sys.executable,"-c","import json,os,sys;_,s=os.waitpid(int(sys.argv[1]),0);print(json.dumps(dict(signaled=os.WIFSIGNALED(s),signal=os.WTERMSIG(s) if os.WIFSIGNALED(s) else None)))",pidfile.read_text()])
threading.Thread(target=replace,daemon=True).start()
p.heartbeat_request_json(SimpleNamespace(api_base=sys.argv[5],api_key="synthetic"),{})
'''
        parent = subprocess.Popen([sys.executable, '-I', '-c', code, str(support.MODULE_PATH), str(entered), str(child_pid), str(sampled), self.cfg.api_base],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = parent.communicate(timeout=6)
            self.assertEqual(parent.returncode, 0, stderr.decode())
            self.assertTrue(entered.exists(), 'parent must exec during child connect')
            self.assertEqual(json.loads(stdout), dict(signaled=True, signal=signal.SIGALRM))
            elapsed = pull.heartbeat_monotonic()-float(sampled.read_text())
            self.assertLess(elapsed, 1.8)  # scaled1s deadline, including exec/reap scheduling
            self.assertTrue(all(age <= 3.0 for age in received_ages))  # scaled M=2+1
            self.assertEqual(received_ages, [])
            with self.assertRaises(ProcessLookupError):
                os.kill(int(child_pid.read_text()), 0)
            print('v5 parent exec during connect: child SIGALRM/reaped in %.3fs for1s budget; no payload' % elapsed)
        finally:
            if parent.poll() is None:
                parent.kill(); parent.wait()
            parent.stdout.close(); parent.stderr.close()
            if child_pid.exists():
                try: os.kill(int(child_pid.read_text()), signal.SIGKILL)
                except ProcessLookupError: pass

    def test_child_rechecks_age_after_connect_before_any_post(self):
        received = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                received.append(True); self.send_response(200); self.end_headers()
        self.server(Handler)
        child = pull.R2_COPY_HTTP_CHILD.replace('class FreshHTTP(FreshSend,http.client.HTTPConnection):pass',
            'class FreshHTTP(FreshSend,http.client.HTTPConnection):\n    def connect(self):\n        super().connect();time.sleep(.3)')
        with mock.patch.object(pull, 'R2_COPY_HTTP_CHILD', child):
            with self.assertRaises(pull.R2CopyHold):
                pull.r2_copy_http(self.cfg.api_base, 'POST', {}, body=b'{}', timeout=2, send_deadline=pull.heartbeat_monotonic()+.15)
        self.assertEqual(received, [])


if __name__ == '__main__':
    unittest.main()
