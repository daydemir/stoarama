#!/usr/bin/env python3
"""Explicit, single-attempt invoice evidence transport; no staging or retry."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import stat
import subprocess
import sys
import time
import types
from enum import Enum

REMOTE_HOME = Path('/Users/jelly')
REMOTE_SOURCE = REMOTE_HOME / 'stoarama-evidence/capture-invoice-gaps-20261115/capture_invoice_gaps.py'
REMOTE_CREDENTIALS = REMOTE_HOME / 'Build/stoarama/local/do-capture.env'
PYTHON = '/usr/bin/python3'
SSH = ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', '-o', 'ForwardAgent=no', 'jelly@jellys-mac-mini']
LOCAL_SECONDS = 60
REAP_SECONDS = 5
ENV_KEYS = {'HOME', 'PATH', 'LANG', 'TMPDIR', 'USER', 'LOGNAME'}


class Refused(ValueError):
    pass


class Interrupted(BaseException):
    pass


class Outcome(str, Enum):
    HOLD = 'HOLD'
    COMPLETE = 'complete'


class Stage(str, Enum):
    PREPARE = 'prepare'
    CREDENTIAL = 'credential'
    EXEC = 'exec'


def require(ok, message):
    if not ok:
        raise Refused(message)


def checked_file(path, sha, limit):
    require(re.fullmatch('[a-f0-9]{64}', sha), 'invalid checksum')
    path = Path(path)
    require(path.is_absolute() and path.parent.resolve(strict=True) == path.parent, 'noncanonical source parent')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_uid == os.getuid()
                and not before.st_mode & 0o022 and 0 < before.st_size <= limit, 'source ownership/type/mode/size')
        raw = os.read(fd, limit + 1)
        after = os.fstat(fd)
        fields = ('st_dev', 'st_ino', 'st_mode', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        require(tuple(getattr(before, k) for k in fields) == tuple(getattr(after, k) for k in fields)
                and len(raw) == before.st_size and hashlib.sha256(raw).hexdigest() == sha, 'source changed or checksum differs')
        return raw
    finally:
        os.close(fd)


def core(raw):
    module = types.ModuleType('invoice_core')
    exec(compile(raw, '<checksum-pinned-invoice-core>', 'exec'), module.__dict__)
    return module


def token_file():
    path = REMOTE_CREDENTIALS
    require(path.parent.resolve(strict=True) == path.parent, 'noncanonical credential parent')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                and stat.S_IMODE(info.st_mode) == 0o600 and 0 < info.st_size <= 65536, 'credential ownership/type/mode/size')
        raw = os.read(fd, 65537)
        after = os.fstat(fd)
        fields = ('st_dev', 'st_ino', 'st_mode', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        require(len(raw) == info.st_size and tuple(getattr(info, k) for k in fields)
                == tuple(getattr(after, k) for k in fields), 'credential changed during read')
    finally:
        os.close(fd)
    values = {}
    for line in raw.decode('utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[7:]
        key, value = line.split('=', 1)
        require(re.fullmatch('[A-Z][A-Z0-9_]*', key) and key not in values, 'credential binding syntax')
        parts = shlex.split(value, comments=True, posix=True)
        require(len(parts) <= 1, 'credential literal syntax')
        values[key] = parts[0] if parts else ''
    token = values.get('DIGITALOCEAN_TOKEN', '')
    require(token and token.strip() == token and all(32 < ord(c) < 127 for c in token)
            and not any(c in token for c in '$`\\'), 'credential literal unavailable')
    return token


def reap(child):
    if child.poll() is None:
        child.terminate()  # Only our own process; never SIGKILL or remote kill.
    child.wait(timeout=REAP_SECONDS)


def write_receipt(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as output:
        json.dump(value, output, sort_keys=True, allow_nan=False)
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())
    directory = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def remote(source_sha):
    require(Path.home() == REMOTE_HOME, 'Jelly required')
    raw = checked_file(REMOTE_SOURCE, source_sha, 65536)
    module = core(raw)
    start = time.monotonic()
    end = start + module.RUN_SECONDS
    env = {k: v for k, v in os.environ.items() if k in ENV_KEYS}
    child = None
    stage = Stage.PREPARE
    with module.Deadline(start, end) as deadline:
        watcher = module.ChannelWatch(sys.stdin.buffer)
        try:
            watcher.start()
            common = ['--source-sha', source_sha, '--monotonic-start', repr(start), '--deadline', repr(end)]
            child = subprocess.Popen([PYTHON, '-I', '-S', '-B', '-c', raw.decode(), 'prepare', *common],
                                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
            require(child.wait(timeout=max(0.001, end - time.monotonic())) == 0, 'preparation refused')
            deadline.check()
            stage = Stage.CREDENTIAL
            env['DIGITALOCEAN_TOKEN'] = token_file()  # Reservation and preparer exit precede auth access.
            deadline.check()
            watcher.close()
            require(not watcher.disconnected.is_set(), 'owned input changed before exec')
            deadline.check()
            stage = Stage.EXEC
            # Exact verified bytes; no mutable executable path reopened. The
            # absolute deadline and armed timer survive exec; no budget reset.
            os.execve(PYTHON, [PYTHON, '-I', '-S', '-B', '-c', raw.decode(), 'collect', *common], env)
        except (Exception, module.Cancelled) as error:
            if child is not None:
                reap(child)
            attempt = module.REMOTE_OUT / 'attempt.json'
            if attempt.exists():
                value = module.strict_json(module.read_regular(attempt, 4096))
                if value.get('shell_pid') == os.getpid() and value.get('monotonic_start') == start:
                    failure = module.REMOTE_OUT / 'failure.json'
                    if not failure.exists():
                        module.write_new(failure, module.canonical({'stage': stage, 'outcome': deadline.outcome if isinstance(error, module.Cancelled) else 'refused', 'exception_type': type(error).__name__, 'apply_authority': False}))
            raise Refused('owned remote attempt refused; no retry') from None
        finally:
            watcher.close()


def command(collector_path, source_sha, transport_path, transport_sha):
    checked_file(collector_path, source_sha, 65536)
    script = checked_file(transport_path, transport_sha, 65536).decode()
    return SSH + [shlex.join([PYTHON, '-I', '-S', '-B', '-c', script, 'remote', '--source-sha', source_sha])]


def run(argv, capture):
    capture = Path(capture)
    require(capture.is_absolute() and capture.parent.resolve(strict=True) == capture.parent, 'capture parent is not canonical')
    capture.mkdir(mode=0o700)  # Exclusive BEFORE SSH: failed attempts are consumed.
    write_receipt(capture / 'intent.json', {'schema': 'stoarama.capture-invoice-transport.v1', 'started_monotonic': time.monotonic(), 'retry': False})
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}

    def stop(*_):
        raise Interrupted()

    for sig in previous:
        signal.signal(sig, stop)
    child = None
    result = {'outcome': Outcome.HOLD, 'reaped': False, 'exit_code': None, 'retry': False, 'apply_authority': False}
    deadline = time.monotonic() + LOCAL_SECONDS
    try:
        with os.fdopen(os.open(capture / 'stdout.bin', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as stdout, os.fdopen(os.open(capture / 'stderr.bin', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as stderr:
            env = {k: v for k, v in os.environ.items() if k in ENV_KEYS | {'SSH_AUTH_SOCK'}}
            child = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=stdout, stderr=stderr, env=env)
            result['exit_code'] = child.wait(timeout=max(0.001, deadline - time.monotonic()))
            result['reaped'] = True
            stdout.flush()
            stderr.flush()
            os.fsync(stdout.fileno())
            os.fsync(stderr.fileno())
            require(result['exit_code'] == 0 and stderr.tell() == 0 and stdout.tell() <= 65536, 'transport did not return clean bounded success')
            def unique(pairs):
                body = {}
                for key, value in pairs:
                    require(key not in body, 'duplicate result field')
                    body[key] = value
                return body
            body = json.loads((capture / 'stdout.bin').read_bytes(), object_pairs_hook=unique)
            require(body == {'outcome': 'complete', 'requests': 7, 'manifest_sha256': body.get('manifest_sha256'), 'apply_authority': False}
                    and body['apply_authority'] is False and type(body['requests']) is int
                    and type(body['manifest_sha256']) is str and re.fullmatch('[a-f0-9]{64}', body['manifest_sha256']), 'remote result contract')
            result.update(outcome=Outcome.COMPLETE, manifest_sha256=body['manifest_sha256'])
    except (Exception, Interrupted):
        result['outcome'] = Outcome.HOLD
        if child is not None:
            if child.stdin is not None:
                child.stdin.close()
            try:
                reap(child)
                result['reaped'] = True
                result['exit_code'] = child.returncode
            except subprocess.TimeoutExpired:
                result['reaped'] = False
    finally:
        if child is not None and child.stdin is not None and not child.stdin.closed:
            child.stdin.close()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        for name in ['stdout.bin', 'stderr.bin']:
            path = capture / name
            if path.exists():
                digest = hashlib.sha256()
                with path.open('rb') as stream:
                    for chunk in iter(lambda: stream.read(65536), b''):
                        digest.update(chunk)
                result[name] = {'size_bytes': path.stat().st_size, 'sha256': digest.hexdigest()}
        write_receipt(capture / 'transport-result.json', result)
    require(result['outcome'] == Outcome.COMPLETE, 'transport HOLD; preserve captures, no retry; inspect reaped status')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['plan', 'collect', 'remote'])
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--collector-file', type=Path)
    parser.add_argument('--transport-file', type=Path)
    parser.add_argument('--transport-sha')
    parser.add_argument('--capture-dir', type=Path)
    args = parser.parse_args()
    if args.mode == 'remote':
        remote(args.source_sha)
    else:
        require(args.collector_file and args.transport_file and args.transport_sha, 'both public source pins required')
        argv = command(args.collector_file, args.source_sha, args.transport_file, args.transport_sha)
        if args.mode == 'plan':
            print(json.dumps({'argv': argv, 'execution_authorized': False}))
        else:
            require(args.capture_dir, 'new local capture directory required')
            print(json.dumps(run(argv, args.capture_dir)))


if __name__ == '__main__':
    try:
        main()
    except (Exception, Interrupted):
        raise SystemExit('invoice transport refused; HOLD, no retry') from None
