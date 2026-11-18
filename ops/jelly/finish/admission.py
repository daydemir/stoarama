"""Durable admission journal shared by the feeder and recovery wrapper."""
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
import shutil
from pathlib import Path
import tempfile
import uuid

LANES = {'tick': 'lock', 'recovery': 'recovery.lock'}


class Latched(Exception):
    pass


def sync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def publish(path: Path, record: dict) -> None:
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as output:
        temp = Path(output.name)
        try:
            json.dump(record, output)
            output.flush()
            os.fsync(output.fileno())
            output.close()
            os.replace(temp, path)
            sync_dir(path.parent)
        finally:
            temp.unlink(missing_ok=True)


@contextmanager
def gate(state: Path):
    with (state / 'admission.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def load(state: Path) -> dict:
    return json.loads((state / 'admission.json').read_text())


def stop_status(state: Path, reason: str) -> None:
    publish(state / 'status.json', {'schema': 'stoarama.finish-status.v1',
            'updated_at': datetime.now(timezone.utc).isoformat(), 'stop_reason': reason})


def interrupted(state: Path, record: dict, lane: str, finishing: bool = False) -> bool:
    for active in record['active']:
        if active == lane:
            if not finishing:
                return True
            continue
        with (state / LANES[active]).open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                continue
            return True
    return False


class Attempt:
    @classmethod
    def start(cls, state: Path, lane: str, stack):
        # Acquire the lane lock AND validate old markers under the same gate.
        # A replacement cannot disguise an abandoned attempt's lock as live.
        with gate(state):
            lock = stack.enter_context((state / LANES[lane]).open('a'))
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise Latched() from None
            return cls(state, lane)

    def __init__(self, state: Path, lane: str):
        # Construction is internal to start(), under the shared gate.
        self.state = state
        self.lane = lane
        journal = state / 'admission.json'
        if journal.exists():
            record = load(state)
        else:
            record = {'failed': False, 'active': {}}
            # Upgrade must not erase the already-recorded failed D attempt.
            legacy = None
            for name in ('status.json', 'recovery.json'):
                path = state / name
                if path.exists():
                    previous = json.loads(path.read_text())
                    if not isinstance(previous, dict):
                        raise ValueError('invalid previous status')
                    if (previous.get('stop_reason') or previous.get('refresh_error')
                            or previous.get('problems', 0) > 0):
                        if legacy is None:
                            legacy = state / ('legacy-' + uuid.uuid4().hex)
                            legacy.mkdir(mode=0o700)
                        with (legacy / name).open('xb') as archived:
                            os.chmod(archived.name, 0o600)
                            archived.write(path.read_bytes())
                            archived.flush()
                            os.fsync(archived.fileno())
                        sync_dir(legacy)
                        record['failed'] = True
                        record['active']['legacy'] = legacy.name
            publish(journal, record)
        if record['failed'] or (lane == 'tick' and record.get('recovery_required')):
            raise Latched()
        # A durable active marker precedes every subprocess. Under the lane
        # flock, a previous own marker is necessarily an interrupted attempt.
        record['failed'] = interrupted(state, record, lane)
        if record['failed']:
            publish(journal, record)
            stop_status(state, 'evidence_unavailable')
            raise Latched()
        self.path = state / (lane + '-' + uuid.uuid4().hex)
        self.path.mkdir(mode=0o700)
        publish(self.path / 'attempt.json', {'lane': lane,
                'started_at': datetime.now(timezone.utc).isoformat()})
        record['active'][lane] = self.path.name
        publish(journal, record)

    def finish(self, reason: str, output: dict) -> bool:
        # Serialize success publication with failure latching. A sibling that
        # was already in flight may retain evidence but cannot publish an OK.
        with gate(self.state):
            record = load(self.state)
            for path in self.path.iterdir():
                with path.open('rb') as evidence:
                    os.fsync(evidence.fileno())
            sync_dir(self.path)
            failed = bool(reason) or record['failed'] or interrupted(self.state, record, self.lane, finishing=True)
            if failed:
                reason = reason or 'evidence_unavailable'
                record['failed'] = True
                publish(self.state / 'admission.json', record)
                publish(self.path / 'failure.json', {'reason': reason})
                if self.lane == 'recovery':
                    publish(self.state / 'recovery.json', output if reason == 'cleanup_recovery_problems'
                            else {'refresh_error': reason})
                    stop_status(self.state, 'cleanup_recovery_problems' if reason == 'cleanup_recovery_problems'
                                else 'evidence_unavailable')
                else:
                    if output.get('stop_reason') == reason:
                        publish(self.state / 'status.json', output)
                    else:
                        stop_status(self.state, reason)
                return False
            if self.lane == 'tick':
                publish(self.state / 'status.json', output)
            else:
                publish(self.state / 'recovery.json', output)
                publish(self.state / 'recovery-last-good.json', output)
            if self.lane == 'recovery':
                record['recovery_required'] = False
            del record['active'][self.lane]
            publish(self.state / 'admission.json', record)
            shutil.rmtree(self.path)
            return True


def binding(state: Path) -> str:
    """Bind the journal and every retained active attempt's evidence bytes."""
    paths = [state / 'admission.json']
    for name in load(state)['active'].values():
        paths.extend(sorted((state / name).iterdir()))
    hashes = {str(p.relative_to(state)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    return hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
