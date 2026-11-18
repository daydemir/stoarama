"""Disposable CLI binding for the existing offline wrapper integration tests."""
import hashlib
import json
from pathlib import Path
from cli_exec_guard import guard_environment


def pin(path):
    return dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def environment(root, launcher):
    binary = root / 'stoaramactl.bin'
    binary.write_bytes(launcher.read_bytes())
    binary.chmod(0o700)
    folder = Path(__file__).parent
    record = dict(schema='stoarama.cli-binding.v1', binary=pin(binary), launcher=pin(launcher),
                  references=dict(runner=pin(folder.parent/'cleanup-runner/runner.py'),
                                  tick=pin(folder/'tick.py'), recovery=pin(folder/'refresh_recovery.py'),
                                  cli_env=pin(folder/'cli_env.py'), launcher=pin(launcher)))
    path = root / 'cli-binding.json'
    raw = json.dumps(record).encode()
    path.write_bytes(raw)
    return dict(guard_environment(root), STOARAMA_CLI_BINDING=str(path),
                STOARAMA_CLI_BINDING_SHA256=hashlib.sha256(raw).hexdigest())
