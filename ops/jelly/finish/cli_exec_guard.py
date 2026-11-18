"""Test-only audit guard: no retired verifier and only the approved CLI artifact."""
import os
from pathlib import Path
import shutil
import sys
import json
import cli_binding

APPROVED_SHA256 = 'a62153521a57ee83f71bb1fd61ceae2ecb47ea5885cc05d559c84031541d18ef'


def check_executable(executable):
    value = os.fsdecode(executable)
    resolved = shutil.which(value) if not os.path.isabs(value) else value
    if resolved is None or not Path(resolved).is_file():
        return None  # A missing legacy path must reach the kernel's ENOENT.
    path = Path(resolved)
    actual = cli_binding.digest(path)
    if actual == cli_binding.RETIRED_CLI_SHA256:
        raise AssertionError('retired verifier selected for execution')
    if not path.name.lower().startswith('stoaramactl'):
        return None
    try:
        record = cli_binding.load()
    except cli_binding.ContractError:
        raise AssertionError('CLI execution has no valid shared binding') from None
    # The one pinned dispatcher is a launcher, not a second CLI binary.
    launcher = record['launcher']
    if (path != Path(record['binary']['path']) and path == Path(launcher['path'])
            and actual == launcher['sha256']):
        return dict(path=str(path), sha256=actual, role='bound_launcher')
    if path != Path(record['binary']['path']) or actual != APPROVED_SHA256:
        raise AssertionError('non-approved Stoarama CLI selected for execution')
    return dict(path=str(path), sha256=actual, role='approved_binary')


def install_guard(log=None):
    events = []
    def audit(event, args):
        if event not in ('subprocess.Popen', 'os.exec'):
            return
        proof = check_executable(args[0])
        if proof is not None:
            events.append(proof)
            if log is not None:
                with Path(log).open('a') as output:
                    output.write(json.dumps(proof, sort_keys=True) + '\n')
    sys.addaudithook(audit)
    return events


def guard_environment(root):
    """Install the same test guard before a fresh Python child runs any code."""
    folder = Path(root).resolve() / 'python-exec-guard'
    folder.mkdir(exist_ok=True)
    (folder / 'sitecustomize.py').write_text(
        'import os\n'
        'try:\n'
        '    from cli_exec_guard import install_guard\n'
        '    install_guard()\n'
        'except BaseException:\n'
        '    os._exit(125)\n')
    paths = [str(folder), str(Path(__file__).resolve().parent)]
    if os.environ.get('PYTHONPATH'):
        paths.append(os.environ['PYTHONPATH'])
    return dict(PYTHONPATH=os.pathsep.join(paths))


def guard_test_module():
    import tempfile
    import unittest
    from unittest.mock import patch
    install_guard()
    root = tempfile.TemporaryDirectory()
    unittest.addModuleCleanup(root.cleanup)
    change = patch.dict(os.environ, guard_environment(root.name))
    change.start()
    unittest.addModuleCleanup(change.stop)
