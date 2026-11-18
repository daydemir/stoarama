"""Resolve the single Stoarama CLI artifact and its existing environment shim."""
import ast
import hashlib
import json
import os
from pathlib import Path
import re

# Retained forever as incident evidence; its report ABI lacks checked_at.
RETIRED_CLI_SHA256 = 'a88984c75bf337a790e0ac34569435f3346cca812f2d131fa2ea5c29f253b42e'


class ContractError(RuntimeError):
    pass


def require(ok, reason):
    if not ok:
        raise ContractError('CLI binding: ' + reason)


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def verify_references(text, binary, launcher):
    """Check CLI references only; this is not an execution-closure analyzer."""
    allowed = {str(binary['path']), str(launcher['path'])}
    require(RETIRED_CLI_SHA256 not in text, 'retired stoaramactl hash is referenced')
    for path in re.findall(r'/[^\s\'"`<>\[\]{}(),;]+', text):
        if Path(path).name.startswith('stoaramactl'):
            require(path in allowed, 'another stoaramactl path is referenced')
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return  # Shell shims are checked for CLI paths above and pinned below.
    for node in ast.walk(tree):
        values = []
        if isinstance(node, ast.Assign):
            names = [x.id for x in node.targets if isinstance(x, ast.Name)]
            if any('CLI' in x.upper() and 'SHA' in x.upper() for x in names):
                values.append(node.value)
        if isinstance(node, ast.keyword) and node.arg and 'cli' in node.arg.lower() and 'sha' in node.arg.lower():
            values.append(node.value)
        for value in values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                require(value.value == binary['sha256'], 'another stoaramactl hash is referenced')


def _load():
    path = Path(os.environ['STOARAMA_CLI_BINDING'])
    expected = os.environ['STOARAMA_CLI_BINDING_SHA256']
    require(re.fullmatch('[a-f0-9]{64}', expected) and digest(path) == expected,
            'manifest digest differs')
    record = json.loads(path.read_bytes())
    require(set(record) == {'schema', 'binary', 'launcher', 'references'},
            'unknown binding or second CLI lineage')
    require(record['schema'] == 'stoarama.cli-binding.v1', 'schema differs')
    binary, launcher = record['binary'], record['launcher']
    require(binary['sha256'] != RETIRED_CLI_SHA256, 'retired CLI lineage is forbidden')
    require(Path(binary['path']).is_absolute() and Path(binary['path']).name == 'stoaramactl.bin',
            'binary must use the shared installed artifact slot')
    require(Path(launcher['path']).is_absolute(), 'launcher must be absolute')
    for item in (binary, launcher):
        require(re.fullmatch('[a-f0-9]{64}', item['sha256']) and digest(item['path']) == item['sha256'],
                'artifact digest differs')
    references = record['references']
    require(set(references) == {'runner', 'tick', 'recovery', 'cli_env', 'launcher'},
            'CLI caller binding is incomplete')
    require(references['launcher'] == launcher, 'launcher source binding differs')
    for item in references.values():
        require(Path(item['path']).is_absolute() and digest(item['path']) == item['sha256'],
                'caller digest differs')
        verify_references(Path(item['path']).read_text(), binary, launcher)
    return record


def load():
    try:
        return _load()
    except ContractError:
        raise
    except (KeyError, TypeError, ValueError, OSError, UnicodeError):
        raise ContractError('CLI binding is unavailable or malformed') from None


def bound_cli():
    return Path(load()['binary']['path'])


def bound_launcher(requested=None):
    path = Path(load()['launcher']['path'])
    require(requested is None or Path(requested) == path, 'caller selected a different CLI')
    return path
