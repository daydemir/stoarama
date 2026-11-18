"""Resolve the single Stoarama CLI artifact and its existing environment shim."""
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import shlex

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
    for name, value in re.findall(r'\b([A-Za-z_]\w*)\s*=\s*[\'\"]?([a-f0-9]{64})\b', text):
        if cli_digest_name(name):
            require(value == binary['sha256'], 'another stoaramactl hash is referenced')
    try:
        tree = ast.parse(text)
    except SyntaxError:
        for token in shlex.split(text, comments=True):
            value = token.partition('=')[2] if '=' in token else token
            if Path(value).name.startswith('stoaramactl'):
                require(value in allowed, 'unbound stoaramactl shell reference')
        return
    # The shim's exec argv[0] is a display name, not executable selection.
    # It is allowed only when exec's actual target comes from bound_cli().
    bound_names = {target.id for assign in ast.walk(tree) if isinstance(assign, ast.Assign)
                   and isinstance(assign.value, ast.Call) and isinstance(assign.value.func, ast.Name)
                   and assign.value.func.id == 'bound_cli' for target in assign.targets if isinstance(target, ast.Name)}
    display_names = set()
    for call in ast.walk(tree):
        if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                and isinstance(call.func.value, ast.Name) and call.func.value.id == 'os'
                and call.func.attr == 'execv' and len(call.args) == 2):
            target = call.args[0]
            if (isinstance(target, ast.Call) and isinstance(target.func, ast.Name)
                    and target.func.id == 'str' and len(target.args) == 1):
                target = target.args[0]
            if (isinstance(target, ast.Name) and target.id in bound_names
                    or isinstance(target, ast.Call) and isinstance(target.func, ast.Name)
                    and target.func.id == 'bound_cli' and not target.args and not target.keywords):
                argv = call.args[1]
                if isinstance(argv, ast.BinOp) and isinstance(argv.op, ast.Add):
                    argv = argv.left
                if isinstance(argv, ast.List) and argv.elts:
                    display_names.add(id(argv.elts[0]))
    for node in ast.walk(tree):
        values = []
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and Path(node.value).name.startswith('stoaramactl'):
            require(node.value in allowed or node.value == 'stoaramactl' and id(node) in display_names,
                    'unbound stoaramactl path component')
        if isinstance(node, ast.Assign):
            names = [x.id for x in node.targets if isinstance(x, ast.Name)]
            if any(cli_digest_name(x) for x in names):
                values.append(node.value)
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and cli_digest_name(node.target.id):
            values.append(node.value)
        if isinstance(node, ast.keyword) and node.arg and cli_digest_name(node.arg):
            values.append(node.value)
        for value in values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                require(value.value == binary['sha256'], 'another stoaramactl hash is referenced')


def cli_digest_name(name):
    name = name.upper()
    return ('CLI' in name or 'STOARAMACTL' in name) and ('SHA' in name or 'HASH' in name)


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
