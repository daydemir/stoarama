"""Read the same immutable folder/audit scope as admission; no media or writes."""
import collections
import gzip
import hashlib
import io
import json
import os
import pathlib
import re
import subprocess

REPORT_SHA = '64eee0bc589c9be4dcab5b5ea7668270291a07ce4fd0db98dcd0a484e19d84c1'
REPORT_KEY = 'staging/best14-20261021/nas-retention-20261025/current-folder-preparation-20261031/' + REPORT_SHA + '/retention-only-folder-report-109.json.gz'
INDEX_SHA = '82b067b28b4e1ad0226f1ef32d670a22af11d825bf8bd1bc04d2ab26b90c4e20'
PREFIX = 'staging/best14-20261021/nas-retention-20261025/frozen-index-20261102/'
INDEX_KEY = PREFIX + INDEX_SHA + '/index.json.gz'
AUDIT_SHA = '480d85a49288d79e201b14405dc72d0f6fc979a0fd06f1b7a95663053583865b'


def read_pinned(key, sha, compressed_limit, plain_limit):
    url = 'https://' + os.environ['R2_ACCOUNT_ID'] + '.r2.cloudflarestorage.com/' + os.environ['R2_BUCKET'] + '/' + key
    result = subprocess.run([
        'curl', '--fail', '--silent', '--show-error', '--connect-timeout', '10', '--max-time', '45',
        '--max-filesize', str(compressed_limit), '--aws-sigv4', 'aws:amz:auto:s3',
        '--variable', '%R2_ACCESS_KEY_ID', '--variable', '%R2_SECRET_ACCESS_KEY',
        '--expand-user', '{{R2_ACCESS_KEY_ID}}:{{R2_SECRET_ACCESS_KEY}}',
        '--user-agent', 'stoarama-operator/1', url,
    ], capture_output=True, timeout=50)
    if result.returncode or len(result.stdout) > compressed_limit or hashlib.sha256(result.stdout).hexdigest() != sha:
        raise RuntimeError('reviewed cleanup metadata unavailable or hash differs')
    with gzip.GzipFile(fileobj=io.BytesIO(result.stdout)) as archive:
        plain = archive.read(plain_limit + 1)
    if len(plain) > plain_limit:
        raise RuntimeError('reviewed cleanup metadata exceeds decompressed bound')
    return json.loads(plain)


def valid_path(path):
    return (isinstance(path, str) and bool(path) and not path.startswith(('/', '#', '@'))
            and not any(c in path for c in ('\\', '\x00', '\r', '\n'))
            and all(p not in ('', '.', '..') for p in path.split('/')))


def load_scope():
    report = read_pinned(REPORT_KEY, REPORT_SHA, 500000, 32 << 20)
    nodes = report['complete_node_actions']
    owners = {n['path']: n['owner_stream_ids'][0] for n in nodes if len(n['owner_stream_ids']) == 1}
    index = read_pinned(INDEX_KEY, INDEX_SHA, 4 << 20, 32 << 20)
    if (len(nodes) != 3304 or len(owners) != 3288 or index['schema'] != 'nas-cleanup-frozen-index-v2'
            or index['source_audit_sha256'] != AUDIT_SHA or index['report_sha256'] != REPORT_SHA
            or index['audit_id'] != '6cd48108-ca05-4df7-988b-0bd7e4d320fe'
            or index['source_audit_bytes'] != 88667812 or index['entry_totals']['entries'] != 2943864
            or {p: v['owner_stream_id'] for p, v in index['parents'].items()} != owners):
        raise RuntimeError('reviewed cleanup scope provenance differs')
    return index


def admissible_candidates(rows):
    index = load_scope()
    groups = collections.defaultdict(list)
    counts = collections.Counter(r['path'] for r in rows)
    identities = collections.Counter((r['kind'], r['source_id']) for r in rows)
    names = counts.copy()
    names.update(r['path'] + '.stoarama.json' for r in rows if r['kind'] == 'raw')
    for row in rows:
        path = row['path']
        parent = str(pathlib.PurePosixPath(path).parent)
        owner = index['parents'].get(parent)
        if (row['kind'] in ('raw', 'collated') and valid_path(path) and owner and owner['owner_stream_id'] == row['stream_id'] > 0
                and identities[row['kind'], row['source_id']] == 1 and names[path] == 1
                and (row['kind'] != 'raw' or names[path + '.stoarama.json'] == 1)
                and 0 < row['size_bytes'] <= 32 << 30
                and re.fullmatch('[0-9a-f]{64}', row['sha256'])):
            groups[parent].append(row)
    accepted = set()
    for parent, candidates in groups.items():
        metadata = index['parents'][parent]
        wanted = {r['path'] for r in candidates}
        wanted.update(r['path'] + '.stoarama.json' for r in candidates if r['kind'] == 'raw')
        found = {}
        entries = 0
        for ref in metadata['shards']:
            if (not re.fullmatch('[0-9a-f]{64}', ref['sha256']) or ref['key'] != PREFIX + ref['sha256'] + '/files.json.gz'
                    or not 0 < ref['bytes'] <= 131072 or not 0 < ref['entries'] <= 20000):
                raise RuntimeError('invalid reviewed cleanup shard reference')
            shard = read_pinned(ref['key'], ref['sha256'], ref['bytes'], 2 << 20)
            if shard['parent'] != parent or len(shard['entries']) != ref['entries']:
                raise RuntimeError('reviewed cleanup shard accounting differs')
            entries += len(shard['entries'])
            for item in shard['entries']:
                path = item['path']
                if path not in wanted:
                    continue
                fp = item['frozen']
                valid = (path not in found and str(pathlib.PurePosixPath(path).parent) == parent
                         and not item.get('path_bytes_b64') and item['kind'] == 'file' and not item.get('error')
                         and item['links'] == 1 and len(fp) == 5 and fp[3] > 0 and fp[4] > 0)
                found[path] = fp if valid else None
        if entries != metadata['entries']:
            raise RuntimeError('reviewed cleanup parent accounting differs')
        for row in candidates:
            fp = found.get(row['path'])
            sidecar = found.get(row['path'] + '.stoarama.json')
            if fp and fp[0] == row['size_bytes'] and (row['kind'] != 'raw' or sidecar and 0 < sidecar[0] <= 1 << 20):
                accepted.add((row['kind'], row['source_id']))
    return [r for r in rows if (r['kind'], r['source_id']) in accepted]
