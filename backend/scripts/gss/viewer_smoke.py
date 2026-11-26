"""Republish gate: can the GSS page open every hour of every stream?

Checks each stream of a generation the way the hour viewer loads it
(backend/web/global-delivery/app.js openDetail and coverage-growth.js
ensureDownloadPaths):
  - metadata.json lists the stream; its data asset (and .gz twin) parses, has its
    id and 14 days of 12 hours, every hour and part array well formed;
  - download paths: the data asset's current_download_paths for the growth.json
    generation, else the manifest (identity, period, generation, and every
    available output's path exactly as ensureDownloadPaths checks it);
  - every available manifest output has a download path (every file is reachable);
  - every served path is a delivery name (delivery_naming.check_paths), under the
    stream's catalog folder;
  - growth.json current_stream_counts period is the stream's period (registered(), which
    the viewer needs to list each assembly once), and every hour lists each served file
    once, through current-parts.js's projection, with every served file on some hour.

  viewer_smoke.py --registry DIR/registry.json.gz --assets DIR/objects IN/assets   (a build, before staging)
  viewer_smoke.py --current [--origin https://stoarama.com] [--generation G]     (the live public API)

Exits nonzero listing every broken stream.
"""
import argparse
import gzip
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import delivery_naming  # noqa: E402

API = '/api/v1/global-street-scores-delivery'
CONTROL = re.compile(r'[\x00-\x1f]')


class Offline:
    """A built or staged registry, its asset bodies looked up by R2 key in the given directories."""

    def __init__(self, registry, dirs):
        raw = Path(registry).read_bytes()
        self.registry = json.loads(gzip.decompress(raw) if raw[:2] == b'\x1f\x8b' else raw)
        self.dirs = dirs
        self.generation = self.registry['generation']

    def raw(self, path, generation=None):
        o = self.registry['assets'].get(path)
        if o is None:
            raise LookupError(f'asset {path} is not in the registry')
        for d in self.dirs:
            p = Path(d, o['key'])
            if p.exists():
                return p.read_bytes()
        raise LookupError(f'asset body {o["key"]} is in none of the asset directories')

    def get(self, path, generation=None):
        body = self.raw(path)
        obj = json.loads(gzip.decompress(body) if path.endswith('.gz') else body)
        if path.startswith('/api/coverage-delivery/manifest?'):
            # What the public metadata route injects.
            obj['generation'] = self.generation
            obj['download_script_sha256'] = self.registry['assets']['/download-script.py']['sha256']
        return obj


class Live:
    """The public delivery API, as the page reads it."""

    def __init__(self, origin, generation):
        self.origin, self.pinned = origin.rstrip('/'), generation
        self.generation = generation

    def raw(self, path, generation=None):
        q = {'path': path}
        if generation or self.pinned:
            q['generation'] = generation or self.pinned
        req = urllib.request.Request(f'{self.origin}{API}/metadata?{urllib.parse.urlencode(q)}',
                                     headers={'User-Agent': 'gss-viewer-smoke'})
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.read()

    def get(self, path, generation=None):
        body = self.raw(path, generation)
        return json.loads(gzip.decompress(body) if path.endswith('.gz') else body)


def path_problem(rid, output, seen):
    """ensureDownloadPaths' rule for one available manifest output, or None."""
    value = output.get('path')
    if output.get('recording_id') != rid or not isinstance(value, str) or not value.startswith(f'{rid}_') \
            or value.startswith('/') or '\\' in value or '..' in value.split('/') or CONTROL.search(value) \
            or str(output.get('id')) in seen:
        return f'unsafe current download path {value!r} for output {output.get("id")}'
    return None


def project(parts, assemblies, all_parts):
    """current-parts.js: the files an hour lists, each assembly once in its first input's hour."""
    originals = {str(p[0]): p for p in all_parts}
    if len(originals) != len(all_parts):
        raise ValueError('Duplicate original timeline identity')
    hour_ids, by_input = {str(p[0]) for p in parts}, {}
    for a in assemblies:
        ids = [str(i) for i in a['input_ids']]
        if any(i not in originals for i in ids):
            continue
        if len(set(ids)) != len(ids) or len(ids) < 2 or not a.get('seconds', 0) > 0:
            raise ValueError('Invalid current assembly')
        for i in ids:
            if i in by_input:
                raise ValueError('Overlapping current assemblies')
            by_input[i] = a
    seen, out = set(), []
    for p in parts:
        a = by_input.get(str(p[0]))
        if a is None:
            out.append(str(p[0]))
        elif str(a['input_ids'][0]) in hour_ids and a['sha256'] not in seen:
            seen.add(a['sha256'])
            out.append('sha256:' + a['sha256'])
    return out


def check_stream(src, r, gen, names, growth, catalog, mode):
    rid = r['id']
    problems = []
    data = src.get(r['data'])
    gz = r['data'] + '.gz'
    try:
        if src.get(gz) != data:
            problems.append(f'{gz} differs from {r["data"]}')
    except LookupError:
        pass
    if data.get('id') != rid or len(data.get('days', ())) != 14:
        return [f'stream data identity differs (id {data.get("id")}, {len(data.get("days", ()))} days)']
    for day in data['days']:
        hours = day.get('hours')
        if not isinstance(hours, list) or len(hours) != 12:
            problems.append(f'{day.get("date")}: {len(hours or ())} hours')
            continue
        for i, h in enumerate(hours):
            if len(h) != 12 or not isinstance(h[9], list) or not isinstance(h[10], list) or not h[7] < h[8]:
                problems.append(f'{day["date"]} hour {i + 1}: malformed hour')
            elif any(len(p) != 11 for p in h[9]):
                problems.append(f'{day["date"]} hour {i + 1}: malformed part')
    manifest = src.get(f'/api/coverage-delivery/manifest?stream={rid}&scope=available', gen)
    available = [o for o in manifest['outputs'] if o.get('available')]
    if data.get('current_download_generation') == gen and data.get('current_download_paths'):
        paths = data['current_download_paths']
    else:
        if manifest.get('recording_id') != rid or manifest.get('period') != [r['start'], r['end']] or manifest.get('generation') != gen:
            return problems + [f'current path identity differs: manifest recording {manifest.get("recording_id")}, '
                               f'period {manifest.get("period")} vs {[r["start"], r["end"]]}, generation {manifest.get("generation")} vs {gen}']
        paths = {}
        for o in available:
            bad = path_problem(rid, o, paths)
            if bad:
                return problems + [bad]
            paths[str(o['id'])] = o['path']
    missing = [str(o['id']) for o in available if str(o['id']) not in paths]
    if missing:
        problems.append(f'{len(missing)} available files have no download path, e.g. {missing[:3]}')
    served = sorted((rid, o['local_date'], paths[str(o['id'])]) for o in available if str(o['id']) in paths)
    problems += delivery_naming.check_paths(names, served)
    folder = catalog[rid].get('folder')
    if served and {p.split('/', 1)[0] for _, _, p in served} != {folder}:
        problems.append(f'catalog folder {folder!r} is not the folder of every served path')
    # coverage-growth.js registered(): without it the viewer lists each assembly once per input part.
    counts = growth.get('current_stream_counts', {}).get(str(rid))
    if counts is None or counts.get('period') != [r['start'], r['end']]:
        problems.append(f'growth.json current_stream_counts period {counts and counts.get("period")} is not {[r["start"], r["end"]]}')
        return problems
    assemblies = [a for a in growth.get('repairs', ()) if a['recording_id'] == rid and r['start'] <= a['local_date'] <= r['end']
                  and (not a.get('collections') or mode in a['collections'])]
    listed = []
    for day in data['days']:
        all_parts = [p for h in day['hours'] for p in h[9]]
        for i, h in enumerate(day['hours']):
            ids = project(h[9], assemblies, all_parts)
            dup = sorted({x for x in ids if ids.count(x) > 1})
            if dup:
                problems.append(f'{day["date"]} hour {i + 1:02d} lists {dup[:2]} more than once')
            listed += [x for x in ids if x in paths]
            unreachable = [x for x in ids if x not in paths]
            if unreachable:
                problems.append(f'{day["date"]} hour {i + 1:02d} lists files without a download path: {unreachable[:2]}')
    twice = sorted({x for x in listed if listed.count(x) > 1}) if len(listed) != len(set(listed)) else []
    if twice:
        problems.append(f'{len(twice)} files are listed in more than one hour, e.g. {twice[:2]}')
    unlisted = sorted(set(paths) - set(listed))
    if unlisted:
        problems.append(f'{len(unlisted)} served files are on no hour, e.g. {unlisted[:2]}')
    return problems


def smoke(src):
    """{recording_id: [problems]} for every broken stream, and the stream count."""
    growth = src.get('/public/growth.json')
    gen = growth.get('generation') or growth.get('updated_at')
    broken = {}
    if gen is None or (src.generation is not None and gen != src.generation):
        return {0: [f'growth.json generation {gen} is not the registry generation {src.generation}']}, 0
    md = src.get('/public/metadata.json')
    streams, mode = md['streams'], md.get('mode') or 'audited'
    catalog = src.get('/public/catalog.json')
    names = delivery_naming.catalog_names(catalog)
    by_id = {c['id']: c for c in catalog['streams']}
    for r in streams:
        try:
            p = check_stream(src, r, gen, names, growth, by_id, mode)
        except (LookupError, OSError, ValueError, KeyError) as e:
            p = [f'{type(e).__name__}: {e}']
        if p:
            broken[r['id']] = p
    return broken, len(streams)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument('--registry', help='registry.json.gz of a build or staged generation')
    mode.add_argument('--current', action='store_true', help='check the live public API')
    ap.add_argument('--assets', nargs='+', default=[], help='directories holding asset bodies at their R2 keys')
    ap.add_argument('--origin', default='https://stoarama.com')
    ap.add_argument('--generation', help='with --current: a staged generation instead of the served one')
    a = ap.parse_args(argv)
    src = Live(a.origin, a.generation) if a.current else Offline(a.registry, a.assets)
    broken, n = smoke(src)
    for rid, problems in sorted(broken.items()):
        print(f'BROKEN {rid}: ' + '; '.join(problems[:5]) + (f' (+{len(problems) - 5} more)' if len(problems) > 5 else ''))
    print(f'viewer smoke: {n - len(broken)}/{n} streams open every hour')
    sys.exit(1 if broken else 0)


if __name__ == '__main__':
    main()
