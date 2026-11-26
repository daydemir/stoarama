"""Republish gate: what would each stream's one-line download command write?

For every stream and each choice the page offers (week 1, week 2, all 14 days),
this runs the generation's own /download-script.py the way one-line.js's command
does (--stream <id> --scope available [--week N] --generation <gen>), without
downloading: the script must match the manifest's download_script_sha256, accept
the manifest (validate_manifest), select the week (week_outputs), and place every
available file (destination) under a scratch folder. Every path it would write
must be a delivery name (delivery_naming.check_paths).

  download_smoke.py --registry DIR/registry.json.gz --assets DIR/objects IN/assets   (a build, before staging)
  download_smoke.py --current [--origin https://stoarama.com] [--generation G]     (the live public API)

Exits nonzero listing every broken stream.
"""
import argparse
import hashlib
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import delivery_naming  # noqa: E402
from viewer_smoke import API, Live, Offline  # noqa: E402

WEEKS = (1, 2, 0)  # one-line.js: Week 1, Week 2, All 14 days


def load_script(src, gen, manifest):
    """The generation's download script as a module namespace, checked as one-line.js checks it."""
    body = src.raw('/download-script.py', gen)
    if len(body) > 1048576 or hashlib.sha256(body).hexdigest() != manifest.get('download_script_sha256'):
        raise ValueError('Download tool verification failed')
    ns = {'__name__': 'gss_download_script'}
    exec(compile(body, 'download-script.py', 'exec'), ns)
    # The globals main() sets from the one-line command's --dataset-api and --generation.
    ns.update(DATASET_API=API, DELIVERY_GENERATION=gen)
    return ns


def check_stream(src, script, rid, gen, manifest, names, root):
    script['validate_manifest'](manifest, rid)
    problems = []
    for week in WEEKS:
        outputs = script['week_outputs'](manifest, week) if week else manifest['outputs']
        written = []
        for o in outputs:
            if o['available']:
                rel = script['destination'](root, o['path']).relative_to(root).as_posix()
                written.append((rid, o['local_date'], rel))
        problems += [f'week {week or "all"}: {p}' for p in delivery_naming.check_paths(names, sorted(written))]
    return problems


def smoke(src):
    growth = src.get('/public/growth.json')
    gen = growth['generation']
    names = delivery_naming.catalog_names(src.get('/public/catalog.json'))
    streams = src.get('/public/metadata.json')['streams']
    broken, script = {}, None
    with tempfile.TemporaryDirectory() as d:
        root = Path(d).resolve()
        for r in streams:
            rid = r['id']
            try:
                manifest = src.get(f'/api/coverage-delivery/manifest?stream={rid}&scope=available', gen)
                if manifest.get('generation') != gen:
                    raise ValueError(f'manifest generation {manifest.get("generation")} is not {gen}')
                script = script or load_script(src, gen, manifest)
                p = check_stream(src, script, rid, gen, manifest, names, root)
            except (LookupError, OSError, ValueError, KeyError) as e:
                p = [f'{type(e).__name__}: {e}']
            if p:
                broken[rid] = p
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
    print(f'download smoke: {n - len(broken)}/{n} streams write only delivery names')
    sys.exit(1 if broken else 0)


if __name__ == '__main__':
    main()
