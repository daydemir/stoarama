#!/usr/bin/env python3
"""Copy a paused queue for cloud cutover. Never reads credentials or media."""
import argparse
import contextlib
import datetime
import gzip
import hashlib
import json
import pathlib
import shutil
import sqlite3
import tarfile


def copy_state(source, audit, output, target, controller):
    if output.exists():
        raise ValueError('cloud_snapshot_destination_exists')
    output.mkdir(mode=0o700, parents=True)
    (output / 'work').mkdir(mode=0o700)
    (output / 'audit').mkdir(mode=0o700)
    for name in ('run-authority.json', 'work-queue-summary.json', 'execution-workers.json',
                 'execution-status.json', 'budget-ledger.json', 'structure-screen-status.json'):
        p = source / name
        if p.is_symlink():
            raise ValueError('snapshot_symlink')
        shutil.copyfile(p, output / name)
        (output / name).chmod(0o600)
    for name in ('execution.sqlite3', 'structure-screen.sqlite3'):
        with contextlib.closing(sqlite3.connect('file:' + str(source / name) + '?mode=ro', uri=True)) as old:
            with contextlib.closing(sqlite3.connect(output / name)) as new:
                old.backup(new)
    with contextlib.closing(sqlite3.connect(output / 'execution.sqlite3')) as db:
        columns = [r[1] for r in db.execute('PRAGMA table_info(jobs)')]
        before = [list(x) for x in db.execute('SELECT * FROM jobs ORDER BY id')]
        path_index = columns.index('path')
        for row in before:
            job = row[columns.index('id')]
            p = pathlib.Path(row[path_index])
            if p.parent != source / 'work' or p.name != job + '.json.gz' or p.is_symlink():
                raise ValueError('snapshot_work_path_differs')
            raw = gzip.decompress(p.read_bytes())
            work = json.loads(raw)
            if hashlib.sha256(json.dumps(work, sort_keys=True, separators=(',', ':')).encode()).hexdigest() != job:
                raise ValueError('snapshot_work_identity_differs')
            shutil.copyfile(p, output / 'work' / p.name)
            row[path_index] = str(target / 'work' / p.name)
            db.execute('UPDATE jobs SET path=? WHERE id=?', (row[path_index], job))
        db.commit()
        after = [list(x) for x in db.execute('SELECT * FROM jobs ORDER BY id')]
        if before != after or len(before) != 4342 or db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise ValueError('snapshot_queue_transition_differs')
        counts = dict(db.execute('SELECT state,count(*) FROM jobs GROUP BY state'))
    for name in ('selection-final-v2.json', 'stage3-v2-missing-sources.jsonl.gz'):
        p = audit / name
        if p.is_symlink():
            raise ValueError('snapshot_audit_symlink')
        shutil.copyfile(p, output / 'audit' / name)
    (output / 'executor.lock').write_bytes(b'')
    created = datetime.datetime.strptime(controller['created_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc).timestamp()
    reservation = {'schema': 'best14-cloud-controller-v1', 'droplet_id': controller['droplet_id'],
                   'hourly_usd': controller['hourly_usd'], 'name': controller['name'], 'created_at_unix': created,
                   'max_runtime_seconds': 7 * 86400, 'monthly_usd': 12}
    (output / 'controller-reservation.json').write_text(json.dumps(reservation))
    hashes = {str(p.relative_to(output)): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in output.rglob('*') if p.is_file()}
    manifest = {'schema': 'best14-cloud-state-v1', 'frozen_jobs': 4342, 'counts': counts,
                'target_root': str(target), 'files_sha256': hashes, 'credentials_included': False,
                'media_included': False, 'publication_changed': False}
    (output / 'migration-manifest.json').write_text(json.dumps(manifest, indent=2))
    return manifest


def main():
    parser = argparse.ArgumentParser()
    for name in ('source', 'audit', 'output', 'target-root', 'controller'):
        parser.add_argument('--' + name, required=True, type=pathlib.Path)
    args = parser.parse_args()
    manifest = copy_state(args.source.resolve(), args.audit.resolve(), args.output.resolve(), args.target_root,
                          json.loads(args.controller.read_text()))
    print(json.dumps({k: manifest[k] for k in ('frozen_jobs', 'counts', 'credentials_included', 'media_included')}))


if __name__ == '__main__':
    main()
