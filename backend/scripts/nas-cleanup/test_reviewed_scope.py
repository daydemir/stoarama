import gzip
import hashlib
import json
import unittest
from unittest.mock import patch
import reviewed_scope as scope
import prepare_keep28_plans as plans


class ReviewedScopeTests(unittest.TestCase):
    def test_unreachable_metadata_is_a_read_failure_but_wrong_bytes_are_not(self):
        env = {'R2_ACCOUNT_ID': 'a', 'R2_BUCKET': 'b'}
        down = scope.subprocess.CompletedProcess([], 7, b'', b'')
        wrong = scope.subprocess.CompletedProcess([], 0, b'other', b'')
        with patch.dict(scope.os.environ, env):
            with patch('subprocess.run', return_value=down), self.assertRaises(scope.ReadFailed):
                scope.read_pinned('k', 'a'*64, 100, 100)
            with patch('subprocess.run', side_effect=scope.subprocess.TimeoutExpired('curl', 50)), self.assertRaises(scope.ReadFailed):
                scope.read_pinned('k', 'a'*64, 100, 100)
            with patch('subprocess.run', return_value=wrong), self.assertRaisesRegex(RuntimeError, 'hash differs') as caught:
                scope.read_pinned('k', 'a'*64, 100, 100)
            self.assertNotIsInstance(caught.exception, scope.ReadFailed)
            oversized = scope.subprocess.CompletedProcess([], 63, b'', b'')
            with patch('subprocess.run', return_value=oversized), self.assertRaisesRegex(RuntimeError, 'refused') as caught:
                scope.read_pinned('k', 'a'*64, 100, 100)
            self.assertNotIsInstance(caught.exception, scope.ReadFailed)

    def test_cache_is_content_addressed_and_reverified(self):
        import tempfile
        body = gzip.compress(b'{"ok": 1}')
        sha = hashlib.sha256(body).hexdigest()
        fetched = scope.subprocess.CompletedProcess([], 0, body, b'')
        with tempfile.TemporaryDirectory() as cache, patch.dict(scope.os.environ, {'R2_ACCOUNT_ID': 'a', 'R2_BUCKET': 'b'}):
            with patch('subprocess.run', return_value=fetched) as run:
                self.assertEqual(scope.read_pinned('k', sha, 100, 100, cache), {'ok': 1})
                self.assertEqual(scope.read_pinned('k', sha, 100, 100, cache), {'ok': 1})
                self.assertEqual(run.call_count, 1)
                (scope.pathlib.Path(cache) / sha).write_bytes(b'tampered')
                self.assertEqual(scope.read_pinned('k', sha, 100, 100, cache), {'ok': 1})
                self.assertEqual(run.call_count, 2)
            self.assertEqual((scope.pathlib.Path(cache) / sha).read_bytes(), body)

    def test_any_fatal_shard_fetch_outranks_transient_failures(self):
        rows = [dict(kind='raw', source_id=n, stream_id=3, path='p%d/%d.mp4' % (n, n), size_bytes=10, sha256='a'*64) for n in (1, 2)]
        refs = {'p%d' % n: dict(owner_stream_id=3, entries=1, shards=[dict(
            key=scope.PREFIX + c*64 + '/files.json.gz', sha256=c*64, bytes=100, entries=1)]) for n, c in ((1, 'a'), (2, 'b'))}
        def fetch(key, sha, *a):
            raise scope.ReadFailed('down') if sha[0] == 'a' else RuntimeError('reviewed cleanup metadata fetch refused')
        with patch.object(scope, 'load_scope', return_value={'parents': refs}), patch.object(scope, 'fetch_pinned', side_effect=fetch):
            with self.assertRaisesRegex(RuntimeError, 'refused') as caught:
                scope.admissible_candidates(rows)
        self.assertNotIsInstance(caught.exception, scope.ReadFailed)

    def test_selection_requires_frozen_size_single_link_and_raw_sidecar(self):
        rows = [dict(kind='raw', source_id=n, stream_id=3, path='reviewed/%d.mp4' % n,
                     size_bytes=10, sha256='a'*64) for n in range(1, 5)]
        entries = [dict(path=r['path'], kind='file', links=2 if r['source_id'] == 2 else 1,
                        frozen=[9 if r['source_id'] == 3 else 10, 1, 2, 3, 4]) for r in rows]
        entries += [dict(path=r['path']+'.stoarama.json', kind='file', links=1, frozen=[100, 1, 2, 5, 4])
                    for r in rows[:3]]
        ref = dict(key=scope.PREFIX+'a'*64+'/files.json.gz', sha256='a'*64, bytes=100, entries=7)
        reviewed = {'parents': {'reviewed': dict(owner_stream_id=3, entries=7, shards=[ref])}}
        with patch.object(scope, 'load_scope', return_value=reviewed), patch.object(scope, 'fetch_pinned', return_value=gzip.compress(json.dumps(dict(parent='reviewed', entries=entries)).encode())):
            self.assertEqual(scope.admissible_candidates(rows), rows[:1])

    def test_overlapping_media_and_sidecar_names_are_not_proposed(self):
        rows = [dict(kind='raw', source_id=1, stream_id=3, path='reviewed/a.mp4', size_bytes=10, sha256='a'*64),
                dict(kind='collated', source_id=2, stream_id=3, path='reviewed/a.mp4.stoarama.json', size_bytes=100, sha256='b'*64)]
        entries = [dict(path=r['path'], kind='file', links=1, frozen=[r['size_bytes'], 1, 2, 3, 4]) for r in rows]
        ref = dict(key=scope.PREFIX+'a'*64+'/files.json.gz', sha256='a'*64, bytes=100, entries=2)
        reviewed = {'parents': {'reviewed': dict(owner_stream_id=3, entries=2, shards=[ref])}}
        with patch.object(scope, 'load_scope', return_value=reviewed), patch.object(scope, 'fetch_pinned', return_value=gzip.compress(json.dumps(dict(parent='reviewed', entries=entries)).encode())):
            self.assertEqual(scope.admissible_candidates(rows), [])

    def test_17234_unreviewed_collated_parents_are_never_proposed(self):
        parent = 'joined/99992_Asia_Thailand_Bangkok_Bangkok_-_Sukhumvit_Road_Bangkok_Thailand/September/25-Friday'
        rows = [dict(kind='collated', source_id=5996+n, stream_id=17234,
                     path=parent+'/hour-%d.mp4' % n, size_bytes=591700000, sha256='a'*64) for n in range(50)]
        reviewed = {'parents': {'joined/reviewed/day': {'owner_stream_id': 17234, 'entries': 0, 'shards': []}}}
        with patch.object(scope, 'load_scope', return_value=reviewed), patch.object(scope, 'fetch_pinned', side_effect=AssertionError('unreviewed folder must never be fetched')):
            self.assertEqual(scope.admissible_candidates(rows), [])
            with patch.object(plans, 'stream_candidates', return_value=rows):
                self.assertEqual(plans.candidates({17234: {}}, {17234}, 'bucket', 'psql'), [])


if __name__ == '__main__':
    unittest.main()
