import gzip
import hashlib
import json
import unittest
from unittest.mock import patch
import reviewed_scope as scope
import prepare_keep28_plans as plans


class ReviewedScopeTests(unittest.TestCase):
    def test_selection_requires_frozen_size_single_link_and_raw_sidecar(self):
        rows = [dict(kind='raw', source_id=n, stream_id=3, path='reviewed/%d.mp4' % n,
                     size_bytes=10, sha256='a'*64) for n in range(1, 5)]
        entries = [dict(path=r['path'], kind='file', links=2 if r['source_id'] == 2 else 1,
                        frozen=[9 if r['source_id'] == 3 else 10, 1, 2, 3, 4]) for r in rows]
        entries += [dict(path=r['path']+'.stoarama.json', kind='file', links=1, frozen=[100, 1, 2, 5, 4])
                    for r in rows[:3]]
        ref = dict(key=scope.PREFIX+'a'*64+'/files.json.gz', sha256='a'*64, bytes=100, entries=7)
        reviewed = {'parents': {'reviewed': dict(owner_stream_id=3, entries=7, shards=[ref])}}
        with patch.object(scope, 'load_scope', return_value=reviewed), patch.object(scope, 'read_pinned', return_value=dict(parent='reviewed', entries=entries)):
            self.assertEqual(scope.admissible_candidates(rows), rows[:1])

    def test_overlapping_media_and_sidecar_names_are_not_proposed(self):
        rows = [dict(kind='raw', source_id=1, stream_id=3, path='reviewed/a.mp4', size_bytes=10, sha256='a'*64),
                dict(kind='collated', source_id=2, stream_id=3, path='reviewed/a.mp4.stoarama.json', size_bytes=100, sha256='b'*64)]
        entries = [dict(path=r['path'], kind='file', links=1, frozen=[r['size_bytes'], 1, 2, 3, 4]) for r in rows]
        ref = dict(key=scope.PREFIX+'a'*64+'/files.json.gz', sha256='a'*64, bytes=100, entries=2)
        reviewed = {'parents': {'reviewed': dict(owner_stream_id=3, entries=2, shards=[ref])}}
        with patch.object(scope, 'load_scope', return_value=reviewed), patch.object(scope, 'read_pinned', return_value=dict(parent='reviewed', entries=entries)):
            self.assertEqual(scope.admissible_candidates(rows), [])

    def test_17234_unreviewed_collated_parents_are_never_proposed(self):
        parent = 'joined/99992_Asia_Thailand_Bangkok_Bangkok_-_Sukhumvit_Road_Bangkok_Thailand/September/25-Friday'
        rows = [dict(kind='collated', source_id=5996+n, stream_id=17234,
                     path=parent+'/hour-%d.mp4' % n, size_bytes=591700000, sha256='a'*64) for n in range(50)]
        reviewed = {'parents': {'joined/reviewed/day': {'owner_stream_id': 17234, 'entries': 0, 'shards': []}}}
        with patch.object(scope, 'load_scope', return_value=reviewed), patch.object(scope, 'read_pinned', side_effect=AssertionError('unreviewed folder must never be fetched')):
            self.assertEqual(scope.admissible_candidates(rows), [])
            with patch.object(plans, 'stream_candidates', return_value=rows):
                self.assertEqual(plans.candidates({17234: {}}, {17234}, 'bucket', 'psql'), [])


if __name__ == '__main__':
    unittest.main()
