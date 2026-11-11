import unittest
import tempfile
from unittest.mock import patch

import prepare_keep28_plans as k


def row(sid, kind, n, size=1, purged=False):
    return {'stream_id': sid, 'kind': kind, 'source_id': n, 'path': f'p/{n}.mp4', 'size_bytes': size, 'r2_purged': purged}


class Keep28PlanTests(unittest.TestCase):
    def test_policy_is_pinned_and_complete(self):
        policy = k.load_policy(k.POLICY_PATH)
        self.assertEqual(len(policy), 109)
        self.assertEqual(policy[3]['best_start'], '2026-09-19')

    def test_batches_never_mix_streams_kinds_or_recovery_and_cap_items(self):
        rows = [row(3, 'raw', n) for n in range(1, 1202)] + [row(3, 'collated', 5000), row(3, 'raw', 6000, purged=True), row(4, 'raw', 7000)]
        plans = k.batches(rows, 'k28', 500, 0)
        self.assertEqual([l for l, _ in plans], ['k28-s3-col-0001', 'k28-s3-raw-0001', 'k28-s3-raw-0002', 'k28-s3-raw-0003', 'k28-s3-raw-nasonly-0001', 'k28-s4-raw-0001'])
        self.assertTrue(all(1 <= len(items) <= 500 for _, items in plans))
        for _, items in plans:
            self.assertEqual(len({(r['stream_id'], r['kind'], r['r2_purged']) for r in items}), 1)
        self.assertEqual(sum(len(i) for _, i in plans), len(rows))

    def test_default_plans_fill_up_to_10000_items_and_100_gb(self):
        rows = [row(3, 'raw', n, size=17_000_000) for n in range(1, 12001)]
        plans = k.batches(rows, 'k28', k.MAX_PLAN_ITEMS, k.MAX_PLAN_BYTES)
        self.assertEqual([len(i) for _, i in plans], [5882, 5882, 236])
        self.assertTrue(all(sum(r['size_bytes'] for r in i) <= k.MAX_PLAN_BYTES for _, i in plans))
        plans = k.batches([row(3, 'raw', n, size=1) for n in range(1, 10002)], 'k28', k.MAX_PLAN_ITEMS, k.MAX_PLAN_BYTES)
        self.assertEqual([len(i) for _, i in plans], [10000, 1])

    def test_batch_digest_matches_backend_vector(self):
        plans = [{'id': 1388, 'plan_sha256': 'a4a8bc7137d732ec9bf68df6e823b7ce4527e7cb4a13afdd05b87bc544601ccc'}, {'id': 1389, 'plan_sha256': 'b' * 64}]
        self.assertEqual(k.batch_digest(k.POLICY_SHA, plans), 'f2e8d37dca0fcb75e902fc2a1e4271022dc8cc23b5f6f1d4cbca53b9be8e3328')

    def test_batch_manifests_group_by_stream_and_skip_nas_only(self):
        receipts = [{'id': n, 'label': 'k28-s%d-raw-%04d' % (s, n), 'stream_id': s, 'plan_sha256': '%064x' % n, 'items': 10, 'bytes': 100} for n, s in ((1, 3), (2, 3), (3, 4))]
        receipts.append({'id': 4, 'label': 'k28-s3-raw-nasonly-0001', 'stream_id': 3, 'plan_sha256': 'c' * 64, 'items': 1, 'bytes': 1})
        batches = k.batch_manifests(receipts, 'k28', 'stream')
        self.assertEqual([(b['name'], [p['id'] for p in b['plans']]) for b in batches], [('k28-s3', [1, 2]), ('k28-s4', [3])])
        self.assertEqual(batches[0]['batch_sha256'], k.batch_digest(k.POLICY_SHA, batches[0]['plans']))
        self.assertEqual(len(k.batch_manifests(receipts, 'k28', 'all')[0]['plans']), 3)

    def test_runner_timestamp_prefix_fits_immutable_labels(self):
        prefix = 'k28auto-20261111-120000'
        with tempfile.TemporaryDirectory() as out, \
             patch('sys.argv', ['prepare', '--label-prefix', prefix, '--out', out]), \
             patch.object(k, 'candidates', return_value=[row(3, 'raw', 1)]), \
             patch('builtins.print'):
            k.main()
            label = k.batches([row(3, 'raw', 1)], prefix, 10000, k.MAX_PLAN_BYTES)[0][0]
            self.assertTrue(k.LABEL.fullmatch(label))

    def test_byte_cap_for_pilot(self):
        plans = k.batches([row(3, 'raw', n, size=2_000_000_000) for n in range(1, 6)], 'k28', 500, 5_000_000_000)
        self.assertEqual([len(i) for _, i in plans], [2, 2, 1])


if __name__ == '__main__':
    unittest.main()
