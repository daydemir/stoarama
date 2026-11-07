import unittest

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

    def test_byte_cap_for_pilot(self):
        plans = k.batches([row(3, 'raw', n, size=2_000_000_000) for n in range(1, 6)], 'k28', 500, 5_000_000_000)
        self.assertEqual([len(i) for _, i in plans], [2, 2, 1])


if __name__ == '__main__':
    unittest.main()
