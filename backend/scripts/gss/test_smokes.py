import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import viewer_smoke as V  # noqa: E402


class PathRuleTests(unittest.TestCase):
    def test_ensure_download_paths_rule(self):
        ok = {'id': 1, 'recording_id': 379, 'path': '379_Europe_Italy_Assisi_X/July/Thursday/379_X_2026_July_W4_Thursday_hour_02.mp4'}
        self.assertIsNone(V.path_problem(379, ok, {}))
        for bad in ('32_Europe_Italy_Assisi/August/03-Monday/x.mp4', '/379_x/a.mp4', '379_x/../a.mp4', '379_x\\a.mp4', '379_x/\x01.mp4'):
            self.assertIsNotNone(V.path_problem(379, dict(ok, path=bad), {}))
        self.assertIsNotNone(V.path_problem(379, dict(ok, recording_id=380), {}))
        self.assertIsNotNone(V.path_problem(379, ok, {'1': 'x'}))


if __name__ == '__main__':
    unittest.main()
