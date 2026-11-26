import copy
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import delivery_naming as N  # noqa: E402

CATALOG = {'naming': dict(N.NAMING_SPEC), 'streams': [
    {'id': 379, 'name': 'Assisi - Town Square Assisi', 'folder': '379_Europe_Italy_Assisi_Assisi_-_Town_Square_Assisi',
     'metadata': {'continent': 'Europe', 'country': 'Italy', 'city': 'Assisi', 'plaza_name': 'Assisi - Town Square Assisi'}},
    {'id': 339, 'name': 'Public square 052: Marktplatz', 'folder': None, 'metadata': {}},
    {'id': 402, 'name': '동대문운동장', 'folder': None,
     'metadata': {'continent': 'Asia', 'country': 'South Korea', 'city': 'Seoul', 'plaza_name': '동대문운동장'}},
]}


class NamingTests(unittest.TestCase):
    def setUp(self):
        self.names = N.catalog_names(CATALOG)

    def test_the_live_generation_names(self):
        # Paths of the live generation ae14e746.
        self.assertEqual(N.delivery_path(self.names, 379, '2026-07-23', 2, 1, 3),
                         '379_Europe_Italy_Assisi_Assisi_-_Town_Square_Assisi/July/Thursday/'
                         '379_Assisi_-_Town_Square_Assisi_2026_July_W4_Thursday_hour_02_part_01.mp4')
        self.assertEqual(N.delivery_path(self.names, 379, '2026-08-04', 12, 1, 1),
                         '379_Europe_Italy_Assisi_Assisi_-_Town_Square_Assisi/August/Tuesday/'
                         '379_Assisi_-_Town_Square_Assisi_2026_August_W1_Tuesday_hour_12.mp4')

    def test_calendar_month_weeks(self):
        for day, week in ((1, 1), (7, 1), (8, 2), (14, 2), (15, 3), (21, 3), (22, 4), (28, 4), (29, 5), (31, 5)):
            self.assertIn(f'_W{week}_', N.delivery_path(self.names, 379, f'2026-08-{day:02d}', 1, 1, 1))

    def test_hour_is_the_slot_ordinal(self):
        self.assertTrue(N.delivery_path(self.names, 379, '2026-08-04', 1, 1, 1).endswith('_hour_01.mp4'))
        for bad in (0, 13):
            with self.assertRaises(N.NamingError):
                N.delivery_path(self.names, 379, '2026-08-04', bad, 1, 1)

    def test_unresolved_geography_and_display_name(self):
        self.assertEqual(self.names[339], ('339_Unresolved_Continent_Unresolved_Country_Unresolved_City_Public_square_052_Marktplatz',
                                           '339_Public_square_052_Marktplatz'))
        self.assertEqual(self.names[402], ('402_Asia_South_Korea_Seoul_동대문운동장', '402_동대문운동장'))

    def test_sanitation(self):
        self.assertEqual(N.sanitize('  Soliman Street, Agdao '), 'Soliman_Street_Agdao')
        self.assertEqual(N.sanitize('Valašské Meziříčí (Valassk'), 'Valašské_Meziříčí_Valassk')
        with self.assertRaises(N.NamingError):
            N.sanitize('aux')

    def test_catalog_drift_fails(self):
        drifted = copy.deepcopy(CATALOG)
        drifted['naming']['week'] = 'ISO weeks'
        with self.assertRaisesRegex(N.NamingError, 'naming differs'):
            N.catalog_names(drifted)
        moved = copy.deepcopy(CATALOG)
        moved['streams'][0]['folder'] = '379_Elsewhere'
        with self.assertRaisesRegex(N.NamingError, 'differs from its name'):
            N.catalog_names(moved)

    def test_check_paths(self):
        p = lambda d, h, k, n: N.delivery_path(self.names, 379, d, h, k, n)
        good = [(379, '2026-08-04', p('2026-08-04', 3, 1, 3)), (379, '2026-08-04', p('2026-08-04', 3, 3, 3)),
                (379, '2026-08-04', p('2026-08-04', 4, 1, 1))]
        self.assertEqual(N.check_paths(self.names, good), [])  # a kept part number may skip one not yet delivered
        bad = good + [(379, '2026-08-04', p('2026-08-04', 4, 1, 1).upper().replace('.MP4', '.mp4'))]
        self.assertTrue(any('not a delivery name' in x for x in N.check_paths(self.names, bad)))
        mixed = good + [(379, '2026-08-04', p('2026-08-04', 4, 2, 2))]
        self.assertTrue(any('plain file' in x for x in N.check_paths(self.names, mixed)))
        dup = good + [good[0]]
        self.assertTrue(any('case-fold' in x for x in N.check_paths(self.names, dup)))
        self.assertTrue(any('not a delivery name' in x for x in N.check_paths(self.names, [(379, '2026-08-04', '379_x/a_180030-181335.mp4')])))
        self.assertTrue(any('not a delivery name' in x for x in N.check_paths(self.names, [(379, '2026-08-05', good[0][2])])))


if __name__ == '__main__':
    unittest.main()
