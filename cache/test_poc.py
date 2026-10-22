import unittest
from poc import Cache, Predictor, content


class CacheTests(unittest.TestCase):
    def test_lru_budget_and_useful_prefetch(self):
        c = Cache(12, 30)
        c.put('a', b'aaaaaa', 'a', True)
        c.put('b', b'bbbbbb', 'b', True)
        c.get('a', demand=True)
        c.get('a', demand=True)
        c.put('c', b'cccccc', 'c', False)
        self.assertIsNone(c.get('b'))
        self.assertEqual(c.bytes, 12)
        self.assertEqual(c.used_prefetch, 1)
        self.assertEqual(c.evicted_unused, 1)

    def test_expired_segment_is_not_a_hit(self):
        c = Cache(12, 2)
        c.put('a', b'aaaaaa', 'a', True)
        c.items['a']['created'] -= 3
        self.assertIsNone(c.get('a', demand=True))
        self.assertEqual(c.bytes, 0)
        self.assertEqual(c.evicted_unused, 1)

    def test_live_segments_are_repeatable_and_distinct(self):
        self.assertEqual(content(0, 1, 100), content(0, 1, 100))
        self.assertNotEqual(content(0, 1, 100), content(0, 2, 100))
        self.assertNotEqual(content(0, 1, 100), content(1, 1, 100))


class PredictorTests(unittest.TestCase):
    def test_learns_stream_transition_and_interval(self):
        p = Predictor(3)
        for n in range(80):
            p.observe({'user': 0, 'zone': 1, 'stream': n % 2,
                       'seq': n//2, 'when': n*2.0, 'period': 1.0})
        key, confidence, interval = p.observe({'user': 0, 'zone': 1, 'stream': 0,
                                              'seq': 40, 'when': 160.0, 'period': 1.0})
        self.assertEqual(key, (1, 40))
        self.assertGreater(confidence, .55)
        self.assertAlmostEqual(interval, 2, places=4)

    def test_cold_start_predicts_next_segment_not_same_segment(self):
        p = Predictor(8)
        key, confidence, interval = p.observe({'user': 2, 'zone': 1, 'stream': 4,
                                               'seq': 9, 'when': 10.0, 'period': 1.5})
        self.assertEqual(key, (4, 10))
        self.assertGreater(confidence, .55)
        self.assertEqual(interval, 1.5)


if __name__ == '__main__':
    unittest.main()
