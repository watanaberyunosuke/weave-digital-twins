"""Junction demand arithmetic (HDH-73).

    python3 -m unittest discover -s model -p 'test_*.py'
"""

import unittest

from demands import junction_demands


class JunctionDemandsTest(unittest.TestCase):

    def test_meters_at_one_junction_are_summed(self):
        result = junction_demands([("m1", 7, 10.0), ("m2", 7, 20.0)])
        self.assertAlmostEqual(result.by_node[7], 0.0005, places=12)

    def test_reporting_frequency_does_not_weight_a_meter(self):
        # Rows are already one per meter, so a meter that sent six readings
        # and one that sent one count once each.
        frequent, rare = ("m1", 7, 10.0), ("m2", 7, 20.0)
        self.assertEqual(junction_demands([frequent, rare]).by_node,
                         junction_demands([rare, frequent]).by_node)

    def test_silent_meters_add_nothing_and_are_counted(self):
        result = junction_demands([("m1", 7, 10.0), ("m2", 7, None), ("m3", 8, None)])
        self.assertAlmostEqual(result.by_node[7], 10.0 / 60_000)
        self.assertNotIn(8, result.by_node)
        self.assertEqual((result.meters, result.silent), (3, 2))

    def test_junctions_are_kept_apart(self):
        result = junction_demands([("m1", 7, 6.0), ("m2", 8, 12.0)])
        self.assertAlmostEqual(result.by_node[7], 0.0001)
        self.assertAlmostEqual(result.by_node[8], 0.0002)


if __name__ == "__main__":
    unittest.main()
