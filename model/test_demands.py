"""Junction demands: the arithmetic (HDH-73) and applying them per solve (HDH-72).

    python3 -m unittest discover -s model -p 'test_*.py'

The per-solve tests run EPANET when wntr is installed and skip otherwise.
"""

import os
import tempfile
import unittest

from demands import Demands, apply_demands, junction_demands


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


class ApplyDemandsTest(unittest.TestCase):
    """The same network.inp, solved again as readings change."""

    def setUp(self):
        try:
            import wntr
        except ImportError:
            self.skipTest("wntr not installed")
        self.wntr = wntr
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        wn = wntr.network.WaterNetworkModel()
        wn.add_reservoir("R1", base_head=70.0)
        wn.add_junction("N7", base_demand=0.0, elevation=50.0)
        wn.add_junction("N8", base_demand=0.0, elevation=50.0)
        wn.add_pipe("P1", "R1", "N7", length=100, diameter=0.15, roughness=100)
        wn.add_pipe("P2", "N7", "N8", length=100, diameter=0.15, roughness=100)
        wn.options.time.duration = 0
        # Written with zero demand, as a bootstrap build before telemetry is.
        self.inp = os.path.join(self.tmp.name, "network.inp")
        wntr.network.write_inpfile(wn, self.inp)

    def solve(self, demands: Demands) -> tuple[float, float]:
        wn = self.wntr.network.WaterNetworkModel(self.inp)
        unmodelled = apply_demands(wn, demands)
        results = self.wntr.sim.EpanetSimulator(wn).run_sim(
            file_prefix=os.path.join(self.tmp.name, "solve"))
        return float(results.link["flowrate"]["P1"].iloc[-1]), unmodelled

    def test_each_solve_uses_the_readings_it_is_given(self):
        before, _ = self.solve(Demands({}, meters=2, silent=2))
        first, _ = self.solve(Demands({7: 0.0005}, meters=2, silent=0))
        second, _ = self.solve(Demands({7: 0.0002, 8: 0.0001}, meters=2, silent=0))
        self.assertAlmostEqual(before, 0.0, places=6)
        self.assertAlmostEqual(first, 0.0005, places=6)
        self.assertAlmostEqual(second, 0.0003, places=6)

    def test_a_meter_gone_silent_stops_drawing(self):
        wn = self.wntr.network.WaterNetworkModel(self.inp)
        apply_demands(wn, Demands({7: 0.0005}, meters=1, silent=0))
        apply_demands(wn, Demands({}, meters=1, silent=1))
        self.assertEqual(wn.get_node("N7").demand_timeseries_list[0].base_value, 0.0)

    def test_demand_outside_the_model_is_reported(self):
        flow, unmodelled = self.solve(Demands({7: 0.0004, 99: 0.0001}, meters=2, silent=0))
        self.assertAlmostEqual(flow, 0.0004, places=6)
        self.assertAlmostEqual(unmodelled, 0.0001)


if __name__ == "__main__":
    unittest.main()
