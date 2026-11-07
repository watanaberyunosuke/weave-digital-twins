"""Pressure zones: bands, heads, PRVs and boosters, without a solver; plus one
EPANET solve that checks the zoned pressures land inside the service limits.
The solve skips when wntr is not installed."""

import unittest

from subgraph import Pipe
from zones import plan_zones


def line(*nodes):
    """Pipes joining consecutive nodes, named by their ends."""
    return [Pipe(f"p{a}-{b}", a, b, 100.0, 0.15) for a, b in zip(nodes, nodes[1:])]


class PlanZonesTest(unittest.TestCase):
    def test_flat_network_is_one_band(self):
        pipes = line(1, 2, 3)
        plan = plan_zones(pipes, {1: 50, 2: 50, 3: 50}, {"r": 1}, min_pressure_m=20, max_pressure_m=80, margin_m=5)
        self.assertEqual(set(plan.band_of.values()), {0})
        self.assertEqual(plan.reservoir_head, {"r": 75})
        self.assertEqual(plan.prvs, [])
        self.assertEqual(plan.boosters, [])

    def test_every_node_inside_the_service_limits(self):
        # A 200 m fall from a hilltop reservoir to the valley floor.
        elevation = {n: 200 - 20 * (n - 1) for n in range(1, 12)}
        plan = plan_zones(line(*range(1, 12)), elevation, {"r": 1}, min_pressure_m=20, max_pressure_m=80, margin_m=5)
        self.assertEqual(plan.band_width_m, 55)
        low, high = plan.static_pressure_range(elevation)
        self.assertGreaterEqual(low, 25)
        self.assertLess(high, 80)

    def test_band_crossing_becomes_prv_pointing_downhill(self):
        elevation = {1: 200, 2: 170, 3: 100}
        plan = plan_zones(line(1, 2, 3), elevation, {"r": 1}, min_pressure_m=20, max_pressure_m=80, margin_m=5)
        self.assertEqual([p.pipe_id for p in plan.pipes], ["p1-2"])
        [prv] = plan.prvs
        self.assertEqual((prv.upstream, prv.downstream), (2, 3))
        # Band 1's grade line is 200 - 55 + 25 = 170; node 3 is at 100.
        self.assertAlmostEqual(prv.setting_m, 70)

    def test_pipe_drawn_uphill_still_gets_downhill_prv(self):
        elevation = {1: 100, 2: 200}
        plan = plan_zones(
            [Pipe("x", 1, 2, 100.0, 0.15)], elevation, {"r": 2}, min_pressure_m=20, max_pressure_m=80, margin_m=5
        )
        [prv] = plan.prvs
        self.assertEqual((prv.upstream, prv.downstream), (2, 1))

    def test_valley_reservoir_needs_a_booster_for_the_hill(self):
        # The only source is at the bottom; a PRV cannot pass water uphill.
        elevation = {1: 200, 2: 190, 3: 60}
        plan = plan_zones(line(1, 2, 3), elevation, {"dam": 3}, min_pressure_m=20, max_pressure_m=80, margin_m=5)
        self.assertEqual(plan.boosters, [(1, 225.0)])
        self.assertEqual(plan.reservoir_head["dam"], plan.band_head[plan.band_of[3]])

    def test_hilltop_booster_also_feeds_the_band_below(self):
        # Hill band 0 feeds band 1 through a PRV, so only one booster is needed
        # even though neither band holds the reservoir, which sits apart.
        elevation = {1: 200, 2: 130, 3: 120, 10: 50, 11: 50}
        pipes = line(1, 2, 3) + line(10, 11)
        plan = plan_zones(pipes, elevation, {"r": 10}, min_pressure_m=20, max_pressure_m=80, margin_m=5)
        self.assertEqual(plan.boosters, [(1, 225.0)])

    def test_crowded_crossings_get_one_valve_per_node(self):
        # Two mains from the hill both land on node 3, and a second crossing
        # leaves it: EPANET allows no two PRVs on one node, so the larger main
        # gets the valve and the others are shut.
        elevation = {1: 200, 2: 200, 3: 120, 4: 40}
        pipes = [
            Pipe("a", 1, 3, 100.0, 0.30),
            Pipe("b", 2, 3, 100.0, 0.15),
            Pipe("c", 3, 4, 100.0, 0.15),
            Pipe("d", 1, 2, 100.0, 0.15),
        ]
        plan = plan_zones(pipes, elevation, {"r": 1}, min_pressure_m=20, max_pressure_m=80, margin_m=5)
        self.assertEqual([v.pipe.pipe_id for v in plan.prvs], ["a"])
        self.assertEqual([p.pipe_id for p in plan.closed], ["b", "c"])
        used = [n for v in plan.prvs for n in (v.upstream, v.downstream)]
        self.assertEqual(len(used), len(set(used)))
        # Node 4's only link is shut, so it is fed by a booster.
        self.assertEqual([n for n, _ in plan.boosters], [4])

    def test_same_graph_gives_same_plan(self):
        elevation = {n: 300 - 13 * n for n in range(1, 20)}
        first = plan_zones(line(*range(1, 20)), elevation, {"r": 5})
        second = plan_zones(list(reversed(line(*range(1, 20)))), elevation, {"r": 5})
        self.assertEqual(first, second)

    def test_impossible_limits_are_refused(self):
        pipes = line(1, 2)
        with self.assertRaises(ValueError):
            plan_zones(pipes, {1: 0, 2: 0}, {"r": 1}, min_pressure_m=20, max_pressure_m=25, margin_m=5)


try:
    import wntr
except ImportError:  # the solver lives only in the simulator image
    wntr = None


@unittest.skipIf(wntr is None, "wntr not installed")
class ZonedSolveTest(unittest.TestCase):
    def test_flowing_pressures_stay_inside_the_limits(self):
        # A 240 m fall in 21 nodes, a valley dam as the only real source, and
        # a demand at every node: the hill needs a booster, the slope PRVs.
        n = 21
        elevation = {i: 260 - 12 * i for i in range(1, n + 1)}
        pipes = line(*range(1, n + 1))
        plan = plan_zones(pipes, elevation, {"dam": n}, min_pressure_m=20, max_pressure_m=80, margin_m=5)

        wn = wntr.network.WaterNetworkModel()
        for i in range(1, n + 1):
            wn.add_junction(f"N{i}", base_demand=0.0005, elevation=elevation[i])
        for fid, head in plan.reservoir_head.items():
            wn.add_reservoir(f"R{fid}", base_head=head)
            wn.add_pipe(f"L{fid}", f"R{fid}", f"N{n}", length=10, diameter=0.6, roughness=100)
        for node, head in plan.boosters:
            wn.add_reservoir(f"B{node}", base_head=head)
            wn.add_pipe(f"BL{node}", f"B{node}", f"N{node}", length=10, diameter=0.6, roughness=100)
        for p in plan.pipes:
            wn.add_pipe(
                f"P{p.pipe_id}",
                f"N{p.start_node}",
                f"N{p.end_node}",
                length=p.length_m,
                diameter=p.diameter_m,
                roughness=100,
            )
        for v in plan.prvs:
            wn.add_valve(
                f"V{v.pipe.pipe_id}",
                f"N{v.upstream}",
                f"N{v.downstream}",
                diameter=v.pipe.diameter_m,
                valve_type="PRV",
                initial_setting=v.setting_m,
            )
        wn.options.time.duration = 0
        results = wntr.sim.EpanetSimulator(wn).run_sim()
        pressure = results.node["pressure"].iloc[-1]
        junctions = pressure[[f"N{i}" for i in range(1, n + 1)]]
        self.assertGreaterEqual(junctions.min(), 20 - 1e-6)
        self.assertLessEqual(junctions.max(), 80 + 1e-6)


if __name__ == "__main__":
    unittest.main()
