"""Joining the network: real T-junction splits first, then synthetic gaps and
bridges, each with a reason."""

import os
import random
import time
import unittest

from gaps import connect_pieces, split_at_tjunctions
from subgraph import Pipe, select_supplied_subgraph

M = 1 / 111_000   # about a metre in degrees, near enough for test layouts


def pipe(pid, a, b, length=100.0, dia=0.15):
    return Pipe(pid, a, b, length, dia)


class SplitTest(unittest.TestCase):
    def test_main_ending_on_a_side_is_joined_by_splitting_the_real_pipe(self):
        # Trunk 1-2; a branch 3-4 ends at node 3, 40% along the trunk's side.
        pipes = [pipe("trunk", 1, 2, 200.0, 0.3), pipe("branch", 3, 4)]
        out, links = split_at_tjunctions(pipes, [(3, "trunk", 0.4, 0.8)])
        pieces = {p.pipe_id: p for p in out}
        self.assertEqual(set(pieces), {"trunk~1", "trunk~2", "branch"})
        self.assertEqual((pieces["trunk~1"].start_node, pieces["trunk~1"].end_node), (1, 3))
        self.assertEqual((pieces["trunk~2"].start_node, pieces["trunk~2"].end_node), (3, 2))
        self.assertAlmostEqual(pieces["trunk~1"].length_m, 80.0)
        self.assertEqual(pieces["trunk~1"].kind, "split")
        self.assertEqual(pieces["trunk~1"].asset_id, "trunk")
        [link] = links
        self.assertEqual(link.kind, "split")
        self.assertIn("real:", link.reason)
        self.assertIn("0.8 m", link.reason)

    def test_several_cuts_along_one_pipe_keep_their_order(self):
        out, _ = split_at_tjunctions([pipe("t", 1, 2)], [(8, "t", 0.7, 1), (7, "t", 0.2, 1)])
        self.assertEqual([(p.start_node, p.end_node) for p in out], [(1, 7), (7, 8), (8, 2)])

    def test_cuts_at_the_ends_change_nothing(self):
        out, links = split_at_tjunctions([pipe("t", 1, 2)], [(9, "t", 0.0, 1)])
        self.assertEqual([p.pipe_id for p in out], ["t"])
        self.assertEqual(links, [])


class ConnectTest(unittest.TestCase):
    def layout(self):
        # Supplied line 1-2-3 from a reservoir at node 1, along latitude 0.
        coords = {1: (0, 0), 2: (100 * M, 0), 3: (200 * M, 0)}
        pipes = [pipe("a", 1, 2), pipe("b", 2, 3)]
        return pipes, coords

    def test_short_break_is_a_gap(self):
        pipes, coords = self.layout()
        coords.update({10: (230 * M, 0), 11: (330 * M, 0)})   # 30 m beyond node 3
        pipes.append(pipe("c", 10, 11))
        out, links, unjoined = connect_pieces(pipes, coords, [1], small_m=50, large_m=5000)
        [link] = links
        self.assertEqual(link.kind, "gap")
        self.assertEqual({link.from_node, link.to_node}, {10, 3})
        self.assertAlmostEqual(link.length_m, 30, delta=1)
        self.assertIn("digitising gap", link.reason)
        self.assertEqual(unjoined, (0, 0))
        sel = select_supplied_subgraph(out, [1], 100)
        self.assertIn("c", {p.pipe_id for p in sel.pipes})

    def test_long_break_is_a_bridge_and_says_it_is_invented(self):
        pipes, coords = self.layout()
        coords.update({20: (1200 * M, 0), 21: (1300 * M, 0)})  # 1 km beyond
        pipes.append(pipe("far", 20, 21))
        _, links, unjoined = connect_pieces(pipes, coords, [1], small_m=50, large_m=5000)
        [link] = links
        self.assertEqual(link.kind, "bridge")
        self.assertIn("invented", link.reason)
        self.assertEqual(unjoined, (0, 0))

    def test_bridges_off_leaves_the_far_piece_out(self):
        pipes, coords = self.layout()
        coords.update({20: (1200 * M, 0), 21: (1300 * M, 0)})
        pipes.append(pipe("far", 20, 21))
        _, links, unjoined = connect_pieces(pipes, coords, [1], small_m=50, large_m=0)
        self.assertEqual(links, [])
        self.assertEqual(unjoined, (1, 1))

    def test_chain_prefers_short_gaps_over_one_long_bridge(self):
        # A piece 600 m out, then a second piece 20 m past it. Once the first
        # is bridged, the second must join it by a short gap, not by its own
        # long bridge back to the original network.
        pipes, coords = self.layout()
        coords.update({30: (800 * M, 0), 31: (900 * M, 0), 40: (920 * M, 0), 41: (1020 * M, 0)})
        pipes += [pipe("p1", 30, 31), pipe("p2", 40, 41)]
        _, links, _ = connect_pieces(pipes, coords, [1], small_m=50, large_m=5000)
        self.assertEqual(sorted(l.kind for l in links), ["bridge", "gap"])
        gap = next(l for l in links if l.kind == "gap")
        self.assertEqual({gap.from_node, gap.to_node}, {40, 31})

    def test_links_take_the_larger_diameter_at_their_ends(self):
        pipes, coords = self.layout()
        pipes[1] = pipe("b", 2, 3, dia=0.3)
        coords.update({10: (230 * M, 0), 11: (330 * M, 0)})
        pipes.append(pipe("c", 10, 11, dia=0.1))
        out, _, _ = connect_pieces(pipes, coords, [1], small_m=50, large_m=5000)
        self.assertEqual(out[-1].diameter_m, 0.3)
        self.assertEqual(out[-1].kind, "gap")
        self.assertIsNone(out[-1].asset_id)

    def test_real_network_untouched_when_already_supplied(self):
        pipes, coords = self.layout()
        out, links, unjoined = connect_pieces(pipes, coords, [1])
        self.assertEqual(out, pipes)
        self.assertEqual(links, [])

    def test_large_network_joins_in_reasonable_time(self):
        # Three-node fragments scattered over 30 km, one source. FRAGMENTS=40000
        # is about the node count of a whole loaded network (about 14 s).
        rng = random.Random(4)
        coords, pipes = {}, []
        for i in range(int(os.environ.get("FRAGMENTS", "5000"))):
            x, y = rng.uniform(0, 30_000), rng.uniform(0, 30_000)
            a, b, c = 3 * i + 1, 3 * i + 2, 3 * i + 3
            coords[a] = (x * M, y * M)
            coords[b] = ((x + 40) * M, y * M)
            coords[c] = ((x + 80) * M, y * M)
            pipes += [pipe(f"f{i}a", a, b, 40.0), pipe(f"f{i}b", b, c, 40.0)]
        started = time.monotonic()
        _, links, unjoined = connect_pieces(pipes, coords, [1], small_m=50, large_m=5000)
        elapsed = time.monotonic() - started
        self.assertEqual(unjoined, (0, 0))
        self.assertGreater(len(links), 0)
        self.assertLess(elapsed, 60)


if __name__ == "__main__":
    unittest.main()
