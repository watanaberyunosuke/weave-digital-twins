"""Regression fixtures for the capped, supplied model selection (HDH-79).

    python3 -m unittest discover -s model -p 'test_*.py'

Standard library only, so it runs without the stack. The last test also
solves the selection in EPANET when wntr is installed, and skips otherwise.
"""

import random
import tempfile
import unittest
from pathlib import Path

from subgraph import EPANET_MAX_ID, Pipe, element_name, select_supplied_subgraph


def grid(width: int, height: int, first_node: int = 1) -> list[Pipe]:
    """A looped grid of pipes, node ids row by row from first_node."""
    def nid(x, y):
        return first_node + y * width + x

    pipes = []
    for y in range(height):
        for x in range(width):
            if x + 1 < width:
                pipes.append(Pipe(f"h:{x}:{y}:{first_node}", nid(x, y), nid(x + 1, y), 100.0, 0.15))
            if y + 1 < height:
                pipes.append(Pipe(f"v:{x}:{y}:{first_node}", nid(x, y), nid(x, y + 1), 100.0, 0.15))
    return pipes


def supplied(selection, sources) -> bool:
    """True when every selected pipe has a path back to a source."""
    adjacency = {}
    for p in selection.pipes:
        adjacency.setdefault(p.start_node, []).append(p.end_node)
        adjacency.setdefault(p.end_node, []).append(p.start_node)
    seen = set(s for s in sources if s in adjacency)
    stack = list(seen)
    while stack:
        for other in adjacency[stack.pop()]:
            if other not in seen:
                seen.add(other)
                stack.append(other)
    return selection.nodes <= seen


class SelectSuppliedSubgraph(unittest.TestCase):

    def test_above_cap_is_connected_to_supply_and_exactly_capped(self):
        pipes = grid(10, 10)                      # 180 pipes
        sel = select_supplied_subgraph(pipes, [1], cap=50)
        self.assertEqual(len(sel.pipes), 50)
        self.assertEqual(sel.capped_pipes, len(pipes) - 50)
        self.assertTrue(supplied(sel, [1]))

    def test_same_graph_gives_same_model_whatever_the_row_order(self):
        pipes = grid(8, 8)
        first = select_supplied_subgraph(pipes, [1, 64], cap=40)
        shuffled = pipes[:]
        random.Random(7).shuffle(shuffled)
        second = select_supplied_subgraph(shuffled, [64, 1, 1], cap=40)
        self.assertEqual([p.pipe_id for p in first.pipes],
                         [p.pipe_id for p in second.pipes])

    def test_unsupplied_component_is_left_out_and_counted(self):
        fed = grid(4, 4, first_node=1)            # 24 pipes, holds the source
        island = grid(3, 3, first_node=1000)      # 12 pipes, no source
        sel = select_supplied_subgraph(fed + island, [1], cap=1000)
        self.assertEqual(len(sel.pipes), len(fed))
        self.assertEqual(sel.unsupplied_pipes, len(island))
        self.assertEqual(sel.capped_pipes, 0)
        self.assertFalse(sel.nodes & {p.start_node for p in island})

    def test_loops_between_reached_nodes_are_kept(self):
        # A square: four pipes, one loop. A tree walk would keep three.
        square = [Pipe("a", 1, 2, 1, 0.1), Pipe("b", 2, 3, 1, 0.1),
                  Pipe("c", 3, 4, 1, 0.1), Pipe("d", 4, 1, 1, 0.1)]
        sel = select_supplied_subgraph(square, [1], cap=10)
        self.assertEqual(sorted(p.pipe_id for p in sel.pipes), ["a", "b", "c", "d"])

    def test_self_loops_are_skipped(self):
        pipes = [Pipe("a", 1, 2, 1, 0.1), Pipe("loop", 2, 2, 1, 0.1)]
        sel = select_supplied_subgraph(pipes, [1], cap=10)
        self.assertEqual([p.pipe_id for p in sel.pipes], ["a"])
        self.assertEqual(sel.skipped_pipes, 1)

    def test_source_with_no_pipe_gives_an_empty_selection(self):
        sel = select_supplied_subgraph(grid(3, 3, first_node=10), [1], cap=10)
        self.assertEqual(sel.pipes, [])
        self.assertEqual(sel.sources_without_pipes, [1])
        self.assertEqual(sel.unsupplied_pipes, 12)

    def test_no_sources_selects_nothing(self):
        sel = select_supplied_subgraph(grid(3, 3), [], cap=10)
        self.assertEqual(sel.pipes, [])
        self.assertEqual(sel.unsupplied_pipes, 12)


class ElementName(unittest.TestCase):

    def test_plain_source_id_is_kept_so_results_join_back(self):
        self.assertEqual(element_name("P", "water_main:12345"), "Pwater_main:12345")

    def test_long_or_unsafe_ids_hash_stably_within_epanet_limits(self):
        for source_id in ("x" * 40, "has space:1", "semi;colon", 'quo"te'):
            name = element_name("P", source_id)
            self.assertLessEqual(len(name), EPANET_MAX_ID)
            self.assertNotRegex(name, r'[\s;"]')
            self.assertEqual(name, element_name("P", source_id))
        self.assertNotEqual(element_name("P", "x" * 40), element_name("P", "y" * 40))


class EpanetSolve(unittest.TestCase):

    def test_selection_solves_in_epanet(self):
        try:
            import wntr
        except ImportError:
            self.skipTest("wntr not installed")
        fed = grid(6, 6)
        sel = select_supplied_subgraph(fed + grid(2, 2, first_node=500), [1], cap=30)
        wn = wntr.network.WaterNetworkModel()
        for n in sorted(sel.nodes):
            wn.add_junction(f"N{n}", base_demand=0.001, elevation=10.0)
        wn.add_reservoir(element_name("R", "reservoir:1"), base_head=80.0)
        wn.add_pipe(element_name("L", "reservoir:1"), element_name("R", "reservoir:1"),
                    "N1", length=10.0, diameter=0.6, roughness=100)
        for p in sel.pipes:
            wn.add_pipe(element_name("P", p.pipe_id), f"N{p.start_node}", f"N{p.end_node}",
                        length=p.length_m, diameter=p.diameter_m, roughness=100)
        wn.options.time.duration = 0
        # EPANET writes temp.* scratch files into the working directory unless
        # given a prefix, which left them in the repository root.
        with tempfile.TemporaryDirectory() as tmp:
            results = wntr.sim.EpanetSimulator(wn).run_sim(file_prefix=str(Path(tmp) / "solve"))
        pressure = results.node["pressure"]
        self.assertEqual(set(pressure.columns) - set(wn.reservoir_name_list),
                         {f"N{n}" for n in sel.nodes})
        self.assertTrue((pressure.iloc[-1] > 0).all())


if __name__ == "__main__":
    unittest.main()
