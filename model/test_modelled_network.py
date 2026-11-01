"""select_modelled() against a stub cursor: the snap limit and the cap are
applied the same way for the seeder as for the model build."""

import unittest

from modelled_network import select_modelled


class StubCursor:
    """Answers the two queries in order: reservoirs, then pipes."""

    def __init__(self, reservoirs, pipes):
        self._results = [reservoirs, pipes]
        self._current = None

    def execute(self, sql, params=None):
        self._current = self._results.pop(0)

    def fetchall(self):
        return self._current


# A line of nodes 1-2-3-4 and a detached pair 10-11 no reservoir feeds.
PIPES = [("a", 1, 2, 100.0, 0.15), ("b", 2, 3, 100.0, 0.15),
         ("c", 3, 4, 100.0, 0.15), ("z", 10, 11, 100.0, 0.15)]


class SelectModelledTest(unittest.TestCase):
    def test_far_reservoir_is_left_out(self):
        reservoirs = [("near", -37.0, 145.0, 1, 40.0, 20.0),
                      ("far", -37.5, 145.5, 10, 90.0, 5000.0)]
        m = select_modelled(StubCursor(reservoirs, PIPES), max_pipes=100, max_snap_m=500)
        self.assertEqual([r[0] for r in m.reservoirs], ["near"])
        self.assertEqual([r[0] for r in m.too_far], ["far"])
        # The detached pair would only be modelled through the far reservoir.
        self.assertEqual([p.pipe_id for p in m.selection.pipes], ["a", "b", "c"])
        self.assertEqual(m.pipe_count, 4)

    def test_cap_limits_the_walk(self):
        reservoirs = [("near", -37.0, 145.0, 1, 40.0, 20.0)]
        m = select_modelled(StubCursor(reservoirs, PIPES), max_pipes=2, max_snap_m=500)
        self.assertEqual([p.pipe_id for p in m.selection.pipes], ["a", "b"])

    def test_no_reservoir_selects_nothing(self):
        m = select_modelled(StubCursor([], PIPES), max_pipes=100, max_snap_m=500)
        self.assertEqual(m.selection.pipes, [])


if __name__ == "__main__":
    unittest.main()
