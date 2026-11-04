"""select_modelled() against a stub cursor: the snap limit and the cap are
applied the same way for the seeder as for the model build."""

import os
import unittest
from unittest import mock

from modelled_network import excluded_pipes, select_modelled


class StubCursor:
    """Answers each query by what it reads: reservoirs, pipes, T-junction
    hits, node coordinates."""

    def __init__(self, reservoirs, pipes, hits=(), coords=None):
        # Reservoirs may be given as (id, lat, lon, node, elevation, distance),
        # read as a service reservoir whose only candidate is that node, or as
        # the query's own rows (id, lat, lon, type, node, elevation, distance, rank).
        rows = [r if len(r) == 8 else (r[0], r[1], r[2], "service_reservoir", r[3], r[4], r[5], 1)
                for r in reservoirs]
        self._answers = {"facility": rows, "pipes": pipes, "hits": list(hits),
                         "coords": [(n, x, y) for n, (x, y) in (coords or {}).items()]}
        self._current = None

    def execute(self, sql, params=None):
        if "asset.facility" in sql:
            self._current = self._answers["facility"]
        elif "ST_LineLocatePoint" in sql:
            self._current = self._answers["hits"]
        elif "FROM asset.node" in sql:
            self._current = self._answers["coords"]
        else:
            self._current = self._answers["pipes"]

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

    def test_real_split_comes_before_any_synthetic_link(self):
        # z's end node 10 lies on the side of pipe b: splitting b joins it
        # for real, so no gap or bridge is needed even with coordinates given.
        reservoirs = [("near", -37.0, 145.0, 1, 40.0, 20.0)]
        coords = {1: (145.0, -37.0), 2: (145.001, -37.0), 3: (145.002, -37.0),
                  4: (145.003, -37.0), 10: (145.0015, -37.0), 11: (145.0015, -37.001)}
        m = select_modelled(StubCursor(reservoirs, PIPES, hits=[(10, "b", 0.5, 0.5)],
                                       coords=coords), max_pipes=100, max_snap_m=500)
        self.assertEqual([l.kind for l in m.links], ["split"])
        self.assertIn("z", {p.pipe_id for p in m.selection.pipes})
        self.assertEqual({p.asset_id for p in m.selection.pipes}, {"a", "b", "c", "z"})

    def test_far_piece_is_bridged_and_says_so(self):
        reservoirs = [("near", -37.0, 145.0, 1, 40.0, 20.0)]
        coords = {1: (145.0, -37.0), 2: (145.001, -37.0), 3: (145.002, -37.0),
                  4: (145.003, -37.0), 10: (145.013, -37.0), 11: (145.014, -37.0)}
        m = select_modelled(StubCursor(reservoirs, PIPES, coords=coords),
                            max_pipes=100, max_snap_m=500)
        [link] = m.links
        self.assertEqual(link.kind, "bridge")
        self.assertIn("invented", link.reason)
        self.assertIn("z", {p.pipe_id for p in m.selection.pipes})
        off = select_modelled(StubCursor(reservoirs, PIPES, coords=coords), max_pipes=100,
                              max_snap_m=500, small_m=0, large_m=0)
        self.assertEqual(off.links, [])
        self.assertNotIn("z", {p.pipe_id for p in off.selection.pipes})

    def test_reservoir_snaps_past_a_node_with_only_a_zero_length_pipe(self):
        # Node 9 is nearest but its only pipe collapsed to a self-loop.
        pipes = PIPES + [("stub", 9, 9, 1.0, 0.15)]
        rows = [("r", -37.0, 145.0, "service_reservoir", 9, 40.0, 5.0, 1),
                ("r", -37.0, 145.0, "service_reservoir", 1, 40.0, 30.0, 2)]
        m = select_modelled(StubCursor(rows, pipes), max_pipes=100, max_snap_m=500)
        self.assertEqual([(r[0], r[3]) for r in m.reservoirs], [("r", 1)])
        [note] = m.snap_notes
        self.assertIn("N9", note)
        self.assertIn("zero-length", note)
        self.assertEqual({p.pipe_id for p in m.selection.pipes}, {"a", "b", "c"})

    def test_snap_limit_can_differ_by_type(self):
        rows = [("svc", -37.0, 145.0, "service_reservoir", 1, 40.0, 2000.0, 1),
                ("dam", -37.1, 145.1, "storage_dam", 10, 40.0, 2000.0, 1)]
        env = {"SIM_MAX_SNAP_M_SERVICE_RESERVOIR": "3200", "SIM_MAX_SNAP_M_STORAGE_DAM": "500"}
        with mock.patch.dict(os.environ, env):
            m = select_modelled(StubCursor(rows, PIPES), max_pipes=100, max_snap_m=500,
                                small_m=0, large_m=0)
        self.assertEqual([r[0] for r in m.reservoirs], ["svc"])
        self.assertEqual([r[0] for r in m.too_far], ["dam"])
        self.assertEqual(m.limits, {"service_reservoir": 3200.0, "storage_dam": 500.0})

    def test_type_limits_default_to_the_shared_one(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            for k in ("SIM_MAX_SNAP_M_SERVICE_RESERVOIR", "SIM_MAX_SNAP_M_STORAGE_DAM"):
                os.environ.pop(k, None)
            m = select_modelled(StubCursor([("r", -37.0, 145.0, 1, 40.0, 20.0)], PIPES),
                                max_pipes=100, max_snap_m=700)
        self.assertEqual(m.limits, {"service_reservoir": 700.0, "storage_dam": 700.0})

    def test_every_left_out_pipe_has_a_reason(self):
        pipes = PIPES + [("joint", 3, 3, 1.0, 0.15)]
        m = select_modelled(StubCursor([("r", -37.0, 145.0, 1, 40.0, 20.0)], pipes),
                            max_pipes=2, max_snap_m=500, small_m=0, large_m=0)
        out = excluded_pipes(m, max_pipes=2, large_m=0)
        self.assertEqual({k: v[0] for k, v in out.items()},
                         {"c": "over_cap", "z": "unsupplied", "joint": "zero_length"})
        self.assertTrue(all(reason for _, reason in out.values()))

    def test_no_reservoir_selects_nothing(self):
        m = select_modelled(StubCursor([], PIPES), max_pipes=100, max_snap_m=500)
        self.assertEqual(m.selection.pipes, [])


if __name__ == "__main__":
    unittest.main()
