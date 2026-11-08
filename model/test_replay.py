"""Tests for the choice of solves to replay; standard library only."""

import unittest
from datetime import datetime, timedelta, timezone

from replay import replay_times

T0 = datetime(2026, 11, 5, 0, 0, tzinfo=timezone.utc)
MIN = timedelta(minutes=1)


class ReplayTimesTests(unittest.TestCase):
    def test_steady_cadence_has_no_gap(self):
        solves = [T0 + 15 * MIN * i for i in range(10)]
        self.assertEqual(replay_times(solves, 900), [])

    def test_slow_solve_is_not_a_gap(self):
        # 20 minutes is within 1.5 intervals.
        self.assertEqual(replay_times([T0, T0 + 20 * MIN], 900), [])

    def test_gap_is_filled_at_the_interval(self):
        times = replay_times([T0, T0 + 60 * MIN], 900)
        self.assertEqual(times, [T0 + 15 * MIN, T0 + 30 * MIN, T0 + 45 * MIN])

    def test_replay_keeps_clear_of_the_live_row(self):
        # The live row 5 minutes after the last step would crowd it.
        times = replay_times([T0, T0 + 50 * MIN], 900)
        self.assertEqual(times, [T0 + 15 * MIN, T0 + 30 * MIN])

    def test_only_closed_gaps(self):
        # Nothing after the last solve: an open gap is left alone.
        self.assertEqual(replay_times([T0], 900), [])
        self.assertEqual(replay_times([], 900), [])

    def test_several_gaps_oldest_first(self):
        solves = [T0, T0 + 45 * MIN, T0 + 60 * MIN, T0 + 120 * MIN]
        times = replay_times(solves, 900)
        self.assertEqual(times, sorted(times))
        self.assertEqual(len(times), 2 + 3)

    def test_rerun_after_replay_finds_nothing(self):
        solves = [T0, T0 + 60 * MIN]
        filled = sorted(solves + replay_times(solves, 900))
        self.assertEqual(replay_times(filled, 900), [])


if __name__ == "__main__":
    unittest.main()
