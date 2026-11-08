"""Tests for the hydrometry poller's parsing and resume rule; no network."""

import os
import unittest
from datetime import datetime, timezone

os.environ.setdefault("HYDRO_SITES", "G1")
import hydrometry_poller as hp  # noqa: E402

PAYLOAD = {
    "error_num": 0,
    "return": {
        "traces": [
            {
                "trace": [
                    {"t": 20261108010000, "v": "1.20", "q": 1},
                    {"t": 20261108020000, "v": "1.25", "q": 1},
                    {"t": 20261108030000, "v": "1.30", "q": 1},
                ]
            }
        ]
    },
}


class ResumeTests(unittest.TestCase):
    def test_parses_points_as_utc(self):
        records = hp.parse_timeseries(PAYLOAD, "G1", "100.00")
        self.assertEqual([r["variable"] for r in records], ["level_m"] * 3)
        self.assertEqual(records[0]["event_ts"], "2026-11-08T01:00:00+00:00")

    def test_only_points_after_the_last_stored_are_published(self):
        records = hp.parse_timeseries(PAYLOAD, "G1", "100.00")
        last = datetime(2026, 11, 8, 2, 0, tzinfo=timezone.utc)
        self.assertEqual([r["value"] for r in hp.newer_than(records, last)], ["1.30"])

    def test_nothing_stored_publishes_everything(self):
        records = hp.parse_timeseries(PAYLOAD, "G1", "100.00")
        self.assertEqual(len(hp.newer_than(records, None)), 3)


if __name__ == "__main__":
    unittest.main()
