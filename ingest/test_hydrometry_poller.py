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

    def test_message_timestamp_is_the_measurement_time(self):
        self.assertEqual(hp.event_ms("2026-11-08T01:00:00+00:00"), 1794099600000)

    def test_nothing_stored_publishes_everything(self):
        records = hp.parse_timeseries(PAYLOAD, "G1", "100.00")
        self.assertEqual(len(hp.newer_than(records, None)), 3)


class KiwisTests(unittest.TestCase):
    LIST = [
        ["station_no", "parametertype_name", "ts_id"],
        ["G1", "Water Course Level", "101"],
        ["G1", "Water Course Discharge", "102"],
        ["G1", "Rainfall", "103"],
        ["G2", "Water Course Level", "201"],
    ]

    def test_series_map_level_and_flow_only(self):
        series = hp.kiwis_series(self.LIST)
        self.assertEqual(series, {("G1", "level_m"): "101", ("G1", "flow_cumecs"): "102", ("G2", "level_m"): "201"})

    def test_stations_carry_name_location_and_measures(self):
        listing = [
            ["station_no", "station_name", "station_latitude", "station_longitude", "parametertype_name", "ts_id"],
            ["G1", "RIVER AT A", "-37.78", "145.02", "Water Course Level", "101"],
            ["G1", "RIVER AT A", "-37.78", "145.02", "Water Course Discharge", "102"],
            ["G1", "RIVER AT A", "-37.78", "145.02", "Rainfall", "103"],
        ]
        st = hp.kiwis_stations(listing)
        self.assertEqual(list(st), ["G1"])
        self.assertEqual(st["G1"]["name"], "RIVER AT A")
        self.assertEqual(st["G1"]["measures"], {"level_m": "m", "flow_cumecs": "m3/s"})
        self.assertEqual(hp.kiwis_series(listing), {("G1", "level_m"): "101", ("G1", "flow_cumecs"): "102"})

    def test_series_rejects_an_error_answer(self):
        with self.assertRaises(ValueError):
            hp.kiwis_series({"type": "error", "message": "x"})

    def test_records_are_utc_with_units(self):
        t = datetime.fromisoformat("2026-11-07T10:00:00+10:00")
        (record,) = hp.kiwis_records([(t, 1.5)], "G1", "flow_cumecs")
        self.assertEqual(record["event_ts"], "2026-11-07T00:00:00+00:00")
        self.assertEqual(record["unit"], "m3/s")
        self.assertEqual(record["value"], 1.5)
        self.assertEqual(record["site_id"], "G1")

    def test_kiwis_records_resume_like_hydstra(self):
        points = [(datetime(2026, 11, 7, h, tzinfo=timezone.utc), float(h)) for h in range(3)]
        records = hp.kiwis_records(points, "G1", "level_m")
        last = datetime(2026, 11, 7, 1, tzinfo=timezone.utc)
        self.assertEqual([r["value"] for r in hp.newer_than(records, last)], [2.0])


if __name__ == "__main__":
    unittest.main()
