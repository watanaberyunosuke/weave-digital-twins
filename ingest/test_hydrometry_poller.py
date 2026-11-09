"""Tests for the hydrometry poller's parsing and resume rule; no network."""

import os
import unittest
from datetime import datetime, timedelta, timezone

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
        self.assertEqual(records[0]["event_ts"], "2026-11-08T01:00:00Z")

    def test_only_points_after_the_last_stored_are_published(self):
        records = hp.parse_timeseries(PAYLOAD, "G1", "100.00")
        last = datetime(2026, 11, 8, 2, 0, tzinfo=timezone.utc)
        self.assertEqual([r["value"] for r in hp.newer_than(records, last)], ["1.30"])

    def test_message_timestamp_is_the_measurement_time(self):
        self.assertEqual(hp.event_ms("2026-11-08T01:00:00Z"), 1794099600000)
        self.assertEqual(hp.event_ms("2026-11-08T01:00:00+00:00"), 1794099600000)

    def test_published_times_are_utc_with_a_z(self):
        # Flink's ISO-8601 reader rejects a +00:00 offset (it reads as NULL), so
        # every producer writes Z, whatever zone the source reported in.
        from common import iso_z

        aest = timezone(timedelta(hours=10))
        self.assertEqual(iso_z(datetime(2026, 11, 8, 11, 0, tzinfo=aest)), "2026-11-08T01:00:00Z")
        half = datetime(2026, 11, 8, 1, 0, 0, 500000, tzinfo=timezone.utc)
        self.assertEqual(iso_z(half), "2026-11-08T01:00:00.500000Z")

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
        self.assertEqual(record["event_ts"], "2026-11-07T00:00:00Z")
        self.assertEqual(record["unit"], "m3/s")
        self.assertEqual(record["value"], 1.5)
        self.assertEqual(record["site_id"], "G1")

    def test_kiwis_records_resume_like_hydstra(self):
        points = [(datetime(2026, 11, 7, h, tzinfo=timezone.utc), float(h)) for h in range(3)]
        records = hp.kiwis_records(points, "G1", "level_m")
        last = datetime(2026, 11, 7, 1, tzinfo=timezone.utc)
        self.assertEqual([r["value"] for r in hp.newer_than(records, last)], [2.0])


class AreaTests(unittest.TestCase):
    HEADER = [
        "station_no",
        "station_name",
        "station_latitude",
        "station_longitude",
        "parametertype_name",
        "ts_id",
        "from",
        "to",
    ]
    NOW = datetime(2026, 11, 8, tzinfo=timezone.utc)

    def row(self, no, lat, lon, to="2026-11-07T00:00:00.000+10:00"):
        return [no, f"SITE {no}", lat, lon, "Water Course Level", f"{no}1", "2017-01-01T00:00:00.000+10:00", to]

    def test_area_keeps_reporting_sites_inside_the_box(self):
        listing = [
            self.HEADER,
            self.row("IN", "-37.8", "145.0"),
            self.row("OUT", "-30.0", "145.0"),
            self.row("STALE", "-37.8", "145.0", to="2017-11-29T00:00:01.000+10:00"),
            self.row("NOCOORD", "", "145.0"),
            self.row("NEVER", "-37.8", "145.0", to=""),
        ]
        kept = hp.filter_listing(listing, (143.5, -38.9, 146.7, -37.0), self.NOW)
        self.assertEqual([r[0] for r in kept[1:]], ["IN"])
        self.assertEqual(kept[0], self.HEADER)

    def test_no_area_keeps_every_reporting_site(self):
        listing = [self.HEADER, self.row("A", "", ""), self.row("B", "-30", "120", to="2017-01-01T00:00:00+10:00")]
        self.assertEqual([r[0] for r in hp.filter_listing(listing, None, self.NOW)[1:]], ["A"])

    def test_batches_group_by_start_and_cap_the_size(self):
        early, late = datetime(2026, 11, 1, tzinfo=timezone.utc), datetime(2026, 11, 6, tzinfo=timezone.utc)
        starts = {f"t{i:02}": (early if i % 2 else late) for i in range(45)}
        got = hp.batches(starts, size=20)
        self.assertEqual([len(ids) for _, ids in got], [20, 20, 5])
        self.assertEqual(sorted(i for _, ids in got for i in ids), sorted(starts))
        # Each batch reads from its oldest need, and the early series come first.
        self.assertEqual(got[0][0], early)
        for start, ids in got:
            self.assertEqual(start, min(starts[i] for i in ids))


if __name__ == "__main__":
    unittest.main()
