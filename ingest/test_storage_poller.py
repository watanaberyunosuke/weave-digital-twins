"""Parsing in storage_poller.py, against responses shaped like a live KiWIS
service's (header row, then rows; values as ts_id blocks)."""

import unittest
from datetime import datetime, timezone

from storage_poller import Series, parse_bbox, parse_series_list, parse_values

NOW = datetime(2026, 11, 1, tzinfo=timezone.utc)
HEADER = ["station_name", "station_no", "ts_id", "station_latitude", "station_longitude", "from", "to"]
BBOX = (144.0, -38.5, 146.5, -37.0)


def row(no, ts_id, lat, lon, to, name=None):
    return [name or f"Storage {no}", no, ts_id, lat, lon, "1984-07-31T00:00:00.000+10:00", to]


class SeriesListTest(unittest.TestCase):
    def test_keeps_reporting_storages_inside_the_area(self):
        payload = [HEADER,
                   row("A1", "1", "-37.84", "146.40", "2026-10-30T00:00:00.000+10:00"),
                   row("OUT", "2", "-33.80", "115.07", "2026-10-30T00:00:00.000+10:00"),
                   row("OLD", "3", "-37.50", "145.00", "2021-09-02T00:00:00.000+10:00")]
        got = parse_series_list(payload, BBOX, NOW)
        self.assertEqual([s.station_no for s in got], ["A1"])
        self.assertEqual(got[0], Series("1", "A1", "Storage A1", -37.84, 146.40,
                                        "2026-10-30T00:00:00.000+10:00"))

    def test_drops_garbled_coordinates_and_empty_coverage(self):
        payload = [HEADER,
                   row("BAD", "1", "", "146.4", "2026-10-30T00:00:00.000+10:00"),
                   row("NONE", "2", "-37.5", "145.0", "")]
        self.assertEqual(parse_series_list(payload, BBOX, NOW), [])

    def test_one_series_per_station_the_most_recent(self):
        payload = [HEADER,
                   row("A1", "old", "-37.8", "146.4", "2026-10-01T00:00:00.000+10:00"),
                   row("A1", "new", "-37.8", "146.4", "2026-10-31T00:00:00.000+10:00")]
        self.assertEqual([s.ts_id for s in parse_series_list(payload, BBOX, NOW)], ["new"])

    def test_error_body_is_refused(self):
        with self.assertRaises(ValueError):
            parse_series_list({"type": "error", "message": "Error connecting to WDP."}, BBOX, NOW)


class ValuesTest(unittest.TestCase):
    def test_values_by_series_skipping_gaps(self):
        payload = [{"ts_id": "1", "rows": "3", "columns": "Timestamp,Value",
                    "data": [["2026-10-29T00:00:00.000+10:00", 120000.5],
                             ["2026-10-30T00:00:00.000+10:00", None],
                             ["2026-10-31T00:00:00.000+10:00", 120100.0]]}]
        got = parse_values(payload)["1"]
        self.assertEqual([v for _, v in got], [120000.5, 120100.0])
        self.assertEqual(got[0][0].utcoffset().total_seconds(), 36000)

    def test_column_order_comes_from_the_response(self):
        payload = [{"ts_id": "9", "columns": "Value, Timestamp",
                    "data": [[5.0, "2026-10-31T00:00:00.000+10:00"]]}]
        self.assertEqual(parse_values(payload)["9"][0][1], 5.0)


class BboxTest(unittest.TestCase):
    def test_parses_and_rejects_inverted(self):
        self.assertEqual(parse_bbox("144,-38.5,146.5,-37"), (144.0, -38.5, 146.5, -37.0))
        with self.assertRaises(ValueError):
            parse_bbox("146,-38,144,-37")


if __name__ == "__main__":
    unittest.main()
