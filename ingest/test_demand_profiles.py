"""Synthetic meter demand: sourced volumes, daily shapes and the reading
format the Flink meter source expects."""

import unittest
from datetime import datetime, timedelta, timezone
from statistics import mean

from demand_profiles import (COMMERCIAL, COMMERCIAL_L_PER_M2_DAY, LITRES_PER_PERSON_DAY,
                             PEOPLE_PER_HOUSEHOLD, RESIDENTIAL_WEEKDAY, RESIDENTIAL_WEEKEND,
                             interval_litres, meter_profile)

# A Tuesday, so weekday shapes apply.
TUESDAY = datetime(2026, 11, 3, tzinfo=timezone.utc)


def day_of_intervals(profile, day, minutes=15):
    return [interval_litres(profile, day + timedelta(minutes=minutes * i), minutes)
            for i in range(1440 // minutes)]


class ShapeTest(unittest.TestCase):
    def test_hourly_shapes_average_one(self):
        for shape in (RESIDENTIAL_WEEKDAY, RESIDENTIAL_WEEKEND, COMMERCIAL):
            self.assertEqual(len(shape), 24)
            self.assertAlmostEqual(mean(shape), 1.0)

    def test_residential_peaks_in_the_morning_and_is_quiet_overnight(self):
        self.assertIn(RESIDENTIAL_WEEKDAY.index(max(RESIDENTIAL_WEEKDAY)), (7, 8))
        self.assertLess(max(RESIDENTIAL_WEEKDAY[1:5]), 0.3)


class VolumeTest(unittest.TestCase):
    def test_households_average_the_published_figure(self):
        expected = LITRES_PER_PERSON_DAY * PEOPLE_PER_HOUSEHOLD
        volumes = [meter_profile(f"H{i}", "residential").daily_litres for i in range(4000)]
        self.assertAlmostEqual(mean(volumes) / expected, 1.0, delta=0.03)

    def test_commercial_volume_is_area_times_rate(self):
        p = meter_profile("C1", "commercial")
        use, area = p.detail.split(", ")
        self.assertAlmostEqual(p.daily_litres,
                               float(area.split()[0]) * COMMERCIAL_L_PER_M2_DAY[use], delta=1.0)

    def test_a_day_of_readings_sums_to_about_the_daily_volume(self):
        # Interval noise and quiet zeros are drawn to keep the mean, so over
        # many meters a day sums close to the profile (the day factor too).
        ratios = []
        for i in range(300):
            p = meter_profile(f"H{i}", "residential")
            ratios.append(sum(day_of_intervals(p, TUESDAY)) / p.daily_litres)
        self.assertAlmostEqual(mean(ratios), 1.0, delta=0.06)

    def test_commercial_weekend_is_lower(self):
        p = meter_profile("C7", "commercial")
        weekday = sum(day_of_intervals(p, TUESDAY))
        saturday = sum(day_of_intervals(p, TUESDAY + timedelta(days=4)))
        self.assertLess(saturday, weekday)


class IntervalTest(unittest.TestCase):
    def test_households_go_quiet_overnight_but_bulk_meters_do_not(self):
        night = [TUESDAY + timedelta(hours=2, minutes=15 * i) for i in range(8)]
        house = [interval_litres(meter_profile(f"H{i}", "residential"), t, 15)
                 for i in range(50) for t in night]
        bulk = [interval_litres(meter_profile(f"B{i}", "bulk"), t, 15)
                for i in range(50) for t in night]
        self.assertGreater(sum(1 for v in house if v == 0) / len(house), 0.5)
        self.assertEqual(sum(1 for v in bulk if v == 0), 0)

    def test_same_meter_and_time_give_the_same_reading(self):
        p = meter_profile("H42", "residential")
        t = TUESDAY + timedelta(hours=7)
        self.assertEqual(interval_litres(p, t, 15), interval_litres(p, t, 15))
        self.assertEqual(meter_profile("H42", "residential"), p)

    def test_unknown_type_is_read_as_a_household(self):
        self.assertEqual(meter_profile("X1", None).meter_type, "residential")


try:
    from synthetic_meters import readings
except ImportError:  # psycopg and kafka live in the ingest image
    readings = None


@unittest.skipIf(readings is None, "ingest requirements not installed")
class ReadingTest(unittest.TestCase):
    def test_reading_matches_the_flink_meter_source(self):
        end = TUESDAY + timedelta(hours=8)
        [row] = readings([("DMA-001-M0001", "DMA-001", "residential")], end, 15)
        self.assertEqual(set(row), {"meter_id", "dma_id", "litres", "flow_lpm",
                                    "battery_pct", "event_ts", "is_synthetic"})
        self.assertEqual(row["dma_id"], "DMA-001")
        self.assertAlmostEqual(row["flow_lpm"], row["litres"] / 15, places=2)
        self.assertEqual(row["event_ts"], int(end.timestamp() * 1000))
        self.assertTrue(55 <= row["battery_pct"] <= 100)
        self.assertIs(row["is_synthetic"], True)


if __name__ == "__main__":
    unittest.main()
