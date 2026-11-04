"""Gap finding and row drawing in backfill.py, and a whole pass against a scratch
database when WEAVE_TEST_DSN names a role that can create one."""

import json
import os
import random
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import backfill
from backfill import (PressureSpec, closed_gaps, load_pressure_spec, missing_interval_ends,
                      pressure_rows)


def at(hour, minute=0, second=0):
    return datetime(2026, 11, 4, hour, minute, second, tzinfo=timezone.utc)


SPEC = PressureSpec(["L1", "L2"], ["D1", "D2"], (18.0, 62.0), (0.5, 85.0))


class ClosedGapsTest(unittest.TestCase):
    def test_finds_only_spaces_with_a_row_after_them(self):
        times = [at(9), at(9, 15), at(14, 4), at(14, 19)]
        self.assertEqual(closed_gaps(times, timedelta(minutes=22)), [(at(9, 15), at(14, 4))])

    def test_a_feed_that_stopped_has_no_closed_gap(self):
        # The last row being hours old may be a dead pipeline, so it is left alone.
        self.assertEqual(closed_gaps([at(9), at(9, 15)], timedelta(minutes=22)), [])
        self.assertEqual(closed_gaps([at(9)], timedelta(minutes=22)), [])
        self.assertEqual(closed_gaps([], timedelta(minutes=22)), [])


class MissingIntervalsTest(unittest.TestCase):
    def test_leaves_the_intervals_the_edge_rows_report(self):
        # A row at 14:19 reports the interval ending 14:15, and one at 15:38 the
        # interval ending 15:30, so only the ends between them are missing.
        ends = missing_interval_ends(at(14, 19), at(15, 38), 900)
        self.assertEqual(ends, [at(14, 30), at(14, 45), at(15), at(15, 15)])

    def test_a_filled_gap_has_nothing_left_to_fill(self):
        # After filling, the last filled row and the live row can still sit more
        # than the threshold apart, so this is what makes a second pass do nothing.
        self.assertEqual(missing_interval_ends(at(15, 15), at(15, 38), 900), [])

    def test_a_row_exactly_on_the_boundary_still_counts_as_that_interval(self):
        self.assertEqual(missing_interval_ends(at(14, 15), at(14, 45), 900), [at(14, 30)])


class PressureRowsTest(unittest.TestCase):
    def test_stays_inside_the_gap_and_the_schema_ranges(self):
        rows = list(pressure_rows(SPEC, at(10), at(11), 1.5, random.Random(1)))
        self.assertGreater(len(rows), 2000)     # 3,600 s at about one per 1.5 s
        self.assertLess(len(rows), 2800)
        for when, logger, dma, pressure, flow, synthetic in rows:
            self.assertTrue(at(10) < when < at(11))
            self.assertIn(logger, SPEC.loggers)
            self.assertIn(dma, SPEC.dmas)
            self.assertTrue(18.0 <= pressure <= 62.0)
            self.assertTrue(0.5 <= flow <= 85.0)
            self.assertIs(synthetic, True)

    def test_reads_what_datagen_reads(self):
        schema = {"fields": [
            {"name": "logger_id", "type": {"type": "string", "arg.properties": {"options": ["L1"]}}},
            {"name": "dma_id", "type": {"type": "string", "arg.properties": {"options": ["D1"]}}},
            {"name": "pressure_m", "type": {"type": "double",
                                            "arg.properties": {"range": {"min": 18.0, "max": 62.0}}}},
            {"name": "flow_ls", "type": {"type": "double",
                                         "arg.properties": {"range": {"min": 0.5, "max": 85.0}}}},
            {"name": "event_ts", "type": {"type": "long", "arg.properties": {"iteration": {"start": 0}}}},
        ]}
        with tempfile.NamedTemporaryFile("w", suffix=".avsc") as fh:
            json.dump(schema, fh)
            fh.flush()
            got = load_pressure_spec(fh.name)
        self.assertEqual(got, PressureSpec(["L1"], ["D1"], (18.0, 62.0), (0.5, 85.0)))


DSN = os.environ.get("WEAVE_TEST_DSN")
SCRATCH = "weave_backfill_test"
SCHEMA = """
    CREATE EXTENSION IF NOT EXISTS timescaledb;
    CREATE SCHEMA ref; CREATE SCHEMA twin;
    CREATE TABLE ref.water_meter (meter_id TEXT PRIMARY KEY, dma_id TEXT, meter_type TEXT);
    CREATE TABLE twin.meter_reading (ts TIMESTAMPTZ NOT NULL, meter_id TEXT NOT NULL, dma_id TEXT,
        litres DOUBLE PRECISION, flow_lpm DOUBLE PRECISION, battery_pct INTEGER,
        is_synthetic BOOLEAN DEFAULT true);
    CREATE TABLE twin.pressure_reading (ts TIMESTAMPTZ NOT NULL, logger_id TEXT NOT NULL, dma_id TEXT,
        pressure_m DOUBLE PRECISION, flow_ls DOUBLE PRECISION, is_synthetic BOOLEAN DEFAULT true);
    CREATE TABLE twin.dma_balance (window_start TIMESTAMPTZ NOT NULL, dma_id TEXT NOT NULL,
        inflow_l DOUBLE PRECISION, consumption_l DOUBLE PRECISION, loss_l DOUBLE PRECISION,
        loss_pct DOUBLE PRECISION, min_pressure_m DOUBLE PRECISION, meter_count BIGINT,
        PRIMARY KEY (window_start, dma_id));
"""


@unittest.skipUnless(DSN, "WEAVE_TEST_DSN not set")
class WholePassTest(unittest.TestCase):
    """Against Postgres, because the guarantees are about what ends up in tables."""

    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.conninfo import make_conninfo
        cls.psycopg = psycopg
        cls.admin = psycopg.connect(DSN, autocommit=True)
        cls.admin.execute(f"DROP DATABASE IF EXISTS {SCRATCH}")
        cls.admin.execute(f"CREATE DATABASE {SCRATCH}")
        cls.dsn = make_conninfo(DSN, dbname=SCRATCH)
        with psycopg.connect(cls.dsn) as conn:
            conn.execute(SCHEMA)

    @classmethod
    def tearDownClass(cls):
        cls.admin.execute(f"DROP DATABASE IF EXISTS {SCRATCH} WITH (FORCE)")
        cls.admin.close()

    def setUp(self):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        self.t0 = backfill.floor_to(now - timedelta(hours=4), 900) + timedelta(minutes=4)
        self.t1 = self.t0 + timedelta(hours=2)
        with self.psycopg.connect(self.dsn) as conn:
            conn.execute("TRUNCATE ref.water_meter, twin.meter_reading, twin.pressure_reading, "
                         "twin.dma_balance")
            conn.execute("DROP TABLE IF EXISTS twin.backfill_log")
            conn.execute("INSERT INTO ref.water_meter VALUES ('M1', 'D1', 'residential'), "
                         "('M2', 'D1', 'bulk')")
            for ts in (self.t0, self.t1):
                conn.execute("INSERT INTO twin.meter_reading (ts, meter_id, dma_id, litres) "
                             "VALUES (%s, 'M1', 'D1', 5), (%s, 'M2', 'D1', 5)", (ts, ts))
                conn.execute("INSERT INTO twin.pressure_reading (ts, logger_id, dma_id, pressure_m) "
                             "VALUES (%s, 'L1', 'D1', 40)", (ts,))

    def one_pass(self, since=None):
        with mock.patch.object(backfill, "DSN", self.dsn), \
                mock.patch.object(backfill, "load_pressure_spec", return_value=SPEC):
            return backfill.run_once({} if since is None else since)

    def rows(self, sql):
        with self.psycopg.connect(self.dsn) as conn:
            return conn.execute(sql).fetchall()

    def test_fills_logs_and_rebuilds_the_balance(self):
        self.assertGreater(self.one_pass(), 0)
        # Two meters, for the seven interval ends between the two live rows.
        self.assertEqual(self.rows("SELECT count(*) FROM twin.meter_reading")[0][0], 4 + 7 * 2)
        logged = dict(self.rows("SELECT stream, row_count FROM twin.backfill_log"))
        self.assertEqual(logged["meter_reading"], 14)
        self.assertGreater(logged["pressure_reading"], 3000)
        self.assertTrue(all(r[0] for r in self.rows("SELECT is_synthetic FROM twin.meter_reading "
                                                    "WHERE battery_pct IS NOT NULL")))
        balance = self.rows("SELECT consumption_l, min_pressure_m, meter_count FROM twin.dma_balance "
                            f"WHERE dma_id = 'D1' AND window_start = '{self.t0 + timedelta(minutes=56)}'")
        self.assertEqual(len(balance), 1)
        self.assertEqual(balance[0][2], 2)

    def test_a_second_pass_adds_nothing(self):
        self.one_pass()
        before = self.rows("SELECT (SELECT count(*) FROM twin.meter_reading), "
                           "(SELECT count(*) FROM twin.pressure_reading), "
                           "(SELECT count(*) FROM twin.backfill_log)")
        self.assertEqual(self.one_pass(), 0)
        self.assertEqual(self.rows("SELECT (SELECT count(*) FROM twin.meter_reading), "
                                   "(SELECT count(*) FROM twin.pressure_reading), "
                                   "(SELECT count(*) FROM twin.backfill_log)"), before)

    def test_a_feed_silent_since_its_last_row_is_left_alone(self):
        with self.psycopg.connect(self.dsn) as conn:
            conn.execute("DELETE FROM twin.meter_reading WHERE ts = %s", (self.t1,))
            conn.execute("DELETE FROM twin.pressure_reading WHERE ts = %s", (self.t1,))
        self.assertEqual(self.one_pass(), 0)
        self.assertEqual(self.rows("SELECT count(*) FROM twin.backfill_log")[0][0], 0)

    def test_finds_a_gap_that_closes_after_the_last_check(self):
        since = {}
        with self.psycopg.connect(self.dsn) as conn:
            conn.execute("DELETE FROM twin.meter_reading WHERE ts = %s", (self.t1,))
            conn.execute("DELETE FROM twin.pressure_reading WHERE ts = %s", (self.t1,))
        self.assertEqual(self.one_pass(since), 0)
        with self.psycopg.connect(self.dsn) as conn:
            conn.execute("INSERT INTO twin.meter_reading (ts, meter_id, dma_id, litres) "
                         "VALUES (%s, 'M1', 'D1', 5)", (self.t1,))
        self.assertGreater(self.one_pass(since), 0)
        self.assertEqual(self.rows("SELECT stream FROM twin.backfill_log"), [("meter_reading",)])


if __name__ == "__main__":
    unittest.main()
