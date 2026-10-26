"""Database fixtures for conforming GIS pipes into asset.pipe (HDH-76).

    WEAVE_TEST_DSN=postgresql://postgres@localhost:5432/postgres \
        python3 -m unittest discover -s gis -p 'test_*.py'

The loader's behaviour lives in SQL (ST_LineMerge, ST_Dump, the upserts), so
these tests need a real PostGIS. Each run creates a throwaway database from
postgres/init/02_asset_graph.sql and drops it afterwards; WEAVE_TEST_DSN must
therefore name a role that can create databases. Without it every test skips,
so the stack-free test run keeps working. Features are fed in directly, so no
GIS service is contacted.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

import load_gis

TEST_DSN = os.environ.get("WEAVE_TEST_DSN")
SCHEMA_SQL = Path(__file__).resolve().parent.parent / "postgres" / "init" / "02_asset_graph.sql"
HINTS = {"asset_name": ["name"], "diameter_mm": ["diameter"], "material": ["material"],
         "capacity_ml": ["capacity"]}
PIPE_LAYER = {"item_id": "fixture", "target": "pipe"}

# Two parts that do not meet end to end, so ST_LineMerge cannot join them.
DISJOINT = {"type": "MultiLineString",
            "coordinates": [[[145.000, -37.800], [145.001, -37.800]],
                            [[145.010, -37.800], [145.011, -37.800]]]}
# Two parts sharing an end point, which ST_LineMerge joins into one line.
JOINED = {"type": "MultiLineString",
          "coordinates": [[[145.000, -37.800], [145.001, -37.800]],
                          [[145.001, -37.800], [145.002, -37.800]]]}
SINGLE = {"type": "LineString", "coordinates": [[145.000, -37.800], [145.001, -37.800]]}
# Fails ST_GeomFromGeoJSON, aborting the transaction mid-layer.
BROKEN = {"type": "LineString", "coordinates": [[145.000]]}


def feature(fid: str, geometry: dict) -> dict:
    return {"id": fid, "properties": {"diameter": 150}, "geometry": geometry}


@unittest.skipUnless(TEST_DSN, "set WEAVE_TEST_DSN to a PostGIS role that can create databases")
class LoadPipesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.psycopg = psycopg
        cls.dbname = f"weave_test_{uuid.uuid4().hex[:8]}"
        with psycopg.connect(TEST_DSN, autocommit=True) as admin:
            admin.execute(f'CREATE DATABASE "{cls.dbname}"')
        cls.dsn = psycopg.conninfo.make_conninfo(TEST_DSN, dbname=cls.dbname)
        with psycopg.connect(cls.dsn) as conn:
            conn.execute(SCHEMA_SQL.read_text())

    @classmethod
    def tearDownClass(cls):
        with cls.psycopg.connect(TEST_DSN, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{cls.dbname}" WITH (FORCE)')

    def setUp(self):
        self.conn = self.psycopg.connect(self.dsn)
        self.conn.execute("TRUNCATE ref.water_meter, asset.dma, asset.pipe, asset.facility, asset.gis_raw CASCADE")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def load(self, layer: str, features: list[dict]) -> int:
        with mock.patch.object(load_gis, "layer_features", return_value=iter(features)):
            return load_gis.load_layer(self.conn, layer, PIPE_LAYER, None, HINTS, "fixture")

    def pipes(self, layer: str) -> dict[str, str]:
        rows = self.conn.execute(
            "SELECT pipe_id, ST_GeometryType(geom) FROM asset.pipe WHERE layer = %s",
            (layer,)).fetchall()
        return dict(rows)

    def test_disjoint_parts_become_numbered_line_pipes(self):
        self.assertEqual(self.load("mains", [feature("7", DISJOINT)]), 1)
        self.assertEqual(self.pipes("mains"),
                         {"mains:7:1": "ST_LineString", "mains:7:2": "ST_LineString"})
        raw = self.conn.execute("SELECT count(*) FROM asset.gis_raw WHERE layer = 'mains'").fetchone()
        self.assertEqual(raw[0], 1, "the raw copy keeps one row per source feature")

    def test_parts_that_meet_keep_the_plain_id(self):
        self.load("mains", [feature("8", JOINED)])
        self.assertEqual(self.pipes("mains"), {"mains:8": "ST_LineString"})

    def test_reload_after_a_split_removes_the_unsplit_pipe(self):
        self.load("mains", [feature("9", SINGLE)])
        self.load("mains", [feature("9", DISJOINT)])
        self.assertEqual(set(self.pipes("mains")), {"mains:9:1", "mains:9:2"})

    def test_reload_after_a_merge_removes_the_part_pipes(self):
        self.load("mains", [feature("10", DISJOINT)])
        self.load("mains", [feature("10", SINGLE)])
        self.assertEqual(set(self.pipes("mains")), {"mains:10"})

    def test_stale_part_referenced_by_a_meter_is_released_not_blocking(self):
        self.load("mains", [feature("11", SINGLE)])
        self.conn.execute(
            "INSERT INTO ref.water_meter (meter_id, nearest_pipe) VALUES ('m1', 'mains:11')")
        self.conn.commit()
        self.load("mains", [feature("11", DISJOINT)])
        self.assertEqual(set(self.pipes("mains")), {"mains:11:1", "mains:11:2"})
        meter = self.conn.execute(
            "SELECT nearest_pipe FROM ref.water_meter WHERE meter_id = 'm1'").fetchone()
        self.assertIsNone(meter[0], "the meter keeps its row; re-seeding reassigns its pipe")

    def test_other_features_with_a_shared_prefix_are_left_alone(self):
        self.load("mains", [feature("12", SINGLE), feature("123", SINGLE)])
        self.load("mains", [feature("12", DISJOINT)])
        self.assertEqual(set(self.pipes("mains")), {"mains:12:1", "mains:12:2", "mains:123"})

    def test_reloading_non_line_layers_is_unaffected(self):
        point = {"type": "Point", "coordinates": [145.0, -37.8]}
        polygon = {"type": "Polygon", "coordinates": [[[145.0, -37.8], [145.001, -37.8],
                                                       [145.001, -37.801], [145.0, -37.8]]]}
        layer = {"item_id": "fixture", "target": "facility", "facility_type": "service_reservoir"}
        for _ in range(2):
            with mock.patch.object(load_gis, "layer_features",
                                   return_value=iter([feature("r1", point), feature("r2", polygon)])):
                self.assertEqual(
                    load_gis.load_layer(self.conn, "tanks", layer, None, HINTS, "fixture"), 2)
        count = self.conn.execute("SELECT count(*) FROM asset.facility").fetchone()[0]
        self.assertEqual(count, 2)

    def test_failed_layer_does_not_stop_the_next_one(self):
        with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as fh:
            fh.write("region: {source_id: fixture}\n"
                     "attribute_hints: {asset_name: [name], diameter_mm: [diameter],"
                     " material: [material], capacity_ml: [capacity]}\n"
                     "layers:\n"
                     "  broken: {item_id: fixture, target: pipe}\n"
                     "  valid: {item_id: fixture, target: pipe}\n")
        self.addCleanup(os.unlink, fh.name)
        sources = {"broken": [feature("1", DISJOINT), feature("2", BROKEN)],
                   "valid": [feature("3", DISJOINT)]}
        with mock.patch.object(load_gis, "layer_features",
                               side_effect=lambda name, *a: iter(sources[name])), \
             mock.patch.object(load_gis, "DSN", self.dsn), \
             mock.patch.object(sys, "argv", ["load_gis.py", "--config", fh.name, "--all"]):
            self.assertEqual(load_gis.main(), 0)
        self.assertEqual(set(self.pipes("valid")), {"valid:3:1", "valid:3:2"})


if __name__ == "__main__":
    unittest.main()
