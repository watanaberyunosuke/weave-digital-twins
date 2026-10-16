#!/usr/bin/env python3
"""Derive synthetic DMAs and meters from the REAL loaded pipe network, then
emit Datagen Avro schemas whose id fields are drawn from those real assets.

This is what stops the synthetic telemetry being noise: every generated meter
sits inside a DMA derived from the real mains of the loaded region, and every
generated reading references an id that exists in asset.dma / ref.water_meter.

    python scripts/seed_synthetic_network.py --dmas 40 --meters-per-dma 25
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys

import psycopg

DSN = os.environ.get("TWIN_DSN", "postgresql://weave:weave@localhost:5432/weave_twin")
NAMESPACE = os.environ.get("SCHEMA_NAMESPACE", "weave")
SCHEMA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "schemas")


def build_dmas(conn, n_dmas: int) -> list[str]:
    """Cluster real pipes into synthetic District Metered Areas."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM asset.pipe")
        pipe_count = cur.fetchone()[0]
        if pipe_count == 0:
            print("! asset.pipe is empty — run gis/load_gis.py --all first")
            sys.exit(1)
        print(f"clustering {pipe_count} real pipes into {n_dmas} DMAs ...")

        cur.execute("TRUNCATE asset.dma CASCADE")
        cur.execute(
            """
            WITH clustered AS (
                SELECT pipe_id, geom,
                       ST_ClusterKMeans(ST_Centroid(geom), %s) OVER () AS cid
                FROM asset.pipe
                WHERE geom IS NOT NULL
            ),
            grouped AS (
                SELECT cid,
                       count(*) AS pipe_count,
                       sum(ST_Length(geom::geography)) AS total_length_m,
                       ST_ConvexHull(ST_Collect(geom)) AS hull,
                       ST_Centroid(ST_Collect(geom)) AS centroid
                FROM clustered GROUP BY cid
            )
            INSERT INTO asset.dma (dma_id, name, pipe_count, total_length_m, geom, centroid)
            SELECT 'DMA-' || lpad((cid + 1)::text, 3, '0'),
                   'Synthetic DMA ' || (cid + 1),
                   pipe_count, total_length_m,
                   CASE WHEN ST_GeometryType(hull) = 'ST_Polygon' THEN hull
                        ELSE ST_Buffer(hull, 0.002) END,
                   centroid
            FROM grouped
            """,
            (n_dmas,),
        )
        conn.commit()
        cur.execute("SELECT dma_id FROM asset.dma ORDER BY dma_id")
        dma_ids = [r[0] for r in cur.fetchall()]
    print(f"  created {len(dma_ids)} DMAs")
    return dma_ids


def build_meters(conn, dma_ids: list[str], per_dma: int) -> list[str]:
    """Place synthetic meters along real pipes inside each DMA."""
    print(f"placing {per_dma} meters per DMA along real mains ...")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE ref.water_meter CASCADE")
        cur.execute("TRUNCATE ref.pressure_logger CASCADE")

        for dma_id in dma_ids:
            cur.execute(
                """
                INSERT INTO ref.water_meter (meter_id, dma_id, meter_type, nearest_pipe, geom)
                SELECT %s || '-M' || lpad(gs::text, 4, '0'),
                       %s,
                       (ARRAY['residential','residential','residential',
                              'commercial','bulk'])[1 + floor(random() * 5)::int],
                       p.pipe_id,
                       ST_LineInterpolatePoint(
                           ST_LineMerge(p.geom), random())
                FROM generate_series(1, %s) gs
                CROSS JOIN LATERAL (
                    SELECT pipe_id, geom FROM asset.pipe
                    WHERE ST_Intersects(geom, (SELECT geom FROM asset.dma WHERE dma_id = %s))
                      AND ST_GeometryType(ST_LineMerge(geom)) = 'ST_LineString'
                    ORDER BY random() LIMIT 1
                ) p
                ON CONFLICT (meter_id) DO NOTHING
                """,
                (dma_id, dma_id, per_dma, dma_id),
            )

            # Two pressure loggers per DMA — that's realistic density.
            cur.execute(
                """
                INSERT INTO ref.pressure_logger (logger_id, dma_id, geom)
                SELECT %s || '-P' || gs, %s,
                       (SELECT centroid FROM asset.dma WHERE dma_id = %s)
                FROM generate_series(1, 2) gs
                ON CONFLICT (logger_id) DO NOTHING
                """,
                (dma_id, dma_id, dma_id),
            )
        conn.commit()

        cur.execute("SELECT count(*) FROM ref.water_meter")
        meters = cur.fetchone()[0]
        cur.execute("SELECT meter_id FROM ref.water_meter ORDER BY random() LIMIT 200")
        sample = [r[0] for r in cur.fetchall()]
    print(f"  created {meters} meters")
    return sample


def build_electricity_meters(conn, per_substation: int) -> list[str]:
    """Synthetic electricity meters hung off real OSM substations."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM asset.substation")
        if cur.fetchone()[0] == 0:
            print("  (no substations loaded — skipping electricity meters)")
            return []
        cur.execute("TRUNCATE ref.electricity_meter CASCADE")
        cur.execute(
            """
            INSERT INTO ref.electricity_meter
                   (meter_id, substation_id, customer_type, has_solar, geom)
            SELECT 'EM' || lpad((row_number() OVER ())::text, 8, '0'),
                   s.substation_id,
                   (ARRAY['residential','residential','commercial','industrial'])
                       [1 + floor(random() * 4)::int],
                   random() < 0.35,
                   ST_Translate(s.geom, (random() - 0.5) * 0.01, (random() - 0.5) * 0.01)
            FROM asset.substation s
            CROSS JOIN generate_series(1, %s)
            ON CONFLICT (meter_id) DO NOTHING
            """,
            (per_substation,),
        )
        conn.commit()
        cur.execute("SELECT meter_id FROM ref.electricity_meter ORDER BY random() LIMIT 200")
        return [r[0] for r in cur.fetchall()]


def write_schema(filename: str, schema: dict) -> None:
    os.makedirs(SCHEMA_DIR, exist_ok=True)
    path = os.path.join(SCHEMA_DIR, filename)
    with open(path, "w") as fh:
        json.dump(schema, fh, indent=2)
    print(f"  wrote {path}")


def emit_schemas(meter_ids: list[str], dma_ids: list[str],
                 elec_ids: list[str]) -> None:
    print("generating Datagen schemas seeded with real asset ids ...")

    write_schema("water_meter_reading.avsc", {
        "type": "record",
        "name": "WaterMeterReading",
        "namespace": f"{NAMESPACE}.water",
        "fields": [
            {"name": "meter_id", "type": {"type": "string",
                "arg.properties": {"options": meter_ids}}},
            {"name": "dma_id", "type": {"type": "string",
                "arg.properties": {"options": dma_ids}}},
            {"name": "litres", "type": {"type": "double",
                "arg.properties": {"range": {"min": 0.0, "max": 45.0}}}},
            {"name": "flow_lpm", "type": {"type": "double",
                "arg.properties": {"range": {"min": 0.0, "max": 30.0}}}},
            {"name": "battery_pct", "type": {"type": "int",
                "arg.properties": {"range": {"min": 55, "max": 100}}}},
            {"name": "event_ts", "type": {"type": "long",
                "arg.properties": {"iteration": {"start": 1767225600000, "step": 900000}}}},
        ],
    })

    write_schema("pressure_reading.avsc", {
        "type": "record",
        "name": "PressureReading",
        "namespace": f"{NAMESPACE}.water",
        "fields": [
            {"name": "logger_id", "type": {"type": "string",
                "arg.properties": {"options": [f"{d}-P1" for d in dma_ids] +
                                              [f"{d}-P2" for d in dma_ids]}}},
            {"name": "dma_id", "type": {"type": "string",
                "arg.properties": {"options": dma_ids}}},
            {"name": "pressure_m", "type": {"type": "double",
                "arg.properties": {"range": {"min": 18.0, "max": 62.0}}}},
            {"name": "flow_ls", "type": {"type": "double",
                "arg.properties": {"range": {"min": 0.5, "max": 85.0}}}},
            {"name": "event_ts", "type": {"type": "long",
                "arg.properties": {"iteration": {"start": 1767225600000, "step": 300000}}}},
        ],
    })

    if elec_ids:
        write_schema("electricity_interval.avsc", {
            "type": "record",
            "name": "ElectricityInterval",
            "namespace": f"{NAMESPACE}.electricity",
            "fields": [
                {"name": "meter_id", "type": {"type": "string",
                    "arg.properties": {"options": elec_ids}}},
                {"name": "kwh_import", "type": {"type": "double",
                    "arg.properties": {"range": {"min": 0.0, "max": 12.0}}}},
                {"name": "kwh_export", "type": {"type": "double",
                    "arg.properties": {"range": {"min": 0.0, "max": 6.0}}}},
                {"name": "voltage_v", "type": {"type": "double",
                    "arg.properties": {"range": {"min": 225.0, "max": 253.0}}}},
                {"name": "event_ts", "type": {"type": "long",
                    "arg.properties": {"iteration": {"start": 1767225600000, "step": 1800000}}}},
            ],
        })


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dmas", type=int, default=40)
    ap.add_argument("--meters-per-dma", type=int, default=25)
    ap.add_argument("--meters-per-substation", type=int, default=20)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    random.seed(args.seed)

    with psycopg.connect(DSN) as conn:
        dma_ids = build_dmas(conn, args.dmas)
        meter_sample = build_meters(conn, dma_ids, args.meters_per_dma)
        elec_sample = build_electricity_meters(conn, args.meters_per_substation)

    # Datagen picks uniformly from options, so a sample keeps the schema small
    # while still producing ids that exist in the registry tables.
    emit_schemas(meter_sample, dma_ids, elec_sample)

    print("\nnext: ./scripts/register-connectors.sh")
    return 0


if __name__ == "__main__":
    sys.exit(main())
