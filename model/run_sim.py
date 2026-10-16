#!/usr/bin/env python3
"""Build an EPANET model from the real pipe network and simulate it.

    python model/run_sim.py --build        # PostGIS -> EPANET .inp
    python model/run_sim.py --once         # one hydraulic solve + residuals
    python model/run_sim.py --loop         # every 15 minutes

The model geometry is real (the loaded region's mains, reservoirs, pumps).
Demands are synthetic, derived from the generated meter readings. That split
is deliberate and is recorded on every row written back to Postgres.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import networkx as nx
import psycopg
import wntr

DSN = os.environ.get("TWIN_DSN", "postgresql://weave:weave@postgres:5432/weave_twin")
INP_PATH = os.environ.get("INP_PATH", "/app/out/network.inp")
INTERVAL_S = int(os.environ.get("SIM_INTERVAL_S", "900"))
MAX_PIPES = int(os.environ.get("SIM_MAX_PIPES", "5000"))


# --------------------------------------------------------------- build model
def build_inp() -> str:
    """Convert the PostGIS asset graph into an EPANET input file."""
    wn = wntr.network.WaterNetworkModel()

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM asset.pipe WHERE start_node IS NOT NULL")
        if cur.fetchone()[0] == 0:
            sys.exit("! topology not built — run SELECT asset.build_topology();")

        # Junctions from graph nodes, with a nominal elevation.
        cur.execute("""
            SELECT node_id, ST_Y(geom), ST_X(geom), COALESCE(elevation_m, 50)
            FROM asset.node
        """)
        for node_id, lat, lon, elev in cur.fetchall():
            wn.add_junction(f"N{node_id}", base_demand=0.0,
                            elevation=float(elev), coordinates=(lon, lat))

        # Reservoirs from real service reservoirs and storage dams. Each is
        # snapped to its nearest graph node so it connects to the network.
        cur.execute("""
            SELECT f.facility_id, f.name, ST_Y(f.geom), ST_X(f.geom),
                   (SELECT n.node_id FROM asset.node n
                     ORDER BY n.geom <-> f.geom LIMIT 1)
            FROM asset.facility f
            WHERE f.facility_type IN ('service_reservoir', 'storage_dam')
        """)
        for fid, name, lat, lon, near_node in cur.fetchall():
            rid = f"R{abs(hash(fid)) % 100000}"
            wn.add_reservoir(rid, base_head=80.0, coordinates=(lon, lat))
            if near_node:
                wn.add_pipe(f"L{rid}", rid, f"N{near_node}",
                            length=10.0, diameter=0.6, roughness=100)

        # Pipes as links.
        cur.execute("""
            SELECT pipe_id, start_node, end_node,
                   COALESCE(length_m, ST_Length(geom::geography)),
                   COALESCE(diameter_mm, 150) / 1000.0
            FROM asset.pipe
            WHERE start_node IS NOT NULL AND end_node IS NOT NULL
              AND start_node <> end_node
            LIMIT %s
        """, (MAX_PIPES,))
        added = 0
        for pipe_id, s, e, length, dia in cur.fetchall():
            try:
                wn.add_pipe(f"P{added}", f"N{s}", f"N{e}",
                            length=max(float(length or 1), 1.0),
                            diameter=max(float(dia or 0.15), 0.05),
                            roughness=100)
                added += 1
            except Exception:
                continue

        # Public GIS is fragmentary, and skipped pipes (zero-length after
        # snapping, over the cap, failed adds) strand more nodes. EPANET
        # rejects a junction with no link and cannot solve a fragment with
        # no fixed head, so only the parts a reservoir feeds are kept.
        graph = wn.to_graph().to_undirected()
        reservoirs = set(wn.reservoir_name_list)
        fed = set()
        for component in nx.connected_components(graph):
            if reservoirs & component:
                fed |= component
        for link_name in list(wn.link_name_list):
            if wn.get_link(link_name).start_node_name not in fed:
                wn.remove_link(link_name)
        dropped = [n for n in wn.junction_name_list if n not in fed]
        for name in dropped:
            wn.remove_node(name)
        if dropped:
            print(f"  dropped {len(dropped)} junctions in fragments no reservoir feeds")

        # Demands from synthetic meters, aggregated to the nearest node.
        cur.execute("""
            SELECT n.node_id, COALESCE(avg(m.flow_lpm), 0) / 60000.0
            FROM ref.water_meter wm
            JOIN LATERAL (SELECT node_id FROM asset.node
                          ORDER BY geom <-> wm.geom LIMIT 1) n ON true
            LEFT JOIN twin.meter_reading m
                   ON m.meter_id = wm.meter_id
                  AND m.ts > now() - interval '1 hour'
            GROUP BY n.node_id
        """)
        for node_id, demand_cms in cur.fetchall():
            name = f"N{node_id}"
            if name in wn.junction_name_list:
                wn.get_node(name).demand_timeseries_list[0].base_value = float(demand_cms)

    os.makedirs(os.path.dirname(INP_PATH), exist_ok=True)
    wn.options.time.duration = 0                       # snapshot solve
    wntr.network.write_inpfile(wn, INP_PATH)
    print(f"wrote {INP_PATH}: {len(wn.junction_name_list)} junctions, "
          f"{len(wn.pipe_name_list)} pipes, {len(wn.reservoir_name_list)} reservoirs")
    return INP_PATH


# ------------------------------------------------------------------ simulate
def simulate() -> str:
    if not os.path.exists(INP_PATH):
        build_inp()

    wn = wntr.network.WaterNetworkModel(INP_PATH)
    sim = wntr.sim.EpanetSimulator(wn)
    # EPANET's scratch files default to temp.* in the working directory, so
    # the --loop container and a --once run from the console would overwrite
    # each other's mid-solve.
    results = sim.run_sim(file_prefix=f"/tmp/weave-sim-{os.getpid()}")

    run_id = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc)
    rows = []

    pressure = results.node["pressure"]
    for node_name in pressure.columns:
        rows.append((run_id, now, "node", node_name, "pressure",
                     float(pressure[node_name].iloc[-1])))

    flow = results.link["flowrate"]
    for link_name in flow.columns:
        rows.append((run_id, now, "link", link_name, "flow",
                     float(flow[link_name].iloc[-1])))

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO twin.sim_result
                 (run_id, ts, element_type, element_id, variable, value)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""", rows)
        conn.commit()
    print(f"run {run_id}: wrote {len(rows)} simulated values")
    return run_id


# ------------------------------------------------------------- residual check
def compute_residuals(run_id: str) -> int:
    """Compare modelled pressure against observed logger readings per DMA."""
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("""
            WITH observed AS (
                SELECT pr.dma_id,
                       avg(pr.pressure_m) AS observed
                FROM twin.pressure_reading pr
                WHERE pr.ts > now() - interval '30 minutes'
                GROUP BY pr.dma_id
            ),
            modelled AS (
                SELECT d.dma_id, avg(sr.value) AS modelled
                FROM twin.sim_result sr
                -- Reservoirs (R...) have pressure results too but no graph
                -- node, so only junction ids are cast.
                JOIN asset.node n ON n.node_id = CASE WHEN sr.element_id ~ '^N[0-9]+$'
                                                      THEN substr(sr.element_id, 2)::bigint END
                JOIN asset.dma d ON ST_Contains(d.geom, n.geom)
                WHERE sr.run_id = %s AND sr.variable = 'pressure'
                GROUP BY d.dma_id
            )
            INSERT INTO twin.sim_residual
                (ts, dma_id, element_id, variable, modelled, observed, residual, z_score, flagged)
            SELECT now(), o.dma_id, o.dma_id, 'pressure',
                   m.modelled, o.observed,
                   m.modelled - o.observed,
                   (m.modelled - o.observed) / NULLIF(stddev_samp(m.modelled)
                        OVER (), 0),
                   abs(m.modelled - o.observed) > 5
            FROM observed o JOIN modelled m USING (dma_id)
        """, (run_id,))
        count = cur.rowcount

        cur.execute("""
            INSERT INTO twin.dma_latest
                (dma_id, ts, min_pressure_m, status)
            SELECT dma_id, now(), observed,
                   CASE WHEN flagged THEN 'SUSPECTED_LEAK'
                        WHEN abs(residual) > 2 THEN 'WATCH'
                        ELSE 'OK' END
            FROM twin.sim_residual
            -- now() is fixed for the transaction, so this is exactly the
            -- rows inserted above; a window would also catch a concurrent
            -- run's rows and upsert the same DMA twice.
            WHERE ts = now()
            ON CONFLICT (dma_id) DO UPDATE SET
                ts = EXCLUDED.ts,
                min_pressure_m = EXCLUDED.min_pressure_m,
                status = EXCLUDED.status
        """)
        conn.commit()
    print(f"  {count} DMA residuals")
    return count


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--loop", action="store_true")
    args = ap.parse_args()

    if args.build:
        build_inp()
        return 0

    if args.once:
        compute_residuals(simulate())
        return 0

    if args.loop:
        while True:
            try:
                compute_residuals(simulate())
            except Exception as exc:
                print(f"! simulation failed: {exc}")
            time.sleep(INTERVAL_S)

    ap.error("pass --build, --once or --loop")
    return 1


if __name__ == "__main__":
    sys.exit(main())
