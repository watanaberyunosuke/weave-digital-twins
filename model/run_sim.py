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

from demands import DEMAND_WINDOW, METER_FLOWS_SQL, apply_demands, junction_demands
from residuals import (ALL_DMAS, BASELINE_DAYS, MIN_SAMPLES, RELEARN_HOURS, RESET_TABLE_SQL,
                       RESIDUALS_SQL, Z_LEAK, Z_WATCH, classify)
from modelled_network import MAX_PIPES, MAX_SNAP_M, select_modelled
from subgraph import element_name

DSN = os.environ.get("TWIN_DSN", "postgresql://weave:weave@postgres:5432/weave_twin")
INP_PATH = os.environ.get("INP_PATH", "/app/out/network.inp")
INTERVAL_S = int(os.environ.get("SIM_INTERVAL_S", "900"))
# A reservoir's head is the ground level at its node plus this depth of
# water. A fixed head only worked while every node sat at the same default
# elevation; with a DEM, anything above it came out at negative pressure.
RESERVOIR_DEPTH_M = float(os.environ.get("SIM_RESERVOIR_DEPTH_M", "10"))
DEFAULT_ELEVATION_M = 50.0


# --------------------------------------------------------------- build model
def build_inp() -> str:
    """Convert the PostGIS asset graph into an EPANET input file."""
    wn = wntr.network.WaterNetworkModel()

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM asset.pipe WHERE start_node IS NOT NULL")
        if cur.fetchone()[0] == 0:
            sys.exit("! topology not built — run SELECT asset.build_topology();")

        # Reservoirs and the capped walk out from them come from
        # modelled_network.py, which the seeder also uses, so synthetic DMAs
        # sit where this model can score them.
        m = select_modelled(cur)
        reservoirs, too_far, sel = m.reservoirs, m.too_far, m.selection
        if too_far:
            print(f"  {len(too_far)} reservoirs left out: further than "
                  f"SIM_MAX_SNAP_M={MAX_SNAP_M:g} m from any node "
                  f"(furthest {max(r[5] for r in too_far):.0f} m)")
        if not reservoirs:
            sys.exit("! no supply: load a service_reservoir or storage_dam layer "
                     "within SIM_MAX_SNAP_M of the network so the model has a "
                     "fixed head")
        print(f"  selected {len(sel.pipes)} of {m.pipe_count} pipes from "
              f"{len(reservoirs)} reservoirs (SIM_MAX_PIPES={MAX_PIPES}): "
              f"{sel.capped_pipes} over the cap, {sel.unsupplied_pipes} in "
              f"components no reservoir feeds, {sel.skipped_pipes} zero-length")
        if not sel.pipes:
            sys.exit("! no supplied network: no reservoir snaps to a node with a "
                     "connected pipe — check facility locations and topology")

        # Junctions only for nodes the selected pipes touch.
        cur.execute("""
            SELECT node_id, ST_Y(geom), ST_X(geom), COALESCE(elevation_m, %s)
            FROM asset.node
            WHERE node_id = ANY(%s)
            ORDER BY node_id
        """, (DEFAULT_ELEVATION_M, sorted(sel.nodes)))
        for node_id, lat, lon, elev in cur.fetchall():
            wn.add_junction(f"N{node_id}", base_demand=0.0,
                            elevation=float(elev), coordinates=(lon, lat))

        # Element ids carry the source asset id (element_name), so results
        # join back to asset.facility and asset.pipe across builds.
        unattached = []
        for fid, lat, lon, near_node, ground, _ in reservoirs:
            if near_node not in sel.nodes:
                unattached.append(fid)
                continue
            rid = element_name("R", fid)
            head = float(ground if ground is not None else DEFAULT_ELEVATION_M)
            wn.add_reservoir(rid, base_head=head + RESERVOIR_DEPTH_M,
                             coordinates=(lon, lat))
            wn.add_pipe(element_name("L", fid), rid, f"N{near_node}",
                        length=10.0, diameter=0.6, roughness=100)
        if unattached:
            print(f"  {len(unattached)} reservoirs left out: their node has no "
                  f"selected pipe (no connected pipe, or beyond the cap)")

        failed = 0
        for pipe in sel.pipes:
            try:
                wn.add_pipe(element_name("P", pipe.pipe_id),
                            f"N{pipe.start_node}", f"N{pipe.end_node}",
                            length=pipe.length_m, diameter=pipe.diameter_m,
                            roughness=100)
            except Exception:
                failed += 1
        if failed:
            print(f"  {failed} selected pipes failed to add")

        # The walk guarantees supply, but a pipe that failed to add can strand
        # what lay beyond it, and EPANET rejects a junction with no link or a
        # fragment with no fixed head. Keep only what a reservoir still feeds.
        graph = wn.to_graph().to_undirected()
        reservoir_names = set(wn.reservoir_name_list)
        fed = set()
        for component in nx.connected_components(graph):
            if reservoir_names & component:
                fed |= component
        for link_name in list(wn.link_name_list):
            if wn.get_link(link_name).start_node_name not in fed:
                wn.remove_link(link_name)
        dropped = [n for n in wn.junction_name_list if n not in fed]
        for name in dropped:
            wn.remove_node(name)
        if dropped:
            print(f"  dropped {len(dropped)} junctions stranded by failed pipes")

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
    # The .inp holds topology only; demands come from the meters each solve.
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(METER_FLOWS_SQL, {"window": DEMAND_WINDOW})
        demands = junction_demands(cur.fetchall())
    unmodelled = apply_demands(wn, demands)
    print(f"  demands: {demands.meters} meters, {demands.silent} with no reading"
          f" in the last {DEMAND_WINDOW}; {sum(demands.by_node.values()):.6f} m³/s,"
          f" {unmodelled:.6f} of it at nodes outside the model")
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
    """Score each DMA's pressure residual against its own baseline."""
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(RESET_TABLE_SQL)
        cur.execute(RESIDUALS_SQL, {"run_id": run_id, "baseline_s": BASELINE_DAYS * 86400,
                                    "relearn_s": RELEARN_HOURS * 3600, "all_dmas": ALL_DMAS})
        rows = []
        for dma_id, modelled, observed, n, mean, sd in cur.fetchall():
            score = classify(modelled - observed, n, mean, sd)
            rows.append((dma_id, modelled, observed, score))

        # now() is fixed for the transaction, so every row of this run shares
        # one timestamp and a concurrent run's rows can't be mistaken for it.
        cur.executemany(
            """INSERT INTO twin.sim_residual
                 (ts, dma_id, element_id, variable, modelled, observed,
                  residual, z_score, flagged)
               VALUES (now(), %s, %s, 'pressure', %s, %s, %s, %s, %s)""",
            [(dma, dma, m, o, s.residual, s.z_score, s.flagged)
             for dma, m, o, s in rows])
        cur.executemany(
            """INSERT INTO twin.dma_latest (dma_id, ts, min_pressure_m, status)
               VALUES (%s, now(), %s, %s)
               ON CONFLICT (dma_id) DO UPDATE SET
                 ts = EXCLUDED.ts,
                 min_pressure_m = EXCLUDED.min_pressure_m,
                 status = EXCLUDED.status""",
            [(dma, o, s.status) for dma, _m, o, s in rows])
        conn.commit()

    counts = {}
    for *_, score in rows:
        counts[score.status] = counts.get(score.status, 0) + 1
    print(f"  {len(rows)} DMA residuals: "
          + ", ".join(f"{k} {v}" for k, v in sorted(counts.items()))
          + f" (baseline {BASELINE_DAYS:g} days, at least {MIN_SAMPLES} residuals,"
            f" watch |z| >= {Z_WATCH:g}, flag |z| >= {Z_LEAK:g},"
            f" flagged rows rejoin after {RELEARN_HOURS:g} h)")
    return len(rows)


def reset_baseline(dmas: list[str]) -> int:
    """Start the named DMAs' baselines afresh, or every DMA's when none are named.

    For after a genuine lasting change, such as a model recalibration, when the
    old residuals no longer describe normal. Nothing is deleted: residuals
    before the reset stay in twin.sim_residual and simply stop counting, and
    the DMA reads UNKNOWN until it has the minimum number of new ones.
    """
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(RESET_TABLE_SQL)
        if dmas:
            cur.execute("SELECT dma_id FROM asset.dma WHERE dma_id = ANY(%s)", (dmas,))
            unknown = sorted(set(dmas) - {row[0] for row in cur.fetchall()})
            if unknown:
                sys.exit(f"! no such DMA: {', '.join(unknown)} — nothing reset")
        targets = dmas or [ALL_DMAS]
        cur.executemany(
            """INSERT INTO twin.residual_baseline_reset (dma_id, reset_at)
               VALUES (%s, now())
               ON CONFLICT (dma_id) DO UPDATE SET reset_at = EXCLUDED.reset_at""",
            [(d,) for d in targets])
        conn.commit()
    print(f"baseline reset for {', '.join(dmas) if dmas else 'every DMA'}; "
          f"status is UNKNOWN until {MIN_SAMPLES} new residuals exist")
    return len(targets)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--reset-baseline", action="store_true",
                    help="start residual baselines afresh (all DMAs unless --dma is given)")
    ap.add_argument("--dma", action="append", default=[],
                    help="with --reset-baseline: a DMA id; repeat for several")
    args = ap.parse_args()

    if args.dma and not args.reset_baseline:
        ap.error("--dma only applies with --reset-baseline")
    if args.reset_baseline:
        reset_baseline(args.dma)
        return 0

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

    ap.error("pass --build, --once, --loop or --reset-baseline")
    return 1


if __name__ == "__main__":
    sys.exit(main())
