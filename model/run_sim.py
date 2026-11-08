#!/usr/bin/env python3
"""Build an EPANET model from the real pipe network and simulate it.

    python model/run_sim.py --build        # PostGIS -> EPANET .inp
    python model/run_sim.py --once         # one hydraulic solve + residuals
    python model/run_sim.py --loop         # every 15 minutes, replaying missed solves in between
    python model/run_sim.py --replay       # replay solves missed while the simulator was down

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
from gaps import BRIDGE_LARGE_M, BRIDGE_SMALL_M, TJUNCTION_M, Link
from modelled_network import MAX_PIPES, MAX_SNAP_M, excluded_pipes, select_modelled
from replay import ENABLED as replay_enabled
from replay import LOG_SQL
from replay import MAX_AGE_DAYS as REPLAY_MAX_AGE_DAYS
from replay import METHOD as REPLAY_METHOD
from replay import READY_SQL as REPLAY_READY_SQL
from replay import SOLVE_TIMES_SQL
from replay import STREAM as REPLAY_STREAM
from replay import replay_times
from residuals import (
    ALL_DMAS,
    BASELINE_DAYS,
    MIN_SAMPLES,
    RELEARN_HOURS,
    RESET_TABLE_SQL,
    RESIDUALS_SQL,
    Z_LEAK,
    Z_WATCH,
    classify,
)
from subgraph import element_name
from zones import HEAD_MARGIN_M, MAX_PRESSURE_M, MIN_PRESSURE_M, plan_zones

DSN = os.environ.get("TWIN_DSN", "postgresql://weave:weave@postgres:5432/weave_twin")
INP_PATH = os.environ.get("INP_PATH", "/app/out/network.inp")
INTERVAL_S = int(os.environ.get("SIM_INTERVAL_S", "900"))
# Only the latest run is read (residuals.py joins on run_id), so older results
# are pruned each solve. Unpruned, a whole-network model writes about 240,000
# rows a solve, several GB a day, and the table never shrank.
RESULT_RETENTION_HOURS = float(os.environ.get("SIM_RESULT_RETENTION_HOURS", "24"))
# Volumes initialised before 03_telemetry.sql indexed ts need it for the prune;
# the primary key leads with run_id, so it can't serve a ts range.
RESULT_TS_INDEX_SQL = "CREATE INDEX IF NOT EXISTS sim_result_ts_idx ON twin.sim_result (ts)"
# A reservoir's head is the ground level at its node plus this depth of
# water. A fixed head only worked while every node sat at the same default
# elevation; with a DEM, anything above it came out at negative pressure.
RESERVOIR_DEPTH_M = float(os.environ.get("SIM_RESERVOIR_DEPTH_M", "10"))
DEFAULT_ELEVATION_M = 50.0
# "zoned" draws pressure zones from elevation (model/zones.py): band grade
# lines, PRVs between bands and boosters where no source reaches. "ground" is
# the earlier rule, each reservoir at its ground level plus
# SIM_RESERVOIR_DEPTH_M with no zones, kept to compare against.
HEAD_MODE = os.environ.get("SIM_HEAD_MODE", "zoned")
# Every split, gap, bridge and trunk main the build makes is written here and
# to asset.model_link, each with the reason it was needed.
LINKS_PATH = os.path.join(os.path.dirname(INP_PATH), "model_links.csv")
TRUNK_DIAMETER_M = 0.6
MODEL_LINK_SQL = """
    CREATE TABLE IF NOT EXISTS asset.model_link (
        element_id   TEXT PRIMARY KEY,      -- id in network.inp
        kind         TEXT NOT NULL,         -- split | gap | trunk | bridge
        synthetic    BOOLEAN NOT NULL,      -- false only for splits of real pipes
        pipe_id      TEXT,                  -- the real pipe a split cuts
        facility_id  TEXT,                  -- the reservoir a trunk main serves
        from_node    BIGINT,
        to_node      BIGINT,
        length_m     DOUBLE PRECISION,
        reason       TEXT NOT NULL,
        geom         GEOMETRY(Geometry, 4326),
        built_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""


def element_for(pipe) -> str:
    """P<asset id>[~n] for real pipes and their split pieces, S<id> for
    synthetic links, so a result never passes for a real asset."""
    return element_name("S" if pipe.kind in ("gap", "bridge") else "P", pipe.pipe_id)


def record_links(cur, links: list[Link], trunks: list[tuple]) -> None:
    """Write every change the build made, and why, to a CSV and asset.model_link."""
    import csv

    rows = []
    for link in links:
        if link.kind == "split":
            rows.append(
                (
                    f"split@{link.element_id}@N{link.from_node}",
                    "split",
                    False,
                    link.element_id,
                    None,
                    link.from_node,
                    None,
                    link.length_m,
                    link.reason,
                )
            )
        else:
            rows.append(
                (
                    element_name("S", link.element_id),
                    link.kind,
                    True,
                    None,
                    None,
                    link.from_node,
                    link.to_node,
                    link.length_m,
                    link.reason,
                )
            )
    for fid, node, dist, reason in trunks:
        rows.append((element_name("L", fid), "trunk", True, None, fid, None, node, dist, reason))

    os.makedirs(os.path.dirname(LINKS_PATH), exist_ok=True)
    with open(LINKS_PATH, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            ["element_id", "kind", "synthetic", "pipe_id", "facility_id", "from_node", "to_node", "length_m", "reason"]
        )
        w.writerows(rows)

    cur.execute(MODEL_LINK_SQL)
    cur.execute("TRUNCATE asset.model_link")
    cur.executemany(
        """INSERT INTO asset.model_link (element_id, kind, synthetic, pipe_id, facility_id,
                                         from_node, to_node, length_m, reason)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        rows,
    )
    # Geometry for the map and GIS clients: a split is the point where the
    # pipe was cut, a gap or bridge the straight line it assumes, a trunk main
    # the line from the reservoir to its node.
    cur.execute("""
        UPDATE asset.model_link l SET geom = CASE
            WHEN l.kind = 'split' THEN
                (SELECT geom FROM asset.node WHERE node_id = l.from_node)
            WHEN l.kind = 'trunk' THEN ST_MakeLine(
                (SELECT geom FROM asset.facility WHERE facility_id = l.facility_id),
                (SELECT geom FROM asset.node WHERE node_id = l.to_node))
            ELSE ST_MakeLine(
                (SELECT geom FROM asset.node WHERE node_id = l.from_node),
                (SELECT geom FROM asset.node WHERE node_id = l.to_node))
        END
    """)


EXCLUSION_SQL = """
    CREATE TABLE IF NOT EXISTS asset.model_exclusion (
        pipe_id      TEXT PRIMARY KEY,      -- asset.pipe the model leaves out
        reason_code  TEXT NOT NULL,         -- zero_length | unsupplied | over_cap | not_in_graph
        reason       TEXT NOT NULL,
        built_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""


def record_exclusions(cur, excluded: dict[str, tuple[str, str]]) -> dict[str, int]:
    """Write every real pipe the model leaves out, and why, to asset.model_exclusion;
    returns the count per reason."""
    cur.execute(EXCLUSION_SQL)
    cur.execute("TRUNCATE asset.model_exclusion")
    with cur.copy("COPY asset.model_exclusion (pipe_id, reason_code, reason) FROM STDIN") as copy:
        for pipe_id, (code, reason) in excluded.items():
            copy.write_row((pipe_id, code, reason))
    # Pipes the topology build gave no nodes never reach the selection at all.
    cur.execute("""
        INSERT INTO asset.model_exclusion (pipe_id, reason_code, reason)
        SELECT pipe_id, 'not_in_graph',
               'not in the topology: asset.build_topology() gave it no nodes'
        FROM asset.pipe WHERE start_node IS NULL OR end_node IS NULL
        ON CONFLICT (pipe_id) DO NOTHING
    """)
    cur.execute("SELECT reason_code, count(*) FROM asset.model_exclusion GROUP BY 1 ORDER BY 2 DESC")
    return dict(cur.fetchall())


def report_links(links: list[Link], trunks: list[tuple], unjoined: tuple) -> None:
    """Counts by kind in the build log, with one example reason each."""
    by_kind: dict[str, list] = {}
    for link in links:
        by_kind.setdefault(link.kind, []).append(link)
    names = {
        "split": "real pipes split at junctions the data never split",
        "gap": "synthetic gap links",
        "bridge": "synthetic bridge mains",
    }
    for kind in ("split", "gap", "bridge"):
        found = by_kind.get(kind, [])
        if found:
            total = sum(l.length_m for l in found) if kind != "split" else None
            extra = f", {total / 1000:.1f} km in all" if total else ""
            print(f"  {len(found)} {names[kind]}{extra}; e.g. {found[0].reason}")
    if trunks:
        print(
            f"  {len(trunks)} synthetic reservoir trunk mains, "
            f"{sum(t[2] for t in trunks) / 1000:.1f} km in all; e.g. {trunks[0][3]}"
        )
    if unjoined[0]:
        print(
            f"  {unjoined[0]} pieces ({unjoined[1]} pipes) still without supply: no real "
            f"connection and nothing within SIM_BRIDGE_LARGE_M={BRIDGE_LARGE_M:g} m"
        )
    print(f"  every change and its reason: {LINKS_PATH} and asset.model_link")


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
        for note in m.snap_notes:
            print(f"  {note}")
        if too_far:
            limits = ", ".join(f"{k} {v:g} m" for k, v in m.limits.items())
            print(
                f"  {len(too_far)} reservoirs left out: further from a usable node than "
                f"their type's snap limit ({limits}; SIM_MAX_SNAP_M_<TYPE> overrides "
                f"SIM_MAX_SNAP_M) (furthest {max(r[5] for r in too_far):.0f} m)"
            )
        if not reservoirs:
            sys.exit(
                "! no supply: load a service_reservoir or storage_dam layer "
                "within SIM_MAX_SNAP_M of the network so the model has a "
                "fixed head"
            )
        real = len({p.asset_id for p in sel.pipes if p.asset_id is not None})
        synthetic = sum(1 for p in sel.pipes if p.asset_id is None)
        print(
            f"  selected {real} of {m.pipe_count} real pipes, plus {synthetic} synthetic "
            f"links, from {len(reservoirs)} reservoirs (SIM_MAX_PIPES={MAX_PIPES}): "
            f"{sel.capped_pipes} over the cap, {sel.unsupplied_pipes} in "
            f"components no reservoir feeds, {sel.skipped_pipes} zero-length"
        )
        if not sel.pipes:
            sys.exit(
                "! no supplied network: no reservoir snaps to a node with a "
                "connected pipe — check facility locations and topology"
            )
        print(
            f"  joining pieces: real splits within SIM_TJUNCTION_M={TJUNCTION_M:g} m first, "
            f"then synthetic gaps up to SIM_BRIDGE_SMALL_M={BRIDGE_SMALL_M:g} m and bridges "
            f"up to SIM_BRIDGE_LARGE_M={BRIDGE_LARGE_M:g} m"
        )

        # Junctions only for nodes the selected pipes touch.
        cur.execute(
            """
            SELECT node_id, ST_Y(geom), ST_X(geom), COALESCE(elevation_m, %s)
            FROM asset.node
            WHERE node_id = ANY(%s)
            ORDER BY node_id
        """,
            (DEFAULT_ELEVATION_M, sorted(sel.nodes)),
        )
        elevation, coords = {}, {}
        for node_id, lat, lon, elev in cur.fetchall():
            elevation[node_id] = float(elev)
            coords[node_id] = (lon, lat)
            wn.add_junction(f"N{node_id}", base_demand=0.0, elevation=float(elev), coordinates=(lon, lat))

        unattached = [r[0] for r in reservoirs if r[3] not in sel.nodes]
        attached = [r for r in reservoirs if r[3] in sel.nodes]
        if unattached:
            print(
                f"  {len(unattached)} reservoirs left out: their node has no "
                f"selected pipe (no connected pipe, or beyond the cap)"
            )

        if HEAD_MODE == "zoned":
            plan = plan_zones(sel.pipes, elevation, {r[0]: r[3] for r in attached})
            heads = plan.reservoir_head
            pipes, prvs, boosters = plan.pipes, plan.prvs, plan.boosters
            low, high = plan.static_pressure_range(elevation)
            print(
                f"  zones: {len(plan.band_head)} elevation bands of "
                f"{plan.band_width_m:g} m (service pressure {MIN_PRESSURE_M:g}-"
                f"{MAX_PRESSURE_M:g} m, margin {HEAD_MARGIN_M:g} m), "
                f"{len(prvs)} PRVs, {len(plan.closed)} boundary pipes closed, "
                f"{len(boosters)} boosters; "
                f"static pressure {low:.0f}-{high:.0f} m"
            )
        elif HEAD_MODE == "ground":
            heads = {
                fid: float(ground if ground is not None else DEFAULT_ELEVATION_M) + RESERVOIR_DEPTH_M
                for fid, _lat, _lon, _node, ground, _d in attached
            }
            pipes, prvs, boosters = sel.pipes, [], []
        else:
            sys.exit(f"! SIM_HEAD_MODE={HEAD_MODE!r}: use zoned or ground")

        # Element ids carry the source asset id (element_name), so results
        # join back to asset.facility and asset.pipe across builds.
        # The main from a reservoir to the network isn't in the published data,
        # so it is assumed: as long as the snap distance, at trunk size.
        trunks = []
        for fid, lat, lon, near_node, _ground, dist in attached:
            rid = element_name("R", fid)
            length = max(float(dist), 10.0)
            wn.add_reservoir(rid, base_head=heads[fid], coordinates=(lon, lat))
            wn.add_pipe(
                element_name("L", fid), rid, f"N{near_node}", length=length, diameter=TRUNK_DIAMETER_M, roughness=100
            )
            trunks.append(
                (
                    fid,
                    near_node,
                    length,
                    f"synthetic: reservoir {fid} is {float(dist):.0f} m from its nearest "
                    f"node N{near_node}; the trunk main between them is not in the "
                    f"published data, so a {TRUNK_DIAMETER_M * 1000:.0f} mm main of that "
                    f"length is assumed",
                )
            )

        # A booster stands in for a pump station or elevated tank the public
        # data doesn't show; B<node_id> keeps it apart from real reservoirs.
        for node_id, head in boosters:
            wn.add_reservoir(f"B{node_id}", base_head=head, coordinates=coords[node_id])
            wn.add_pipe(f"BL{node_id}", f"B{node_id}", f"N{node_id}", length=10.0, diameter=0.6, roughness=100)

        failed = 0
        for pipe in pipes:
            try:
                wn.add_pipe(
                    element_for(pipe),
                    f"N{pipe.start_node}",
                    f"N{pipe.end_node}",
                    length=pipe.length_m,
                    diameter=pipe.diameter_m,
                    roughness=100,
                )
            except Exception:
                failed += 1
        # Crossings between bands that didn't get a PRV stay in the model,
        # shut, as the boundary valves between zones are.
        closed = plan.closed if HEAD_MODE == "zoned" else []
        for pipe in closed:
            try:
                wn.add_pipe(
                    element_for(pipe),
                    f"N{pipe.start_node}",
                    f"N{pipe.end_node}",
                    length=pipe.length_m,
                    diameter=pipe.diameter_m,
                    roughness=100,
                    initial_status="CLOSED",
                )
            except Exception:
                failed += 1
        # A PRV replaces the pipe it sits on; V<pipe_id> still joins back to
        # asset.pipe, and the valve points from the higher grade line down.
        for prv in prvs:
            try:
                wn.add_valve(
                    element_name("V", prv.pipe.pipe_id),
                    f"N{prv.upstream}",
                    f"N{prv.downstream}",
                    diameter=prv.pipe.diameter_m,
                    valve_type="PRV",
                    initial_setting=prv.setting_m,
                )
            except Exception:
                failed += 1
        if failed:
            print(f"  {failed} selected pipes or valves failed to add")

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

        modelled = {p.pipe_id for p in sel.pipes}
        used = [l for l in m.links if l.kind == "split" or l.element_id in modelled]
        report_links(used, trunks, m.unjoined)
        record_links(cur, used, trunks)
        counts = record_exclusions(cur, excluded_pipes(m))
        conn.commit()
        if counts:
            print(
                "  real pipes left out, by reason (asset.model_exclusion; the console map "
                "greys them, click one for why): " + ", ".join(f"{code} {n}" for code, n in counts.items())
            )

    os.makedirs(os.path.dirname(INP_PATH), exist_ok=True)
    wn.options.time.duration = 0  # snapshot solve
    wntr.network.write_inpfile(wn, INP_PATH)
    print(
        f"wrote {INP_PATH}: {len(wn.junction_name_list)} junctions, "
        f"{len(wn.pipe_name_list)} pipes, {len(wn.reservoir_name_list)} reservoirs"
    )
    return INP_PATH


# ------------------------------------------------------------------ simulate
def solve(as_of: datetime) -> tuple[str, list[tuple]]:
    """One EPANET solve with the meters' demand in the hour to as_of.

    Returns the run id and its rows for twin.sim_result, all dated as_of.
    """
    if not os.path.exists(INP_PATH):
        build_inp()

    wn = wntr.network.WaterNetworkModel(INP_PATH)
    # The .inp holds topology only; demands come from the meters each solve.
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(METER_FLOWS_SQL, {"window": DEMAND_WINDOW, "as_of": as_of})
        demands = junction_demands(cur.fetchall())
    unmodelled = apply_demands(wn, demands)
    print(
        f"  demands: {demands.meters} meters, {demands.silent} with no reading"
        f" in the {DEMAND_WINDOW} to {as_of:%Y-%m-%d %H:%M}Z; {sum(demands.by_node.values()):.6f} m³/s,"
        f" {unmodelled:.6f} of it at nodes outside the model"
    )
    sim = wntr.sim.EpanetSimulator(wn)
    # EPANET's scratch files default to temp.* in the working directory, so
    # the --loop container and a --once run from the console would overwrite
    # each other's mid-solve.
    results = sim.run_sim(file_prefix=f"/tmp/weave-sim-{os.getpid()}")

    run_id = uuid.uuid4().hex[:12]
    rows = []
    pressure = results.node["pressure"]
    for node_name in pressure.columns:
        rows.append((run_id, as_of, "node", node_name, "pressure", float(pressure[node_name].iloc[-1])))
    flow = results.link["flowrate"]
    for link_name in flow.columns:
        rows.append((run_id, as_of, "link", link_name, "flow", float(flow[link_name].iloc[-1])))
    return run_id, rows


def insert_results(cur, rows: list[tuple]) -> None:
    cur.executemany(
        """INSERT INTO twin.sim_result
             (run_id, ts, element_type, element_id, variable, value)
           VALUES (%s, %s, %s, %s, %s, %s)
           ON CONFLICT DO NOTHING""",
        rows,
    )


def simulate() -> tuple[str, datetime]:
    started = time.monotonic()
    # One timestamp for the solve, its results and its residuals, taken from
    # the database so it is on the same clock as the readings it averages.
    with psycopg.connect(DSN) as conn:
        as_of = conn.execute("SELECT now()").fetchone()[0]
    run_id, rows = solve(as_of)
    solve_s = time.monotonic() - started
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        insert_results(cur, rows)
        cur.execute(RESULT_TS_INDEX_SQL)
        cur.execute(
            "DELETE FROM twin.sim_result WHERE ts < now() - make_interval(secs => %s)", (RESULT_RETENTION_HOURS * 3600,)
        )
        pruned = cur.rowcount
        conn.commit()
    # The solve time is logged because SIM_MAX_PIPES trades coverage for it:
    # a solve longer than SIM_INTERVAL_S stretches the loop's cadence.
    print(
        f"run {run_id}: solved in {solve_s:.0f} s, wrote {len(rows)} simulated values,"
        f" pruned {pruned} older than {RESULT_RETENTION_HOURS:g} h"
    )
    return run_id, as_of


# ------------------------------------------------------------- residual check
def score_residuals(cur, run_id: str, as_of: datetime) -> list[tuple]:
    cur.execute(RESET_TABLE_SQL)
    cur.execute(
        RESIDUALS_SQL,
        {
            "run_id": run_id,
            "as_of": as_of,
            "baseline_s": BASELINE_DAYS * 86400,
            "relearn_s": RELEARN_HOURS * 3600,
            "all_dmas": ALL_DMAS,
        },
    )
    rows = []
    for dma_id, modelled, observed, n, mean, sd in cur.fetchall():
        rows.append((dma_id, modelled, observed, classify(modelled - observed, n, mean, sd)))
    # Every row of a solve shares its as_of, so a concurrent run's rows can't
    # be mistaken for it.
    cur.executemany(
        """INSERT INTO twin.sim_residual
             (ts, dma_id, element_id, variable, modelled, observed,
              residual, z_score, flagged)
           VALUES (%s, %s, %s, 'pressure', %s, %s, %s, %s, %s)""",
        [(as_of, dma, dma, m, o, s.residual, s.z_score, s.flagged) for dma, m, o, s in rows],
    )
    return rows


def compute_residuals(run_id: str, as_of: datetime) -> int:
    """Score each DMA's pressure residual against its own baseline."""
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        rows = score_residuals(cur, run_id, as_of)
        cur.executemany(
            """INSERT INTO twin.dma_latest (dma_id, ts, min_pressure_m, status)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (dma_id) DO UPDATE SET
                 ts = EXCLUDED.ts,
                 min_pressure_m = EXCLUDED.min_pressure_m,
                 status = EXCLUDED.status""",
            [(dma, as_of, o, s.status) for dma, _m, o, s in rows],
        )
        conn.commit()

    counts = {}
    for *_, score in rows:
        counts[score.status] = counts.get(score.status, 0) + 1
    print(
        f"  {len(rows)} DMA residuals: "
        + ", ".join(f"{k} {v}" for k, v in sorted(counts.items()))
        + f" (baseline {BASELINE_DAYS:g} days, at least {MIN_SAMPLES} residuals,"
        f" watch |z| >= {Z_WATCH:g}, flag |z| >= {Z_LEAK:g},"
        f" flagged rows rejoin after {RELEARN_HOURS:g} h)"
    )
    return len(rows)


# --------------------------------------------------------------------- replay
def replay(deadline: float | None = None) -> int:
    """Replay the solves missed while the simulator was down, oldest first.

    Stops at the first time whose telemetry ingest-backfill has not filled yet,
    or when time.monotonic() passes deadline, and carries on next call. A
    replay writes residuals only: twin.dma_latest is the present, and its
    sim_result rows are deleted in the same transaction, since only the latest
    live run is ever read.
    """
    if not replay_enabled:
        return 0
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        # ingest-backfill creates the log; until it has, nothing is filled anyway.
        cur.execute("SELECT to_regclass('twin.backfill_log') IS NOT NULL")
        if not cur.fetchone()[0]:
            return 0
        cur.execute(SOLVE_TIMES_SQL, {"max_age_s": REPLAY_MAX_AGE_DAYS * 86400})
        solves = [r[0] for r in cur.fetchall()]
    # Before the current network.inp was built, the live solves used another
    # network and their baseline describes it; a replay on today's network
    # scored against that baseline flags DMAs for the rebuild, not for water.
    built = datetime.fromtimestamp(os.path.getmtime(INP_PATH), timezone.utc) if os.path.exists(INP_PATH) else None
    times = [t for t in replay_times(solves, INTERVAL_S) if built is None or t > built]
    if not times:
        return 0

    step = timedelta(seconds=INTERVAL_S)
    done, run_start, run_end, run_rows = 0, None, None, 0
    last_solve_s = 0.0

    def log_run() -> None:
        if run_start is None:
            return
        with psycopg.connect(DSN) as conn:
            conn.execute(LOG_SQL, (REPLAY_STREAM, run_start, run_end, run_rows, REPLAY_METHOD))
        print(f"replay: filled {run_start:%Y-%m-%d %H:%M} to {run_end:%Y-%m-%d %H:%M}Z with {run_rows} residuals")

    for as_of in times:
        if deadline is not None and time.monotonic() + last_solve_s * 1.5 > deadline:
            break
        with psycopg.connect(DSN) as conn:
            ready = conn.execute(REPLAY_READY_SQL, {"as_of": as_of, "window": DEMAND_WINDOW}).fetchone()[0]
        if not ready:
            print(f"replay: waiting for backfilled telemetry at {as_of:%Y-%m-%d %H:%M}Z")
            break
        started = time.monotonic()
        try:
            run_id, rows = solve(as_of)
            with psycopg.connect(DSN) as conn, conn.cursor() as cur:
                # Residuals read junction pressures only, so flows aren't written.
                insert_results(cur, [r for r in rows if r[4] == "pressure"])
                scored = score_residuals(cur, run_id, as_of)
                cur.execute("DELETE FROM twin.sim_result WHERE run_id = %s", (run_id,))
                conn.commit()
        except Exception as exc:
            print(f"! replay at {as_of:%Y-%m-%d %H:%M}Z failed: {exc}")
            break
        last_solve_s = time.monotonic() - started
        # A new contiguous run starts when the previous replay was not one step back.
        if run_end is None or as_of - run_end > step * 1.5:
            log_run()
            run_start, run_rows = as_of, 0
        run_end = as_of
        run_rows += len(scored)
        done += 1
    log_run()
    remaining = len(times) - done
    if remaining:
        print(f"replay: {remaining} solves still to replay")
    return done


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
            [(d,) for d in targets],
        )
        conn.commit()
    print(
        f"baseline reset for {', '.join(dmas) if dmas else 'every DMA'}; "
        f"status is UNKNOWN until {MIN_SAMPLES} new residuals exist"
    )
    return len(targets)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--replay", action="store_true", help="replay solves missed in closed gaps, then exit")
    ap.add_argument(
        "--reset-baseline", action="store_true", help="start residual baselines afresh (all DMAs unless --dma is given)"
    )
    ap.add_argument("--dma", action="append", default=[], help="with --reset-baseline: a DMA id; repeat for several")
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
        compute_residuals(*simulate())
        return 0

    if args.replay:
        replay()
        return 0

    if args.loop:
        while True:
            # Sleep the rest of the interval, not a whole one, so a larger
            # model keeps the 15-minute cadence the baseline counts assume.
            started = time.monotonic()
            try:
                compute_residuals(*simulate())
            except Exception as exc:
                print(f"! simulation failed: {exc}")
            elapsed = time.monotonic() - started
            if elapsed > INTERVAL_S:
                print(
                    f"! solve took {elapsed:.0f} s, longer than SIM_INTERVAL_S={INTERVAL_S};"
                    " lower SIM_MAX_PIPES or raise the interval"
                )
            # Missed solves are replayed in the idle part of the interval, so
            # the live cadence holds while a long outage catches up.
            try:
                replay(deadline=started + INTERVAL_S)
            except Exception as exc:
                print(f"! replay failed: {exc}")
            elapsed = time.monotonic() - started
            time.sleep(max(0.0, INTERVAL_S - elapsed))

    ap.error("pass --build, --once, --loop, --replay or --reset-baseline")
    return 1


if __name__ == "__main__":
    sys.exit(main())
