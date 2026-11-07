"""Which reservoirs and pipes the hydraulic model keeps, read from PostGIS.

run_sim.py --build and scripts/seed_synthetic_network.py both call
select_modelled(), so synthetic DMAs and meters are placed only where the
model can score them. Seeded across the whole network, most DMAs had no
modelled junction, got no residual and never left UNKNOWN.

Needs psycopg but not the solver, so the gis-loader image can import it.

    select_modelled(cur) -> Modelled
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from gaps import BRIDGE_LARGE_M, BRIDGE_SMALL_M, TJUNCTION_M, Link, connect_pieces, split_at_tjunctions
from subgraph import Pipe, Selection, select_supplied_subgraph

# Both the simulator and the seeder read these, so compose passes them to both
# from .env; different values would seed DMAs the model does not reach.
# The cap rarely binds in practice: the walk only keeps pipes a reservoir
# feeds, and networks published without reservoirs have none (in the region
# loaded so far, about 9,500 of 156,180 pipes are supplied). 200,000 is there
# so a region with full supply data is modelled whole. Lower it on a small
# host: solves grow faster than linearly (a 160,000-pipe test grid took about
# 3.5 minutes on two cores, against 1 second for 10,000).
MAX_PIPES = int(os.environ.get("SIM_MAX_PIPES", "200000"))
# Reservoirs further than this from any node are left out. Snapping a
# catchment dam onto whatever main is nearest, kilometres away, would put a
# fixed head where there is none.
MAX_SNAP_M = float(os.environ.get("SIM_MAX_SNAP_M", "500"))
SUPPLY_TYPES = ("service_reservoir", "storage_dam")
# How many of a reservoir's nearest nodes are looked at for one with a usable
# pipe. The nearest node can carry only a pipe that collapsed to zero length
# when snapped, which supplies nothing.
SNAP_CANDIDATES = 20


def snap_limits(max_snap_m: float = MAX_SNAP_M) -> dict[str, float]:
    """Snap distance per supply type. SIM_MAX_SNAP_M_<TYPE> overrides
    SIM_MAX_SNAP_M for that type: a service reservoir feeds the distribution
    network and may sit some way off it, while a storage dam is usually
    catchment storage behind a treatment plant, so may warrant less."""
    limits = {}
    for kind in SUPPLY_TYPES:
        value = os.environ.get(f"SIM_MAX_SNAP_M_{kind.upper()}")
        limits[kind] = float(value) if value else max_snap_m
    return limits


RESERVOIR_NODES_SQL = """
    SELECT f.facility_id, ST_Y(f.geom), ST_X(f.geom), f.facility_type,
           near.node_id, near.elevation_m,
           ST_Distance(f.geom::geography, near.geom::geography), near.rank
    FROM asset.facility f
    CROSS JOIN LATERAL (
        SELECT n.node_id, n.elevation_m, n.geom,
               row_number() OVER (ORDER BY n.geom <-> f.geom, n.node_id) AS rank
        FROM (SELECT * FROM asset.node n
              ORDER BY n.geom <-> f.geom, n.node_id LIMIT %(k)s) n) near
    WHERE f.facility_type IN ('service_reservoir', 'storage_dam')
    ORDER BY f.facility_id, near.rank
"""


# Dead ends lying close to the side of a pipe that isn't theirs, where the
# source drew a junction but never split the pipe (gaps.py step 1). The box
# test in degrees is wide enough at any latitude below about 60 degrees; the
# exact distance in metres then decides.
TJUNCTION_SQL = """
    WITH ends AS (
        SELECT node_id FROM (
            SELECT start_node AS node_id FROM asset.pipe WHERE start_node IS NOT NULL
            UNION ALL
            SELECT end_node FROM asset.pipe WHERE end_node IS NOT NULL) x
        GROUP BY node_id HAVING count(*) = 1
    )
    SELECT n.node_id, side.pipe_id, ST_LineLocatePoint(side.geom, n.geom),
           ST_Distance(side.geom::geography, n.geom::geography)
    FROM asset.node n
    JOIN ends USING (node_id)
    CROSS JOIN LATERAL (
        SELECT p.pipe_id, p.geom FROM asset.pipe p
        WHERE p.start_node IS NOT NULL
          AND n.node_id NOT IN (p.start_node, p.end_node)
          AND ST_DWithin(p.geom, n.geom, %(box_deg)s)
        ORDER BY p.geom <-> n.geom, p.pipe_id
        LIMIT 1) side
    WHERE ST_Distance(side.geom::geography, n.geom::geography) <= %(tol_m)s
    ORDER BY n.node_id
"""
NODE_XY_SQL = "SELECT node_id, ST_X(geom), ST_Y(geom) FROM asset.node"


@dataclass
class Modelled:
    # (facility_id, lat, lon, node_id, ground_elevation_m, snap_distance_m)
    reservoirs: list[tuple]
    too_far: list[tuple]
    pipe_count: int  # usable pipes in the whole graph
    selection: Selection
    links: list[Link] = field(default_factory=list)  # splits, gaps, bridges, with reasons
    unjoined: tuple = (0, 0)  # pieces and pipes still without supply
    pipes: list = field(default_factory=list)  # every pipe the walk could use
    snap_notes: list[str] = field(default_factory=list)  # reservoirs snapped past a node
    limits: dict = field(default_factory=dict)  # snap distance per supply type


def select_modelled(
    cur,
    max_pipes: int = MAX_PIPES,
    max_snap_m: float = MAX_SNAP_M,
    tjunction_m: float = TJUNCTION_M,
    small_m: float = BRIDGE_SMALL_M,
    large_m: float = BRIDGE_LARGE_M,
) -> Modelled:
    """Snap supply reservoirs to the graph, join the pieces (real splits
    first, then synthetic gaps and bridges), then walk out to the pipe cap."""
    cur.execute("""
        SELECT pipe_id, start_node, end_node,
               COALESCE(length_m, ST_Length(geom::geography)),
               COALESCE(diameter_mm, 150) / 1000.0
        FROM asset.pipe
        WHERE start_node IS NOT NULL AND end_node IS NOT NULL
    """)
    pipes = [
        Pipe(pipe_id, s, e, max(float(length or 1), 1.0), max(float(dia or 0.15), 0.05))
        for pipe_id, s, e, length, dia in cur.fetchall()
    ]
    # A reservoir snaps to its nearest node that has a usable pipe (one that
    # didn't collapse to zero length); node_id breaks distance ties so the snap
    # is the same every build. Passing over a nearer node is noted with why.
    usable = {n for p in pipes if p.start_node != p.end_node for n in (p.start_node, p.end_node)}
    limits = snap_limits(max_snap_m)
    cur.execute(RESERVOIR_NODES_SQL, {"k": SNAP_CANDIDATES})
    by_facility: dict[str, list] = {}
    for row in cur.fetchall():
        by_facility.setdefault(row[0], []).append(row)
    reservoirs, too_far, snap_notes = [], [], []
    for fid in sorted(by_facility):
        rows = sorted(by_facility[fid], key=lambda r: r[7])
        pick = next((r for r in rows if r[4] in usable), None)
        if pick is None:
            snap_notes.append(
                f"reservoir {fid}: none of its {len(rows)} nearest nodes has a " f"usable pipe, so it is left out"
            )
            continue
        if pick is not rows[0]:
            snap_notes.append(
                f"reservoir {fid}: nearest node N{rows[0][4]} ({rows[0][6]:.0f} m) has no usable "
                f"pipe (only zero-length after snapping), so it snaps to N{pick[4]} "
                f"({pick[6]:.0f} m)"
            )
        fid_, lat, lon, kind, node, elev, dist, _rank = pick
        entry = (fid_, lat, lon, node, elev, dist)
        (reservoirs if dist <= limits.get(kind, max_snap_m) else too_far).append(entry)

    pipe_count = len(pipes)
    sources = [r[3] for r in reservoirs]

    links: list[Link] = []
    if tjunction_m > 0:
        cur.execute(TJUNCTION_SQL, {"tol_m": tjunction_m, "box_deg": tjunction_m / 50_000.0})
        pipes, links = split_at_tjunctions(pipes, cur.fetchall())
    unjoined = (0, 0)
    if max(small_m, large_m) > 0:
        cur.execute(NODE_XY_SQL)
        coords = {n: (x, y) for n, x, y in cur.fetchall()}
        pipes, joins, unjoined = connect_pieces(pipes, coords, sources, small_m, large_m)
        links += joins

    selection = select_supplied_subgraph(pipes, sources, max_pipes)
    return Modelled(reservoirs, too_far, pipe_count, selection, links, unjoined, pipes, snap_notes, limits)


def excluded_pipes(
    m: Modelled, max_pipes: int = MAX_PIPES, large_m: float = BRIDGE_LARGE_M
) -> dict[str, tuple[str, str]]:
    """Every real pipe the model leaves out, with a code and the reason.

    A split pipe counts as modelled if any of its pieces is. Pipes the
    topology build never gave nodes to are not in m.pipes; run_sim.py adds
    those from the database.
    """
    kept = {p.asset_id for p in m.selection.pipes if p.asset_id is not None}
    out: dict[str, tuple[str, str]] = {}
    for p in m.pipes:
        aid = p.asset_id
        if aid is None or aid in kept or aid in out:
            continue
        if p.start_node == p.end_node:
            out[aid] = (
                "zero_length",
                "both ends snapped to the same node: shorter than the snap tolerance, "
                "so it is a joint, not a pipe; the pipes it joined still meet there",
            )
        elif p.start_node in m.selection.reachable:
            out[aid] = ("over_cap", f"connected to supply but beyond SIM_MAX_PIPES={max_pipes}")
        else:
            out[aid] = (
                "unsupplied",
                f"in a piece no reservoir reaches, with no real connection and no other "
                f"network within SIM_BRIDGE_LARGE_M={large_m:g} m to join it to",
            )
    return out
