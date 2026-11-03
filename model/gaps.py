"""Join the published network up: real connections first, synthetic last.

Most of a published network can sit in pieces no reservoir reaches, and a
piece with no source can't be modelled. Some of those breaks are only how the
data was drawn; others are mains the data never had. This module closes them
in a fixed order, so a real connection always wins over an invented one, and
every element it adds says why it was added.

1. Split (real). asset.build_topology() joins pipe ends to pipe ends. A main
   whose end meets the side of another, which the source never split there,
   stays apart. Where a dead end lies within SIM_TJUNCTION_M of another
   pipe's side, that pipe is cut at the point and both join a shared node.
   The pieces are still the real asset: P<pipe_id>~<n> in the model.
2. Gap (synthetic). A dead end within SIM_BRIDGE_SMALL_M of a node in another
   piece is joined by a short pipe: most likely a digitising gap.
3. Reservoir trunk main (synthetic): in run_sim.py, each reservoir's link to
   its node is as long as the snap distance, not a 10 m stub.
4. Bridge (synthetic). A piece still unfed is joined to the nearest supplied
   node within SIM_BRIDGE_LARGE_M: a main the data does not show, invented so
   the piece can be modelled. 0 turns bridges off.

Gaps and bridges are chosen shortest first (Borůvka rounds over the
pieces), so a piece reachable by a short gap is never given a long bridge,
and a chain of pieces joins piece to piece. A piece is joined from its dead
ends where it has any (where breaks in drawn data usually are), else from
any of its nodes. Synthetic links take the larger diameter of the pipes
at their two ends (an assumption: a link carries what the mains it joins
carry). Lengths are straight-line distances.

Standard library only: the seeder imports this in the gis-loader image.

    split_at_tjunctions(pipes, hits) -> (pipes, links)
    connect_pieces(pipes, coords, source_nodes, small_m, large_m) -> (pipes, links, unjoined)
"""

from __future__ import annotations

import math
import os
from collections import defaultdict
from dataclasses import dataclass

from subgraph import Pipe

TJUNCTION_M = float(os.environ.get("SIM_TJUNCTION_M", "2"))
BRIDGE_SMALL_M = float(os.environ.get("SIM_BRIDGE_SMALL_M", "50"))
BRIDGE_LARGE_M = float(os.environ.get("SIM_BRIDGE_LARGE_M", "5000"))


@dataclass(frozen=True)
class Link:
    """One element the model changed or added, and why."""
    kind: str               # split | gap | trunk | bridge
    element_id: str         # pipe_id in the model (before the P/S prefix)
    from_node: int | None
    to_node: int | None
    length_m: float
    reason: str


# ------------------------------------------------------------------ 1. split
def split_at_tjunctions(pipes: list[Pipe], hits) -> tuple[list[Pipe], list[Link]]:
    """Cut pipes where dead ends meet their sides.

    `hits` are (node_id, pipe_id, fraction along the pipe, gap in m) for dead
    ends lying within the tolerance of a pipe that isn't theirs. A pipe met
    at several points is cut at each, in order along it.
    """
    by_pipe: dict[str, list[tuple[float, int, float]]] = defaultdict(list)
    for node, pipe_id, frac, gap_m in hits:
        by_pipe[pipe_id].append((float(frac), int(node), float(gap_m)))

    out: list[Pipe] = []
    links: list[Link] = []
    for pipe in pipes:
        cuts = sorted(by_pipe.get(pipe.pipe_id, ()))
        # Cuts at the very ends join nothing new, and one node can't cut twice.
        seen: set[int] = set()
        cuts = [c for c in cuts if 0.0 < c[0] < 1.0 and c[1] not in seen and not seen.add(c[1])]
        if not cuts or pipe.kind != "real":
            out.append(pipe)
            continue
        stops = [(0.0, pipe.start_node)] + [(f, n) for f, n, _ in cuts] + [(1.0, pipe.end_node)]
        for k, ((f0, a), (f1, b)) in enumerate(zip(stops, stops[1:]), start=1):
            piece = Pipe(f"{pipe.pipe_id}~{k}", a, b, max(pipe.length_m * (f1 - f0), 1.0),
                         pipe.diameter_m, kind="split", source_id=pipe.pipe_id)
            out.append(piece)
        for f, n, gap in cuts:
            links.append(Link(
                "split", pipe.pipe_id, n, None, pipe.length_m,
                f"real: dead end N{n} lies {gap:.1f} m from the side of pipe {pipe.pipe_id} "
                f"({f:.0%} along it); the source draws a junction there but never split the "
                f"pipe, so the topology build left them apart"))
    return out, links


# ------------------------------------------------------- 2 and 4. gap, bridge
class _Grid:
    """Points bucketed by a square cell, for nearest-point search in metres."""

    def __init__(self, cell_m: float):
        self.cell = cell_m
        self.cells: dict[tuple[int, int], list[int]] = defaultdict(list)

    def key(self, xy):
        return int(math.floor(xy[0] / self.cell)), int(math.floor(xy[1] / self.cell))

    def add(self, node, xy):
        self.cells[self.key(xy)].append(node)

    def nearest_outside(self, xy, xy_of, limit_m, root_of, own):
        """(distance, node) of the nearest point within limit_m whose root
        isn't `own`, or None."""
        cx, cy = self.key(xy)
        best = None
        rings = int(math.ceil(limit_m / self.cell)) + 1
        for r in range(rings + 1):
            # Every point in ring r is at least (r - 1) cells away.
            if best is not None and (r - 1) * self.cell > best[0]:
                break
            for dx in range(-r, r + 1):
                for dy in (range(-r, r + 1) if abs(dx) == r else (-r, r)):
                    for node in self.cells.get((cx + dx, cy + dy), ()):
                        if root_of[node] == own:
                            continue
                        ox, oy = xy_of[node]
                        d = math.hypot(ox - xy[0], oy - xy[1])
                        if d <= limit_m and (best is None or (d, node) < best):
                            best = (d, node)
        return best


def _project(coords: dict[int, tuple[float, float]]) -> dict[int, tuple[float, float]]:
    """(lon, lat) to local metres, equirectangular about the mean latitude;
    within a region the error is well under the gap limits."""
    if not coords:
        return {}
    lat0 = math.radians(sum(lat for _, lat in coords.values()) / len(coords))
    kx = 111_320.0 * math.cos(lat0)
    return {n: (lon * kx, lat * 110_540.0) for n, (lon, lat) in coords.items()}


def connect_pieces(pipes: list[Pipe], coords: dict[int, tuple[float, float]],
                   source_nodes, small_m: float = BRIDGE_SMALL_M,
                   large_m: float = BRIDGE_LARGE_M):
    """Join unfed pieces to the supplied network: short gaps, then bridges.

    Borůvka rounds: each round, every unfed set of pieces finds its shortest
    link (from one of its dead ends) to any node outside it, and the round's
    links are taken shortest first. The links taken form the shortest joining
    tree, so a piece reachable by a short gap never gets a long bridge, and a
    chain of pieces joins piece to piece rather than each reaching back
    separately. Supplied sets never start a link, so two supplied networks
    are not joined to each other.

    Returns the pipes with the new links appended, the Link records, and how
    many pieces (and pipes in them) are still unfed.
    """
    limit = max(small_m, large_m)
    usable = [p for p in pipes if p.start_node != p.end_node]
    nodes = sorted({n for p in usable for n in (p.start_node, p.end_node)} & coords.keys())
    if not nodes or limit <= 0:
        return pipes, [], _unfed_summary(usable, nodes, source_nodes)
    xy = _project({n: coords[n] for n in nodes})

    parent = {n: n for n in nodes}

    def find(n):
        while parent[n] != n:
            parent[n] = parent[parent[n]]
            n = parent[n]
        return n

    degree: dict[int, int] = defaultdict(int)
    diameter: dict[int, float] = defaultdict(float)
    for p in usable:
        for n in (p.start_node, p.end_node):
            degree[n] += 1
            diameter[n] = max(diameter[n], p.diameter_m)
        if p.start_node in parent and p.end_node in parent:
            ra, rb = find(p.start_node), find(p.end_node)
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)

    piece_of = {n: find(n) for n in nodes}          # the published pieces
    piece_pipes: dict[int, int] = defaultdict(int)
    for p in usable:
        if p.start_node in piece_of:
            piece_pipes[piece_of[p.start_node]] += 1
    fed = {find(n) for n in source_nodes if n in parent}

    grid = _Grid(50.0)
    for n in nodes:
        grid.add(n, xy[n])

    candidates: list[Link] = []
    while True:
        root_of = {n: find(n) for n in nodes}
        unfed_starts: dict[int, list[int]] = defaultdict(list)
        unfed_all: dict[int, list[int]] = defaultdict(list)
        for n in nodes:
            r = root_of[n]
            if r not in fed:
                unfed_all[r].append(n)
                if degree[n] == 1:
                    unfed_starts[r].append(n)
        if not unfed_all:
            break
        best = {}
        for r, members in unfed_all.items():
            top = None
            for n in unfed_starts.get(r) or members:
                cap = limit if top is None else top[0]
                hit = grid.nearest_outside(xy[n], xy, cap, root_of, r)
                if hit and (top is None or (hit[0], n, hit[1]) < top):
                    top = (hit[0], n, hit[1])
            if top:
                best[r] = top
        if not best:
            break
        joined = False
        for dist, a, b in sorted(best.values()):
            ra, rb = find(a), find(b)
            if ra == rb or (ra in fed and rb in fed):
                continue
            kind = "gap" if dist <= small_m else "bridge"
            candidates.append(Link(kind, f"{kind}:{a}:{b}", a, b, dist, ""))
            merged_fed = ra in fed or rb in fed
            fed.discard(ra)
            fed.discard(rb)
            parent[max(ra, rb)] = min(ra, rb)
            if merged_fed:
                fed.add(min(ra, rb))
            joined = True
        if not joined:
            break

    # Only links that ended up joined to supply are kept; the rest would sit
    # in pieces the model leaves out anyway.
    added: list[Pipe] = []
    links: list[Link] = []
    for c in candidates:
        if find(c.from_node) not in fed:
            continue
        added.append(Pipe(c.element_id, c.from_node, c.to_node, max(c.length_m, 1.0),
                          max(diameter[c.from_node], diameter[c.to_node], 0.1), kind=c.kind))
        size = piece_pipes[piece_of[c.from_node]]
        if c.kind == "gap":
            why = (f"synthetic: {c.length_m:.0f} m gap from N{c.from_node} to N{c.to_node}; the "
                   f"piece of {size} pipes at N{c.from_node} had no real connection to supply, "
                   f"and a break this short (SIM_BRIDGE_SMALL_M={small_m:g}) is most likely a "
                   f"digitising gap")
        else:
            why = (f"synthetic: no real connection or short gap reaches the piece of {size} "
                   f"pipes at N{c.from_node}; joined to the nearest other network node "
                   f"N{c.to_node}, {c.length_m:.0f} m away (SIM_BRIDGE_LARGE_M={large_m:g}); "
                   f"the main is invented so the piece can be modelled")
        links.append(Link(c.kind, c.element_id, c.from_node, c.to_node, c.length_m, why))

    left = {piece_of[n] for n in nodes if find(n) not in fed}
    return pipes + added, links, (len(left), sum(piece_pipes[r] for r in left))


def _unfed_summary(usable, nodes, source_nodes):
    """Pieces and pipes no source reaches, when nothing is being joined."""
    parent = {n: n for n in nodes}

    def find(n):
        while parent[n] != n:
            parent[n] = parent[parent[n]]
            n = parent[n]
        return n

    for p in usable:
        if p.start_node in parent and p.end_node in parent:
            ra, rb = find(p.start_node), find(p.end_node)
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)
    fed = {find(n) for n in source_nodes if n in parent}
    left = {find(n) for n in nodes} - fed
    return len(left), sum(1 for p in usable
                          if p.start_node in parent and find(p.start_node) in left)
