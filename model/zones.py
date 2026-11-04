"""Pressure zones and supply heads for a network with no published levels.

Public GIS gives reservoir locations but not their top water levels, and
with a DEM under the nodes, one head per reservoir (ground level plus a fixed
depth) put every node much above a reservoir at negative pressure and every
node far below it at hundreds of metres. Real networks avoid both with
pressure zones: each zone is supplied at a hydraulic grade line that keeps
its highest customer above the minimum service pressure and its lowest below
the maximum, with pressure reducing valves (PRVs) feeding lower zones from
higher ones and boosters lifting water into zones no gravity source reaches.

This module draws those zones from elevation alone, because elevation is all
the public data supports:

* Bands. The supplied nodes are cut into elevation bands of width
  max - min - margin, counted down from the highest node. A band's grade line
  is its top elevation plus the minimum pressure plus the margin, so every
  node in it sits between min + margin and max metres of static pressure.
* Sources. A reservoir supplies at its node's band grade line; its real top
  water level is not published, and the band line is what a zone at that
  elevation would be designed to.
* PRVs. A pipe joining two bands becomes a PRV from the higher grade line to
  the lower, set to hold the lower band's line at its downstream node. EPANET
  rejects PRVs that share a node (two feeding one node, or two in series), so
  where crossings crowd together the largest main gets the valve and the
  others are closed, as boundary valves between real zones are. That also
  matches practice: a zone is fed through a few PRVs, not every crossing.
* Boosters. Part of a band that neither a reservoir nor a PRV reaches (a
  hilltop above every reservoir) gets a synthetic fixed-head source at its
  lowest node id, standing in for the booster pump or elevated tank a real
  network would have there.

The defaults come from the Water Supply Code of Australia (WSA 03) service
pressure limits as published in a Victorian water corporation's supplement:
minimum allowable 20 m, maximum allowable 80 m. The margin is an assumption:
static pressure is set, and the margin is the allowance for headloss so
flowing pressure stays near the minimum at the top of each band.

Standard library only, so it can be tested without wntr or a database.

    plan_zones(pipes, elevation, reservoir_nodes, ...) -> ZonePlan
"""

from __future__ import annotations

import math
import os
from collections import defaultdict
from dataclasses import dataclass, field

from subgraph import Pipe

MIN_PRESSURE_M = float(os.environ.get("SIM_MIN_SERVICE_PRESSURE_M", "20"))
MAX_PRESSURE_M = float(os.environ.get("SIM_MAX_SERVICE_PRESSURE_M", "80"))
HEAD_MARGIN_M = float(os.environ.get("SIM_HEAD_MARGIN_M", "5"))


@dataclass(frozen=True)
class Prv:
    pipe: Pipe
    upstream: int            # node on the higher grade line
    downstream: int          # node whose pressure the valve holds
    setting_m: float         # pressure held at the downstream node


@dataclass
class ZonePlan:
    band_width_m: float
    band_of: dict[int, int]                  # node -> band (0 is the highest)
    band_head: dict[int, float]              # band -> grade line, m
    reservoir_head: dict[str, float]         # facility_id -> head, m
    prvs: list[Prv] = field(default_factory=list)
    pipes: list[Pipe] = field(default_factory=list)       # kept as pipes
    closed: list[Pipe] = field(default_factory=list)      # boundary valves shut
    boosters: list[tuple[int, float]] = field(default_factory=list)  # (node, head)

    def static_pressure_range(self, elevation: dict[int, float]) -> tuple[float, float]:
        """Lowest and highest static pressure over the zoned nodes."""
        values = [self.band_head[b] - elevation[n] for n, b in self.band_of.items()]
        return (min(values), max(values)) if values else (0.0, 0.0)


def plan_zones(pipes: list[Pipe], elevation: dict[int, float],
               reservoir_nodes: dict[str, int],
               min_pressure_m: float = MIN_PRESSURE_M,
               max_pressure_m: float = MAX_PRESSURE_M,
               margin_m: float = HEAD_MARGIN_M) -> ZonePlan:
    """Band the nodes by elevation and work out heads, PRVs and boosters.

    `pipes` is the supplied selection, `elevation` covers every node they
    touch, and `reservoir_nodes` maps each attached reservoir to its node.
    """
    width = max_pressure_m - min_pressure_m - margin_m
    if width <= 0:
        raise ValueError(f"no room for a band: max {max_pressure_m:g} m - min "
                         f"{min_pressure_m:g} m - margin {margin_m:g} m <= 0")

    nodes = sorted({n for p in pipes for n in (p.start_node, p.end_node)}
                   | set(reservoir_nodes.values()))
    if not nodes:
        return ZonePlan(width, {}, {}, {})
    top = max(elevation[n] for n in nodes)

    # The tiny epsilon keeps a node exactly on a band's lower edge in that
    # band, so floating-point noise can't push it to the one below.
    band_of = {n: int(math.floor((top - elevation[n]) / width + 1e-9)) for n in nodes}
    band_head = {b: top - b * width + min_pressure_m + margin_m
                 for b in set(band_of.values())}

    kept: list[Pipe] = []
    crossings: list[Pipe] = []
    for pipe in sorted(pipes, key=lambda p: p.pipe_id):
        if band_of[pipe.start_node] == band_of[pipe.end_node]:
            kept.append(pipe)
        else:
            crossings.append(pipe)

    # Largest main first, then pipe id, so the same graph gives the same valves.
    prvs: list[Prv] = []
    closed: list[Pipe] = []
    valve_nodes: set[int] = set()
    for pipe in sorted(crossings, key=lambda p: (-p.diameter_m, p.pipe_id)):
        a, b = band_of[pipe.start_node], band_of[pipe.end_node]
        # The lower band index is the higher grade line, so it is upstream.
        up, down = (pipe.start_node, pipe.end_node) if a < b else (pipe.end_node, pipe.start_node)
        if up in valve_nodes or down in valve_nodes:
            closed.append(pipe)
            continue
        valve_nodes.update((up, down))
        prvs.append(Prv(pipe, up, down, band_head[band_of[down]] - elevation[down]))
    prvs.sort(key=lambda v: v.pipe.pipe_id)
    closed.sort(key=lambda p: p.pipe_id)

    reservoir_head = {fid: band_head[band_of[node]]
                      for fid, node in sorted(reservoir_nodes.items())}
    boosters = _boosters(nodes, kept, prvs, band_of, band_head,
                         set(reservoir_nodes.values()))
    return ZonePlan(width, band_of, band_head, reservoir_head, prvs, kept, closed, boosters)


def _component_finder(nodes, kept):
    """A lookup from node to its component: the lowest node id joined to it by
    kept pipes, so components are named the same however the pipes are ordered."""
    parent = {n: n for n in nodes}

    def find(n):
        while parent[n] != n:
            parent[n] = parent[parent[n]]
            n = parent[n]
        return n

    for pipe in kept:
        ra, rb = find(pipe.start_node), find(pipe.end_node)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    return find


def _boosters(nodes, kept, prvs, band_of, band_head, sources):
    """A source for each same-band component no reservoir or PRV reaches.

    Components are linked by PRVs (upstream to downstream only, as a PRV
    does not pass reverse flow). Feeding spreads from the components holding
    a reservoir; whatever is still unfed gets a booster, which then feeds on
    down through its own PRVs, so boosters are added top band first and
    only where they are needed.
    """
    find = _component_finder(nodes, kept)

    members: dict[int, list[int]] = defaultdict(list)
    for n in nodes:
        members[find(n)].append(n)
    feeds: dict[int, set[int]] = defaultdict(set)
    for prv in prvs:
        feeds[find(prv.upstream)].add(find(prv.downstream))

    fed: set[int] = set()

    def spread(start):
        stack = [start]
        while stack:
            comp = stack.pop()
            if comp in fed:
                continue
            fed.add(comp)
            stack.extend(feeds[comp] - fed)

    for n in sorted(sources):
        spread(find(n))

    boosters = []
    # Highest band first, so a booster on a hilltop also feeds the slopes
    # below it before they are given boosters of their own.
    for comp in sorted(members, key=lambda c: (band_of[c], c)):
        if comp in fed:
            continue
        node = min(members[comp])
        boosters.append((node, band_head[band_of[node]]))
        spread(comp)
    return boosters
