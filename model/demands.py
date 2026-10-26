"""Junction demands from synthetic meter readings.

Standard library only, so the arithmetic is testable without wntr or a
database. run_sim.py runs METER_FLOWS_SQL and hands the rows here.
"""

from __future__ import annotations

from dataclasses import dataclass

LPM_PER_CMS = 60_000.0  # 1 m³/s is 60,000 L/min

# Each meter's mean flow over the window, then summed per junction in Python.
# Averaging per meter first means a meter that reports twice as often does
# not count twice; averaging every reading at a junction, as the build used
# to, turned two meters at 10 and 20 L/min into 15 rather than 30. A meter
# with no valid reading in the window comes back with a NULL mean, so it can
# be counted as silent instead of vanishing. node_id breaks distance ties so
# a meter maps to the same junction on every run.
METER_FLOWS_SQL = """
    SELECT wm.meter_id, n.node_id, avg(m.flow_lpm) AS flow_lpm
    FROM ref.water_meter wm
    JOIN LATERAL (SELECT node_id FROM asset.node
                  ORDER BY geom <-> wm.geom, node_id LIMIT 1) n ON true
    LEFT JOIN twin.meter_reading m
           ON m.meter_id = wm.meter_id
          AND m.ts > now() - %(window)s::interval
          AND m.flow_lpm >= 0
    GROUP BY wm.meter_id, n.node_id
"""
DEMAND_WINDOW = "1 hour"


@dataclass
class Demands:
    by_node: dict[int, float]   # m³/s per graph node
    meters: int                 # meters mapped to a node
    silent: int                 # of those, with no valid reading in the window


def junction_demands(rows) -> Demands:
    """Sum (meter_id, node_id, mean L/min) rows into m³/s per node.

    A silent meter contributes no demand: its junction is modelled as drawing
    nothing from it rather than at a guessed rate, and the count is reported
    so a stale feed shows up instead of reading as quiet consumption.
    """
    by_node: dict[int, float] = {}
    meters = silent = 0
    for _meter_id, node_id, flow_lpm in rows:
        meters += 1
        if flow_lpm is None:
            silent += 1
            continue
        by_node[node_id] = by_node.get(node_id, 0.0) + float(flow_lpm) / LPM_PER_CMS
    return Demands(by_node, meters, silent)
