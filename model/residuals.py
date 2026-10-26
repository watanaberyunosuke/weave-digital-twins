"""Scoring a DMA's pressure residual against its own recent behaviour.

Standard library only, so the classification is testable without wntr or a
database. run_sim.py runs RESIDUALS_SQL and hands each row to classify().

A residual (modelled minus observed pressure) is never zero in practice: the
model has default elevations, guessed roughness and synthetic demand. What
matters is whether it has moved away from where it usually sits for that
DMA. So each DMA is scored against the mean and spread of its own residuals
over a rolling baseline, not against one fixed number for every DMA, and a
DMA without enough history is reported as UNKNOWN rather than OK.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass

BASELINE_DAYS = float(os.environ.get("SIM_RESIDUAL_BASELINE_DAYS", "7"))
# Fewer residuals than this and the DMA is UNKNOWN. 24 is six hours of the
# default 15-minute solves.
MIN_SAMPLES = int(os.environ.get("SIM_RESIDUAL_MIN_SAMPLES", "24"))
Z_WATCH = float(os.environ.get("SIM_RESIDUAL_Z_WATCH", "2"))
Z_LEAK = float(os.environ.get("SIM_RESIDUAL_Z_LEAK", "3"))
# A floor under the baseline's standard deviation. A DMA whose residual has
# been perfectly steady would otherwise turn a millimetre's change into an
# infinite z-score; 0.1 m is about the resolution of a pressure logger.
SIGMA_FLOOR_M = float(os.environ.get("SIM_RESIDUAL_SIGMA_FLOOR_M", "0.1"))
Z_TOLERANCE = 1e-9

# Current residual per DMA with the statistics of its baseline: earlier rows
# in twin.sim_residual inside the window. Rows that were flagged are left out
# of the baseline, so a leak that persists is not learned as normal and stops
# being flagged after a few days. The current run is not yet written when
# this runs, so it is never part of its own baseline.
RESIDUALS_SQL = """
    WITH observed AS (
        SELECT pr.dma_id, avg(pr.pressure_m) AS observed
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
        WHERE sr.run_id = %(run_id)s AND sr.variable = 'pressure'
        GROUP BY d.dma_id
    ),
    baseline AS (
        SELECT dma_id, count(*) AS n, avg(residual) AS mean,
               stddev_samp(residual) AS sd
        FROM twin.sim_residual
        WHERE variable = 'pressure'
          AND ts > now() - make_interval(secs => %(baseline_s)s)
          AND residual IS NOT NULL
          AND NOT coalesce(flagged, false)
        GROUP BY dma_id
    )
    SELECT o.dma_id, m.modelled, o.observed,
           coalesce(b.n, 0), b.mean, b.sd
    FROM observed o
    JOIN modelled m USING (dma_id)
    LEFT JOIN baseline b USING (dma_id)
    ORDER BY o.dma_id
"""


@dataclass(frozen=True)
class Score:
    residual: float
    z_score: float | None   # None while the baseline is too short
    flagged: bool
    status: str             # OK | WATCH | SUSPECTED_LEAK | UNKNOWN


def classify(residual: float, n: int, mean: float | None, sd: float | None,
             min_samples: int = MIN_SAMPLES, z_watch: float = Z_WATCH,
             z_leak: float = Z_LEAK, sigma_floor: float = SIGMA_FLOOR_M) -> Score:
    """Score one DMA's residual against its baseline (n, mean, sd)."""
    if n < min_samples or mean is None:
        return Score(residual, None, False, "UNKNOWN")
    sigma = max(sd if sd is not None and not math.isnan(sd) else 0.0, sigma_floor)
    z = (residual - mean) / sigma
    # A step of exactly the threshold counts; without the tolerance, float
    # rounding (0.3 / 0.1 is 2.9999999999999996) would decide it.
    if abs(z) >= z_leak - Z_TOLERANCE:
        return Score(residual, z, True, "SUSPECTED_LEAK")
    if abs(z) >= z_watch - Z_TOLERANCE:
        return Score(residual, z, False, "WATCH")
    return Score(residual, z, False, "OK")
