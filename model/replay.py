"""Which past solves to replay after the simulator was down.

Standard library only, so the gap arithmetic is testable without wntr or a
database. run_sim.py runs the SQL here and replays the times replay_times()
returns, oldest first, so each replayed residual is scored on the baseline it
would have had live.

The rule follows ingest/backfill.py. Only a closed gap is replayed, meaning one
with a residual after it: a gap still open at the present can't be told apart
from a stopped simulator, and filling it would make a dead model look fresh.
The replayed rows close the gap, so a second pass finds nothing, which is what
makes a re-run safe on a table with no key.

A replay solves today's network.inp, because past topology is not kept. So
nothing before that file was built is replayed: the live solves then used
another network, and a replay scored against their baseline flags DMAs for
the rebuild rather than for anything in the water. Every replayed range is
written to twin.backfill_log with its method so it can't pass for a live solve.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

# Off with 0, for a deployment that would rather keep the gap than a replay.
ENABLED = os.environ.get("SIM_REPLAY", "1") != "0"
# Seven days matches the residual baseline and what Elasticsearch keeps, as in
# ingest/backfill.py; older residuals change nothing anyone reads.
MAX_AGE_DAYS = float(os.environ.get("SIM_REPLAY_MAX_AGE_DAYS", "7"))
STREAM = "sim_residual"
METHOD = "EPANET replay on the current network.inp, demands and observed pressure as recorded at the time"

# One timestamp per solve: every DMA scored by a solve shares its ts.
SOLVE_TIMES_SQL = """
    SELECT DISTINCT ts FROM twin.sim_residual
    WHERE ts > now() - make_interval(secs => %(max_age_s)s)
    ORDER BY ts
"""

# A replay needs the telemetry of its time. A gap in the simulator is usually a
# gap in the feeds too, which ingest-backfill fills only once they are back, so
# a time without meter readings in the demand window or pressure in the
# residual's 30 minutes waits for a later pass instead of solving on nothing.
READY_SQL = """
    SELECT EXISTS (SELECT 1 FROM twin.meter_reading
                   WHERE ts > %(as_of)s - %(window)s::interval AND ts <= %(as_of)s)
       AND EXISTS (SELECT 1 FROM twin.pressure_reading
                   WHERE ts > %(as_of)s - interval '30 minutes' AND ts <= %(as_of)s)
"""

LOG_SQL = """
    INSERT INTO twin.backfill_log (stream, gap_start, gap_end, row_count, method)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (stream, gap_start) DO UPDATE SET
      gap_end = EXCLUDED.gap_end,
      row_count = twin.backfill_log.row_count + EXCLUDED.row_count,
      filled_at = now()
"""


def replay_times(solves: list[datetime], interval_s: float) -> list[datetime]:
    """Times to solve inside each closed gap between consecutive solves.

    A gap is more than 1.5 intervals between solves, the same margin
    ingest/backfill.py gives the meters, so a slow solve is not a gap. Replays
    step one interval from the solve before the gap and stop more than half an
    interval short of the one after it, so they never crowd the live row.
    """
    step = timedelta(seconds=interval_s)
    out = []
    for before, after in zip(solves, solves[1:]):
        if (after - before).total_seconds() <= 1.5 * interval_s:
            continue
        t = before + step
        while (after - t).total_seconds() > 0.5 * interval_s:
            out.append(t)
            t += step
    return out
