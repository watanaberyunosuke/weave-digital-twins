#!/usr/bin/env python3
"""Fill gaps in the synthetic telemetry once the feed has come back.

Water use doesn't stop when the host sleeps, but the generators do, so a laptop
with its lid shut leaves hours with no readings. This draws those hours from the
same sources as the live feeds: meters from ingest/demand_profiles.py, loggers
from the ranges in schemas/pressure_reading.avsc, which Datagen also reads.

Only closed gaps are filled, meaning one with a live row after it. A gap still
open at the present can't be told apart from a broken pipeline, and filling it
would make a dead feed look fresh. The closing row also proves where the gap
ends, so nothing overlaps live data and a filled gap is no longer a gap, which
is what makes a re-run safe on tables with no key to deduplicate on.

Rows go straight to Postgres, as ingest/storage_poller.py's do. Through Kafka
they would pass the bronze sinks, but the DMA balance's windows have already
closed by then, so that table is recomputed here over the same range. Every
fill is written to twin.backfill_log with its range and method, because
is_synthetic can't separate these rows from live synthetic ones; the console
marks those ranges on its charts and Logstash re-reads them.

Real feeds are never filled here: a gap in an observation is a fact.

    python backfill.py            # check every BACKFILL_INTERVAL_S
    python backfill.py --once     # one pass over the window, then exit
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg

import common  # noqa: F401  (configures logging)
from synthetic_meters import INTERVAL_S as METER_INTERVAL_S
from synthetic_meters import readings

DSN = os.environ.get("TWIN_DSN", "postgresql://weave:weave@postgres:5432/weave_twin")
CHECK_S = int(os.environ.get("BACKFILL_INTERVAL_S", "60"))
# Seven days matches the residual baseline and what Elasticsearch keeps; older
# gaps change nothing anyone reads.
MAX_AGE_DAYS = int(os.environ.get("BACKFILL_MAX_AGE_DAYS", "7"))
PRESSURE_SCHEMA = os.environ.get("BACKFILL_PRESSURE_SCHEMA", "/schemas/pressure_reading.avsc")
# Live loggers report about every 1.5 s (Datagen waits 0 to 3 s), so five minutes
# of silence is an outage, not chance. It sits well inside the 30 minutes
# of readings each residual averages.
PRESSURE_GAP_S = int(os.environ.get("BACKFILL_PRESSURE_GAP_S", "300"))
PRESSURE_EVERY_S = float(os.environ.get("BACKFILL_PRESSURE_EVERY_S", "1.5"))
# Flink's DMA balance window in flink/sql/03_pipelines.sql.
BALANCE_WINDOW_S = 900

METER_METHOD = "demand_profiles.interval_litres per registered meter"
PRESSURE_METHOD = "uniform draw from the ranges in schemas/pressure_reading.avsc"

log = logging.getLogger("backfill")

CREATE_LOG = """
    CREATE TABLE IF NOT EXISTS twin.backfill_log (
        stream     TEXT        NOT NULL,
        gap_start  TIMESTAMPTZ NOT NULL,
        gap_end    TIMESTAMPTZ NOT NULL,
        row_count  BIGINT      NOT NULL,
        method     TEXT        NOT NULL,
        filled_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (stream, gap_start)
    )
"""

# Mirrors the balance pipeline: only windows with meter readings, and pressure
# joined where it exists. A window is rebuilt from every row in it, live and
# filled, because a gap usually starts and ends part way through one.
RECOMPUTE_BALANCE = """
    INSERT INTO twin.dma_balance (window_start, dma_id, consumption_l, min_pressure_m, meter_count)
    SELECT m.window_start, m.dma_id, m.consumption_l, p.min_pressure_m, m.meter_count
    FROM (
        SELECT time_bucket('15 minutes', ts) AS window_start, dma_id,
               sum(litres) AS consumption_l, count(DISTINCT meter_id) AS meter_count
        FROM twin.meter_reading
        WHERE ts >= %(lo)s AND ts < %(hi)s
        GROUP BY 1, 2
    ) m
    LEFT JOIN (
        SELECT time_bucket('15 minutes', ts) AS window_start, dma_id,
               min(pressure_m) AS min_pressure_m
        FROM twin.pressure_reading
        WHERE ts >= %(lo)s AND ts < %(hi)s
        GROUP BY 1, 2
    ) p USING (window_start, dma_id)
    ON CONFLICT (window_start, dma_id) DO UPDATE
    SET consumption_l = EXCLUDED.consumption_l,
        min_pressure_m = EXCLUDED.min_pressure_m,
        meter_count = EXCLUDED.meter_count
"""


@dataclass(frozen=True)
class PressureSpec:
    loggers: list[str]
    dmas: list[str]
    pressure_m: tuple[float, float]
    flow_ls: tuple[float, float]


def load_pressure_spec(path: str = PRESSURE_SCHEMA) -> PressureSpec:
    """What Datagen draws from. Read from its schema rather than copied, because
    the seeder rewrites that file and an old copy would name ids no longer in use."""
    with open(path, encoding="utf-8") as fh:
        schema = json.load(fh)
    args = {
        f["name"]: f["type"]["arg.properties"]
        for f in schema["fields"]
        if isinstance(f["type"], dict) and "arg.properties" in f["type"]
    }
    span = lambda name: (args[name]["range"]["min"], args[name]["range"]["max"])  # noqa: E731
    return PressureSpec(args["logger_id"]["options"], args["dma_id"]["options"], span("pressure_m"), span("flow_ls"))


def closed_gaps(buckets: list[datetime], min_gap: timedelta) -> list[tuple[datetime, datetime]]:
    """Pairs of neighbouring times further apart than min_gap. Being pairs of
    rows that exist, they can only describe a gap with a row after it."""
    return [(a, b) for a, b in zip(buckets, buckets[1:]) if b - a > min_gap]


def floor_to(when: datetime, seconds: int) -> datetime:
    return when - timedelta(seconds=when.timestamp() % seconds)


def missing_interval_ends(gap_start: datetime, gap_end: datetime, seconds: int) -> list[datetime]:
    """Interval ends no live reading covers. A live row reports the interval
    ending at the clock boundary before it (synthetic_meters.publish_once), so
    both edges of the gap have an interval of their own already."""
    first = floor_to(gap_start, seconds) + timedelta(seconds=seconds)
    last = floor_to(gap_end, seconds)
    ends = []
    while first < last:
        ends.append(first)
        first += timedelta(seconds=seconds)
    return ends


def meter_rows(meters, ends: list[datetime]):
    minutes = METER_INTERVAL_S // 60
    for end in ends:
        for r in readings(meters, end, minutes):
            yield (end, r["meter_id"], r["dma_id"], r["litres"], r["flow_lpm"], r["battery_pct"], r["is_synthetic"])


def pressure_rows(
    spec: PressureSpec,
    start: datetime,
    end: datetime,
    every_s: float = PRESSURE_EVERY_S,
    rng: random.Random | None = None,
):
    """Same shape as Datagen's: a random logger and a random DMA, drawn apart from
    each other, at a random wait around the live rate. Pressure carries no daily
    pattern because the live feed has none, so a filled hour matches a live one."""
    rng = rng or random.Random()
    when = start
    while True:
        when += timedelta(seconds=rng.uniform(0, 2 * every_s))
        if when >= end:
            return
        yield (
            when,
            rng.choice(spec.loggers),
            rng.choice(spec.dmas),
            rng.uniform(*spec.pressure_m),
            rng.uniform(*spec.flow_ls),
            True,
        )


def recent_buckets(cur, table: str, since: datetime) -> list[datetime]:
    # One-minute buckets because the rows of one meter run land over a moment,
    # and they must count as one point in time.
    cur.execute(
        f"SELECT DISTINCT time_bucket('1 minute', ts) AS t FROM twin.{table} " "WHERE ts >= %s ORDER BY t", (since,)
    )
    return [row[0] for row in cur.fetchall()]


def write_fill(conn, stream: str, columns: str, rows, gap: tuple[datetime, datetime], method: str) -> int:
    """One gap, one transaction: the rows, their log entry and the balance they
    change land together, so a failure leaves the gap there to be found again."""
    count = 0
    with conn.cursor() as cur:
        with cur.copy(f"COPY twin.{stream} ({columns}) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)
                count += 1
        if count:
            cur.execute(
                "INSERT INTO twin.backfill_log (stream, gap_start, gap_end, row_count, method) "
                "VALUES (%s, %s, %s, %s, %s)",
                (stream, gap[0], gap[1], count, method),
            )
            cur.execute(
                RECOMPUTE_BALANCE,
                {
                    "lo": floor_to(gap[0], BALANCE_WINDOW_S),
                    "hi": floor_to(gap[1], BALANCE_WINDOW_S) + timedelta(seconds=BALANCE_WINDOW_S),
                },
            )
    conn.commit()
    if count:
        log.info("%s: filled %s to %s with %s rows", stream, gap[0].isoformat(), gap[1].isoformat(), count)
    return count


def fill_meters(conn, gaps) -> int:
    if not gaps:
        return 0
    with conn.cursor() as cur:
        # Today's registry, because a gap can span a re-seed and the old ids are gone.
        cur.execute(
            "SELECT meter_id, dma_id, coalesce(meter_type, 'residential') " "FROM ref.water_meter ORDER BY meter_id"
        )
        meters = cur.fetchall()
    total = 0
    for gap in gaps:
        ends = missing_interval_ends(gap[0], gap[1], METER_INTERVAL_S)
        total += write_fill(
            conn,
            "meter_reading",
            "ts, meter_id, dma_id, litres, flow_lpm, battery_pct, is_synthetic",
            meter_rows(meters, ends),
            gap,
            METER_METHOD,
        )
    return total


def fill_pressure(conn, gaps, spec: PressureSpec) -> int:
    total = 0
    for gap in gaps:
        # The start is a one-minute bucket, so its rows run up to a minute past it.
        start = gap[0] + timedelta(minutes=1)
        total += write_fill(
            conn,
            "pressure_reading",
            "ts, logger_id, dma_id, pressure_m, flow_ls, is_synthetic",
            pressure_rows(spec, start, gap[1]),
            gap,
            PRESSURE_METHOD,
        )
    return total


def run_once(since: dict[str, datetime]) -> int:
    """One pass. `since` remembers how far each stream has been checked, so only
    the first pass after a start reads the whole window."""
    floor = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    total = 0
    with psycopg.connect(DSN) as conn:
        if not since:
            # Volumes older than this table have no init script to create it.
            conn.execute(CREATE_LOG)
            conn.commit()
        with conn.cursor() as cur:
            buckets = recent_buckets(cur, "meter_reading", since.get("meter_reading", floor))
        total += fill_meters(conn, closed_gaps(buckets, timedelta(seconds=METER_INTERVAL_S * 1.5)))
        since["meter_reading"] = buckets[-1] if buckets else since.get("meter_reading", floor)

        with conn.cursor() as cur:
            buckets = recent_buckets(cur, "pressure_reading", since.get("pressure_reading", floor))
        gaps = closed_gaps(buckets, timedelta(seconds=PRESSURE_GAP_S))
        if gaps:
            total += fill_pressure(conn, gaps, load_pressure_spec())
        since["pressure_reading"] = buckets[-1] if buckets else since.get("pressure_reading", floor)
    return total


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()
    since: dict[str, datetime] = {}
    if args.once:
        log.info("filled %s rows", run_once(since))
        return 0
    while True:
        try:
            run_once(since)
        except Exception:
            # Nothing was marked as checked, so the gap is found again next pass.
            log.exception("pass failed; continuing")
        time.sleep(CHECK_S)


if __name__ == "__main__":
    raise SystemExit(main())
