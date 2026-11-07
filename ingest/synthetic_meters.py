#!/usr/bin/env python3
"""Synthetic water meter readings with typical residential and commercial use.

Replaces the Datagen water-meter connector, which drew litres and flow
uniformly at random and paired each meter with a random DMA, so readings had
no daily pattern and demand at a junction was noise. Here every meter in
ref.water_meter reports once per interval, from the profile its type and id
fix (ingest/demand_profiles.py), on the same topic and in the same shape the
Flink meter source already reads.

The model takes its junction demands from these readings at every solve
(model/demands.py), so this is what drives the hydraulic model.

    python synthetic_meters.py            # every METER_INTERVAL_S
    python synthetic_meters.py --once     # one interval, then exit
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg

from common import make_producer, poll_forever
from demand_profiles import interval_litres, meter_profile

DSN = os.environ.get("TWIN_DSN", "postgresql://weave:weave@postgres:5432/weave_twin")
TOPIC = os.environ.get("METER_TOPIC", "water.meter-readings.raw.v1")
INTERVAL_S = int(os.environ.get("METER_INTERVAL_S", "900"))
# The daily shape is in local time, so the morning peak falls in the region's
# morning; the zone is region config (.env), not a default baked in here.
LOCAL_TZ = ZoneInfo(os.environ.get("METER_TZ", "UTC"))

log = logging.getLogger("synthetic-meters")


def read_meters() -> list[tuple[str, str, str]]:
    """Every registered meter, re-read each run because seeding truncates them."""
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT meter_id, dma_id, coalesce(meter_type, 'residential') " "FROM ref.water_meter ORDER BY meter_id"
        )
        return cur.fetchall()


def battery_pct(meter_id: str) -> int:
    """Steady per meter, in the 55-100 range the Datagen schema used."""
    digest = hashlib.sha256(f"battery|{meter_id}".encode()).digest()
    return 55 + digest[0] % 46


def readings(meters, interval_end: datetime, minutes: int) -> list[dict]:
    start = (interval_end - timedelta(minutes=minutes)).astimezone(LOCAL_TZ)
    event_ms = int(interval_end.timestamp() * 1000)
    out = []
    for meter_id, dma_id, meter_type in meters:
        litres = interval_litres(meter_profile(meter_id, meter_type), start, minutes)
        out.append(
            {
                "meter_id": meter_id,
                "dma_id": dma_id,
                "litres": round(litres, 2),
                "flow_lpm": round(litres / minutes, 3),
                "battery_pct": battery_pct(meter_id),
                "event_ts": event_ms,
                "is_synthetic": True,
            }
        )
    return out


def publish_once(producer) -> int:
    minutes = INTERVAL_S // 60
    # Interval ends fall on the clock (:00, :15, ...) so every meter in a run
    # shares one, and a restart doesn't shift the series.
    now = datetime.now(timezone.utc)
    end = now - timedelta(seconds=(now.timestamp() % INTERVAL_S))
    rows = readings(read_meters(), end.replace(microsecond=0), minutes)
    for row in rows:
        producer.send(TOPIC, key=row["meter_id"], value=row)
    producer.flush()
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()
    if INTERVAL_S < 60 or INTERVAL_S % 60:
        raise SystemExit(f"! METER_INTERVAL_S={INTERVAL_S}: use whole minutes")
    producer = make_producer()
    if args.once:
        log.info("published %s records", publish_once(producer))
        return 0
    poll_forever(lambda: publish_once(producer), INTERVAL_S, "synthetic-meters")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
