#!/usr/bin/env python3
"""Load daily storage volumes for the region's reservoirs into twin.storage_level.

Written against a Kisters KiWIS web service, the query API behind national
water data portals. The endpoint and the area to keep are env config, so this
adapter carries no region of its own.

Storage volumes are daily means, published about a day late, so they go
straight to Postgres in batches (as the table's schema comment intends)
rather than through Kafka: a stream adds nothing to one value a day.

Calls are kept few because public KiWIS services rate-limit and screen for
scraping: one series list per run (the service ignores a bounding box, so the
area is filtered here), then the values for every series in a handful of
batched requests. Each series is read from its last stored day, so a run
after the first fetches a day or two per storage.

    python storage_poller.py            # poll every STORAGE_INTERVAL_S
    python storage_poller.py --once     # one run, then exit
    python storage_poller.py --list     # print the storages in the area; writes nothing
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg
import requests

from common import require_env

DSN = os.environ.get("TWIN_DSN", "postgresql://weave:weave@postgres:5432/weave_twin")
INTERVAL_S = int(os.environ.get("STORAGE_INTERVAL_S", "21600"))
# After a failed run. The service's backend drops out for minutes at a time,
# so waiting the full interval would leave the storages hours behind.
RETRY_S = int(os.environ.get("STORAGE_RETRY_S", "900"))
PARAMETER = os.environ.get("STORAGE_PARAMETER", "Storage Volume")
# The quality-coded daily mean. A station often carries several series for the
# same parameter (received, harmonised, merged); naming one keeps a storage to
# a single value per day.
TS_NAME = os.environ.get("STORAGE_TS_NAME", "DMQaQc.Merged.DailyMean.24HR")
# How far back the first run reads each storage.
BACKFILL_DAYS = int(os.environ.get("STORAGE_BACKFILL_DAYS", "90"))
# A storage is linked to the nearest supply asset within this distance. Gauges
# sit on the dam wall or the outlet, so a few hundred metres off the asset's
# point is normal.
MATCH_M = float(os.environ.get("STORAGE_MATCH_M", "1500"))
# Series whose last value is older than this are retired gauges.
STALE_DAYS = int(os.environ.get("STORAGE_STALE_DAYS", "30"))
BATCH = 20                    # series per values request
ATTEMPTS = 4                  # the service fails transiently with DatasourceError

log = logging.getLogger("storage")


@dataclass(frozen=True)
class Series:
    ts_id: str
    station_no: str
    name: str
    lat: float
    lon: float
    last: str                 # ISO timestamp of the latest value, '' if none


# ------------------------------------------------------------------ parsing
def parse_bbox(text: str) -> tuple[float, float, float, float]:
    """'xmin,ymin,xmax,ymax' in degrees, as the region config's extent."""
    parts = [float(p) for p in text.split(",")]
    if len(parts) != 4 or parts[0] >= parts[2] or parts[1] >= parts[3]:
        raise ValueError(f"STORAGE_BBOX must be xmin,ymin,xmax,ymax, not {text!r}")
    return parts[0], parts[1], parts[2], parts[3]


def parse_series_list(payload, bbox, now: datetime) -> list[Series]:
    """Series inside the area that are still reporting, one per station.

    KiWIS returns a header row then data rows. Rows with coordinates that
    don't parse are dropped: some publishers' entries are garbled, and a
    point can't be placed without them.
    """
    if not isinstance(payload, list) or not payload:
        raise ValueError(f"unexpected series list: {str(payload)[:200]}")
    header, rows = payload[0], payload[1:]
    xmin, ymin, xmax, ymax = bbox
    cutoff = now - timedelta(days=STALE_DAYS)
    keep: dict[str, Series] = {}
    for row in rows:
        r = dict(zip(header, row))
        try:
            lat, lon = float(r["station_latitude"]), float(r["station_longitude"])
        except (KeyError, TypeError, ValueError):
            continue
        if not (xmin <= lon <= xmax and ymin <= lat <= ymax):
            continue
        last = r.get("to") or ""
        if not last or parse_ts(last) < cutoff:
            continue
        s = Series(str(r["ts_id"]), str(r["station_no"]), str(r["station_name"]).strip(),
                   lat, lon, last)
        # The most recent series wins if a station still has two.
        if s.station_no not in keep or s.last > keep[s.station_no].last:
            keep[s.station_no] = s
    return sorted(keep.values(), key=lambda s: s.station_no)


def parse_values(payload) -> dict[str, list[tuple[datetime, float]]]:
    """ts_id -> [(timestamp, value)], skipping gaps (null values)."""
    if not isinstance(payload, list):
        raise ValueError(f"unexpected values response: {str(payload)[:200]}")
    out: dict[str, list[tuple[datetime, float]]] = {}
    for block in payload:
        columns = [c.strip() for c in str(block.get("columns", "")).split(",")]
        ti, vi = columns.index("Timestamp"), columns.index("Value")
        out[str(block["ts_id"])] = [(parse_ts(row[ti]), float(row[vi]))
                                    for row in block.get("data", []) if row[vi] is not None]
    return out


def parse_ts(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


# ------------------------------------------------------------------ service
def kiwis(params: dict):
    """One KiWIS query, retried with backoff on the service's transient errors."""
    url = require_env("STORAGE_URL", "the KiWIS endpoint that serves storage volumes")
    base = {"service": "kisters", "type": "queryServices", "datasource": "0", "format": "json"}
    for attempt in range(1, ATTEMPTS + 1):
        resp = requests.get(url, params={**base, **params}, timeout=60)
        # The service reports its transient backend failure as HTTP 500 with a
        # JSON error body, so the body is read before the status.
        try:
            body = resp.json()
        except ValueError:
            resp.raise_for_status()
            raise
        if resp.ok and not (isinstance(body, dict) and body.get("type") == "error"):
            return body
        message = body.get("message") if isinstance(body, dict) else resp.reason
        log.warning("%s: %s (attempt %s of %s)", resp.status_code, message, attempt, ATTEMPTS)
        if attempt < ATTEMPTS:
            time.sleep(10 * attempt)
    raise RuntimeError(f"KiWIS kept failing: {message}")


def list_series(bbox) -> list[Series]:
    payload = kiwis({
        "request": "getTimeseriesList", "parametertype_name": PARAMETER, "ts_name": TS_NAME,
        "returnfields": "station_name,station_no,ts_id,station_latitude,station_longitude,coverage",
    })
    return parse_series_list(payload, bbox, datetime.now(timezone.utc))


def fetch_values(series: list[Series], since: dict[str, datetime]) -> dict[str, list]:
    """Values for every series, batched; each batch starts at its oldest need."""
    out: dict[str, list] = {}
    default = datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)
    for i in range(0, len(series), BATCH):
        chunk = series[i:i + BATCH]
        start = min(since.get(s.station_no, default) for s in chunk)
        out.update(parse_values(kiwis({
            "request": "getTimeseriesValues", "ts_id": ",".join(s.ts_id for s in chunk),
            # A plain date: the service is fussy about offsets and fractions.
            "from": start.date().isoformat(), "returnfields": "Timestamp,Value",
        })))
    return out


# ------------------------------------------------------------------ storage
# postgres/init creates both of these on a new volume. It only runs on an
# empty one, so existing volumes get them here; the statements match.
SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS ref.storage_site (
        station_no   TEXT PRIMARY KEY,
        storage_name TEXT,
        facility_id  TEXT,
        match_m      DOUBLE PRECISION,
        source       TEXT,
        geom         GEOMETRY(Point, 4326),
        synthetic    BOOLEAN DEFAULT false
    );
    CREATE INDEX IF NOT EXISTS storage_site_geom_idx ON ref.storage_site USING GIST (geom);
    ALTER TABLE twin.storage_level ADD COLUMN IF NOT EXISTS station_no TEXT;
    CREATE INDEX IF NOT EXISTS storage_level_station_idx ON twin.storage_level (station_no, ts DESC);
"""

# Nearest supply asset of any storage kind. Capacity comes from the asset
# layer when it publishes one; percent full is left empty otherwise rather
# than guessed.
MATCH_SQL = """
    SELECT f.facility_id, f.capacity_ml,
           ST_Distance(f.geom::geography, ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography)
    FROM asset.facility f
    WHERE f.facility_type IN ('service_reservoir', 'storage_dam', 'osm_storage', 'registered_storage')
    ORDER BY f.geom <-> ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326), f.facility_id
    LIMIT 1
"""


def run_once(list_only: bool = False) -> int:
    bbox = parse_bbox(require_env("STORAGE_BBOX", "the area to keep, xmin,ymin,xmax,ymax"))
    series = list_series(bbox)
    log.info("%s storages report %s inside %s", len(series), PARAMETER, bbox)
    if list_only:
        for s in series:
            print(f"{s.station_no:<14} {s.name:<40} {s.lat:9.4f} {s.lon:9.4f}  last {s.last[:10]}")
        return 0
    if not series:
        return 0

    source = os.environ.get("STORAGE_PROVIDER", "kiwis")
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        cur.execute("SELECT station_no, max(ts) FROM twin.storage_level"
                    " WHERE station_no IS NOT NULL GROUP BY 1")
        since = {no: ts for no, ts in cur.fetchall()}
        values = fetch_values(series, since)

        written = unmatched = 0
        for s in series:
            cur.execute(MATCH_SQL, {"lat": s.lat, "lon": s.lon})
            hit = cur.fetchone()
            matched = bool(hit) and hit[2] <= MATCH_M
            facility_id, capacity = (hit[0], hit[1]) if matched else (None, None)
            unmatched += not matched
            # The gauge is the site of record; the asset link is a best guess
            # by distance, kept with its distance so it can be judged.
            cur.execute("""
                INSERT INTO ref.storage_site
                    (station_no, storage_name, facility_id, match_m, source, geom, synthetic)
                VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), false)
                ON CONFLICT (station_no) DO UPDATE SET
                    storage_name = EXCLUDED.storage_name, facility_id = EXCLUDED.facility_id,
                    match_m = EXCLUDED.match_m, source = EXCLUDED.source, geom = EXCLUDED.geom
            """, (s.station_no, s.name, facility_id, hit[2] if matched else None,
                  source, s.lon, s.lat))
            rows = [(ts, s.name, vol, capacity,
                     100.0 * vol / float(capacity) if capacity else None, s.station_no)
                    for ts, vol in values.get(s.ts_id, [])]
            # A re-read day overwrites itself: the daily mean can be revised.
            cur.executemany("""
                INSERT INTO twin.storage_level
                    (ts, storage_name, volume_ml, capacity_ml, percent_full, station_no, is_synthetic)
                VALUES (%s, %s, %s, %s, %s, %s, false)
                ON CONFLICT (ts, storage_name) DO UPDATE SET
                    volume_ml = EXCLUDED.volume_ml, capacity_ml = EXCLUDED.capacity_ml,
                    percent_full = EXCLUDED.percent_full, station_no = EXCLUDED.station_no
            """, rows)
            written += len(rows)
        conn.commit()
    log.info("wrote %s daily values; %s storages have no supply asset within %g m",
             written, unmatched, MATCH_M)
    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--list", action="store_true", help="print the storages in the area")
    args = ap.parse_args()
    if args.list:
        return run_once(list_only=True)
    if args.once:
        run_once()
        return 0
    while True:
        try:
            run_once()
            wait = INTERVAL_S
        except Exception:
            log.exception("storage run failed; retrying in %s s", RETRY_S)
            wait = RETRY_S
        time.sleep(wait)


if __name__ == "__main__":
    sys.exit(main())
