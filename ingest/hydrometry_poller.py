#!/usr/bin/env python3
"""Poll real water telemetry into Kafka.

Two adapters for the two Kisters query APIs public water agencies expose,
chosen by HYDRO_API: "hydstra" (the default) calls a Hydstra webservice's
get_ts_traces, and "kiwis" calls a KiWIS service's getTimeseriesValues, the
API behind national water data portals. The endpoint, the provider label and
the gauge list are all env config, so neither carries a region of its own.
Both hand parse_*() the same record shape, so everything after it is shared.

A KiWIS portal usually publishes its quality-coded hourly means about a day
late, so that feed runs a day behind the present; it is still observed data.

IMPORTANT: the exact parameter names and response shape vary between
deployments and should be confirmed against a live response before you rely on
them. Run:

    python ingest/hydrometry_poller.py --probe --site <gauge>

which prints the raw response (with HYDRO_API=kiwis, the parsed series) so you can adjust parse_timeseries() below.
Everything downstream of parse_*() is stable regardless of what the wire format
turns out to be — that is the point of keeping the adapters small and isolated.

Each poll asks for the series from the last reading already stored for that
gauge and variable, up to HYDRO_MAX_AGE_DAYS back, and publishes only newer
points. So an outage is recovered from the source's own history when the
poller comes back, which is the only fill a real feed gets. Overlapping polls
write nothing twice, and a message replayed anyway updates its row, because
twin.hydrometry is keyed on (site_id, variable, event_ts).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg
import requests

from common import envelope, iso_z, make_producer, poll_forever, require_env

PROVIDER = os.environ.get("HYDRO_PROVIDER", "hydrometry")
TOPIC = os.environ.get("HYDRO_TOPIC", "water.hydrometry.raw.v1")
INTERVAL_S = int(os.environ.get("HYDRO_INTERVAL_S", "900"))
DSN = os.environ.get("TWIN_DSN", "postgresql://weave:weave@postgres:5432/weave_twin")
# Seven days matches the residual baseline and what Elasticsearch keeps. A
# gauge with nothing stored starts here too: a KiWIS feed a day late would
# have nothing in a shorter first window.
MAX_AGE_DAYS = float(os.environ.get("HYDRO_MAX_AGE_DAYS", "7"))
API = os.environ.get("HYDRO_API", "hydstra").strip().lower()
# The quality-coded hourly mean. A KiWIS station carries many series per
# parameter (received, harmonised, merged, daily, monthly); naming one keeps a
# gauge to one value an hour, as the Hydstra adapter's hourly traces do.
KIWIS_TS_NAME = os.environ.get("HYDRO_TS_NAME", "DMQaQc.Merged.HourlyMean.HR")
# An area to monitor instead of a list: every site inside it with a series
# still reporting. KiWIS ignores a bounding box, so the area is filtered here,
# as storage_poller.py does. HYDRO_SITES, when also set, narrows it.
BBOX = os.environ.get("HYDRO_BBOX", "").strip()
STALE_DAYS = float(os.environ.get("HYDRO_STALE_DAYS", "7"))
# The series list is re-read this often, so a new or retired site is picked up.
RELIST_S = int(os.environ.get("HYDRO_RELIST_S", "86400"))
BATCH = 20  # series per values request, as storage_poller.py
LIST_TIMEOUT_S = float(os.environ.get("HYDRO_LIST_TIMEOUT_S", "300"))
KIWIS_PARAMETERS = {
    "Water Course Level": ("level_m", "m"),
    "Water Course Discharge": ("flow_cumecs", "m3/s"),
}


def sites() -> list[str]:
    """Site numbers for the catchments you care about — see .env.example.

    With HYDRO_BBOX set (KiWIS only) the list is optional and narrows the area.
    """
    if BBOX and API == "kiwis":
        raw = os.environ.get("HYDRO_SITES", "")
    else:
        raw = require_env("HYDRO_SITES", "a comma-separated list of site ids, or HYDRO_BBOX with HYDRO_API=kiwis")
    return [s.strip() for s in raw.split(",") if s.strip()]


VARIABLES = {
    "100.00": ("level_m", "m"),
    "141.00": ("flow_cumecs", "m3/s"),
    "10.00": ("rainfall_mm", "mm"),
}

producer = None


def fetch_timeseries(site: str, variable: str, start: datetime, end: datetime | None = None) -> dict:
    """Kisters webservices take the whole query as JSON in the query string.

    Sent as function/version/params form fields instead, the service answers
    401 or "Missing: site_list, ..." because it never parses them.
    """
    url = require_env("HYDRO_URL", "the hydrometry webservice endpoint")
    end = end or datetime.now(timezone.utc)
    query = {
        "function": "get_ts_traces",
        "version": "2",
        "params": {
            "site_list": site,
            "datasource": "A",
            "varfrom": variable,
            "varto": variable,
            "start_time": start.strftime("%Y%m%d%H%M%S"),
            "end_time": end.strftime("%Y%m%d%H%M%S"),
            "data_type": "mean",
            "interval": "hour",
            "multiplier": 1,
        },
    }
    resp = requests.get(f"{url}?{json.dumps(query, separators=(',', ':'))}", timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    # error 126 is an empty period, which is an answer, not a failure.
    if payload.get("error_num") not in (None, 0, 126):
        raise RuntimeError(f"error {payload.get('error_num')}: {payload.get('error_msg')}")
    return payload


def stored_latest() -> dict[tuple[str, str], datetime]:
    """Latest stored reading per (site, variable name), to resume from."""
    with psycopg.connect(DSN) as conn:
        rows = conn.execute(
            """SELECT site_id, variable, max(event_ts) FROM twin.hydrometry
               WHERE event_ts > now() - make_interval(secs => %s) AND NOT is_synthetic
               GROUP BY site_id, variable""",
            (MAX_AGE_DAYS * 86400,),
        ).fetchall()
    return {(site, var): ts for site, var, ts in rows}


def event_ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).timestamp() * 1000)


def newer_than(records: list[dict], after: datetime | None) -> list[dict]:
    if after is None:
        return records
    return [r for r in records if datetime.fromisoformat(r["event_ts"]) > after]


def parse_timeseries(payload: dict, site: str, variable: str) -> list[dict]:
    """ADAPTER — confirm against a --probe response and adjust if needed."""
    var_name, unit = VARIABLES.get(variable, (variable, ""))
    out = []
    traces = (payload.get("return") or {}).get("traces") or []
    for trace in traces:
        for point in trace.get("trace") or []:
            raw_t = str(point.get("t"))
            try:
                ts = iso_z(datetime.strptime(raw_t, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc))
            except ValueError:
                continue
            out.append(
                {
                    "site_id": site,
                    "variable": var_name,
                    "unit": unit,
                    "value": point.get("v"),
                    "quality_code": str(point.get("q", "")),
                    "provider": PROVIDER,
                    "event_ts": ts,
                }
            )
    return out


# What this process has published, so the next poll starts after it even
# before Flink has written it to Postgres.
published: dict[tuple[str, str], datetime] = {}


# ------------------------------------------------------------------ KiWIS
def kiwis_series(payload) -> dict[tuple[str, str], str]:
    """(station_no, variable name) -> ts_id from a getTimeseriesList answer.

    The list holds one row per series with a header row first; parameters
    this poller doesn't map are dropped.
    """
    if not isinstance(payload, list) or not payload:
        raise ValueError(f"unexpected series list: {str(payload)[:200]}")
    header = payload[0]
    out = {}
    for row in payload[1:]:
        r = dict(zip(header, row))
        mapped = KIWIS_PARAMETERS.get(r.get("parametertype_name"))
        if mapped and r.get("ts_id"):
            out[(str(r["station_no"]), mapped[0])] = str(r["ts_id"])
    return out


def kiwis_records(points: list[tuple[datetime, float]], site: str, var_name: str) -> list[dict]:
    """Records in parse_timeseries()'s shape from parsed KiWIS values."""
    unit = next(u for name, u in KIWIS_PARAMETERS.values() if name == var_name)
    return [
        {
            "site_id": site,
            "variable": var_name,
            "unit": unit,
            "value": value,
            "quality_code": "",
            "provider": PROVIDER,
            "event_ts": iso_z(ts),
        }
        for ts, value in points
    ]


def kiwis_stations(payload) -> dict[str, dict]:
    """station_no -> name, location and what it measures, for ref.hydrometry_site."""
    header = payload[0]
    out: dict[str, dict] = {}
    for row in payload[1:]:
        r = dict(zip(header, row))
        mapped = KIWIS_PARAMETERS.get(r.get("parametertype_name"))
        if not mapped:
            continue
        site = out.setdefault(
            str(r["station_no"]),
            {
                "name": r.get("station_name"),
                "lat": r.get("station_latitude"),
                "lon": r.get("station_longitude"),
                "measures": {},
            },
        )
        site["measures"][mapped[0]] = mapped[1]
    return out


# A registry row, not telemetry, so it goes straight to Postgres as the storage
# poller's ref.storage_site does. variable and unit list what the gauge reports.
UPSERT_SITE = """
    INSERT INTO ref.hydrometry_site (site_id, site_name, provider, variable, unit, geom, props)
    VALUES (%(site_id)s, %(name)s, %(provider)s, %(variable)s, %(unit)s,
            CASE WHEN %(lon)s::float8 IS NULL OR %(lat)s::float8 IS NULL THEN NULL
                 ELSE ST_SetSRID(ST_MakePoint(%(lon)s::float8, %(lat)s::float8), 4326) END,
            %(props)s::jsonb)
    ON CONFLICT (site_id) DO UPDATE SET
      site_name = EXCLUDED.site_name, provider = EXCLUDED.provider,
      variable = EXCLUDED.variable, unit = EXCLUDED.unit,
      geom = EXCLUDED.geom, props = EXCLUDED.props
"""


def register_sites(stations: dict[str, dict]) -> None:
    def coord(v):
        return float(v) if v not in (None, "") else None

    rows = [
        {
            "site_id": site_id,
            "name": st["name"],
            "provider": PROVIDER,
            "variable": ",".join(st["measures"]),
            "unit": ",".join(st["measures"].values()),
            "lat": coord(st["lat"]),
            "lon": coord(st["lon"]),
            "props": json.dumps({"ts_name": KIWIS_TS_NAME, "measures": st["measures"]}),
        }
        for site_id, st in stations.items()
    ]
    with psycopg.connect(DSN) as conn:
        conn.cursor().executemany(UPSERT_SITE, rows)
    print(f"  registered {len(rows)} sites in ref.hydrometry_site")


def filter_listing(payload, bbox: tuple[float, float, float, float] | None, now: datetime):
    """The series list cut to the area and to series still reporting.

    Keeps the header row, so kiwis_series() and kiwis_stations() read it as
    they read the service's answer. Rows whose coordinates don't parse are
    dropped when there is an area to place them in.
    """
    if not isinstance(payload, list) or not payload:
        raise ValueError(f"unexpected series list: {str(payload)[:200]}")
    header = payload[0]
    cutoff = now - timedelta(days=STALE_DAYS)
    rows = [(row, dict(zip(header, row))) for row in payload[1:]]
    return [header] + [row for row, r in rows if reporting(r, cutoff) and inside(r, bbox)]


def reporting(r: dict, cutoff: datetime) -> bool:
    last = r.get("to") or ""
    return bool(last) and datetime.fromisoformat(last.replace("Z", "+00:00")) >= cutoff


def inside(r: dict, bbox: tuple[float, float, float, float] | None) -> bool:
    if not bbox:
        return True
    try:
        lat, lon = float(r["station_latitude"]), float(r["station_longitude"])
    except (KeyError, TypeError, ValueError):
        return False
    return bbox[0] <= lon <= bbox[2] and bbox[1] <= lat <= bbox[3]


def batches(starts: dict[str, datetime], size: int = BATCH) -> list[tuple[datetime, list[str]]]:
    """ts_ids grouped by when they need reading from, size to a request.

    Sorting by start keeps series that are equally behind together, so a
    batch's from-date (its oldest need) asks for little that is then thrown
    away by newer_than().
    """
    ordered = sorted(starts, key=lambda ts_id: (starts[ts_id], ts_id))
    return [(starts[ordered[i]], ordered[i : i + size]) for i in range(0, len(ordered), size)]


_series: dict[tuple[str, str], str] = {}
_listed_at = 0.0


def kiwis_listing(url: str) -> None:
    """Refresh the series map and site registry when the list is due."""
    global _listed_at
    from storage_poller import kiwis, parse_bbox

    if _series and time.monotonic() - _listed_at < RELIST_S:
        return
    params = {
        "request": "getTimeseriesList",
        "parametertype_name": ",".join(KIWIS_PARAMETERS),
        "ts_name": KIWIS_TS_NAME,
        "returnfields": "station_no,station_name,station_latitude,station_longitude,parametertype_name,ts_id,coverage",
    }
    if sites():
        params["station_no"] = ",".join(sites())
    # Without station numbers the list is every level and flow series the
    # service holds (about 13,000 nationally), which takes over a minute.
    listing = filter_listing(
        kiwis(params, url, timeout=LIST_TIMEOUT_S), parse_bbox(BBOX) if BBOX else None, datetime.now(timezone.utc)
    )
    _series.clear()
    _series.update(kiwis_series(listing))
    _listed_at = time.monotonic()
    print(f"  {len(_series)} series at {len(kiwis_stations(listing))} sites still reporting")
    try:
        register_sites(kiwis_stations(listing))
    except Exception as exc:
        # Names are a convenience; readings still flow without them.
        print(f"  ! could not register sites: {exc}")


def kiwis_fetch(site: str, var_name: str, start: datetime) -> list[dict]:
    """One series, for --probe."""
    from storage_poller import kiwis, parse_values

    url = require_env("HYDRO_URL", "the KiWIS endpoint that serves hydrometry")
    kiwis_listing(url)
    ts_id = _series.get((site, var_name))
    if ts_id is None:
        return []
    values = parse_values(
        kiwis(
            {
                "request": "getTimeseriesValues",
                "ts_id": ts_id,
                "from": start.date().isoformat(),
                "returnfields": "Timestamp,Value",
            },
            url,
        )
    )
    return kiwis_records(values.get(ts_id, []), site, var_name)


def publish_kiwis(stored: dict, floor: datetime) -> int:
    """Every series in the listing, BATCH to a values request."""
    from storage_poller import kiwis, parse_values

    url = require_env("HYDRO_URL", "the KiWIS endpoint that serves hydrometry")
    kiwis_listing(url)
    by_ts = {ts_id: key for key, ts_id in _series.items()}
    lasts = {key: max(filter(None, (stored.get(key), published.get(key))), default=None) for key in _series}
    starts = {ts_id: max(lasts[key], floor) if lasts[key] else floor for key, ts_id in _series.items()}
    total = 0
    for start, ts_ids in batches(starts):
        try:
            values = parse_values(
                kiwis(
                    {
                        "request": "getTimeseriesValues",
                        "ts_id": ",".join(ts_ids),
                        # A plain date: the service is fussy about offsets and fractions.
                        "from": start.date().isoformat(),
                        "returnfields": "Timestamp,Value",
                    },
                    url,
                )
            )
        except Exception as exc:
            print(f"  ! values for {len(ts_ids)} series from {start:%Y-%m-%d}: {exc}")
            continue
        for ts_id in ts_ids:
            site, var_name = by_ts[ts_id]
            records = newer_than(kiwis_records(values.get(ts_id, []), site, var_name), lasts[(site, var_name)])
            total += send_records(site, var_name, records, lasts[(site, var_name)], floor)
    return total


def fetch_records(site: str, variable: str, start: datetime, end: datetime) -> list[dict]:
    return parse_timeseries(fetch_timeseries(site, variable, start, end), site, variable)


def send_records(site: str, var_name: str, records: list[dict], last: datetime | None, floor: datetime) -> int:
    key = (site, var_name)
    for record in records:
        ts = record.pop("event_ts")
        # Flink dates the row by the message timestamp
        # (flink/sql/03_pipelines.sql), so the message carries the
        # measurement's time; sent at publish time, a backfilled week would
        # all land on the minute it was fetched.
        producer.send(
            TOPIC,
            key=f"{site}:{record['variable']}",
            value=envelope(PROVIDER, record, event_ts=ts),
            timestamp_ms=event_ms(ts),
        )
        published[key] = max(published.get(key, last or floor), datetime.fromisoformat(ts))
    return len(records)


def publish() -> int:
    now = datetime.now(timezone.utc)
    floor = now - timedelta(days=MAX_AGE_DAYS)
    try:
        stored = stored_latest()
    except Exception as exc:
        # Postgres down: keep polling from what this process knows.
        print(f"  ! could not read stored readings: {exc}")
        stored = {}
    total = publish_kiwis(stored, floor) if API == "kiwis" else publish_hydstra(stored, floor, now)
    producer.flush()
    return total


def publish_hydstra(stored: dict, floor: datetime, now: datetime) -> int:
    """Each listed site and variable, one get_ts_traces request apiece."""
    total = 0
    for site in sites():
        for variable, (var_name, _u) in VARIABLES.items():
            key = (site, var_name)
            last = max(filter(None, (stored.get(key), published.get(key))), default=None)
            start = max(last, floor) if last else floor
            try:
                records = newer_than(fetch_records(site, variable, start, now), last)
                total += send_records(site, var_name, records, last, floor)
            except Exception as exc:
                print(f"  ! {site}/{variable}: {exc}")
    return total


def main() -> int:
    global producer
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--site", help="gauge id (default: first of HYDRO_SITES)")
    ap.add_argument("--variable", default="141.00")
    args = ap.parse_args()

    if args.probe and API == "kiwis":
        if not (args.site or sites()):
            ap.error("--probe with HYDRO_BBOX needs --site")
        site = args.site or sites()[0]
        start = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
        for _name, (var_name, _unit) in KIWIS_PARAMETERS.items():
            records = kiwis_fetch(site, var_name, start)
            print(f"{site} {var_name}: {len(records)} values, latest {records[-1] if records else '-'}")
        return 0

    if args.probe:
        site = args.site or sites()[0]
        payload = fetch_timeseries(site, args.variable, datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS))
        print(json.dumps(payload, indent=2)[:4000])
        print("\n--- parsed ---")
        print(json.dumps(parse_timeseries(payload, site, args.variable)[:5], indent=2))
        return 0

    producer = make_producer()
    poll_forever(publish, INTERVAL_S, "hydrometry")
    return 0


if __name__ == "__main__":
    sys.exit(main())
