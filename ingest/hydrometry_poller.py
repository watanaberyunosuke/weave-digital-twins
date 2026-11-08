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
poller comes back, which is the only fill a real feed gets, and overlapping
polls write nothing twice into twin.hydrometry, which has no key.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg
import requests

from common import envelope, make_producer, poll_forever, require_env

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
KIWIS_PARAMETERS = {
    "Water Course Level": ("level_m", "m"),
    "Water Course Discharge": ("flow_cumecs", "m3/s"),
}


def sites() -> list[str]:
    """Gauge numbers for the catchments you care about — see .env.example."""
    return [
        s.strip() for s in require_env("HYDRO_SITES", "a comma-separated list of gauge ids").split(",") if s.strip()
    ]


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
            """SELECT site_id, variable, max(ts) FROM twin.hydrometry
               WHERE ts > now() - make_interval(secs => %s) AND NOT is_synthetic
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
                ts = datetime.strptime(raw_t, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).isoformat()
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
            "event_ts": ts.astimezone(timezone.utc).isoformat(),
        }
        for ts, value in points
    ]


_series: dict[tuple[str, str], str] = {}


def kiwis_fetch(site: str, var_name: str, start: datetime) -> list[dict]:
    from storage_poller import kiwis, parse_values

    url = require_env("HYDRO_URL", "the KiWIS endpoint that serves hydrometry")
    if not _series:
        # One list for every gauge, kept for the life of the process.
        _series.update(
            kiwis_series(
                kiwis(
                    {
                        "request": "getTimeseriesList",
                        "station_no": ",".join(sites()),
                        "parametertype_name": ",".join(KIWIS_PARAMETERS),
                        "ts_name": KIWIS_TS_NAME,
                        "returnfields": "station_no,parametertype_name,ts_id",
                    },
                    url,
                )
            )
        )
    ts_id = _series.get((site, var_name))
    if ts_id is None:
        return []
    values = parse_values(
        kiwis(
            {
                "request": "getTimeseriesValues",
                "ts_id": ts_id,
                # A plain date: the service is fussy about offsets and fractions.
                "from": start.date().isoformat(),
                "returnfields": "Timestamp,Value",
            },
            url,
        )
    )
    return kiwis_records(values.get(ts_id, []), site, var_name)


def fetch_records(site: str, variable: str, var_name: str, start: datetime, end: datetime) -> list[dict]:
    if API == "kiwis":
        return kiwis_fetch(site, var_name, start)
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
    total = 0
    now = datetime.now(timezone.utc)
    floor = now - timedelta(days=MAX_AGE_DAYS)
    try:
        stored = stored_latest()
    except Exception as exc:
        # Postgres down: keep polling from what this process knows.
        print(f"  ! could not read stored readings: {exc}")
        stored = {}
    # KiWIS has no rainfall at a stream gauge, so it maps level and flow only.
    variables = (
        [(None, name) for name, _u in KIWIS_PARAMETERS.values()]
        if API == "kiwis"
        else [(v, name) for v, (name, _u) in VARIABLES.items()]
    )
    for site in sites():
        for variable, var_name in variables:
            key = (site, var_name)
            last = max(filter(None, (stored.get(key), published.get(key))), default=None)
            start = max(last, floor) if last else floor
            try:
                records = newer_than(fetch_records(site, variable, var_name, start, now), last)
                total += send_records(site, var_name, records, last, floor)
            except Exception as exc:
                print(f"  ! {site}/{variable or var_name}: {exc}")
    producer.flush()
    return total


def main() -> int:
    global producer
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--site", help="gauge id (default: first of HYDRO_SITES)")
    ap.add_argument("--variable", default="141.00")
    args = ap.parse_args()

    if args.probe and API == "kiwis":
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
