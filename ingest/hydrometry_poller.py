#!/usr/bin/env python3
"""Poll real water telemetry into Kafka.

Written against a Kisters-style hydrometry webservice, which is what most
public water agencies expose. The endpoint, the provider label and the gauge
list are all env config, so this adapter carries no region of its own.

IMPORTANT: the exact parameter names and response shape vary between
deployments and should be confirmed against a live response before you rely on
them. Run:

    python ingest/hydrometry_poller.py --probe --site <gauge>

which prints the raw response so you can adjust parse_timeseries() below.
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
# Seven days matches the residual baseline and what Elasticsearch keeps.
MAX_AGE_DAYS = float(os.environ.get("HYDRO_MAX_AGE_DAYS", "7"))
# Without a stored reading, a fresh stack starts this far back.
FIRST_HOURS = float(os.environ.get("HYDRO_FIRST_HOURS", "24"))


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
    for site in sites():
        for variable, (var_name, _unit) in VARIABLES.items():
            key = (site, var_name)
            last = max(filter(None, (stored.get(key), published.get(key))), default=None)
            start = max(last, floor) if last else now - timedelta(hours=FIRST_HOURS)
            try:
                payload = fetch_timeseries(site, variable, start, now)
                records = newer_than(parse_timeseries(payload, site, variable), last)
                for record in records:
                    ts = record.pop("event_ts")
                    producer.send(
                        TOPIC,
                        key=f"{site}:{record['variable']}",
                        value=envelope(PROVIDER, record, event_ts=ts),
                    )
                    published[key] = max(published.get(key, last or floor), datetime.fromisoformat(ts))
                    total += 1
            except Exception as exc:
                print(f"  ! {site}/{variable}: {exc}")
    producer.flush()
    return total


def main() -> int:
    global producer
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--site", help="gauge id (default: first of HYDRO_SITES)")
    ap.add_argument("--variable", default="141.00")
    args = ap.parse_args()

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
