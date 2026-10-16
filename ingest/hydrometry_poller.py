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
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import requests

from common import envelope, make_producer, poll_forever, require_env

PROVIDER = os.environ.get("HYDRO_PROVIDER", "hydrometry")
TOPIC = os.environ.get("HYDRO_TOPIC", "water.hydrometry.raw.v1")
INTERVAL_S = int(os.environ.get("HYDRO_INTERVAL_S", "900"))


def sites() -> list[str]:
    """Gauge numbers for the catchments you care about — see .env.example."""
    return [s.strip() for s in require_env(
        "HYDRO_SITES", "a comma-separated list of gauge ids").split(",") if s.strip()]

VARIABLES = {
    "100.00": ("level_m", "m"),
    "141.00": ("flow_cumecs", "m3/s"),
    "10.00":  ("rainfall_mm", "mm"),
}

producer = None


def fetch_timeseries(site: str, variable: str, hours: int = 6) -> dict:
    """Kisters webservices take the query as a JSON 'params' argument."""
    url = require_env("HYDRO_URL", "the hydrometry webservice endpoint")
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)
    params = {
        "function": "get_ts_traces",
        "version": "2",
        "params": json.dumps({
            "site_list": site,
            "datasource": "A",
            "varfrom": variable,
            "varto": variable,
            "start_time": start.strftime("%Y%m%d%H%M%S"),
            "end_time": end.strftime("%Y%m%d%H%M%S"),
            "data_type": "mean",
            "interval": "hour",
            "multiplier": 1,
        }),
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def parse_timeseries(payload: dict, site: str, variable: str) -> list[dict]:
    """ADAPTER — confirm against a --probe response and adjust if needed."""
    var_name, unit = VARIABLES.get(variable, (variable, ""))
    out = []
    traces = (payload.get("return") or {}).get("traces") or []
    for trace in traces:
        for point in trace.get("trace") or []:
            raw_t = str(point.get("t"))
            try:
                ts = datetime.strptime(raw_t, "%Y%m%d%H%M%S").replace(
                    tzinfo=timezone.utc).isoformat()
            except ValueError:
                continue
            out.append({
                "site_id": site,
                "variable": var_name,
                "unit": unit,
                "value": point.get("v"),
                "quality_code": str(point.get("q", "")),
                "provider": PROVIDER,
                "event_ts": ts,
            })
    return out


def publish() -> int:
    total = 0
    for site in sites():
        for variable in VARIABLES:
            try:
                payload = fetch_timeseries(site, variable)
                for record in parse_timeseries(payload, site, variable):
                    ts = record.pop("event_ts")
                    producer.send(
                        TOPIC, key=f"{site}:{record['variable']}",
                        value=envelope(PROVIDER, record, event_ts=ts),
                    )
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
        payload = fetch_timeseries(site, args.variable)
        print(json.dumps(payload, indent=2)[:4000])
        print("\n--- parsed ---")
        print(json.dumps(parse_timeseries(payload, site, args.variable)[:5], indent=2))
        return 0

    producer = make_producer()
    poll_forever(publish, INTERVAL_S, "hydrometry")
    return 0


if __name__ == "__main__":
    sys.exit(main())
