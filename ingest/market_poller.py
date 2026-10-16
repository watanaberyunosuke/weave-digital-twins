#!/usr/bin/env python3
"""Poll a wholesale electricity market summary and publish one region's
dispatch to Kafka.

Real data, no authentication, 5-minute cadence. Written against AEMO's public
NEM summary report, which is the reference implementation — the endpoint and
the market region are both env config, so neither this file nor anything
downstream names a place.

    MARKET_DISPATCH_URL=...  MARKET_REGION=...  python ingest/market_poller.py

Endpoints move: AEMO has been migrating NEMweb during 2026, so a migration is
a .env change, not a code change. Run with --probe to dump one raw response
and check the field names still line up with parse().
"""

from __future__ import annotations

import argparse
import os
import sys

import requests

from common import envelope, make_producer, poll_forever, require_env

TOPIC = os.environ.get("MARKET_TOPIC", "electricity.market-dispatch.raw.v1")
INTERVAL_S = int(os.environ.get("MARKET_INTERVAL_S", "300"))

producer = None


def fetch() -> list[dict]:
    url = require_env("MARKET_DISPATCH_URL", "the market summary endpoint")
    resp = requests.post(url, json={"timeScale": ["5MIN"]}, timeout=30)
    if resp.status_code == 405:                     # some deployments want GET
        resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    # The summary array has been published under a couple of different keys.
    for key in ("ELEC_NEM_SUMMARY", "data", "records"):
        if isinstance(payload, dict) and key in payload:
            return payload[key]
    if isinstance(payload, list):
        return payload
    raise RuntimeError(f"unexpected payload shape: {list(payload)[:5]}")


def num(row: dict, *names):
    for n in names:
        if n in row and row[n] is not None:
            try:
                return float(row[n])
            except (TypeError, ValueError):
                pass
    return None


def publish() -> int:
    region = require_env("MARKET_REGION", "the market region id to publish")
    rows = [r for r in fetch() if r.get("REGIONID") == region]
    for row in rows:
        msg = envelope("market_summary", {
            "region_id": row.get("REGIONID"),
            "settlement_ts": row.get("SETTLEMENTDATE"),
            "price_mwh": num(row, "PRICE", "RRP"),
            "total_demand_mw": num(row, "TOTALDEMAND"),
            "net_interchange_mw": num(row, "NETINTERCHANGE"),
            "scheduled_generation_mw": num(row, "SCHEDULEDGENERATION"),
            "semi_scheduled_generation_mw": num(row, "SEMISCHEDULEDGENERATION"),
        }, event_ts=row.get("SETTLEMENTDATE"))
        producer.send(TOPIC, key=row.get("REGIONID"), value=msg)
    producer.flush()
    return len(rows)


def main() -> int:
    global producer
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", action="store_true",
                    help="print one raw response and exit")
    args = ap.parse_args()

    if args.probe:
        import json
        rows = fetch()
        print(json.dumps(rows[:3], indent=2))
        return 0

    producer = make_producer()
    poll_forever(publish, INTERVAL_S, "market")
    return 0


if __name__ == "__main__":
    sys.exit(main())
