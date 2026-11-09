"""Shared helpers for the real-source pollers."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "broker:29092")

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def require_env(name: str, hint: str) -> str:
    """Region-specific settings are config, never defaults baked into code."""
    val = os.environ.get(name)
    if not val:
        raise SystemExit(f"! {name} is not set — {hint}\n  see .env.example")
    return val


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode() if k else None,
        retries=5,
        linger_ms=200,
    )


def iso_z(when: datetime) -> str:
    """UTC with a trailing Z, the one form Flink's ISO-8601 reader accepts
    (a +00:00 offset reads as NULL). Used for every timestamp a feed publishes."""
    return when.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def envelope(source: str, payload: dict, event_ts: str | None = None) -> dict:
    """Every real feed carries the same wrapper as the synthetic ones.

    ingest_ts is when this process received the reading and event_ts when the
    source measured it; Flink dates the stored row by event_ts."""
    now = iso_z(datetime.now(timezone.utc))
    return {
        "source": source,
        "ingest_ts": now,
        "event_ts": event_ts or now,
        "is_synthetic": False,
        **payload,
    }


def poll_forever(fn, interval_s: int, name: str) -> None:
    """Run fn() on a fixed interval, logging and surviving failures."""
    log = logging.getLogger(name)
    while True:
        started = time.time()
        try:
            count = fn()
            log.info("published %s records", count)
        except Exception:
            log.exception("poll failed; continuing")
        elapsed = time.time() - started
        time.sleep(max(1.0, interval_s - elapsed))
