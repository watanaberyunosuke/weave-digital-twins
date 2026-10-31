"""Roles, the demo session and the relay token for the public console.

Standard library only, so it is testable without FastAPI and adds nothing to
the hash-locked requirements.

The console runs in one of two modes. In admin mode (the default) it is the
existing loopback console: whoever reaches it is the admin, and on the hosted
stack Cloudflare Access is what decides who reaches it. In public mode it is
open to anyone, read-only, with a demo login whose password ("demo" unless
set otherwise) is printed on the login form. The password is not a secret: it
only marks the switch from looking to trying, so nothing here guards it. What
guards the live stack is that a demo user's real runs are limited to
SAFE_ACTIONS and to one per interval across all demo users; every other
action is simulated.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time

PUBLIC, DEMO, ADMIN = "public", "demo", "admin"

# Actions a demo user runs for real. Each one leaves nothing that needs
# undoing: discover only reads the region's catalogue, and the simulator
# already runs a solve on its own interval. Everything else changes what other
# viewers see, so a demo user only ever gets a simulation of it.
SAFE_ACTIONS = frozenset({"discover", "run_sim_once"})

SESSION_COOKIE = "weave_demo"
SESSION_TTL_S = 8 * 3600


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def password_matches(given: str, expected: str) -> bool:
    return hmac.compare_digest(given.encode(), expected.encode())


def sign(payload: dict, secret: str) -> str:
    body = _b64(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode())
    mac = _b64(hmac.new(secret.encode(), body.encode(), hashlib.sha256).digest())
    return f"{body}.{mac}"


def unsign(token: str, secret: str, now: float | None = None) -> dict | None:
    """The payload, or None when the token is forged, malformed or expired."""
    try:
        body, mac = token.split(".")
    except (AttributeError, ValueError):
        return None
    want = _b64(hmac.new(secret.encode(), body.encode(), hashlib.sha256).digest())
    if not hmac.compare_digest(mac, want):
        return None
    try:
        payload = json.loads(_unb64(body))
    except ValueError:
        return None
    now = time.time() if now is None else now
    if not isinstance(payload, dict) or payload.get("exp", 0) < now:
        return None
    return payload


def new_session(secret: str, now: float | None = None) -> tuple[str, str]:
    """A fresh session id and its signed cookie value.

    Every login gets a new id, and simulated history is kept per id, so a
    login always starts from a clean slate. The cookie is signed so a visitor
    can't pick another session's id and read its history.
    """
    now = time.time() if now is None else now
    sid = secrets.token_hex(12)
    return sid, sign({"sid": sid, "exp": int(now + SESSION_TTL_S)}, secret)


def token_matches(given: str | None, expected: str) -> bool:
    return bool(expected) and bool(given) and hmac.compare_digest(given, expected)


class Cooldown:
    """At most one real run per key per interval.

    Logging in is free, so a per-session limit alone would let anyone open
    sessions and run continuously; callers use one shared key for that.
    """

    def __init__(self, interval_s: int = 60):
        self.interval_s = interval_s
        self._last: dict[str, float] = {}

    def wait_s(self, key: str, now: float | None = None) -> int:
        if key not in self._last:
            return 0
        now = time.time() if now is None else now
        left = self.interval_s - (now - self._last[key])
        return max(0, int(left + 0.999))

    def mark(self, key: str, now: float | None = None) -> None:
        self._last[key] = time.time() if now is None else now
