"""Roles, demo login and the relay token for the public console.

Standard library only, so it is testable without FastAPI and adds nothing to
the hash-locked requirements.

The console runs in one of two modes. In admin mode (the default) it is the
existing loopback console: whoever reaches it is the admin, and on the hosted
stack Cloudflare Access is what decides who reaches it. In public mode it is
open to anyone, read-only, with one optional demo login. A demo user's safe
actions are relayed to the admin console; every other action is simulated.

Create a demo password hash for .env with:

    python console/access.py hash
"""

from __future__ import annotations

import base64
import getpass
import hashlib
import hmac
import json
import secrets
import sys
import time
from collections import defaultdict, deque

PUBLIC, DEMO, ADMIN = "public", "demo", "admin"

# Actions a demo user runs for real. Each one leaves nothing that needs
# undoing: discover only reads the region's catalogue, and the simulator
# already runs a solve on its own interval. Everything else changes what other
# viewers see, so a demo user only ever gets a simulation of it.
SAFE_ACTIONS = frozenset({"discover", "run_sim_once"})

SESSION_COOKIE = "weave_demo"
SESSION_TTL_S = 8 * 3600

# scrypt at these costs takes tens of milliseconds and 16 MiB, which keeps a
# guessing run slow without letting a burst of logins exhaust memory.
_SCRYPT = {"n": 2 ** 14, "r": 8, "p": 1}


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def hash_password(password: str, salt: bytes | None = None) -> str:
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, dklen=32, **_SCRYPT)
    # Colon-separated: a "$" in .env would be read by compose as a variable.
    return f"scrypt:{_SCRYPT['n']}:{_SCRYPT['r']}:{_SCRYPT['p']}:{_b64(salt)}:{_b64(digest)}"


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, n, r, p, salt, digest = stored.split(":")
        if scheme != "scrypt":
            return False
        got = hashlib.scrypt(password.encode(), salt=_unb64(salt), dklen=32,
                             n=int(n), r=int(r), p=int(p))
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(got, _unb64(digest))


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
    if not isinstance(payload, dict) or payload.get("exp", 0) < (time.time() if now is None else now):
        return None
    return payload


def new_session(secret: str, now: float | None = None) -> tuple[str, str]:
    """A fresh session id and its signed cookie value.

    Every login gets a new id, and simulated history is kept per id, so a
    login always starts from a clean slate.
    """
    sid = secrets.token_hex(12)
    return sid, sign({"sid": sid, "exp": int((time.time() if now is None else now) + SESSION_TTL_S)}, secret)


def token_matches(given: str | None, expected: str) -> bool:
    return bool(expected) and bool(given) and hmac.compare_digest(given, expected)


class Throttle:
    """Failed logins per client, plus a global ceiling.

    The client key comes from a header Cloudflare sets, which anyone could
    forge on a direct connection; the global ceiling still holds then, and the
    hosted console is reachable only through the tunnel anyway.
    """

    def __init__(self, per_key: int = 5, overall: int = 50, window_s: int = 900):
        self.per_key, self.overall, self.window_s = per_key, overall, window_s
        self._fails: dict[str, deque] = defaultdict(deque)
        self._all: deque = deque()

    def _trim(self, q: deque, now: float) -> None:
        while q and now - q[0] > self.window_s:
            q.popleft()

    def blocked(self, key: str, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        self._trim(self._fails[key], now)
        self._trim(self._all, now)
        return len(self._fails[key]) >= self.per_key or len(self._all) >= self.overall

    def fail(self, key: str, now: float | None = None) -> None:
        now = time.time() if now is None else now
        self._fails[key].append(now)
        self._all.append(now)

    def succeed(self, key: str) -> None:
        self._fails.pop(key, None)


class Cooldown:
    """At most one real run per session per interval."""

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
        if len(self._last) > 10_000:   # sessions expire; their entries need not stay
            cutoff = (time.time() if now is None else now) - self.interval_s
            self._last = {k: v for k, v in self._last.items() if v > cutoff}


def _main(argv: list[str]) -> int:
    if argv[1:] != ["hash"]:
        print("usage: python console/access.py hash", file=sys.stderr)
        return 2
    pw = getpass.getpass("Demo password: ")
    if len(pw) < 12 or pw != getpass.getpass("Again: "):
        print("passwords differ or are shorter than 12 characters", file=sys.stderr)
        return 1
    print(hash_password(pw))
    print(f"session secret suggestion: {secrets.token_urlsafe(32)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv))
