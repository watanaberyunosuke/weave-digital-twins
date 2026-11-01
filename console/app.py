#!/usr/bin/env python3
"""Weave console: one page to see the twin and run its pipeline steps.

    uvicorn app:app --host 0.0.0.0 --port 8090

Reads go straight to Postgres in read-only transactions, plus the Connect,
Flink and Kafka endpoints for pipeline state. Writes never happen here: every
operation is one of the documented commands (the ones bootstrap.sh runs),
executed as-is through the Docker CLI, so the console can never drift from
what the scripts do. That needs the Docker socket, which is root on the host,
so the operations are a fixed allow-list with validated parameters, the port
binds to loopback, and requests are checked for Host and a custom header.

Nothing here names a place. The map fits to whatever geometry is loaded, and
the only region input is the layer list read from GIS_REGION_CONFIG.

CONSOLE_MODE=public serves the same page to anyone, read-only and without the
Docker socket, with demo and admin logins (see access.py); the admin login
acts through the admin console's relay routes. Admin mode, the default, is
the loopback console and is unchanged.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import subprocess
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import access
import demo
import httpx
import psycopg
import yaml
from psycopg.rows import dict_row
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

DSN = os.environ.get("TWIN_DSN", "postgresql://weave:weave@postgres:5432/weave_twin")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://connect:8083")
FLINK_URL = os.environ.get("FLINK_URL", "http://flink-jobmanager:9081")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "broker:29092")
WEAVE_DIR = Path(os.environ.get("WEAVE_DIR", "/weave"))
INP_PATH = Path(os.environ.get("INP_PATH", "/sim/network.inp"))
REGION_CONFIG = os.environ.get("GIS_REGION_CONFIG", "")
ALLOWED_HOSTS = {h.strip() for h in
                 os.environ.get("CONSOLE_ALLOWED_HOSTS", "localhost,127.0.0.1").split(",")}
STATIC = Path(__file__).parent / "static"

MODE = os.environ.get("CONSOLE_MODE", "admin")
if MODE not in ("admin", "public"):
    raise SystemExit(f"CONSOLE_MODE must be admin or public, not {MODE!r}")
PUBLIC = MODE == "public"
# Public mode: the demo login is open to everyone and its password is shown
# on the form, so it is configuration, not a secret (see access.py).
DEMO_ENABLED = PUBLIC and os.environ.get("CONSOLE_DEMO", "1") != "0"
DEMO_PASSWORD = os.environ.get("CONSOLE_DEMO_PASSWORD", "demo")
# The admin login on the public page: the operator's password as a scrypt
# hash, optionally with an authenticator secret. It is on only when the relay
# to the admin console is configured, since that is the only way it can act.
ADMIN_HASH = os.environ.get("CONSOLE_ADMIN_PASSWORD_HASH", "")
ADMIN_TOTP = os.environ.get("CONSOLE_ADMIN_TOTP_SECRET", "")
# Signs session cookies. Unset, a fresh one per start: a restart then just
# logs everyone out, which costs nothing because their history is in memory.
SESSION_SECRET = os.environ.get("CONSOLE_SESSION_SECRET") or secrets.token_urlsafe(32)
# Plain http on loopback while developing; the hosted console is https only.
COOKIE_SECURE = os.environ.get("CONSOLE_COOKIE_SECURE", "1") != "0"
# The public console reaches the admin console through RELAY_URL. Two tokens,
# both shared by the two consoles: RELAY_TOKEN admits demo users' safe actions
# only; RELAY_ADMIN_TOKEN admits every action, for the admin login. On the
# admin side each token alone turns its routes on.
RELAY_URL = os.environ.get("CONSOLE_RELAY_URL", "").rstrip("/")
RELAY_TOKEN = os.environ.get("CONSOLE_RELAY_TOKEN", "")
RELAY_ADMIN_TOKEN = os.environ.get("CONSOLE_RELAY_ADMIN_TOKEN", "")
for _name, _tok in (("CONSOLE_RELAY_TOKEN", RELAY_TOKEN),
                    ("CONSOLE_RELAY_ADMIN_TOKEN", RELAY_ADMIN_TOKEN)):
    if _tok and len(_tok) < 32:
        raise SystemExit(f"{_name} must be at least 32 characters")
if RELAY_TOKEN and RELAY_TOKEN == RELAY_ADMIN_TOKEN:
    raise SystemExit("CONSOLE_RELAY_TOKEN and CONSOLE_RELAY_ADMIN_TOKEN must differ")
NOT_FOUND, NO_SUCH_JOB = "not found", "no such job"
# Job ids are uuid4 hex prefixes; anything else is refused before it can
# become part of a relayed path.
JOB_ID = re.compile(r"[0-9a-f]{1,32}")
ADMIN_ENABLED = PUBLIC and bool(ADMIN_HASH) and bool(RELAY_URL) and bool(RELAY_ADMIN_TOKEN)

# The six topics create-topics.sh makes; a step is done when all exist.
TOPICS = [
    "water.hydrometry.raw.v1", "electricity.market-dispatch.raw.v1",
    "transport.vehicle-positions.raw.v1", "water.meter-readings.raw.v1",
    "water.pressure-readings.raw.v1", "electricity.meter-interval.raw.v1",
]

# Telemetry tables and the column that dates a row. The origin label is shown
# beside each so the page never presents synthetic data as observed.
# twin.sim_result is left out for the same reason Logstash skips it: volume,
# and no index on ts.
TELEMETRY = [
    ("hydrometry", "ts", "real"),
    ("storage_level", "ts", "real"),
    ("market_dispatch", "ts", "real"),
    ("vehicle_position", "ts", "real"),
    ("meter_reading", "ts", "synthetic"),
    ("pressure_reading", "ts", "synthetic"),
    ("electricity_interval", "ts", "synthetic"),
    ("dma_balance", "window_start", "derived"),
    ("sim_residual", "ts", "model"),
]

app = FastAPI(title="Weave console", docs_url=None, redoc_url=None)


# ------------------------------------------------------------------ guards
@app.middleware("http")
async def guard(request: Request, call_next):
    # Host check stops DNS rebinding; the header forces a CORS preflight that
    # this app never answers, so another site open in the browser can't
    # trigger an operation through it.
    host = (request.headers.get("host") or "").rsplit(":", 1)[0].strip("[]")
    if host not in ALLOWED_HOSTS:
        return JSONResponse({"detail": "host not allowed"}, status_code=403)
    if request.method == "POST" and request.headers.get("x-weave-console") != "1":
        return JSONResponse({"detail": "missing X-Weave-Console header"}, status_code=403)
    response = await call_next(request)
    # The page has no build step to fingerprint file names, so browsers are
    # told to revalidate; otherwise a rebuilt console serves stale JS or CSS.
    if request.url.path == "/" or request.url.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache"
    return response


def _session(request: Request) -> dict | None:
    return access.unsign(request.cookies.get(access.SESSION_COOKIE, ""), SESSION_SECRET) \
        if PUBLIC else None


def role(request: Request) -> str:
    if not PUBLIC:
        return access.ADMIN
    who = (_session(request) or {}).get("role")
    # A login that has since been switched off stops counting at once.
    if who == access.DEMO and DEMO_ENABLED:
        return access.DEMO
    if who == access.ADMIN and ADMIN_ENABLED:
        return access.ADMIN
    return access.PUBLIC


def session_id(request: Request) -> str | None:
    payload = _session(request)
    return payload["sid"] if payload else None


def remote_admin(request: Request) -> bool:
    """An admin signed in on the public console, acting through the relay."""
    return PUBLIC and role(request) == access.ADMIN


def admin_only():
    # Not routed in public mode at all, so the public console can't leak
    # database connection details or anything else meant for the operator.
    if PUBLIC:
        raise HTTPException(404, NOT_FOUND)


def _bearer(request: Request) -> str:
    return request.headers.get("authorization", "").removeprefix("Bearer ")


def relay_auth(request: Request):
    if PUBLIC or not access.token_matches(_bearer(request), RELAY_TOKEN):
        raise HTTPException(404, NOT_FOUND)


def relay_admin_auth(request: Request):
    if PUBLIC or not access.token_matches(_bearer(request), RELAY_ADMIN_TOKEN):
        raise HTTPException(404, NOT_FOUND)


def relay_any_auth(request: Request):
    tok = _bearer(request)
    if PUBLIC or not (access.token_matches(tok, RELAY_TOKEN)
                      or access.token_matches(tok, RELAY_ADMIN_TOKEN)):
        raise HTTPException(404, NOT_FOUND)


def client_key(request: Request) -> str:
    return request.headers.get("cf-connecting-ip") or (request.client.host if request.client else "?")


def relay_path(prefix: str, ident: str) -> str:
    """A relay route with one caller-supplied segment, escaped so it stays one."""
    return f"{prefix}/{quote(ident, safe='')}"


async def relay_request(method: str, path: str, admin: bool = False, **kwargs) -> httpx.Response:
    """A call to the admin console; the token decides what it may do there."""
    token = RELAY_ADMIN_TOKEN if admin else RELAY_TOKEN
    headers = {"Authorization": f"Bearer {token}", "X-Weave-Console": "1"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            return await client.request(method, f"{RELAY_URL}{path}", headers=headers, **kwargs)
    except httpx.HTTPError as exc:
        raise HTTPException(503, "the live stack is not reachable right now") from exc


def relay_error(r: httpx.Response) -> HTTPException:
    try:
        detail = r.json().get("detail", "")
    except ValueError:
        detail = ""
    # Refusals the user can act on keep their status; anything else is the
    # admin console being unavailable, whatever it said.
    code = r.status_code if r.status_code in (409, 422, 429) else 503
    return HTTPException(code, detail or "the live stack refused the action")


# --------------------------------------------------------------- db helpers
def query(sql: str, params: tuple | dict = ()) -> list[dict]:
    with psycopg.connect(DSN, connect_timeout=3) as conn:
        conn.read_only = True
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def scalar(sql: str, params: tuple | dict = ()):
    rows = query(sql, params)
    return next(iter(rows[0].values())) if rows else None


def geojson(sql: str, params: tuple | dict = ()) -> JSONResponse:
    # Built in SQL so large layers never round-trip through Python objects.
    wrapped = f"""
        SELECT json_build_object('type', 'FeatureCollection',
               'features', COALESCE(json_agg(f.feature), '[]'::json)) AS fc
        FROM ({sql}) f
    """
    try:
        return JSONResponse(scalar(wrapped, params))
    except psycopg.Error as exc:
        raise HTTPException(503, f"postgres: {exc}") from exc


def safe(fn, default=None):
    """Status probes report a failure rather than failing the whole page."""
    try:
        return {"ok": True, "value": fn()}
    except Exception as exc:  # noqa: BLE001 — every probe failure is reported
        return {"ok": False, "error": str(exc).splitlines()[0][:300], "value": default}


# ------------------------------------------------------------------- probes
def probe_db() -> dict:
    row = query("""
        SELECT (SELECT count(*) FROM asset.pipe)                                AS pipes,
               (SELECT count(*) FROM asset.pipe WHERE start_node IS NOT NULL)   AS pipes_in_graph,
               (SELECT count(*) FROM asset.node)                                AS nodes,
               (SELECT count(*) FROM asset.facility)                            AS facilities,
               (SELECT count(*) FROM asset.dma)                                 AS dmas,
               (SELECT count(*) FROM ref.water_meter)                           AS water_meters,
               (SELECT count(*) FROM ref.pressure_logger)                       AS pressure_loggers,
               (SELECT count(*) FROM ref.electricity_meter)                     AS electricity_meters,
               (SELECT count(*) FROM ref.hydrometry_site)                       AS hydrometry_sites,
               (SELECT count(*) FROM asset.substation)                          AS substations,
               (SELECT max(ts) FROM twin.sim_residual)                          AS last_residual_ts
    """)[0]
    row["dma_status"] = {r["status"]: r["n"] for r in query(
        "SELECT COALESCE(status, 'UNKNOWN') AS status, count(*) AS n "
        "FROM twin.dma_latest GROUP BY 1")}
    return row


def probe_topics() -> list[str]:
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP,
                             request_timeout_ms=4000, api_version_auto_timeout_ms=3000)
    try:
        return sorted(consumer.topics())
    finally:
        consumer.close()


def probe_connectors() -> list[dict]:
    r = httpx.get(f"{CONNECT_URL}/connectors", params={"expand": "status"}, timeout=3)
    r.raise_for_status()
    out = []
    for name, body in r.json().items():
        st = body.get("status", {})
        tasks = st.get("tasks", [])
        out.append({"name": name,
                    "state": st.get("connector", {}).get("state"),
                    "tasks": [t.get("state") for t in tasks]})
    return sorted(out, key=lambda c: c["name"])


def probe_flink() -> list[dict]:
    r = httpx.get(f"{FLINK_URL}/jobs/overview", timeout=3)
    r.raise_for_status()
    return [{"id": j["jid"], "name": j["name"], "state": j["state"],
             "start": j.get("start-time")} for j in r.json().get("jobs", [])]


def probe_containers() -> list[dict]:
    project = own_compose_project()
    args = ["docker", "ps", "-a", "--format", "{{json .}}"]
    if project:
        args[3:3] = ["--filter", f"label=com.docker.compose.project={project}"]
    out = subprocess.run(args, capture_output=True, text=True, timeout=5, check=True).stdout
    rows = [json.loads(line) for line in out.splitlines() if line.strip()]
    return sorted(({"name": r["Names"], "state": r["State"], "status": r["Status"]}
                   for r in rows), key=lambda c: c["name"])


_project_cache: dict[str, str | None] = {}


def own_compose_project() -> str | None:
    # Only list this stack's containers, whatever the compose project is
    # called on this machine.
    if "p" not in _project_cache:
        try:
            _project_cache["p"] = subprocess.run(
                ["docker", "inspect", os.environ.get("HOSTNAME", ""), "--format",
                 '{{index .Config.Labels "com.docker.compose.project"}}'],
                capture_output=True, text=True, timeout=5, check=True).stdout.strip() or None
        except Exception:  # noqa: BLE001 — fall back to listing everything
            _project_cache["p"] = None
    return _project_cache["p"]


def probe_model() -> dict:
    if not INP_PATH.exists():
        return {"built": False}
    st = INP_PATH.stat()
    return {"built": True, "size": st.st_size,
            "mtime": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat()}


def probe_schemas() -> list[str]:
    return sorted(p.name for p in (WEAVE_DIR / "schemas").glob("*.avsc"))


_status_cache: dict = {"at": 0.0, "body": None}


@app.get("/api/status")
async def status(request: Request):
    body = await shared_status()
    # Container names are the operator's: the public page shows them only to
    # the admin login, from the same cached body everyone else gets.
    if PUBLIC and not remote_admin(request):
        body = body | {"containers": PUBLIC_CONTAINERS}
    return body


PUBLIC_CONTAINERS = {"ok": True, "value": [], "hidden": True}


async def shared_status():
    # Probes have multi-second timeouts when a service is down, so they run
    # concurrently and the result is shared for a few seconds between tabs.
    if _status_cache["body"] and time.monotonic() - _status_cache["at"] < 5:
        return _status_cache["body"]
    if PUBLIC:
        body = await public_status()
    else:
        names = ["db", "topics", "connectors", "flink", "containers", "model", "schemas"]
        fns = [probe_db, probe_topics, probe_connectors, probe_flink,
               probe_containers, probe_model, probe_schemas]
        results = await asyncio.gather(*(asyncio.to_thread(safe, f) for f in fns))
        body = dict(zip(names, results))
        body["expected_topics"] = TOPICS
        body["job"] = current_job_summary()
    _status_cache.update(at=time.monotonic(), body=body)
    return body


async def public_status() -> dict:
    """The admin console's view; status() hides its container names from all
    but the admin login.

    The public console has no Docker socket, no schemas/ and no model file,
    and the Kafka, Connect and Flink endpoints are on the other host, so its
    own probes would misreport every step. With no relay configured it
    reports what it can see and marks the rest unavailable.
    """
    if RELAY_URL and (RELAY_TOKEN or RELAY_ADMIN_TOKEN):
        try:
            r = await relay_request("GET", "/api/relay/status", admin=not RELAY_TOKEN)
            r.raise_for_status()
            return r.json()
        except (HTTPException, httpx.HTTPError, ValueError) as exc:
            print(f"! relay status failed: {exc}")   # falls back to local probes
    gone = {"ok": False, "error": "not visible from the public console", "value": None}
    return {"db": await asyncio.to_thread(safe, probe_db), "topics": gone,
            "connectors": gone, "flink": gone, "containers": PUBLIC_CONTAINERS,
            "model": gone, "schemas": gone, "expected_topics": TOPICS, "job": None}


@app.get("/api/config")
def config(request: Request):
    providers = basemaps()
    ids = [b["id"] for b in providers]
    default = os.environ.get("CONSOLE_BASEMAP") or ("custom" if "custom" in ids else "none")
    return {
        "region_config": REGION_CONFIG,
        "layers": region_layers(),
        "basemaps": providers,
        "basemap_default": default if default in ids else "none",
        "tile_style": os.environ.get("CONSOLE_TILE_STYLE", "auto"),   # auto | dark | light
        "mode": MODE,
        # The admin tools are the operator's, so the public page shows their
        # links only to the admin login. The GIS connection details stay on
        # the loopback console: they name the database host and its password.
        **(admin_links() if role(request) == access.ADMIN else {}),
        **({} if PUBLIC else {"gis": gis_connections()}),
    }


def admin_links() -> dict:
    return {
        "kibana_url": os.environ.get("KIBANA_PUBLIC_URL", "http://localhost:5601"),
        "flink_url": os.environ.get("FLINK_PUBLIC_URL", "http://localhost:9081"),
        "control_center_url": os.environ.get("CONTROL_CENTER_PUBLIC_URL",
                                             "http://localhost:9021"),
    }


@app.get("/api/session")
def get_session(request: Request):
    return {"mode": MODE, "role": role(request), "demo_enabled": DEMO_ENABLED,
            "demo_password": DEMO_PASSWORD if DEMO_ENABLED else None,
            "admin_enabled": ADMIN_ENABLED, "admin_code": ADMIN_ENABLED and bool(ADMIN_TOTP)}


class LoginRequest(BaseModel):
    password: str
    user: str = access.DEMO
    code: str = ""


def check_admin_login(req: LoginRequest, key: str) -> None:
    if not ADMIN_ENABLED:
        raise HTTPException(404, NOT_FOUND)
    if ADMIN_THROTTLE.blocked(key):
        raise HTTPException(429, "too many attempts; try again later")
    ok = access.verify_password(req.password, ADMIN_HASH) and (
        not ADMIN_TOTP or access.verify_totp(req.code, ADMIN_TOTP))
    if not ok:
        ADMIN_THROTTLE.fail(key)
        raise HTTPException(401, "wrong password or code")
    ADMIN_THROTTLE.succeed(key)


@app.post("/api/login")
def login(req: LoginRequest, request: Request):
    # The role and everything in the cookie come from these constants, never
    # from the request text itself.
    if req.user == access.ADMIN:
        check_admin_login(req, client_key(request))
        who = access.ADMIN
    elif req.user == access.DEMO:
        who = access.DEMO
        if not DEMO_ENABLED:
            raise HTTPException(404, NOT_FOUND)
        if not access.password_matches(req.password, DEMO_PASSWORD):
            raise HTTPException(401, f"the demo password is {DEMO_PASSWORD!r}")
    else:
        raise HTTPException(422, "user must be demo or admin")
    old = session_id(request)
    if old:
        SIM_JOBS.pop(old, None)
    _sid, cookie = access.new_session(SESSION_SECRET, role=who)
    resp = JSONResponse({"role": who})
    resp.set_cookie(access.SESSION_COOKIE, cookie, max_age=access.SESSION_TTL_S[who],
                    httponly=True, secure=COOKIE_SECURE, samesite="strict")
    return resp


@app.post("/api/logout")
def logout(request: Request):
    sid = session_id(request)
    if sid:
        SIM_JOBS.pop(sid, None)
    resp = JSONResponse({"role": access.PUBLIC})
    resp.delete_cookie(access.SESSION_COOKIE)
    return resp


# ------------------------------------------------------------------ basemaps
# Every tile request tells its provider which area is in view, so nothing is
# drawn until a viewer picks a basemap or .env sets CONSOLE_BASEMAP. "kind"
# drives the dark treatment: light tiles may be inverted, dark and imagery
# tiles never are. Google tiles are never altered, as its terms require.
OSM_CREDIT = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
BUILTIN_BASEMAPS = [
    {"id": "osm", "label": "OpenStreetMap", "kind": "light", "max_zoom": 19,
     "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png", "attribution": OSM_CREDIT},
    {"id": "carto-light", "label": "CARTO Positron", "kind": "light", "max_zoom": 20,
     "dark_variant": "carto-dark", "subdomains": "abcd",
     "url": "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
     "attribution": f'{OSM_CREDIT} &copy; <a href="https://carto.com/attributions">CARTO</a>'},
    {"id": "carto-dark", "label": "CARTO Dark Matter", "kind": "dark", "max_zoom": 20,
     "subdomains": "abcd",
     "url": "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
     "attribution": f'{OSM_CREDIT} &copy; <a href="https://carto.com/attributions">CARTO</a>'},
    {"id": "esri-imagery", "label": "Esri World Imagery", "kind": "imagery", "max_zoom": 19,
     "url": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
     "attribution": "Tiles &copy; Esri &mdash; Source: Esri, Maxar, Earthstar Geographics, "
                    "and the GIS User Community"},
    {"id": "esri-topo", "label": "Esri World Topographic", "kind": "light", "max_zoom": 19,
     "url": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}",
     "attribution": "Tiles &copy; Esri &mdash; Esri, HERE, Garmin, FAO, NOAA, USGS, "
                    f"{OSM_CREDIT}, and the GIS User Community"},
    {"id": "opentopomap", "label": "OpenTopoMap", "kind": "light", "max_zoom": 17,
     "subdomains": "abc", "url": "https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
     "attribution": f'{OSM_CREDIT}, SRTM | Style: &copy; '
                    '<a href="https://opentopomap.org">OpenTopoMap</a> (CC-BY-SA)'},
]

GOOGLE_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")
_google_sessions: dict[str, dict] = {}


def google_session(map_type: str) -> str | None:
    """A Map Tiles API session token, reused until shortly before it expires.

    Region and language come from .env like every other region setting; with
    no region set, Google is left out rather than defaulting to a place.
    """
    region = os.environ.get("GOOGLE_MAPS_REGION", "")
    if not GOOGLE_KEY or not region:
        return None
    cached = _google_sessions.get(map_type)
    if cached and cached["expiry"] - time.time() > 3600:
        return cached["session"]
    try:
        r = httpx.post("https://tile.googleapis.com/v1/createSession",
                       params={"key": GOOGLE_KEY}, timeout=5,
                       json={"mapType": map_type, "region": region,
                             "language": os.environ.get("GOOGLE_MAPS_LANGUAGE", "en")})
        r.raise_for_status()
        body = r.json()
        _google_sessions[map_type] = {"session": body["session"], "expiry": int(body["expiry"])}
        return body["session"]
    except Exception as exc:  # noqa: BLE001 — Google is optional; the rest still works
        print(f"! google createSession({map_type}) failed: {exc}")
        return None


def basemaps() -> list[dict]:
    out = [{"id": "none", "label": "None (no tile requests)", "kind": "none"}] + BUILTIN_BASEMAPS
    custom = os.environ.get("CONSOLE_TILE_URL", "")
    if custom:
        out.append({"id": "custom", "label": "Custom (CONSOLE_TILE_URL)", "kind": "light",
                    "max_zoom": 19, "url": custom,
                    "attribution": os.environ.get("CONSOLE_TILE_ATTRIBUTION", "")})
    # The tile URL carries the API key to the browser, which on a public page
    # means to anyone; offer Google there only when deliberately allowed.
    if PUBLIC and os.environ.get("CONSOLE_PUBLIC_GOOGLE") != "1":
        return out
    for map_type, label, kind in (("roadmap", "Google Maps", "fixed"),
                                  ("satellite", "Google Satellite", "imagery")):
        session = google_session(map_type)
        if session:
            out.append({"id": f"google-{map_type}", "label": label, "kind": kind, "max_zoom": 22,
                        "google": True, "session": session,
                        "url": f"https://tile.googleapis.com/v1/2dtiles/{{z}}/{{x}}/{{y}}"
                               f"?session={session}&key={GOOGLE_KEY}",
                        "attribution": "Google"})
    return out


@app.get("/api/basemaps/google-attribution")
def google_attribution(map_id: str, zoom: int, north: float, south: float,
                       east: float, west: float):
    if PUBLIC and os.environ.get("CONSOLE_PUBLIC_GOOGLE") != "1":
        raise HTTPException(404, "Google basemaps are not configured")
    # Google's terms require the copyright for the area in view, which only
    # its viewport endpoint knows; proxied so the key stays in one place.
    session = google_session(map_id.removeprefix("google-"))
    if not session:
        raise HTTPException(404, "Google basemaps are not configured")
    r = httpx.get("https://tile.googleapis.com/tile/v1/viewport", timeout=5, params={
        "session": session, "key": GOOGLE_KEY, "zoom": zoom,
        "north": north, "south": south, "east": east, "west": west})
    if r.status_code != 200:
        raise HTTPException(502, "Google viewport lookup failed")
    return {"copyright": r.json().get("copyright", "")}


# ------------------------------------------------------------ GIS clients
# Desktop GIS (QGIS, ArcGIS Pro) and GDAL connect either to PostGIS directly
# or to the OGC API Features service; both use the read-only role, so a GIS
# client can never change the twin. Hosts and ports are as seen from this
# machine, not from inside the compose network.
GIS = {
    "db_host": os.environ.get("GIS_DB_HOST", "localhost"),
    "db_port": os.environ.get("GIS_DB_PORT", "5432"),
    "db_name": os.environ.get("GIS_DB_NAME", "weave_twin"),
    "db_user": os.environ.get("GIS_DB_USER", "weave_reader"),
    "db_password": os.environ.get("GIS_DB_PASSWORD", "weave_reader"),
    "ogcapi_url": os.environ.get("FEATURESERV_PUBLIC_URL", "http://localhost:9000"),
}
FEATURESERV_URL = os.environ.get("FEATURESERV_URL", "http://featureserv:9000")
# Must not exceed featureserv's PGFS_PAGING_LIMITMAX in docker-compose.override.yml.
OGCAPI_PAGE_SIZE = int(os.environ.get("FEATURESERV_PAGE_SIZE", "50000"))


def gis_connections() -> dict:
    return {**GIS, "ogcapi_url": GIS["ogcapi_url"].rstrip("/")}


@app.get("/api/gis/collections", dependencies=[Depends(admin_only)])
def gis_collections():
    try:
        r = httpx.get(f"{FEATURESERV_URL}/collections.json", timeout=4)
        r.raise_for_status()
    except Exception as exc:  # noqa: BLE001 — shown in the panel
        raise HTTPException(503, f"feature server: {str(exc).splitlines()[0]}") from exc
    return [{"id": c["id"], "title": c.get("title") or c["id"],
             "description": c.get("description", "")} for c in r.json().get("collections", [])]


def _xml_attr(value: str) -> str:
    return (value.replace("&", "&amp;").replace('"', "&quot;")
                 .replace("<", "&lt;").replace(">", "&gt;"))


def _download(body: str, filename: str, media: str = "application/xml") -> Response:
    return Response(body, media_type=media,
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@app.get("/api/gis/qgis/postgis-connection.xml", dependencies=[Depends(admin_only)])
def qgis_postgis_connection():
    # QGIS: Browser > PostgreSQL > right-click > Load Connections.
    g = {k: _xml_attr(v) for k, v in GIS.items()}
    return _download(
        "<!DOCTYPE connections>\n"
        '<qgsPgConnections version="1.0">\n'
        f'  <postgis name="Weave twin (read-only)" host="{g["db_host"]}" port="{g["db_port"]}"'
        f' database="{g["db_name"]}" service="" sslmode="SslDisable"'
        f' username="{g["db_user"]}" password="{g["db_password"]}"'
        ' saveUsername="true" savePassword="true" authcfg=""'
        ' estimatedMetadata="false" geometryColumnsOnly="false" dontResolveType="false"'
        ' allowGeometrylessTables="false" publicOnly="false" projectsInDatabase="false"/>\n'
        "</qgsPgConnections>\n", "weave-postgis.xml")


@app.get("/api/gis/qgis/ogcapi-connection.xml", dependencies=[Depends(admin_only)])
def qgis_ogcapi_connection():
    # QGIS: Browser > WFS / OGC API - Features > right-click > Load Connections.
    # pg_featureserv returns no "next" link, so QGIS treats the first page as
    # the whole layer; one page as large as the server allows avoids that.
    url = _xml_attr(GIS["ogcapi_url"].rstrip("/") + "/")
    return _download(
        "<!DOCTYPE connections>\n"
        '<qgsWFSConnections version="1.0">\n'
        # No credential attributes: the API is anonymous, QGIS reads a
        # missing attribute as empty, and an empty password="" reads to
        # secret scanners as a hardcoded password.
        f'  <wfs name="Weave twin (OGC API)" url="{url}" version="OGC_API_FEATURES"'
        ' ignoreAxisOrientation="0" invertAxisOrientation="0"'
        f' pagingenabled="enabled" pagesize="{OGCAPI_PAGE_SIZE}"/>\n'
        "</qgsWFSConnections>\n", "weave-ogcapi.xml")


def region_layers() -> list[str]:
    if not REGION_CONFIG:
        return []
    try:
        cfg = yaml.safe_load((WEAVE_DIR / REGION_CONFIG).read_text()) or {}
        return list((cfg.get("layers") or {}).keys())
    except Exception:  # noqa: BLE001 — the page shows an empty list instead
        return []


# ---------------------------------------------------------------- map layers
@app.get("/api/geo/dmas")
def geo_dmas():
    return geojson("""
        SELECT json_build_object('type', 'Feature',
               'geometry', ST_AsGeoJSON(d.geom, 6)::json,
               'properties', json_build_object(
                   'dma_id', d.dma_id, 'name', d.name, 'pipe_count', d.pipe_count,
                   'synthetic', d.synthetic,
                   'status', COALESCE(l.status, 'UNKNOWN'),
                   'loss_pct', l.loss_pct, 'min_pressure_m', l.min_pressure_m,
                   'ts', l.ts)) AS feature
        FROM asset.dma d LEFT JOIN twin.dma_latest l USING (dma_id)
        WHERE d.geom IS NOT NULL
    """)


@app.get("/api/geo/pipes")
def geo_pipes(limit: int = Query(5000, ge=1, le=50000),
              bbox: str | None = Query(None, pattern=r"^-?[\d.]+(,-?[\d.]+){3}$")):
    # Largest mains first, so a capped draw still shows the trunk network.
    where, params = "", {"limit": limit}
    if bbox:
        x1, y1, x2, y2 = (float(v) for v in bbox.split(","))
        where = "WHERE geom && ST_MakeEnvelope(%(x1)s, %(y1)s, %(x2)s, %(y2)s, 4326)"
        params.update(x1=x1, y1=y1, x2=x2, y2=y2)
    return geojson(f"""
        SELECT json_build_object('type', 'Feature',
               'geometry', ST_AsGeoJSON(ST_Simplify(geom, 0.00002), 6)::json,
               'properties', json_build_object(
                   'pipe_id', pipe_id, 'layer', layer, 'diameter_mm', diameter_mm,
                   'material', material, 'in_graph', start_node IS NOT NULL)) AS feature
        FROM (SELECT * FROM asset.pipe {where}
              ORDER BY diameter_mm DESC NULLS LAST LIMIT %(limit)s) p
    """, params)


@app.get("/api/geo/facilities")
def geo_facilities():
    return geojson("""
        SELECT json_build_object('type', 'Feature',
               'geometry', ST_AsGeoJSON(geom, 6)::json,
               'properties', json_build_object(
                   'facility_id', facility_id, 'facility_type', facility_type,
                   'name', name, 'capacity_ml', capacity_ml)) AS feature
        FROM asset.facility
    """)


@app.get("/api/geo/gauges")
def geo_gauges():
    return geojson("""
        SELECT json_build_object('type', 'Feature',
               'geometry', ST_AsGeoJSON(geom, 6)::json,
               'properties', json_build_object(
                   'site_id', site_id, 'site_name', site_name,
                   'variable', variable, 'provider', provider)) AS feature
        FROM ref.hydrometry_site WHERE geom IS NOT NULL
    """)


@app.get("/api/geo/meters")
def geo_meters(dma_id: str | None = None, limit: int = Query(20000, ge=1, le=200000)):
    # The whole registry by default, capped: a few thousand synthetic points
    # draw fine on a canvas, a utility-scale fleet would not.
    return geojson("""
        SELECT feature FROM (
            SELECT json_build_object('type', 'Feature',
                   'geometry', ST_AsGeoJSON(geom, 6)::json,
                   'properties', json_build_object(
                       'id', meter_id, 'dma_id', dma_id, 'kind', 'meter',
                       'meter_type', meter_type, 'synthetic', synthetic)) AS feature
            FROM ref.water_meter
            WHERE geom IS NOT NULL AND (%(d)s::text IS NULL OR dma_id = %(d)s)
            UNION ALL
            SELECT json_build_object('type', 'Feature',
                   'geometry', ST_AsGeoJSON(geom, 6)::json,
                   'properties', json_build_object(
                       'id', logger_id, 'dma_id', dma_id, 'kind', 'logger',
                       'synthetic', synthetic))
            FROM ref.pressure_logger
            WHERE geom IS NOT NULL AND (%(d)s::text IS NULL OR dma_id = %(d)s)
        ) m LIMIT %(limit)s
    """, {"d": dma_id, "limit": limit})


# ----------------------------------------------------------------- telemetry
# The residual scoring settings, read with the simulator's names and defaults
# so the page explains the same rule model/residuals.py applies. The console
# image has no model package, so the baseline count below mirrors its
# RESIDUALS_SQL rather than importing it.
RESIDUAL_RULES = {
    "baseline_days": float(os.environ.get("SIM_RESIDUAL_BASELINE_DAYS", "7")),
    "min_samples": int(os.environ.get("SIM_RESIDUAL_MIN_SAMPLES", "24")),
    "z_watch": float(os.environ.get("SIM_RESIDUAL_Z_WATCH", "2")),
    "z_leak": float(os.environ.get("SIM_RESIDUAL_Z_LEAK", "3")),
    "relearn_hours": float(os.environ.get("SIM_RESIDUAL_RELEARN_HOURS", "24")),
    "interval_s": int(os.environ.get("SIM_INTERVAL_S", "900")),
}


def baseline_progress(dma_id: str) -> dict:
    """How many residuals count towards the DMA's baseline, of how many needed.

    Same window as the simulator: inside the baseline days, after the DMA's
    last reset (or a reset of every DMA), leaving out flagged rows still in
    their relearn period.
    """
    r = RESIDUAL_RULES
    # run_sim.py creates the reset table on first use, so it can be missing.
    has_reset = scalar("SELECT to_regclass('twin.residual_baseline_reset') IS NOT NULL")
    reset = """AND ts > coalesce((SELECT max(reset_at) FROM twin.residual_baseline_reset
                                   WHERE dma_id IN (%(d)s, '*')), '-infinity')""" if has_reset else ""
    n = scalar(f"""
        SELECT count(*) FROM twin.sim_residual
        WHERE dma_id = %(d)s AND variable = 'pressure' AND residual IS NOT NULL
          AND ts > now() - make_interval(secs => %(baseline_s)s)
          {reset}
          AND NOT (coalesce(flagged, false) AND ts > now() - make_interval(secs => %(relearn_s)s))
    """, {"d": dma_id, "baseline_s": r["baseline_days"] * 86400,
          "relearn_s": r["relearn_hours"] * 3600})
    # A DMA the model does not reach gets no residual at all, so it would stay
    # UNKNOWN for good; the page must not promise it a status in a few hours.
    # Every DMA scored in one solve shares that solve's ts.
    last = query("""
        SELECT EXISTS (SELECT 1 FROM twin.sim_residual) AS any_run,
               EXISTS (SELECT 1 FROM twin.sim_residual
                       WHERE dma_id = %(d)s AND ts = (SELECT max(ts) FROM twin.sim_residual)) AS in_last_run
    """, {"d": dma_id})[0]
    return {"samples": n or 0, "any_run": last["any_run"], "in_last_run": last["in_last_run"], **r}


DEVICES = {
    # kind: (registry, id column, readings, the reading columns to show)
    "meter": ("ref.water_meter", "meter_id", "twin.meter_reading",
              ["litres", "flow_lpm", "battery_pct"]),
    "logger": ("ref.pressure_logger", "logger_id", "twin.pressure_reading",
               ["pressure_m", "flow_ls"]),
}


@app.get("/api/device/{kind}/{device_id}")
def device_detail(kind: str, device_id: str, hours: int = Query(24, ge=1, le=168)):
    """One meter or logger: registry row, latest reading and 15-minute buckets.

    Only synthetic registries are served, so the public page can show it too.
    Table and column names come from DEVICES, never from the request.
    """
    if kind not in DEVICES:
        raise HTTPException(404, f"no device kind {kind}")
    registry, id_col, readings, cols = DEVICES[kind]

    def run():
        info = query(f"SELECT {id_col} AS id, * FROM {registry} WHERE {id_col} = %s",
                     (device_id,))
        if not info:
            raise HTTPException(404, f"no {kind} {device_id}")
        info = {k: v for k, v in info[0].items() if k != "geom"}
        latest = query(f"""
            SELECT ts, {", ".join(cols)} FROM {readings}
            WHERE {id_col} = %(id)s AND ts > now() - make_interval(hours => %(h)s)
            ORDER BY ts DESC LIMIT 1
        """, {"id": device_id, "h": hours})
        # A meter's litres are per reading, so a bucket sums them; a logger's
        # pressure is a level, so a bucket averages it.
        bucket = ("sum(litres) AS litres, avg(flow_lpm) AS flow_lpm" if kind == "meter"
                  else "avg(pressure_m) AS pressure_m, avg(flow_ls) AS flow_ls")
        series = query(f"""
            SELECT date_bin('15 minutes', ts, TIMESTAMPTZ '2000-01-01') AS ts,
                   {bucket}, count(*) AS readings
            FROM {readings}
            WHERE {id_col} = %(id)s AND ts > now() - make_interval(hours => %(h)s)
            GROUP BY 1 ORDER BY 1
        """, {"id": device_id, "h": hours})
        return {"kind": kind, "info": info, "latest": latest[0] if latest else None,
                "series": series, "hours": hours}
    return db_or_503(run)


def db_or_503(fn):
    try:
        return fn()
    except psycopg.Error as exc:
        raise HTTPException(503, f"postgres: {str(exc).splitlines()[0]}") from exc


@app.get("/api/dma/{dma_id}")
def dma_detail(dma_id: str, hours: int = Query(24, ge=1, le=168)):
    def run():
        info = query("""
            SELECT d.dma_id, d.name, d.pipe_count, d.total_length_m, d.synthetic,
                   l.ts, l.status, l.loss_pct, l.min_pressure_m,
                   (SELECT count(*) FROM ref.water_meter m WHERE m.dma_id = d.dma_id) AS meters,
                   (SELECT count(*) FROM ref.pressure_logger p WHERE p.dma_id = d.dma_id) AS loggers
            FROM asset.dma d LEFT JOIN twin.dma_latest l USING (dma_id)
            WHERE d.dma_id = %s
        """, (dma_id,))
        if not info:
            raise HTTPException(404, f"no DMA {dma_id}")
        window = f"{hours} hours"
        return {
            "info": info[0],
            "baseline": baseline_progress(dma_id),
            "balance": query("""
                SELECT window_start AS ts, consumption_l, inflow_l, loss_pct,
                       min_pressure_m, meter_count
                FROM twin.dma_balance
                WHERE dma_id = %s AND window_start > now() - %s::interval
                ORDER BY window_start
            """, (dma_id, window)),
            "residuals": query("""
                SELECT ts, variable, modelled, observed, residual, z_score, flagged
                FROM twin.sim_residual
                WHERE dma_id = %s AND ts > now() - %s::interval
                ORDER BY ts
            """, (dma_id, window)),
        }
    return db_or_503(run)


@app.get("/api/anomalies")
def anomalies(hours: int = Query(24, ge=1, le=168)):
    return db_or_503(lambda: query("""
        SELECT r.ts, r.dma_id, d.name, r.variable, r.modelled, r.observed,
               r.residual, r.z_score, r.flagged, l.status
        FROM twin.sim_residual r
        LEFT JOIN asset.dma d USING (dma_id)
        LEFT JOIN twin.dma_latest l USING (dma_id)
        WHERE r.ts > now() - %s::interval AND (r.flagged OR abs(r.residual) > 2)
        ORDER BY r.ts DESC, abs(r.residual) DESC
        LIMIT 300
    """, (f"{hours} hours",)))


@app.get("/api/freshness")
def freshness():
    def run():
        out = []
        for table, col, origin in TELEMETRY:
            row = query(f"""
                SELECT max({col}) AS latest,
                       count(*) FILTER (WHERE {col} > now() - interval '1 hour') AS last_hour
                FROM twin.{table}
                WHERE {col} > now() - interval '7 days'
            """)[0]
            out.append({"table": f"twin.{table}", "origin": origin, **row})
        return out
    return db_or_503(run)


@app.get("/api/feeds")
def feeds(hours: int = Query(24, ge=1, le=168)):
    window = f"{hours} hours"
    return db_or_503(lambda: {
        "hydrometry_latest": query("""
            SELECT DISTINCT ON (h.site_id, h.variable)
                   h.site_id, s.site_name, h.variable, h.value, h.quality_code,
                   h.provider, h.ts, h.is_synthetic
            FROM twin.hydrometry h LEFT JOIN ref.hydrometry_site s USING (site_id)
            WHERE h.ts > now() - %s::interval
            ORDER BY h.site_id, h.variable, h.ts DESC
        """, (window,)),
        "hydrometry_series": query("""
            SELECT site_id, variable, ts, value
            FROM twin.hydrometry
            WHERE ts > now() - %s::interval
            ORDER BY site_id, variable, ts
        """, (window,)),
        "market": query("""
            SELECT ts, region_id, price_mwh, total_demand_mw, is_synthetic
            FROM twin.market_dispatch
            WHERE ts > now() - %s::interval
            ORDER BY region_id, ts
        """, (window,)),
    })


# -------------------------------------------------------------- operations
class Param(BaseModel):
    name: str
    label: str
    kind: str                 # int | bool | choice | text
    default: int | bool | str | None = None
    min: int | None = None
    max: int | None = None
    choices: list[str] | None = None


ACTIONS: dict[str, dict] = {
    "create_topics": {
        "label": "Create Kafka topics",
        "step": 1,
        "description": "Creates the six raw topics with explicit retention. Idempotent.",
        "params": [],
    },
    "load_gis": {
        "label": "Load GIS network",
        "step": 2,
        "description": "Pages public GIS features into asset.gis_raw and the conformed "
                       "tables. Use a limit while iterating; a full load is slow.",
        "params": [
            Param(name="layer", label="Layer", kind="choice", default="(all)"),
            Param(name="limit", label="Feature limit (0 = none)", kind="int",
                  default=500, min=0, max=10_000_000),
            Param(name="build_topology", label="Build topology after load",
                  kind="bool", default=True),
        ],
    },
    "seed": {
        "label": "Derive synthetic DMAs and meters",
        "step": 3,
        "destructive": "Truncates asset.dma, ref.water_meter, ref.pressure_logger and "
                       "ref.electricity_meter, and rewrites schemas/. Re-register the "
                       "connectors afterwards.",
        "description": "Clusters real pipes into DMAs, places meters on real geometry, "
                       "and writes Datagen schemas drawn from the new ids.",
        "params": [
            Param(name="dmas", label="DMAs", kind="int", default=40, min=1, max=2000),
            Param(name="meters_per_dma", label="Meters per DMA", kind="int",
                  default=25, min=1, max=5000),
        ],
    },
    "register_connectors": {
        "label": "Register Datagen connectors",
        "step": 4,
        "description": "Deletes then re-posts each Datagen connector. Needs schemas/ "
                       "from the seeding step.",
        "params": [],
    },
    "build_model": {
        "label": "Build EPANET model",
        "step": 5,
        "description": "PostGIS asset graph to /app/out/network.inp. Needs topology.",
        "params": [],
    },
    "start_flink": {
        "label": "Start Flink pipelines",
        "step": 6,
        "description": "Runs 01_sources, 02_sinks and 03_pipelines in one SQL client "
                       "session. Refused while a Flink job is running, because a second "
                       "statement set would write every row twice.",
        "params": [],
    },
    "run_sim_once": {
        "label": "Run one simulation",
        "step": 7,
        "description": "One hydraulic solve plus residuals and DMA status. The simulator "
                       "container also does this on its own every SIM_INTERVAL_S.",
        "params": [],
    },
    "reset_baseline": {
        "label": "Reset residual baselines",
        "step": None,
        "description": "Starts DMA residual baselines afresh after a lasting change such "
                       "as a model recalibration. Earlier residuals stay in the database "
                       "but stop counting, so the DMA shows insufficient data until it has enough "
                       "new ones. Blank resets every DMA.",
        "params": [Param(name="dma", label="DMA id (blank = all)", kind="text", default="")],
    },
    "load_elevation": {
        "label": "Sample node elevations",
        "step": None,
        "description": "Fills asset.node.elevation_m from the region config's DEM tile "
                       "service. Run after the topology build, then rebuild the model.",
        "params": [Param(name="all", label="Re-sample every node", kind="bool",
                         default=False)],
    },
    "discover": {
        "label": "Discover catalogue layers",
        "step": None,
        "description": "Searches the region's ArcGIS/CKAN catalogue for candidate layers "
                       "to add to the region config. Read-only.",
        "params": [Param(name="search", label="Search term", kind="text", default="")],
    },
}


def build_command(action: str, p: dict) -> list[str]:
    """The exact documented command for each action, nothing else."""
    exec_ = ["docker", "exec", "-e", "PYTHONUNBUFFERED=1"]
    run_sim = exec_ + ["simulator", "python", "run_sim.py"]
    if action == "create_topics":
        return ["bash", str(WEAVE_DIR / "scripts/create-topics.sh")]
    if action == "load_gis":
        cmd = exec_ + ["gis-loader", "python", "gis/load_gis.py"]
        cmd += ["--all"] if p["layer"] == "(all)" else ["--layer", p["layer"]]
        if p["limit"]:
            cmd += ["--limit", str(p["limit"])]
        if p["build_topology"]:
            cmd += ["--build-topology"]
        return cmd
    if action == "seed":
        return exec_ + ["gis-loader", "python", "scripts/seed_synthetic_network.py",
                        "--dmas", str(p["dmas"]),
                        "--meters-per-dma", str(p["meters_per_dma"])]
    if action == "register_connectors":
        return ["bash", str(WEAVE_DIR / "scripts/register-connectors.sh")]
    if action == "build_model":
        return run_sim + ["--build"]
    if action == "run_sim_once":
        return run_sim + ["--once"]
    if action == "start_flink":
        # Tables are session-scoped in the SQL client, so the three files have
        # to run in one session — hence one concatenated script.
        return ["docker", "exec", "flink-sql-client", "sh", "-c",
                "cat /sql/01_sources.sql /sql/02_sinks.sql /sql/03_pipelines.sql "
                "> /tmp/weave_pipelines.sql && sql-client.sh -f /tmp/weave_pipelines.sql"]
    if action == "reset_baseline":
        cmd = run_sim + ["--reset-baseline"]
        return cmd + (["--dma", p["dma"]] if p["dma"] else [])
    if action == "load_elevation":
        cmd = exec_ + ["gis-loader", "python", "gis/load_elevation.py"]
        return cmd + (["--all"] if p["all"] else [])
    if action == "discover":
        cmd = exec_ + ["gis-loader", "python", "gis/discover_layers.py"]
        return cmd + (["--search", p["search"]] if p["search"] else [])
    raise HTTPException(404, f"no action {action}")


def validate(action: str, raw: dict) -> dict:
    out = {}
    for prm in ACTIONS[action]["params"]:
        val = raw.get(prm.name, prm.default)
        if prm.kind == "int":
            try:
                val = int(val)
            except (TypeError, ValueError) as exc:
                raise HTTPException(422, f"{prm.name} must be an integer") from exc
            if not prm.min <= val <= prm.max:
                raise HTTPException(422, f"{prm.name} must be {prm.min}..{prm.max}")
        elif prm.kind == "bool":
            val = bool(val)
        elif prm.kind == "choice":
            allowed = ["(all)"] + region_layers()
            if val not in allowed:
                raise HTTPException(422, f"{prm.name} must be one of {allowed}")
        elif prm.kind == "text":
            # Passed as one argv element, never through a shell; the limit
            # only keeps catalogue queries sane.
            val = str(val or "").strip()
            if len(val) > 80 or any(c in val for c in "\n\r\0"):
                raise HTTPException(422, f"{prm.name} must be one short line")
        out[prm.name] = val
    return out


def run_mode(action: str, who: str) -> str:
    """How this role runs an action: real, simulated, or not at all."""
    if who == access.ADMIN:
        return "real"
    if who == access.DEMO:
        live = RELAY_URL and RELAY_TOKEN
        return "real" if action in access.SAFE_ACTIONS and live else "simulated"
    return "none"


@app.get("/api/actions")
def list_actions(request: Request):
    who = role(request)
    return [{"id": k, "run": run_mode(k, who),
             **{**v, "params": [p.model_dump() for p in v["params"]]}}
            for k, v in ACTIONS.items()]


class RunRequest(BaseModel):
    params: dict = {}


@app.post("/api/actions/{action}/preview")
def preview_action(action: str, req: RunRequest, request: Request):
    # Shown before running, so the page teaches the CLI it stands in for.
    if action not in ACTIONS:
        raise HTTPException(404, f"no action {action}")
    if role(request) == access.PUBLIC:
        raise HTTPException(401, "log in as the demo user to try actions")
    return {"command": build_command(action, validate(action, req.params))}


# ----------------------------------------------------------------------- jobs
# In memory: a restart forgets the history, which is fine for a console whose
# real record is Postgres and the container logs.
JOBS: dict[str, dict] = {}
JOB_ORDER: deque[str] = deque(maxlen=50)
MAX_LINES = 5000
_lock = asyncio.Lock()

# Public mode: a demo session's jobs, simulated or relayed, keyed by session id.
# A new login gets a new id, which is what resets a demo user's history.
SIM_JOBS: dict[str, dict[str, dict]] = {}
MAX_SESSIONS, MAX_SESSION_JOBS = 500, 20
# Live demo runs are limited across all demo users, not per session: anyone
# can log in, so a per-session limit would bound nothing. Both consoles check,
# so the admin side holds even if the public one is replaced or bypassed.
DEMO_RUN_INTERVAL_S = int(os.environ.get("CONSOLE_DEMO_COOLDOWN_S", "60"))
DEMO_COOLDOWN = access.Cooldown(DEMO_RUN_INTERVAL_S)
RELAY_COOLDOWN = access.Cooldown(DEMO_RUN_INTERVAL_S)
ALL_DEMO_USERS = "*"
ADMIN_THROTTLE = access.Throttle()


def current_job_summary() -> dict | None:
    for jid in reversed(JOB_ORDER):
        j = JOBS.get(jid)
        if j and j["state"] == "running":
            return {k: j[k] for k in ("id", "action", "label", "started")}
    return None


@app.post("/api/actions/{action}")
async def run_action(action: str, req: RunRequest, request: Request):
    if action not in ACTIONS:
        raise HTTPException(404, f"no action {action}")
    who = role(request)
    if who == access.PUBLIC:
        raise HTTPException(401, "log in as the demo user to try actions")
    params = validate(action, req.params)
    if who == access.DEMO:
        return await demo_action(action, params, session_id(request))
    if PUBLIC:   # the admin, signed in on the public page
        r = await relay_request("POST", relay_path("/api/relay/admin/actions", action), admin=True,
                                json={"params": params})
        if r.status_code != 200:
            raise relay_error(r)
        _status_cache["body"] = None
        return r.json()
    return await start_job(action, params, origin="admin")


def remember_demo_job(sid: str, job: dict) -> None:
    jobs = SIM_JOBS.setdefault(sid, {})
    jobs[job["id"]] = job
    while len(jobs) > MAX_SESSION_JOBS:
        jobs.pop(next(iter(jobs)))
    while len(SIM_JOBS) > MAX_SESSIONS:
        SIM_JOBS.pop(next(iter(SIM_JOBS)))


async def demo_action(action: str, params: dict, sid: str) -> dict:
    cmd = build_command(action, params)
    now = datetime.now(timezone.utc).isoformat()
    job = {"id": uuid.uuid4().hex[:10], "action": action, "label": ACTIONS[action]["label"],
           "params": params, "command": cmd, "started": now, "lines": []}
    if run_mode(action, access.DEMO) == "simulated":
        job |= {"state": "succeeded", "exit_code": 0, "finished": now, "simulated": True,
                "lines": demo.simulated_lines(action, params, cmd)}
        remember_demo_job(sid, job)
        return {"id": job["id"]}
    wait = DEMO_COOLDOWN.wait_s(ALL_DEMO_USERS)
    if wait:
        raise HTTPException(429, f"another demo run just happened; live runs are limited to one "
                                 f"every {DEMO_RUN_INTERVAL_S} s across all demo users. "
                                 f"Try again in {wait} s, or run it simulated.")
    r = await relay_request("POST", relay_path("/api/relay/actions", action),
                            json={"params": params})
    if r.status_code != 200:
        raise relay_error(r)
    DEMO_COOLDOWN.mark(ALL_DEMO_USERS)
    job |= {"state": "running", "exit_code": None, "finished": None,
            "simulated": False, "remote_id": r.json()["id"]}
    remember_demo_job(sid, job)
    return {"id": job["id"]}


async def start_job(action: str, params: dict, origin: str) -> dict:
    if action == "start_flink":
        jobs = (await asyncio.to_thread(safe, probe_flink))["value"] or []
        running = [j for j in jobs if j["state"] == "RUNNING"]
        if running:
            raise HTTPException(409, f"Flink already has {len(running)} running job(s): "
                                     f"{running[0]['name']}. Cancel it in the Flink UI first.")
    if action == "register_connectors":
        # The script waits for Connect indefinitely, which would hold the job
        # lock until someone restarts the console.
        probe = await asyncio.to_thread(safe, probe_connectors)
        if not probe["ok"]:
            raise HTTPException(409, f"Kafka Connect is not answering at {CONNECT_URL}")
    cmd = build_command(action, params)

    # One operation at a time: seeding truncates what loading and model
    # building read, so overlapping runs give results nobody asked for.
    if _lock.locked():
        busy = current_job_summary()
        raise HTTPException(409, f"busy: {busy['label'] if busy else 'another job'} is running")

    jid = uuid.uuid4().hex[:10]
    JOBS[jid] = {"id": jid, "action": action, "label": ACTIONS[action]["label"],
                 "params": params, "command": cmd, "state": "running", "origin": origin,
                 "started": datetime.now(timezone.utc).isoformat(),
                 "finished": None, "exit_code": None, "lines": []}
    JOB_ORDER.append(jid)
    evicted = set(JOBS) - set(JOB_ORDER)
    for old in evicted:
        JOBS.pop(old, None)
    await _lock.acquire()
    asyncio.create_task(_run(jid, cmd))
    return {"id": jid}


async def _run(jid: str, cmd: list[str]):
    job = JOBS[jid]
    env = {**os.environ, "CONNECT_URL": CONNECT_URL}
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            cwd=WEAVE_DIR, env=env)
        async for raw in proc.stdout:
            job["lines"].append(raw.decode(errors="replace").rstrip("\n"))
            if len(job["lines"]) > MAX_LINES:
                del job["lines"][: len(job["lines"]) - MAX_LINES]
        job["exit_code"] = await proc.wait()
        job["state"] = "succeeded" if job["exit_code"] == 0 else "failed"
    except Exception as exc:  # noqa: BLE001 — surfaced in the job log
        job["lines"].append(f"! could not run: {exc}")
        job["state"] = "failed"
    finally:
        job["finished"] = datetime.now(timezone.utc).isoformat()
        _status_cache["body"] = None
        _lock.release()


def _summary(job: dict) -> dict:
    return {k: v for k, v in job.items() if k not in ("lines", "remote_id")} | {
        "line_count": len(job["lines"])}


@app.get("/api/jobs")
async def list_jobs(request: Request):
    if remote_admin(request):
        r = await relay_request("GET", "/api/relay/admin/jobs", admin=True)
        if r.status_code != 200:
            raise relay_error(r)
        return r.json()
    if PUBLIC:
        sid = session_id(request)
        return [_summary(j) for j in reversed(SIM_JOBS.get(sid, {}).values())] if sid else []
    return [_summary(JOBS[j]) for j in reversed(JOB_ORDER) if j in JOBS]


@app.get("/api/jobs/{jid}")
async def get_job(jid: str, request: Request, since: int = Query(0, ge=0)):
    if remote_admin(request):
        return await remote_admin_job(jid, since)
    if PUBLIC:
        job = SIM_JOBS.get(session_id(request) or "", {}).get(jid)
        if not job:
            raise HTTPException(404, NO_SUCH_JOB)
        if job.get("remote_id") and job["state"] == "running":
            await refresh_relayed(job)
    else:
        job = JOBS.get(jid)
        if not job:
            raise HTTPException(404, NO_SUCH_JOB)
    return {k: v for k, v in job.items() if k != "remote_id"} | {
        "offset": len(job["lines"]), "lines": job["lines"][since:]}


async def remote_admin_job(jid: str, since: int) -> dict:
    if not JOB_ID.fullmatch(jid):
        raise HTTPException(404, NO_SUCH_JOB)
    r = await relay_request("GET", relay_path("/api/relay/admin/jobs", jid), admin=True,
                            params={"since": since})
    if r.status_code == 404:
        raise HTTPException(404, NO_SUCH_JOB)
    if r.status_code != 200:
        raise relay_error(r)
    return r.json()


async def refresh_relayed(job: dict) -> None:
    try:
        r = await relay_request("GET", relay_path("/api/relay/jobs", job["remote_id"]),
                                params={"since": len(job["lines"])})
        r.raise_for_status()
    except (HTTPException, httpx.HTTPError):
        return   # shown as still running; the next poll tries again
    body = r.json()
    job["lines"].extend(body["lines"])
    job.update({k: body[k] for k in ("state", "exit_code", "finished")})


# ---------------------------------------------------------------------- relay
# Admin side, reached only by the public console over the private link. The
# token gates it and the safe list bounds it: a leaked token can at most run a
# catalogue search or one extra solve, one at a time like every other job.
@app.post("/api/relay/actions/{action}", dependencies=[Depends(relay_auth)])
async def relay_action(action: str, req: RunRequest):
    if action not in access.SAFE_ACTIONS:
        raise HTTPException(403, f"{action} is not a demo action")
    wait = RELAY_COOLDOWN.wait_s(ALL_DEMO_USERS)
    if wait:
        raise HTTPException(429, f"wait {wait} s before the next demo run")
    started = await start_job(action, validate(action, req.params), origin="demo")
    RELAY_COOLDOWN.mark(ALL_DEMO_USERS)
    return started


@app.get("/api/relay/jobs/{jid}", dependencies=[Depends(relay_auth)])
def relay_job(jid: str, since: int = Query(0, ge=0)):
    job = JOBS.get(jid)
    if not job or job.get("origin") != "demo":
        raise HTTPException(404, NO_SUCH_JOB)
    return {"state": job["state"], "exit_code": job["exit_code"], "finished": job["finished"],
            "lines": job["lines"][since:]}


@app.get("/api/relay/status", dependencies=[Depends(relay_any_auth)])
async def relay_status():
    return await shared_status()


# The admin login's routes. Its token admits every action, so it is kept
# separate from the demo token, and the public console holds it only when
# the admin login is configured there.
@app.post("/api/relay/admin/actions/{action}", dependencies=[Depends(relay_admin_auth)])
async def relay_admin_action(action: str, req: RunRequest):
    if action not in ACTIONS:
        raise HTTPException(404, f"no action {action}")
    return await start_job(action, validate(action, req.params), origin="admin")


@app.get("/api/relay/admin/jobs", dependencies=[Depends(relay_admin_auth)])
def relay_admin_jobs():
    return [_summary(JOBS[j]) for j in reversed(JOB_ORDER) if j in JOBS]


@app.get("/api/relay/admin/jobs/{jid}", dependencies=[Depends(relay_admin_auth)])
def relay_admin_job(jid: str, since: int = Query(0, ge=0)):
    job = JOBS.get(jid)
    if not job:
        raise HTTPException(404, NO_SUCH_JOB)
    return {k: v for k, v in job.items() if k != "remote_id"} | {
        "offset": len(job["lines"]), "lines": job["lines"][since:]}


# ---------------------------------------------------------------------- page
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")
