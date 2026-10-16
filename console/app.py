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
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psycopg
import yaml
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
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
    return await call_next(request)


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
async def status():
    # Probes have multi-second timeouts when a service is down, so they run
    # concurrently and the result is shared for a few seconds between tabs.
    if _status_cache["body"] and time.monotonic() - _status_cache["at"] < 5:
        return _status_cache["body"]
    names = ["db", "topics", "connectors", "flink", "containers", "model", "schemas"]
    fns = [probe_db, probe_topics, probe_connectors, probe_flink,
           probe_containers, probe_model, probe_schemas]
    results = await asyncio.gather(*(asyncio.to_thread(safe, f) for f in fns))
    body = dict(zip(names, results))
    body["expected_topics"] = TOPICS
    body["job"] = current_job_summary()
    _status_cache.update(at=time.monotonic(), body=body)
    return body


@app.get("/api/config")
def config():
    return {
        "region_config": REGION_CONFIG,
        "layers": region_layers(),
        "tile_url": os.environ.get("CONSOLE_TILE_URL", ""),
        "tile_attribution": os.environ.get("CONSOLE_TILE_ATTRIBUTION", ""),
        "kibana_url": os.environ.get("KIBANA_PUBLIC_URL", "http://localhost:5601"),
        "flink_url": os.environ.get("FLINK_PUBLIC_URL", "http://localhost:9081"),
    }


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
def geo_meters(dma_id: str):
    # Per DMA only: the full registry is too many points to be useful at once.
    return geojson("""
        SELECT json_build_object('type', 'Feature',
               'geometry', ST_AsGeoJSON(geom, 6)::json,
               'properties', json_build_object(
                   'id', meter_id, 'kind', 'meter', 'meter_type', meter_type,
                   'synthetic', synthetic)) AS feature
        FROM ref.water_meter WHERE dma_id = %(d)s AND geom IS NOT NULL
        UNION ALL
        SELECT json_build_object('type', 'Feature',
               'geometry', ST_AsGeoJSON(geom, 6)::json,
               'properties', json_build_object(
                   'id', logger_id, 'kind', 'logger', 'synthetic', synthetic))
        FROM ref.pressure_logger WHERE dma_id = %(d)s AND geom IS NOT NULL
    """, {"d": dma_id})


# ----------------------------------------------------------------- telemetry
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
        return exec_ + ["simulator", "python", "run_sim.py", "--build"]
    if action == "run_sim_once":
        return exec_ + ["simulator", "python", "run_sim.py", "--once"]
    if action == "start_flink":
        # Tables are session-scoped in the SQL client, so the three files have
        # to run in one session — hence one concatenated script.
        return ["docker", "exec", "flink-sql-client", "sh", "-c",
                "cat /sql/01_sources.sql /sql/02_sinks.sql /sql/03_pipelines.sql "
                "> /tmp/weave_pipelines.sql && sql-client.sh -f /tmp/weave_pipelines.sql"]
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


@app.get("/api/actions")
def list_actions():
    return [{"id": k, **{**v, "params": [p.model_dump() for p in v["params"]]}}
            for k, v in ACTIONS.items()]


class RunRequest(BaseModel):
    params: dict = {}


@app.post("/api/actions/{action}/preview")
def preview_action(action: str, req: RunRequest):
    # Shown before running, so the page teaches the CLI it stands in for.
    if action not in ACTIONS:
        raise HTTPException(404, f"no action {action}")
    return {"command": build_command(action, validate(action, req.params))}


# ----------------------------------------------------------------------- jobs
# In memory: a restart forgets the history, which is fine for a console whose
# real record is Postgres and the container logs.
JOBS: dict[str, dict] = {}
JOB_ORDER: deque[str] = deque(maxlen=50)
MAX_LINES = 5000
_lock = asyncio.Lock()


def current_job_summary() -> dict | None:
    for jid in reversed(JOB_ORDER):
        j = JOBS.get(jid)
        if j and j["state"] == "running":
            return {k: j[k] for k in ("id", "action", "label", "started")}
    return None


@app.post("/api/actions/{action}")
async def run_action(action: str, req: RunRequest):
    if action not in ACTIONS:
        raise HTTPException(404, f"no action {action}")
    params = validate(action, req.params)
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
                 "params": params, "command": cmd, "state": "running",
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


@app.get("/api/jobs")
def list_jobs():
    return [{k: v for k, v in JOBS[j].items() if k != "lines"} | {"line_count": len(JOBS[j]["lines"])}
            for j in reversed(JOB_ORDER) if j in JOBS]


@app.get("/api/jobs/{jid}")
def get_job(jid: str, since: int = Query(0, ge=0)):
    job = JOBS.get(jid)
    if not job:
        raise HTTPException(404, "no such job")
    return {k: v for k, v in job.items() if k != "lines"} | {
        "offset": len(job["lines"]), "lines": job["lines"][since:]}


# ---------------------------------------------------------------------- page
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")
