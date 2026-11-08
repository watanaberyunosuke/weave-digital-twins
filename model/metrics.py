"""The simulator's Prometheus metrics, served from its --loop process.

Solve time is the one figure Postgres doesn't hold, and the one that says
whether SIM_MAX_PIPES still fits SIM_INTERVAL_S, so the loop publishes it
here for Prometheus to scrape (monitoring/prometheus/prometheus-generated.yml) and alert on
(monitoring/prometheus/weave-alerts.yml). Freshness of the results themselves comes
from the console's /metrics, which reads twin.sim_residual.

Standard library only, so the image gains no dependency. A --once, --replay
or --build run records values too, but nothing serves them: only the loop
calls start().
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# name -> (type, help); every metric is unlabelled.
METRICS = {
    "weave_sim_solve_seconds": ("gauge", "Wall time of the last live solve, residuals included."),
    "weave_sim_last_solve_timestamp_seconds": ("gauge", "Unix time the last successful live solve finished."),
    "weave_sim_interval_seconds": ("gauge", "SIM_INTERVAL_S, the cadence a solve has to fit."),
    "weave_sim_solves_total": ("counter", "Live solves completed since the process started."),
    "weave_sim_solve_failures_total": ("counter", "Live solves that raised since the process started."),
    "weave_sim_replay_pending": ("gauge", "Missed solves still waiting to be replayed after the last pass."),
}

# Rendered with repr, never ":g", which rounds a Unix time to about 100 s.
_values: dict[str, float] = {}
_lock = threading.Lock()


def set_value(name: str, value: float) -> None:
    with _lock:
        _values[name] = float(value)


def inc(name: str, by: float = 1.0) -> None:
    with _lock:
        _values[name] = _values.get(name, 0.0) + by


def render() -> str:
    with _lock:
        values = dict(_values)
    lines = []
    for name, (kind, text) in METRICS.items():
        if name not in values and kind != "counter":
            continue  # unknown yet: absent rather than a misleading 0
        lines += [f"# HELP {name} {text}", f"# TYPE {name} {kind}", f"{name} {values.get(name, 0.0)!r}"]
    return "\n".join(lines) + "\n"


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 — the stdlib's name
        if self.path != "/metrics":
            self.send_error(404)
            return
        body = render().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # a scrape a minute would bury the solve log


def start(port: int) -> None:
    """Serve /metrics on port in a daemon thread; 0 turns it off."""
    if port <= 0:
        return
    server = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
    threading.Thread(target=server.serve_forever, name="metrics", daemon=True).start()
    print(f"metrics on :{port}/metrics")
