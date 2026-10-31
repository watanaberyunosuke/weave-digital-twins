"""Tests for the console's roles, demo login, simulation and relay.

    cd console && python -m unittest discover -p 'test_*.py'

access.py and demo.py are standard library only; the role tests import the
FastAPI app, reloading it per mode because the mode is read at import.
"""

from __future__ import annotations

import importlib
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import access  # noqa: E402
import demo  # noqa: E402


class PasswordTests(unittest.TestCase):
    def test_match(self):
        self.assertTrue(access.password_matches("demo", "demo"))
        self.assertFalse(access.password_matches("Demo", "demo"))
        self.assertFalse(access.password_matches("", "demo"))


class SessionTests(unittest.TestCase):
    secret = "s" * 40

    def test_sign_and_expiry(self):
        sid, token = access.new_session(self.secret, now=1000)
        self.assertEqual(access.unsign(token, self.secret, now=1001)["sid"], sid)
        self.assertIsNone(access.unsign(token, self.secret, now=1000 + access.SESSION_TTL_S + 1))

    def test_forged_or_other_secret(self):
        _, token = access.new_session(self.secret)
        body, mac = token.split(".")
        self.assertIsNone(access.unsign(f"{body}.{mac[:-2]}xx", self.secret))
        self.assertIsNone(access.unsign(token, "t" * 40))
        self.assertIsNone(access.unsign("garbage", self.secret))

    def test_each_login_gets_a_new_session(self):
        self.assertNotEqual(access.new_session(self.secret)[0], access.new_session(self.secret)[0])


class CooldownTests(unittest.TestCase):
    def test_cooldown(self):
        c = access.Cooldown(60)
        self.assertEqual(c.wait_s("s", now=0), 0)
        c.mark("s", now=0)
        self.assertEqual(c.wait_s("s", now=10), 50)
        self.assertEqual(c.wait_s("s", now=60), 0)


class SimulationTests(unittest.TestCase):
    def test_banner_command_and_params(self):
        lines = demo.simulated_lines("seed", {"dmas": 7, "meters_per_dma": 3}, ["docker", "exec", "x"])
        self.assertEqual(lines[0], demo.BANNER)
        self.assertEqual(lines[1], "$ docker exec x")
        self.assertTrue(any("7 DMAs" in line for line in lines))

    def test_every_action_has_a_sample(self):
        for action in ("create_topics", "load_gis", "seed", "register_connectors", "build_model",
                       "start_flink", "reset_baseline", "load_elevation", "discover", "run_sim_once"):
            self.assertNotIn("(no sample", " ".join(demo.simulated_lines(action, {}, ["c"])), action)


def load_app(**env):
    base = {"CONSOLE_ALLOWED_HOSTS": "testserver", "CONSOLE_COOKIE_SECURE": "0"}
    with mock.patch.dict(os.environ, {**base, **env}, clear=False):
        for k in ("CONSOLE_MODE", "CONSOLE_DEMO", "CONSOLE_DEMO_PASSWORD", "CONSOLE_SESSION_SECRET",
                  "CONSOLE_RELAY_URL", "CONSOLE_RELAY_TOKEN"):
            if k not in env:
                os.environ.pop(k, None)
        import app  # noqa: PLC0415
        return importlib.reload(app)


try:
    from fastapi.testclient import TestClient
except ImportError:   # the stdlib tests above still run without the console deps
    TestClient = None

HEADERS = {"X-Weave-Console": "1"}
TOKEN = "t" * 40
PASSWORD = "demo"


@unittest.skipIf(TestClient is None, "console requirements not installed")
class PublicModeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Defaults only: the demo login is on with password "demo" and a
        # per-start session secret.
        cls.app = load_app(CONSOLE_MODE="public")

    def setUp(self):
        self.client = TestClient(self.app.app)
        # Nothing in public mode may start a process.
        patcher = mock.patch("asyncio.create_subprocess_exec",
                             side_effect=AssertionError("public mode ran a process"))
        patcher.start()
        self.addCleanup(patcher.stop)

    def login(self):
        r = self.client.post("/api/login", json={"password": PASSWORD}, headers=HEADERS)
        self.assertEqual(r.status_code, 200)

    def test_anonymous_is_public_and_cannot_act(self):
        session = self.client.get("/api/session").json()
        self.assertEqual((session["role"], session["demo_password"]), ("public", "demo"))
        r = self.client.post("/api/actions/seed", json={"params": {}}, headers=HEADERS)
        self.assertEqual(r.status_code, 401)
        r = self.client.post("/api/actions/seed/preview", json={"params": {}}, headers=HEADERS)
        self.assertEqual(r.status_code, 401)
        self.assertTrue(all(a["run"] == "none" for a in self.client.get("/api/actions").json()))

    def test_admin_only_details_are_absent(self):
        cfg = self.client.get("/api/config").json()
        for key in ("gis", "kibana_url", "flink_url", "control_center_url"):
            self.assertNotIn(key, cfg)
        for path in ("/api/gis/collections", "/api/gis/qgis/postgis-connection.xml",
                     "/api/gis/qgis/ogcapi-connection.xml"):
            self.assertEqual(self.client.get(path).status_code, 404, path)

    def test_wrong_password(self):
        r = self.client.post("/api/login", json={"password": "nope"}, headers=HEADERS)
        self.assertEqual(r.status_code, 401)

    def test_demo_actions_are_simulated_without_a_relay(self):
        self.login()
        self.assertEqual(self.client.get("/api/session").json()["role"], "demo")
        acts = {a["id"]: a["run"] for a in self.client.get("/api/actions").json()}
        self.assertEqual(set(acts.values()), {"simulated"})   # no relay configured
        r = self.client.post("/api/actions/seed", json={"params": {"dmas": 4}}, headers=HEADERS)
        self.assertEqual(r.status_code, 200)
        job = self.client.get(f"/api/jobs/{r.json()['id']}").json()
        self.assertTrue(job["simulated"])
        self.assertEqual(job["lines"][0], demo.BANNER)
        self.assertEqual(len(self.client.get("/api/jobs").json()), 1)

    def test_login_again_resets_history(self):
        self.login()
        self.client.post("/api/actions/create_topics", json={"params": {}}, headers=HEADERS)
        self.assertEqual(len(self.client.get("/api/jobs").json()), 1)
        self.login()
        self.assertEqual(self.client.get("/api/jobs").json(), [])

    def test_sessions_do_not_see_each_other(self):
        self.login()
        r = self.client.post("/api/actions/build_model", json={"params": {}}, headers=HEADERS)
        other = TestClient(self.app.app)
        other.post("/api/login", json={"password": PASSWORD}, headers=HEADERS)
        self.assertEqual(other.get(f"/api/jobs/{r.json()['id']}").status_code, 404)
        self.assertEqual(TestClient(self.app.app).get("/api/jobs").json(), [])

    def test_demo_can_be_switched_off(self):
        app = load_app(CONSOLE_MODE="public", CONSOLE_DEMO="0")
        client = TestClient(app.app)
        self.assertFalse(client.get("/api/session").json()["demo_enabled"])
        r = client.post("/api/login", json={"password": PASSWORD}, headers=HEADERS)
        self.assertEqual(r.status_code, 404)
        type(self).app = load_app(CONSOLE_MODE="public")

    def test_post_needs_the_console_header(self):
        r = self.client.post("/api/login", json={"password": PASSWORD})
        self.assertEqual(r.status_code, 403)


@unittest.skipIf(TestClient is None, "console requirements not installed")
class PublicWithRelayTests(unittest.TestCase):
    def test_only_safe_actions_run_live(self):
        app = load_app(CONSOLE_MODE="public", CONSOLE_RELAY_URL="http://admin:8090",
                       CONSOLE_RELAY_TOKEN=TOKEN)
        client = TestClient(app.app)
        client.post("/api/login", json={"password": PASSWORD}, headers=HEADERS)
        acts = {a["id"]: a["run"] for a in client.get("/api/actions").json()}
        self.assertEqual({k for k, v in acts.items() if v == "real"}, set(access.SAFE_ACTIONS))


@unittest.skipIf(TestClient is None, "console requirements not installed")
class AdminRelayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app(CONSOLE_MODE="admin", CONSOLE_RELAY_TOKEN=TOKEN)

    def setUp(self):
        self.client = TestClient(self.app.app)

    def relay(self, action, token=TOKEN):
        return self.client.post(f"/api/relay/actions/{action}", json={"params": {}},
                                headers={**HEADERS, "Authorization": f"Bearer {token}"})

    def test_admin_mode_is_unchanged(self):
        self.assertEqual(self.client.get("/api/session").json()["role"], "admin")
        self.assertIn("gis", self.client.get("/api/config").json())

    def test_bad_token_looks_like_no_route(self):
        self.assertEqual(self.relay("discover", token="wrong").status_code, 404)
        self.assertEqual(self.client.get("/api/relay/status").status_code, 404)

    def test_unsafe_action_refused(self):
        self.assertEqual(self.relay("seed").status_code, 403)
        self.assertEqual(self.relay("start_flink").status_code, 403)

    def test_live_runs_are_limited_across_all_demo_users(self):
        with mock.patch.object(self.app, "start_job",
                               mock.AsyncMock(return_value={"id": "abc"})):
            self.app.RELAY_COOLDOWN._last.clear()
            self.assertEqual(self.relay("discover").status_code, 200)
            self.assertEqual(self.relay("run_sim_once").status_code, 429)

    def test_safe_action_starts_a_demo_job(self):
        self.app.RELAY_COOLDOWN._last.clear()
        with mock.patch.object(self.app, "start_job",
                               mock.AsyncMock(return_value={"id": "abc"})) as start:
            r = self.relay("run_sim_once")
        self.assertEqual(r.status_code, 200)
        start.assert_awaited_once()
        self.assertEqual(start.await_args.kwargs["origin"], "demo")


if __name__ == "__main__":
    unittest.main()
