"""Tests for the console's roles, demo login, simulation and relay.

    cd console && python -m unittest discover -p 'test_*.py'

access.py and demo.py are standard library only; the role tests import the
FastAPI app, reloading it per mode because the mode is read at import.
"""

from __future__ import annotations

import base64
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
        self.assertIsNone(access.unsign(token, self.secret,
                                        now=1000 + access.SESSION_TTL_S[access.DEMO] + 1))

    def test_role_is_signed_in(self):
        _, token = access.new_session(self.secret, role=access.ADMIN, now=1000)
        payload = access.unsign(token, self.secret, now=1001)
        self.assertEqual(payload["role"], access.ADMIN)
        self.assertIsNone(access.unsign(token, self.secret,
                                        now=1000 + access.SESSION_TTL_S[access.ADMIN] + 1))

    def test_forged_or_other_secret(self):
        _, token = access.new_session(self.secret)
        body, mac = token.split(".")
        self.assertIsNone(access.unsign(f"{body}.{mac[:-2]}xx", self.secret))
        self.assertIsNone(access.unsign(token, "t" * 40))
        self.assertIsNone(access.unsign("garbage", self.secret))

    def test_each_login_gets_a_new_session(self):
        first, _ = access.new_session(self.secret)
        second, _ = access.new_session(self.secret)
        self.assertNotEqual(first, second)


class AdminCredentialTests(unittest.TestCase):
    def test_hash_roundtrip(self):
        stored = access.hash_password("correct horse battery")
        self.assertTrue(access.verify_password("correct horse battery", stored))
        self.assertFalse(access.verify_password("correct horse batterY", stored))
        self.assertFalse(access.verify_password("x", "not-a-hash"))
        self.assertNotIn("$", stored)   # compose would interpolate it

    def test_totp_rfc6238_vector(self):
        # RFC 6238 appendix B, SHA-1: key "12345678901234567890" at T=59 -> 94287082.
        secret = base64.b32encode(b"12345678901234567890").decode()
        self.assertEqual(access.totp(secret, at=59), "287082")
        self.assertTrue(access.verify_totp("287082", secret, now=59 + 30))
        self.assertFalse(access.verify_totp("287082", secret, now=59 + 90))
        self.assertFalse(access.verify_totp("", secret, now=59))

    def test_throttle(self):
        t = access.Throttle(per_key=2, overall=3, window_s=60)
        t.fail("a", now=0)
        t.fail("a", now=1)
        self.assertTrue(t.blocked("a", now=2))
        self.assertFalse(t.blocked("b", now=2))
        t.fail("b", now=2)
        self.assertTrue(t.blocked("c", now=3))    # the global ceiling
        self.assertFalse(t.blocked("a", now=200))


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
                  "CONSOLE_RELAY_URL", "CONSOLE_RELAY_TOKEN", "CONSOLE_RELAY_ADMIN_TOKEN",
                  "CONSOLE_ADMIN_PASSWORD_HASH", "CONSOLE_ADMIN_TOTP_SECRET"):
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
ADMIN_TOKEN = "a" * 40
PASSWORD = "demo"
ADMIN_PASSWORD = "a long admin password"
ADMIN_HASH = access.hash_password(ADMIN_PASSWORD)
TOTP_SECRET = base64.b32encode(b"12345678901234567890").decode()


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

    def test_admin_login_is_off_without_its_settings(self):
        self.assertFalse(self.client.get("/api/session").json()["admin_enabled"])
        r = self.client.post("/api/login", json={"user": "admin", "password": ADMIN_PASSWORD},
                             headers=HEADERS)
        self.assertEqual(r.status_code, 404)

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


class FakeResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code, self._body = status_code, body if body is not None else {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError("unexpected relay failure")


@unittest.skipIf(TestClient is None, "console requirements not installed")
class PublicAdminLoginTests(unittest.TestCase):
    def make(self, **extra):
        app = load_app(CONSOLE_MODE="public", CONSOLE_RELAY_URL="http://admin:8090",
                       CONSOLE_RELAY_TOKEN=TOKEN, CONSOLE_RELAY_ADMIN_TOKEN=ADMIN_TOKEN,
                       CONSOLE_ADMIN_PASSWORD_HASH=ADMIN_HASH, **extra)
        app.ADMIN_THROTTLE = access.Throttle()
        return app, TestClient(app.app)

    def login(self, client, password=ADMIN_PASSWORD, code=""):
        return client.post("/api/login", json={"user": "admin", "password": password, "code": code},
                           headers=HEADERS)

    def test_admin_login_runs_everything_through_the_admin_relay(self):
        app, client = self.make()
        self.assertTrue(client.get("/api/session").json()["admin_enabled"])
        self.assertEqual(self.login(client).status_code, 200)
        self.assertEqual(client.get("/api/session").json()["role"], "admin")
        self.assertEqual({a["run"] for a in client.get("/api/actions").json()}, {"real"})
        relay = mock.AsyncMock(return_value=FakeResponse(body={"id": "j1"}))
        with mock.patch.object(app, "relay_request", relay), \
             mock.patch("asyncio.create_subprocess_exec",
                        side_effect=AssertionError("public mode ran a process")):
            r = client.post("/api/actions/seed", json={"params": {"dmas": 4}}, headers=HEADERS)
        self.assertEqual(r.json(), {"id": "j1"})
        method, path = relay.await_args.args
        self.assertEqual((method, path), ("POST", "/api/relay/admin/actions/seed"))
        self.assertTrue(relay.await_args.kwargs["admin"])
        # Admin-only tools still need the loopback console.
        self.assertEqual(client.get("/api/gis/collections").status_code, 404)

    def test_admin_jobs_come_from_the_admin_console(self):
        app, client = self.make()
        self.login(client)
        relay = mock.AsyncMock(return_value=FakeResponse(body=[{"id": "j1"}]))
        with mock.patch.object(app, "relay_request", relay):
            self.assertEqual(client.get("/api/jobs").json(), [{"id": "j1"}])
        self.assertEqual(relay.await_args.args, ("GET", "/api/relay/admin/jobs"))

    def test_admin_sees_links_and_containers(self):
        app, client = self.make()
        relay_body = {"db": {"ok": True}, "containers": {"ok": True, "value": [{"name": "broker"}]}}
        relay = mock.AsyncMock(return_value=FakeResponse(body=relay_body))
        with mock.patch.object(app, "relay_request", relay):
            app._status_cache["body"] = None
            anon = client.get("/api/status").json()
            self.assertTrue(anon["containers"]["hidden"])
            self.assertNotIn("kibana_url", client.get("/api/config").json())
            self.login(client)
            self.assertEqual(client.get("/api/status").json()["containers"]["value"],
                             [{"name": "broker"}])
            cfg = client.get("/api/config").json()
            self.assertIn("kibana_url", cfg)
            self.assertNotIn("gis", cfg)   # connection details stay on the loopback console
        # Both answers came from one relayed body, so a viewer never shares
        # the admin's copy.
        self.assertEqual(relay.await_count, 1)

    def test_wrong_password_and_throttle(self):
        _, client = self.make()
        for _ in range(5):
            self.assertEqual(self.login(client, password="wrong").status_code, 401)
        self.assertEqual(self.login(client).status_code, 429)
        self.assertEqual(client.get("/api/session").json()["role"], "public")

    def test_authenticator_code(self):
        app, client = self.make(CONSOLE_ADMIN_TOTP_SECRET=TOTP_SECRET)
        self.assertTrue(client.get("/api/session").json()["admin_code"])
        self.assertEqual(self.login(client).status_code, 401)
        self.assertEqual(self.login(client, code=access.totp(TOTP_SECRET)).status_code, 200)

    def test_demo_cookie_cannot_claim_admin(self):
        app, client = self.make()
        client.post("/api/login", json={"password": PASSWORD}, headers=HEADERS)
        self.assertEqual(client.get("/api/session").json()["role"], "demo")
        # A cookie signed by another secret is not a session at all.
        _, forged = access.new_session("x" * 40, role=access.ADMIN)
        client.cookies.set(access.SESSION_COOKIE, forged)
        self.assertEqual(client.get("/api/session").json()["role"], "public")

    def test_tokens_must_differ(self):
        with self.assertRaises(SystemExit):
            load_app(CONSOLE_MODE="public", CONSOLE_RELAY_URL="http://admin:8090",
                     CONSOLE_RELAY_TOKEN=TOKEN, CONSOLE_RELAY_ADMIN_TOKEN=TOKEN)


@unittest.skipIf(TestClient is None, "console requirements not installed")
class AdminRelayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app(CONSOLE_MODE="admin", CONSOLE_RELAY_TOKEN=TOKEN,
                           CONSOLE_RELAY_ADMIN_TOKEN=ADMIN_TOKEN)

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

    def test_demo_token_cannot_use_admin_routes(self):
        r = self.client.post("/api/relay/admin/actions/seed", json={"params": {}},
                             headers={**HEADERS, "Authorization": f"Bearer {TOKEN}"})
        self.assertEqual(r.status_code, 404)
        self.assertEqual(self.client.get("/api/relay/admin/jobs",
                                         headers={"Authorization": f"Bearer {TOKEN}"}).status_code, 404)

    def test_admin_token_runs_any_action(self):
        with mock.patch.object(self.app, "start_job",
                               mock.AsyncMock(return_value={"id": "abc"})) as start:
            r = self.client.post("/api/relay/admin/actions/seed", json={"params": {}},
                                 headers={**HEADERS, "Authorization": f"Bearer {ADMIN_TOKEN}"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(start.await_args.kwargs["origin"], "admin")

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
