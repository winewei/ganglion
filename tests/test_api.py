"""HTTP API: auth, error mapping through the stdlib client, OpenAPI drift, server restart."""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path

from support import BIN, ROOT

try:
    import uvicorn

    from ganglion.control.api import create_app
except ImportError:  # server extra not installed
    uvicorn = None

from ganglion.control.client import HttpClient
from ganglion.control.models import NotFound, StaleAttempt, Unauthorized, Unavailable
from ganglion.control.service import ControlPlane, Settings
from ganglion.control.store import SqliteStore

TOKEN = "s3cret-token-for-tests"
WORKER = {"platform": "linux", "arch": "x86_64", "launchers": ["claude-code"], "capabilities": {"git": True}, "repos": ["app"]}


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    """uvicorn in a thread, stoppable (to simulate a control-plane restart)."""

    def __init__(self, db: Path, port: int, settings: Settings | None = None, tokens=(TOKEN,)):
        self.service = ControlPlane(SqliteStore(db), settings or Settings(lease_ttl=2))
        app = create_app(self.service, list(tokens), sweep_interval=0.2)
        self.server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        deadline = time.time() + 10
        while not self.server.started:
            if time.time() > deadline:
                raise RuntimeError("server did not start")
            time.sleep(0.05)

    def stop(self):
        self.server.should_exit = True
        self.thread.join(10)
        self.service.store.close()


@unittest.skipIf(uvicorn is None, "server extra (fastapi, uvicorn) not installed")
class ApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="ganglion-api-"))
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.srv = Server(self.tmp / "cp.db", self.port)

    def tearDown(self):
        self.srv.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def client(self, worker_id="A", token=TOKEN):
        return HttpClient(self.url, token, worker_id, timeout=5)

    def test_auth_required(self):
        with self.assertRaises(Unauthorized):
            self.client(token="").list_jobs()
        with self.assertRaises(Unauthorized):
            self.client(token="wrong").list_jobs()
        self.assertEqual(self.client().list_jobs(), [])
        with urllib.request.urlopen(f"{self.url}/healthz") as r:  # liveness is unauthenticated
            self.assertEqual(json.loads(r.read()), {"ok": True})

    def test_refuses_to_start_without_token(self):
        with self.assertRaises(RuntimeError):
            create_app(self.srv.service, [])

    def test_full_attempt_cycle_and_fencing_over_http(self):
        a, b = self.client("A"), self.client("B")
        a.register(WORKER)
        b.register(WORKER)
        job, created = a.create_job({"key": "k1", "name": "j1", "repository": {"name": "app"}, "goal": "g"})
        self.assertTrue(created)
        self.assertFalse(a.create_job({"key": "k1", "repository": {"name": "app"}})[1])
        got = a.claim("c-1", [])
        self.assertEqual(got["attempt"]["epoch"], 1)
        self.assertTrue(a.claim("c-1", [])["replay"])
        self.assertIsNone(b.claim("c-2", []))
        att = got["attempt"]
        a.heartbeat(att["id"], 1)
        with self.assertRaises(StaleAttempt) as cm:
            b.heartbeat(att["id"], 1)
        self.assertEqual(cm.exception.extra.get("reason"), "worker_mismatch")
        time.sleep(2.3)  # lease_ttl = 2
        got_b = b.claim("c-3", [])
        self.assertEqual(got_b["attempt"]["epoch"], 2)
        for op in (lambda: a.heartbeat(att["id"], 1),
                   lambda: a.complete(att["id"], 1, "r", {"summary": "stale"}),
                   lambda: a.fail(att["id"], 1, "r2", "x"),
                   lambda: a.release(att["id"], 1, "r3")):
            with self.assertRaises(StaleAttempt):
                op()
        done = b.complete(got_b["attempt"]["id"], 2, "rb", {"summary": "ok", "final_sha": "abc"})
        self.assertEqual(done["job"]["status"], "completed")
        self.assertTrue(b.complete(got_b["attempt"]["id"], 2, "rb", {"summary": "ok"})["replay"])
        full = a.get_job("k1")  # lookup by key
        self.assertEqual([x["status"] for x in full["attempts"]], ["expired", "completed"])
        types = [e["type"] for e in a.events(job_id=job["id"])]
        for t in ("job.created", "job.claimed", "attempt.started", "attempt.heartbeat", "attempt.expired",
                  "attempt.rejected", "attempt.completed", "result.received", "job.completed"):
            self.assertIn(t, types)
        m = a.metrics()
        self.assertEqual(m["jobs_by_status"], {"completed": 1})
        self.assertGreaterEqual(m["stale_attempt_rejections"], 4)
        with urllib.request.urlopen(urllib.request.Request(f"{self.url}/v1/metrics?format=prometheus",
                                                           headers={"Authorization": f"Bearer {TOKEN}"})) as r:
            self.assertIn("ganglion_lease_expirations 1", r.read().decode())

    def test_errors_map_to_types(self):
        c = self.client("A")
        with self.assertRaises(NotFound):
            c.get_job("nope")
        with self.assertRaises(NotFound):
            c.claim("x", [])  # not registered
        self.assertEqual(c.config()["lease_ttl"], 2)

    def test_state_survives_server_restart(self):
        c = self.client("A")
        c.register(WORKER)
        c.create_job({"key": "persist", "repository": {"name": "app"}})
        att = c.claim("p1", [])["attempt"]
        self.srv.stop()
        with self.assertRaises(Unavailable):  # server down: transient, not a failure
            c.heartbeat(att["id"], 1)
        self.srv = Server(self.tmp / "cp.db", self.port)
        c.heartbeat(att["id"], 1)
        self.assertEqual(c.get_job("persist")["status"], "running")
        self.assertEqual(c.complete(att["id"], 1, "done", {"summary": "s"})["job"]["status"], "completed")


@unittest.skipIf(uvicorn is None, "server extra (fastapi, uvicorn) not installed")
class OpenApiContractTest(unittest.TestCase):
    def test_committed_openapi_matches_implementation(self):
        out = subprocess.run([sys.executable, str(BIN), "server", "openapi"], capture_output=True, text=True, check=True).stdout
        committed = (ROOT / "openapi.json").read_text(encoding="utf-8")
        self.assertEqual(json.loads(out), json.loads(committed),
                         "openapi.json is stale: run `bin/ganglion server openapi > openapi.json`")
        paths = json.loads(out)["paths"]
        for p in ("/v1/jobs", "/v1/jobs/{job_id}", "/v1/jobs/claim", "/v1/workers/register",
                  "/v1/workers/{worker_id}/heartbeat", "/v1/attempts/{attempt_id}/heartbeat",
                  "/v1/attempts/{attempt_id}/complete", "/v1/attempts/{attempt_id}/fail",
                  "/v1/attempts/{attempt_id}/release", "/v1/config", "/v1/events", "/v1/metrics"):
            self.assertIn(p, paths)


if __name__ == "__main__":
    unittest.main()
