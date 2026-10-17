"""End-to-end: one control-plane server process + two worker processes over HTTP.

Each worker has its own state dir, workspace root and tmux server (standing in for two
machines); jobs name a repository URL (a local bare repo standing in for a Git host).
Uses PostgreSQL when GANGLION_TEST_PG_DSN is set, SQLite otherwise.

Scenario (the Ganglion 0.2 acceptance scenario):
  1. A and B race for one job; exactly one claims, prepares, launches, heartbeats, completes.
  2. The owning worker is SIGKILLed mid-run (its agent keeps running). The lease expires;
     the other worker claims a new attempt (epoch 2). The old agent then finishes and the
     old worker comes back: it must not be able to report. Epoch 2 completes the job.
  3. The control plane itself is SIGKILLed and restarted mid-run; the job still completes.
"""

from __future__ import annotations

import signal
import subprocess
import sys
import time
import unittest
import uuid
from pathlib import Path

from support import BIN, PG_DSN, Sandbox, toml, wait_for

from ganglion.control.client import HttpClient
from ganglion.control.models import Unavailable
from ganglion.worker.state import WorkerState

try:
    import fastapi  # noqa: F401
    import uvicorn  # noqa: F401

    HAVE_SERVER = True
except ImportError:
    HAVE_SERVER = False

TOKEN = "e2e-" + uuid.uuid4().hex
LEASE_TTL = 3


def free_port() -> int:
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


@unittest.skipUnless(HAVE_SERVER, "server extra (fastapi, uvicorn) not installed")
class TwoWorkerTest(Sandbox):
    def setUp(self):
        super().setUp()
        self.procs: dict[str, subprocess.Popen] = {}
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.token_file = self.tmp / "token"
        self.token_file.write_text(TOKEN + "\n")
        self.token_file.chmod(0o600)
        if PG_DSN:
            import psycopg

            self.schema = f"e2e_{uuid.uuid4().hex[:10]}"
            with psycopg.connect(PG_DSN, autocommit=True) as c:
                c.execute(f"CREATE SCHEMA {self.schema}")
            sep = "&" if "?" in PG_DSN else "?"
            db = f"{PG_DSN}{sep}options=-csearch_path%3D{self.schema}"
        else:
            db = f"sqlite:///{self.tmp / 'cp.db'}"
        self.cp_config = self.tmp / "cp.toml"
        self.cp_config.write_text(toml({
            "worker": {"id": "operator", "control_plane": self.url, "state_dir": str(self.tmp / "state-op")},
            "control": {"database": db, "listen": "127.0.0.1", "port": self.port, "token_file": str(self.token_file),
                        "lease_ttl": LEASE_TTL, "heartbeat_interval": 0.5, "sweep_interval": 0.5,
                        "worker_offline_after": 5, "max_attempts": 3},
            "notify": {"backend": "file", "file_path": str(self.notif)},
        }))
        self.remote = self.make_remote("svc")
        self.start_server()
        self.cfgs = {n: self.worker_config(n, control_plane=self.url, inbox=False, token_file=self.token_file,
                                           worker={"heartbeat_interval": 0.5}, claude={"trust_wait": 0.3})
                     for n in ("A", "B")}
        for n in ("A", "B"):
            self.start_worker(n)
        self.api = HttpClient(self.url, TOKEN, "operator", timeout=5)
        wait_for(lambda: len(self._workers()) == 2, msg="both workers registered")

    def tearDown(self):
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
                p.wait(10)
        super().tearDown()
        if PG_DSN:
            import psycopg

            with psycopg.connect(PG_DSN, autocommit=True) as c:
                c.execute(f"DROP SCHEMA {self.schema} CASCADE")

    # -- process control ---------------------------------------------------------------
    def spawn(self, name: str, *args: str) -> subprocess.Popen:
        with open(self.tmp / f"{name}.log", "ab") as log:  # the child keeps its own descriptor
            p = subprocess.Popen([sys.executable, str(BIN), *args], stdout=log, stderr=subprocess.STDOUT)
        self.procs[name] = p
        return p

    def start_server(self):
        self.spawn("server", "--config", str(self.cp_config), "server", "start", "--allow-sqlite")
        wait_for(self._healthy, timeout=20, msg="control plane up")

    def start_worker(self, name: str):
        self.spawn(name, "--config", str(self.cfgs[name]), "worker", "start")

    def kill(self, name: str):
        p = self.procs[name]
        p.send_signal(signal.SIGKILL)
        p.wait(10)

    def _healthy(self):
        import urllib.request

        try:
            with urllib.request.urlopen(f"{self.url}/healthz", timeout=1):
                return True
        except OSError:
            return False

    def _workers(self):
        try:
            return self.api.list_workers()
        except Unavailable:
            return []

    # -- observation -------------------------------------------------------------------
    def submit(self, name: str) -> dict:
        out = subprocess.run([sys.executable, str(BIN), "--config", str(self.cp_config), "job", "submit",
                              "--repo", "svc", "--url", str(self.remote), "--name", name, "--key", f"e2e:{name}",
                              "--goal", f"# {name}\nimplement it\n"], capture_output=True, text=True, check=True)
        self.assertIn('"created": true', out.stdout)
        return self.api.get_job(f"e2e:{name}")

    def job(self, key: str) -> dict:
        return self.api.get_job(key)

    def local(self, worker: str, name: str) -> dict | None:
        cfg = self.load(self.cfgs[worker])
        recs = [r for r in WorkerState(cfg.state_dir).snapshot()["attempts"].values() if r.get("name") == name]
        return max(recs, key=lambda r: r["epoch"]) if recs else None

    def socket_of(self, worker: str) -> str:
        return self.load(self.cfgs[worker]).claude.tmux_socket

    def go(self, name: str, attempt_id: str):
        (self.ctl / f"{name}.{attempt_id}.go").touch()

    def running_owner(self, key: str):
        j = self.job(key)
        if j["status"] != "running":
            return None
        cur = [a for a in j["attempts"] if a["id"] == j["current_attempt_id"]][0]
        rec = self.local(cur["worker_id"], j["name"])
        return (cur["worker_id"], cur) if rec and rec["status"] == "running" else None

    # -- the scenario ------------------------------------------------------------------
    def test_race_complete_crash_reassign_stale_return_and_cp_restart(self):
        # 1. race + happy path ------------------------------------------------------------
        self.set_mode("happy", "deliver")
        self.submit("happy")
        wait_for(lambda: self.job("e2e:happy")["status"] == "completed", timeout=40, msg="happy completed")
        j = self.job("e2e:happy")
        self.assertEqual(len(j["attempts"]), 1)
        winner = j["attempts"][0]["worker_id"]
        loser = "B" if winner == "A" else "A"
        self.assertIsNone(self.local(loser, "happy"))
        self.assertEqual(len(self.invocations("happy")), 1)
        res = j["result"]["engineering_result"]
        self.assertEqual(res["source"], "delivery-md")
        self.assertTrue(res["final_sha"])
        self.assertIn("delivery line 1", res["summary"])

        # 2. crash -> lease expiry -> reassignment -> stale worker returns ----------------------
        self.set_mode("crash", "ondemand")
        self.submit("crash")
        owner, att1 = wait_for(lambda: self.running_owner("e2e:crash"), timeout=40, msg="crash running")
        other = "B" if owner == "A" else "A"
        old = self.local(owner, "crash")
        self.kill(owner)  # worker process dies; its claude session keeps running
        self.assertIn(old["handle"], self.sessions(self.socket_of(owner)))

        def reassigned():
            j = self.job("e2e:crash")
            return j["epoch"] == 2 and j["status"] == "running" and self.local(other, "crash") \
                and self.local(other, "crash")["status"] == "running"

        wait_for(reassigned, timeout=40, msg="reassigned to the other worker")
        j = self.job("e2e:crash")
        self.assertEqual([(a["worker_id"], a["epoch"], a["status"]) for a in j["attempts"]],
                         [(owner, 1, "expired"), (other, 2, "active")])
        new = self.local(other, "crash")

        self.go("crash", old["attempt_id"])  # the orphaned agent finishes its (now stale) work
        wait_for(lambda: (Path(old["workspace"]) / "DELIVERY.md").exists(), msg="stale agent delivered")
        self.start_worker(owner)  # the old worker comes back
        wait_for(lambda: self.local(owner, "crash")["status"] == "stale", timeout=30, msg="old worker marks stale")
        time.sleep(2)
        j = self.job("e2e:crash")
        self.assertEqual((j["status"], j["current_attempt_id"], j["epoch"]), ("running", new["attempt_id"], 2))
        self.assertIn(old["handle"], self.sessions(self.socket_of(owner)))  # never killed by the stale path
        types = [e["type"] for e in self.api.events(job_id=j["id"], limit=1000)]
        self.assertNotIn("job.completed", types)

        self.go("crash", new["attempt_id"])
        wait_for(lambda: self.job("e2e:crash")["status"] == "completed", timeout=40, msg="epoch 2 completes")
        j = self.job("e2e:crash")
        self.assertEqual(j["result"]["attempt_id"], new["attempt_id"])
        self.assertEqual([a["status"] for a in j["attempts"]], ["expired", "completed"])

        # 3. control plane crash + restart mid-run -------------------------------------------
        self.set_mode("cprestart", "ondemand")
        self.submit("cprestart")
        owner3, att3 = wait_for(lambda: self.running_owner("e2e:cprestart"), timeout=40, msg="cprestart running")
        self.kill("server")
        time.sleep(LEASE_TTL + 1)  # longer than the lease: restart grace must cover the downtime
        self.start_server()
        rec3 = self.local(owner3, "cprestart")
        self.go("cprestart", rec3["attempt_id"])
        wait_for(lambda: self.job("e2e:cprestart")["status"] == "completed", timeout=40, msg="completes after CP restart")
        j = self.job("e2e:cprestart")
        self.assertEqual(len(j["attempts"]), 1)  # no spurious reassignment
        self.assertEqual(j["result"]["attempt_id"], att3["id"])

        m = self.api.metrics()
        self.assertEqual(m["jobs_by_status"], {"completed": 3})
        self.assertGreaterEqual(m["lease_expirations"], 1)
        self.assertGreaterEqual(m["stale_attempt_rejections"], 0)
        self.assertEqual(sum("已交付" in t for t in self.titles()), 3)
        for n in ("A", "B"):  # the workers are still healthy
            self.assertIsNone(self.procs[n].poll(), f"worker {n} died:\n" + (self.tmp / f"{n}.log").read_text()[-3000:])


if __name__ == "__main__":
    unittest.main()
