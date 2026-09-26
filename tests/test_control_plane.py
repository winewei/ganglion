"""Control-plane semantics: claim atomicity, leases, fencing, idempotency, persistence.

Runs against SQLite always, and against PostgreSQL when GANGLION_TEST_PG_DSN is set
(e.g. postgresql://postgres@127.0.0.1:55432/ganglion_test). The Postgres run matters most
for the claim race: there, racing compare-and-sets genuinely contend on a row lock.
"""

from __future__ import annotations

import shutil
import tempfile
import threading
import unittest
import uuid
from pathlib import Path

from support import PG_DSN  # noqa: F401  (also puts the repo on sys.path)

from ganglion.control.models import Conflict, InvalidRequest, NotFound, StaleAttempt
from ganglion.control.service import ControlPlane, Settings
from ganglion.control.store import PostgresStore, SqliteStore
from ganglion.notify import FileNotifier


class Clock:
    def __init__(self, t: float = 1_000_000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


WORKER = {"platform": "linux", "arch": "x86_64", "launchers": ["claude-code"], "capabilities": {"git": True, "gh": True},
          "repos": ["app"]}


class Base:
    """Mixin; concrete classes pick the backend."""

    def make_store(self):  # pragma: no cover
        raise NotImplementedError

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="ganglion-cp-"))
        self.clock = Clock()
        self.notif = self.tmp / "n.jsonl"
        self.settings = Settings(lease_ttl=60, max_depth=2, max_attempts=3)
        self.store = self.make_store()
        self.cp = self.new_cp()
        self.cp.start()
        for w in ("A", "B", "C"):
            self.cp.register_worker(w, WORKER)

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def new_cp(self, store=None) -> ControlPlane:
        return ControlPlane(store or self.store, self.settings, FileNotifier(str(self.notif)), clock=self.clock)

    def job(self, **kw):
        spec = {"repository": {"name": "app"}, "goal": "do it"}
        spec.update(kw)
        self.clock.advance(0.001)  # distinct created_at, as in real life (FIFO order)
        return self.cp.create_job(spec)[0]

    def events(self, job_id, type_=None):
        return [e for e in self.cp.events(job_id=job_id, limit=1000) if type_ is None or e["type"] == type_]

    def titles(self):
        import json

        return [json.loads(l)["title"] for l in self.notif.read_text().splitlines()] if self.notif.exists() else []

    # ---------------------------------------------------------------- claim
    def test_claim_race_exactly_one_winner(self):
        job = self.job()
        n = 8
        for i in range(n):
            self.cp.register_worker(f"R{i}", WORKER)
        barrier = threading.Barrier(n)
        results, errors = [None] * n, []

        def run(i):
            cp = self.new_cp()  # own service object; the store gives each thread its own connection
            try:
                barrier.wait()
                results[i] = cp.claim(f"R{i}", request_id=f"req-{i}")
            except Exception as e:  # pragma: no cover
                errors.append(e)
            finally:
                cp.store.close()

        threads = [threading.Thread(target=run, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        self.assertEqual(errors, [])
        winners = [r for r in results if r]
        self.assertEqual(len(winners), 1)
        full = self.cp.get_job(job["id"])
        self.assertEqual(full["status"], "running")
        self.assertEqual(full["epoch"], 1)
        self.assertEqual(len(full["attempts"]), 1)
        self.assertEqual(full["current_attempt_id"], winners[0]["attempt"]["id"])
        self.assertEqual(len(self.events(job["id"], "job.claimed")), 1)

    def test_claim_race_many_jobs_no_double_assignment(self):
        jobs = [self.job(name=f"j{i}") for i in range(6)]
        n = 6
        for i in range(n):
            self.cp.register_worker(f"R{i}", WORKER)
        barrier = threading.Barrier(n)
        got: list = []
        lock = threading.Lock()

        def run(i):
            cp = self.new_cp()
            try:
                barrier.wait()
                while True:
                    r = cp.claim(f"R{i}", request_id=f"q-{i}-{uuid.uuid4().hex}")
                    if not r:
                        break
                    with lock:
                        got.append(r["job"]["id"])
            finally:
                cp.store.close()

        threads = [threading.Thread(target=run, args=(i,)) for i in range(n)]
        [t.start() for t in threads]
        [t.join(30) for t in threads]
        self.assertEqual(sorted(got), sorted(j["id"] for j in jobs))  # every job exactly once

    def test_lost_claim_response_is_replayed_not_duplicated(self):
        self.job(name="one")
        self.job(name="two")
        first = self.cp.claim("A", request_id="lost-1")
        again = self.cp.claim("A", request_id="lost-1")  # retry after a lost response
        self.assertFalse(first["replay"])
        self.assertTrue(again["replay"])
        self.assertEqual(first["attempt"]["id"], again["attempt"]["id"])
        running = self.cp.list_jobs("running")
        self.assertEqual(len(running), 1)  # "two" was not taken by the retry
        with self.assertRaises(InvalidRequest):
            self.cp.claim("B", request_id="lost-1")

    def test_fifo_and_capability_matching(self):
        a = self.job(name="first")
        self.job(name="second")
        self.cp.register_worker("X", {**WORKER, "repos": ["other"]})
        self.assertIsNone(self.cp.claim("X"))  # does not map "app"
        self.assertEqual(self.cp.claim("A")["job"]["id"], a["id"])
        needs = self.job(name="needs-docker", requirements={"capabilities": {"docker": True}})
        self.cp.claim("A")  # takes "second"
        self.assertIsNone(self.cp.claim("B"))  # "needs-docker" requires docker
        self.cp.register_worker("D", {**WORKER, "capabilities": {"docker": True}})
        self.assertEqual(self.cp.claim("D")["job"]["id"], needs["id"])

    def test_shared_checkout_exclusion(self):
        self.job(name="x")
        self.assertIsNone(self.cp.claim("A", exclude_repos=["app"]))
        self.assertIsNotNone(self.cp.claim("A"))

    def test_unregistered_worker_cannot_claim(self):
        self.job()
        with self.assertRaises(NotFound):
            self.cp.claim("ghost")

    # ---------------------------------------------------------------- lease + fencing
    def claim(self, worker="A"):
        r = self.cp.claim(worker)
        return r["job"], r["attempt"]

    def test_owner_renews_others_cannot(self):
        self.job()
        job, att = self.claim("A")
        self.clock.advance(50)
        hb = self.cp.heartbeat(att["id"], "A", att["epoch"])
        self.assertEqual(hb["lease_until"], self.clock.t + 60)
        self.clock.advance(50)  # still alive thanks to the renewal
        self.cp.heartbeat(att["id"], "A", att["epoch"])
        with self.assertRaises(StaleAttempt) as cm:
            self.cp.heartbeat(att["id"], "B", att["epoch"])
        self.assertEqual(cm.exception.extra["reason"], "worker_mismatch")
        with self.assertRaises(StaleAttempt) as cm:
            self.cp.heartbeat(att["id"], "A", att["epoch"] + 1)
        self.assertEqual(cm.exception.extra["reason"], "epoch_mismatch")

    def test_expired_lease_is_not_authoritative_and_job_is_reclaimable(self):
        self.job()
        job, a1 = self.claim("A")
        self.clock.advance(61)
        with self.assertRaises(StaleAttempt) as cm:
            self.cp.heartbeat(a1["id"], "A", 1)
        self.assertEqual(cm.exception.extra["reason"], "lease_expired")
        full = self.cp.get_job(job["id"])
        self.assertEqual(full["status"], "queued")
        self.assertEqual(full["attempts"][0]["status"], "expired")
        job2, a2 = self.claim("B")
        self.assertEqual(job2["id"], job["id"])
        self.assertEqual(a2["epoch"], 2)

    def test_claim_expires_lapsed_lease_directly(self):
        self.job()
        job, a1 = self.claim("A")
        self.clock.advance(61)
        _, a2 = self.claim("B")  # no sweep in between
        self.assertEqual(a2["epoch"], 2)
        self.assertEqual(self.cp.get_job(job["id"])["attempts"][0]["status"], "expired")
        self.assertEqual(len(self.events(job["id"], "attempt.expired")), 1)

    def test_fencing_stale_worker_cannot_mutate(self):
        """Attempt A (epoch 1) expires; B (epoch 2) owns the job; every A mutation is rejected."""
        job = self.job()
        _, a = self.claim("A")
        self.clock.advance(61)
        _, b = self.claim("B")
        self.assertEqual((a["epoch"], b["epoch"]), (1, 2))
        before = self.cp.get_job(job["id"])

        ops = {
            "heartbeat": lambda: self.cp.heartbeat(a["id"], "A", 1),
            "complete": lambda: self.cp.complete(a["id"], "A", 1, "r1", {"summary": "stale"}),
            "fail": lambda: self.cp.fail(a["id"], "A", 1, "r2", "boom"),
            "release": lambda: self.cp.release(a["id"], "A", 1, "r3"),
            # a stale worker guessing the new epoch on its own attempt id still fails
            "complete-epoch2": lambda: self.cp.complete(a["id"], "A", 2, "r4", {"summary": "stale"}),
            # ... and cannot use B's attempt id either
            "complete-as-b": lambda: self.cp.complete(b["id"], "A", 2, "r5", {"summary": "stale"}),
            "release-b": lambda: self.cp.release(b["id"], "A", 2, "r6"),
        }
        for name, op in ops.items():
            with self.subTest(op=name), self.assertRaises(StaleAttempt):
                op()
        after = self.cp.get_job(job["id"])
        for k in ("status", "epoch", "current_attempt_id", "result", "failures", "lease_until"):
            self.assertEqual(after[k], before[k], k)
        self.assertEqual(after["current_attempt_id"], b["id"])
        # B is unaffected and completes normally
        self.cp.heartbeat(b["id"], "B", 2)
        out = self.cp.complete(b["id"], "B", 2, "rb", {"summary": "real"})
        self.assertEqual(out["job"]["status"], "completed")
        self.assertEqual(out["job"]["result"]["attempt_id"], b["id"])
        self.assertGreaterEqual(len(self.events(job["id"], "attempt.rejected")), len(ops))
        # history: both attempts kept, neither overwritten
        atts = self.cp.get_job(job["id"])["attempts"]
        self.assertEqual([(x["epoch"], x["status"]) for x in atts], [(1, "expired"), (2, "completed")])

    def test_stale_after_completion_by_other(self):
        self.job()
        _, a = self.claim("A")
        self.clock.advance(61)
        _, b = self.claim("B")
        self.cp.complete(b["id"], "B", 2, "x", {"summary": "done"})
        with self.assertRaises(StaleAttempt):
            self.cp.complete(a["id"], "A", 1, "y", {"summary": "late"})

    # ---------------------------------------------------------------- idempotency
    def test_idempotent_completion(self):
        job = self.job()
        _, a = self.claim("A")
        first = self.cp.complete(a["id"], "A", 1, "req-1", {"summary": "ok", "final_sha": "abc"})
        self.assertFalse(first["replay"])
        for rid in ("req-1", "req-1", "req-2"):
            again = self.cp.complete(a["id"], "A", 1, rid, {"summary": "ok", "final_sha": "abc"})
            self.assertTrue(again["replay"])
            self.assertEqual(again["job"]["status"], "completed")
        # also after the lease would have expired: the recorded outcome is simply replayed
        self.clock.advance(500)
        self.assertTrue(self.cp.complete(a["id"], "A", 1, "req-1", {"summary": "ok"})["replay"])
        self.assertEqual(len(self.events(job["id"], "attempt.completed")), 1)
        self.assertEqual(len(self.events(job["id"], "job.completed")), 1)
        self.assertEqual(sum("已交付" in t for t in self.titles()), 1)
        with self.assertRaises(Conflict):
            self.cp.fail(a["id"], "A", 1, "req-3", "late failure")

    def test_idempotent_fail_and_release(self):
        self.job(name="f")
        _, a = self.claim("A")
        self.cp.fail(a["id"], "A", 1, "f1", "prepare_failed", retryable=True)
        self.assertTrue(self.cp.fail(a["id"], "A", 1, "f1", "prepare_failed", retryable=True)["replay"])
        job = self.cp.get_job(a["job_id"])
        self.assertEqual((job["status"], job["failures"]), ("queued", 1))  # counted once
        _, b = self.claim("B")
        self.cp.release(b["id"], "B", 2, "r1")
        self.assertTrue(self.cp.release(b["id"], "B", 2, "r1")["replay"])
        job = self.cp.get_job(a["job_id"])
        self.assertEqual((job["status"], job["failures"]), ("queued", 1))  # release spends no budget
        with self.assertRaises(Conflict):
            self.cp.complete(b["id"], "B", 2, "c", {})

    def test_job_key_idempotent(self):
        j1, c1 = self.cp.create_job({"key": "inbox:x", "repository": {"name": "app"}})
        j2, c2 = self.cp.create_job({"key": "inbox:x", "repository": {"name": "app"}, "goal": "different"})
        self.assertTrue(c1)
        self.assertFalse(c2)
        self.assertEqual(j1["id"], j2["id"])
        self.assertEqual(len(self.cp.list_jobs()), 1)
        # lookup by id, key, or name
        for ident in (j1["id"], "inbox:x", "x"):
            self.assertEqual(self.cp.get_job(ident)["id"], j1["id"])

    def test_job_key_idempotent_under_race(self):
        n = 6
        barrier = threading.Barrier(n)
        ids, errors = [], []

        def run():
            cp = self.new_cp()
            try:
                barrier.wait()
                ids.append(cp.create_job({"key": "inbox:race", "repository": {"name": "app"}})[0]["id"])
            except Exception as e:  # pragma: no cover
                errors.append(e)
            finally:
                cp.store.close()

        ts = [threading.Thread(target=run) for _ in range(n)]
        [t.start() for t in ts]
        [t.join(30) for t in ts]
        self.assertEqual(errors, [])
        self.assertEqual(len(set(ids)), 1)
        self.assertEqual(len(self.cp.list_jobs()), 1)

    # ---------------------------------------------------------------- failure policy
    def test_retryable_failures_until_budget_then_failed(self):
        job = self.job()
        for epoch in (1, 2):
            _, a = self.claim("A")
            self.assertEqual(a["epoch"], epoch)
            self.cp.fail(a["id"], "A", epoch, f"f{epoch}", "launcher_exited", retryable=True)
        _, a = self.claim("A")
        out = self.cp.fail(a["id"], "A", 3, "f3", "launcher_exited", retryable=True)
        self.assertEqual(out["job"]["status"], "failed")
        self.assertIsNone(self.cp.claim("A"))
        self.assertEqual(self.cp.metrics()["launcher_failures"], 3)
        # manual retry: new budget, new attempt, history kept
        self.cp.retry_job(job["id"])
        _, a4 = self.claim("A")
        self.assertEqual(a4["epoch"], 4)
        self.assertEqual(len(self.cp.get_job(job["id"])["attempts"]), 4)

    def test_expiry_exhausts_budget(self):
        job = self.job(max_attempts=2)
        self.claim("A")
        self.clock.advance(61)
        self.claim("B")
        self.clock.advance(61)
        self.cp.sweep()
        self.assertEqual(self.cp.get_job(job["id"])["status"], "failed")
        self.assertEqual(self.cp.metrics()["lease_expirations"], 2)

    def test_non_retryable_failure_and_retry_rules(self):
        job = self.job()
        _, a = self.claim("A")
        self.cp.fail(a["id"], "A", 1, "x", "prepare_failed", "no such repo", retryable=False)
        self.assertEqual(self.cp.get_job(job["id"])["status"], "failed")
        self.assertTrue(any("出错" in t for t in self.titles()))
        with self.assertRaises(InvalidRequest):
            self.cp.retry_job(self.job(name="q")["id"])  # queued: nothing to retry

    # ---------------------------------------------------------------- admission / lineage
    def test_depth_and_lineage(self):
        h = self.job(name="h")
        a1 = self.job(name="a1", source="agent", parent_job_id=h["id"])
        a2 = self.job(name="a2", source="agent", parent_job_id=a1["id"])
        a3 = self.job(name="a3", source="agent", parent_job_id=a2["id"])
        orphan = self.job(name="orphan", source="agent", parent_job_id="nope")
        child_of_rejected = self.job(name="c", source="agent", parent_job_id=orphan["id"])
        noparent = self.job(name="np", source="agent")
        self.assertEqual([h["depth"], a1["depth"], a2["depth"]], [0, 1, 2])
        self.assertEqual(a2["status"], "queued")
        self.assertEqual((a3["status"], a3["depth"]), ("rejected", 3))
        self.assertEqual(orphan["status"], "rejected")
        self.assertEqual(child_of_rejected["status"], "rejected")
        self.assertEqual((noparent["status"], noparent["depth"]), ("queued", 1))
        with self.assertRaises(InvalidRequest):
            self.cp.retry_job(a3["id"])  # rejected jobs are never retried
        # rejected jobs are never claimable
        claimed = set()
        while (r := self.cp.claim("A")) is not None:
            claimed.add(r["job"]["name"])
        self.assertEqual(claimed, {"h", "a1", "a2", "np"})

    def test_admission_checks(self):
        bad = self.job(name="v1.2")
        self.assertEqual(bad["status"], "rejected")
        unmapped = self.job(name="u", repository={"name": "nowhere"})
        self.assertEqual(unmapped["status"], "failed")
        manual = self.job(name="m", target="manual", repository={"name": "later"})
        self.assertEqual(manual["status"], "queued")
        self.assertEqual(len(self.events(manual["id"], "job.waiting")), 1)
        remote = self.job(name="r", repository={"name": "any", "url": "https://example.invalid/r.git"})
        self.assertEqual(remote["status"], "queued")
        with self.assertRaises(InvalidRequest):
            self.cp.create_job({"repository": {"name": "app"}, "target": "sideways"})

    # ---------------------------------------------------------------- restart / persistence
    def test_state_survives_control_plane_restart(self):
        job = self.job()
        _, a = self.claim("A")
        self.store.close()
        store2 = self.make_store(reuse=True)
        cp2 = self.new_cp(store2)
        cp2.start()
        full = cp2.get_job(job["id"])
        self.assertEqual((full["status"], full["epoch"], full["current_attempt_id"]), ("running", 1, a["id"]))
        cp2.heartbeat(a["id"], "A", 1)
        self.assertEqual(cp2.complete(a["id"], "A", 1, "c", {"summary": "after restart"})["job"]["status"], "completed")
        store2.close()

    def test_restart_grace_compensates_downtime(self):
        self.job()
        _, a = self.claim("A")
        self.clock.advance(3600)  # control plane "down" for an hour
        cp2 = self.new_cp()
        cp2.start()
        cp2.heartbeat(a["id"], "A", 1)  # the owner is still authoritative
        self.assertIsNone(cp2.claim("B"))

    def test_register_returns_owned_attempts(self):
        self.job()
        _, a = self.claim("A")
        owned = self.cp.register_worker("A", WORKER)["active_attempts"]
        self.assertEqual([o["attempt"]["id"] for o in owned], [a["id"]])
        self.assertEqual(self.cp.register_worker("B", WORKER)["active_attempts"], [])

    def test_worker_offline_is_observability_only(self):
        self.job()
        _, a = self.claim("A")
        self.clock.advance(130)
        self.cp.heartbeat  # noqa: B018
        swept = self.cp.sweep()
        self.assertGreaterEqual(swept["offline"], 1)
        self.assertEqual({w["id"]: w["status"] for w in self.cp.list_workers()}["A"], "offline")
        self.cp.worker_heartbeat("A")
        self.assertEqual({w["id"]: w["status"] for w in self.cp.list_workers()}["A"], "online")

    def test_metrics(self):
        self.job()
        _, a = self.claim("A")
        self.clock.advance(5)
        self.cp.complete(a["id"], "A", 1, "c", {"summary": "s"}, report_attempts=3)
        m = self.cp.metrics()
        self.assertEqual(m["jobs_created"], 1)
        self.assertEqual(m["jobs_claimed"], 1)
        self.assertEqual(m["result_report_retries"], 2)
        self.assertAlmostEqual(m["attempt_duration_avg_seconds"], 5.0)
        self.assertEqual(m["jobs_by_status"], {"completed": 1})

    def test_result_is_compact(self):
        self.job()
        _, a = self.claim("A")
        out = self.cp.complete(a["id"], "A", 1, "c", {"summary": "x" * 100000, "logs": "never stored", "run_id": "r"})
        res = out["job"]["result"]["engineering_result"]
        self.assertNotIn("logs", res)
        self.assertLess(len(res["summary"]), 5000)


class SqliteControlPlaneTest(Base, unittest.TestCase):
    def make_store(self, reuse: bool = False):
        return SqliteStore(self.tmp / "cp.db")


@unittest.skipUnless(PG_DSN, "set GANGLION_TEST_PG_DSN to run against PostgreSQL")
class PostgresControlPlaneTest(Base, unittest.TestCase):
    def make_store(self, reuse: bool = False):
        if not reuse:
            import psycopg

            self.schema = f"t_{uuid.uuid4().hex[:12]}"
            with psycopg.connect(PG_DSN, autocommit=True) as c:
                c.execute(f"CREATE SCHEMA {self.schema}")
        sep = "&" if "?" in PG_DSN else "?"
        return PostgresStore(f"{PG_DSN}{sep}options=-csearch_path%3D{self.schema}")

    def tearDown(self):
        super().tearDown()
        import psycopg

        with psycopg.connect(PG_DSN, autocommit=True) as c:
            c.execute(f"DROP SCHEMA {self.schema} CASCADE")


if __name__ == "__main__":
    unittest.main()
