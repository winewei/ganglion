"""Worker runtime against a shared control plane (in-process, fake clock).

Two ``Worker`` objects with separate state dirs, workspaces and tmux sockets stand in for
two machines. Real tmux + fake claude; no LLM calls.
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

from support import Sandbox, sh, wait_for

from ganglion.control.client import LocalClient
from ganglion.control.models import InvalidRequest, Unavailable
from ganglion.control.service import ControlPlane, Settings
from ganglion.control.store import SqliteStore
from ganglion.notify import FileNotifier
from ganglion.spine import SpineAdapter
from ganglion.worker.runtime import Worker
from ganglion.worker.state import WorkerState


class Clock:
    def __init__(self):
        self.offset = 0.0

    def __call__(self):
        return time.time() + self.offset

    def advance(self, dt):
        self.offset += dt


class Flaky(LocalClient):
    """LocalClient whose responses can be 'lost' (the call happens, the reply doesn't arrive),
    or which can be entirely unreachable."""

    def __init__(self, service, worker_id):
        super().__init__(service, worker_id)
        self.lose: set[str] = set()
        self.down = False

    def _wrap(self, name, fn, *a, **kw):
        if self.down:
            raise Unavailable("network down")
        out = fn(*a, **kw)
        if name in self.lose:
            self.lose.discard(name)
            raise Unavailable("response lost")
        return out

    def claim(self, *a, **kw):
        return self._wrap("claim", super().claim, *a, **kw)

    reject_complete = False

    def complete(self, *a, **kw):
        if self.reject_complete:  # what the HTTP schema does to a payload it will never accept
            raise InvalidRequest("result.summary: String should have at most 20000 characters")
        return self._wrap("complete", super().complete, *a, **kw)

    def heartbeat(self, *a, **kw):
        return self._wrap("heartbeat", super().heartbeat, *a, **kw)

    def fail(self, *a, **kw):
        return self._wrap("fail", super().fail, *a, **kw)

    def register(self, *a, **kw):
        return self._wrap("register", super().register, *a, **kw)

    def worker_heartbeat(self, *a, **kw):
        return self._wrap("worker_heartbeat", super().worker_heartbeat, *a, **kw)


class WorkerEnv(Sandbox):
    lease_ttl = 60.0
    max_attempts = 3

    def setUp(self):
        super().setUp()
        self.clock = Clock()
        self.remote = self.make_remote("svc")
        self.app = self.make_project("app")
        self.service = ControlPlane(SqliteStore(self.tmp / "cp.db"),
                                    Settings(lease_ttl=self.lease_ttl, max_attempts=self.max_attempts),
                                    FileNotifier(str(self.notif)), clock=self.clock)
        self.service.start()

    def tearDown(self):
        self.service.store.close()
        super().tearDown()

    def worker(self, name: str, **worker_opts) -> Worker:
        opts = {"heartbeat_interval": 0.001}
        opts.update(worker_opts)
        path = self.worker_config(name, inbox=False, repos={"app": self.app}, worker=opts, claude={"trust_wait": 0.3})
        cfg = self.load(path)
        w = Worker(cfg, Flaky(self.service, name), notifier=FileNotifier(str(self.notif)))
        w.start()
        return w

    def restart(self, w: Worker) -> Worker:
        """Same machine, new process: fresh objects over the same local state and tmux server."""
        w2 = Worker(w.cfg, Flaky(self.service, w.cfg.worker_id), notifier=FileNotifier(str(self.notif)))
        w2.start()
        return w2

    def job(self, name="j1", **kw):
        spec = {"name": name, "key": f"test:{name}", "repository": {"name": "svc", "url": str(self.remote)}, "goal": f"goal {name}\n"}
        spec.update(kw)
        return self.service.create_job(spec)[0]

    def full(self, job_id):
        return self.service.get_job(job_id)

    def rec(self, w: Worker, name: str) -> dict | None:
        recs = [r for r in WorkerState(w.cfg.state_dir).snapshot()["attempts"].values() if r.get("name") == name]
        return max(recs, key=lambda r: r["epoch"]) if recs else None

    def alive(self, w: Worker, handle: str) -> bool:
        return handle in self.sessions(w.cfg.claude.tmux_socket)

    def go(self, name: str, attempt_id: str):
        (self.ctl / f"{name}.{attempt_id}.go").touch()


class TestDistributedWorker(WorkerEnv):
    def test_two_workers_one_claim(self):
        a, b = self.worker("A"), self.worker("B")
        j = self.job()
        a.tick()
        b.tick()
        full = self.full(j["id"])
        self.assertEqual(len(full["attempts"]), 1)
        owner = full["attempts"][0]["worker_id"]
        loser = b if owner == "A" else a
        self.assertIsNone(self.rec(loser, "j1"))
        wait_for(lambda: len(self.invocations("j1")) == 1)
        time.sleep(0.3)
        self.assertEqual(len(self.invocations("j1")), 1)

    def test_clone_workspace_complete_and_publish(self):
        self.set_mode("pub", "deliver")
        a = self.worker("A", publish="branch")
        j = self.job("pub")
        a.tick()
        rec = self.rec(a, "pub")
        ws = Path(rec["workspace"])
        self.assertTrue(str(ws).startswith(str(a.cfg.workspace_root)))
        self.assertEqual(rec["mode"], "clone")
        self.assertEqual(sh("git", "-C", str(ws), "remote", "get-url", "origin").strip(), str(self.remote))
        wait_for(lambda: (self.ctl / "pub.ready").exists())
        a.tick()
        full = self.full(j["id"])
        self.assertEqual(full["status"], "completed")
        res = full["result"]["engineering_result"]
        self.assertEqual(res["branch"], rec["branch"])
        self.assertIn(rec["branch"], res["result_uri"])
        # the branch (requirement commit + agent commit) is durable on the remote
        remote_sha = sh("git", "--git-dir", str(self.remote), "rev-parse", f"refs/heads/{rec['branch']}").strip()
        self.assertEqual(remote_sha, res["final_sha"])
        log = sh("git", "--git-dir", str(self.remote), "log", "--format=%s", remote_sha)
        self.assertIn("req: pub", log)
        self.assertIn("deliver pub", log)

    def test_stale_local_process_is_never_reported(self):
        """A's lease expires, B takes over; A's agent then finishes. A must not report it."""
        self.set_mode("j1", "ondemand")
        a = self.worker("A")
        j = self.job()
        a.tick()
        ra = self.rec(a, "j1")
        self.assertEqual(ra["status"], "running")
        wait_for(lambda: (self.ctl / "j1.ready").exists())

        self.clock.advance(self.lease_ttl + 1)  # A is partitioned: no heartbeats reach the server
        b = self.worker("B")
        b.tick()
        rb = self.rec(b, "j1")
        self.assertEqual((rb["status"], rb["epoch"]), ("running", 2))

        self.go("j1", ra["attempt_id"])  # A's old agent finishes after losing ownership
        wait_for(lambda: (Path(ra["workspace"]) / "DELIVERY.md").exists())
        a.tick()
        self.assertEqual(self.rec(a, "j1")["status"], "stale")
        full = self.full(j["id"])
        self.assertEqual((full["status"], full["current_attempt_id"], full["epoch"]), ("running", rb["attempt_id"], 2))
        self.assertTrue(self.alive(a, ra["handle"]))  # conservative: left running for inspection
        self.assertTrue(any("执行权已转移" in t for t in self.titles()))
        self.assertFalse(any("已交付" in t for t in self.titles()))

        self.go("j1", rb["attempt_id"])
        wait_for(lambda: (Path(rb["workspace"]) / "DELIVERY.md").exists())
        b.tick()
        full = self.full(j["id"])
        self.assertEqual(full["status"], "completed")
        self.assertEqual(full["result"]["attempt_id"], rb["attempt_id"])
        self.assertEqual([x["status"] for x in full["attempts"]], ["expired", "completed"])
        types = [e["type"] for e in self.service.events(job_id=j["id"], limit=1000)]
        self.assertIn("attempt.rejected", types)
        self.assertEqual(types.count("job.completed"), 1)

    def test_stale_detected_at_restart_before_acting(self):
        self.set_mode("j1", "ondemand")
        a = self.worker("A")
        j = self.job()
        a.tick()
        ra = self.rec(a, "j1")
        self.clock.advance(self.lease_ttl + 1)
        b = self.worker("B")
        b.tick()
        self.go("j1", ra["attempt_id"])
        wait_for(lambda: (Path(ra["workspace"]) / "DELIVERY.md").exists())
        a2 = self.restart(a)  # A comes back: reconcile against the control plane first
        self.assertEqual(self.rec(a2, "j1")["status"], "stale")
        a2.tick()
        self.assertEqual(self.full(j["id"])["status"], "running")
        self.assertTrue(self.alive(a2, ra["handle"]))

    def test_stale_policy_stop(self):
        self.set_mode("j1", "ondemand")
        a = self.worker("A", stale_policy="stop")
        self.job()
        a.tick()
        ra = self.rec(a, "j1")
        self.clock.advance(self.lease_ttl + 1)
        self.worker("B").tick()
        a.tick()
        self.assertEqual(self.rec(a, "j1")["status"], "stale")
        self.assertFalse(self.alive(a, ra["handle"]))

    def test_worker_restart_adopts_live_session(self):
        self.set_mode("j1", "ondemand")
        a = self.worker("A")
        j = self.job()
        a.tick()
        ra = self.rec(a, "j1")
        wait_for(lambda: (self.ctl / "j1.ready").exists())
        a2 = self.restart(a)
        a2.tick()
        self.assertEqual(self.rec(a2, "j1")["status"], "running")
        self.assertEqual(len(self.invocations("j1")), 1)
        self.go("j1", ra["attempt_id"])
        wait_for(lambda: (Path(ra["workspace"]) / "DELIVERY.md").exists())
        a2.tick()
        self.assertEqual(self.full(j["id"])["status"], "completed")

    def test_worker_restart_with_lost_state_adopts_by_session_env(self):
        self.set_mode("j1", "ondemand")
        a = self.worker("A")
        j = self.job()
        a.tick()
        ra = self.rec(a, "j1")
        (a.cfg.state_dir / "worker.json").unlink()
        a2 = self.restart(a)
        rec = self.rec(a2, "j1")
        self.assertTrue(rec["adopted"])
        self.assertEqual((rec["workspace"], rec["handle"]), (ra["workspace"], ra["handle"]))
        self.go("j1", ra["attempt_id"])
        wait_for(lambda: (Path(ra["workspace"]) / "DELIVERY.md").exists())
        a2.tick()
        self.assertEqual(self.full(j["id"])["status"], "completed")
        self.assertEqual(len(self.invocations("j1")), 1)

    def test_lost_claim_response(self):
        a = self.worker("A")
        j = self.job()
        self.job("other")
        a.client.lose.add("claim")
        a.tick()  # claim committed server-side, reply lost
        self.assertIsNone(self.rec(a, "j1"))
        pending = WorkerState(a.cfg.state_dir).snapshot()["pending_claim"]
        self.assertTrue(pending)
        a.tick()  # retried with the same request id -> the same attempt
        self.assertEqual(self.rec(a, "j1")["status"], "running")
        self.assertEqual(len(self.full(j["id"])["attempts"]), 1)
        self.assertEqual(self.full(j["id"])["attempts"][0]["claim_request_id"], pending)

    def test_lost_completion_response_is_retried_idempotently(self):
        self.set_mode("j1", "deliver")
        a = self.worker("A")
        j = self.job()
        a.tick()
        wait_for(lambda: (self.ctl / "j1.ready").exists())
        a.client.lose.add("complete")
        a.tick()  # committed, reply lost
        self.assertEqual(self.rec(a, "j1")["status"], "reporting")
        a.tick()  # same request id -> replay
        self.assertEqual(self.rec(a, "j1")["status"], "done")
        types = [e["type"] for e in self.service.events(job_id=j["id"], limit=1000)]
        self.assertEqual(types.count("attempt.completed"), 1)
        self.assertEqual(sum("已交付" in t for t in self.titles()), 1)

    def test_report_survives_worker_crash(self):
        self.set_mode("j1", "deliver")
        a = self.worker("A")
        j = self.job()
        a.tick()
        wait_for(lambda: (self.ctl / "j1.ready").exists())
        a.client.down = True
        a.tick()  # result observed, report pending (control plane unreachable)
        self.assertEqual(self.rec(a, "j1")["status"], "reporting")
        a2 = self.restart(a)
        a2.tick()
        self.assertEqual(self.full(j["id"])["status"], "completed")
        self.assertEqual(self.service.metrics()["result_report_retries"], 1)

    def test_rejected_report_becomes_a_failure_not_a_loop(self):
        self.set_mode("j1", "deliver")
        a = self.worker("A")
        j = self.job()
        a.tick()
        wait_for(lambda: (self.ctl / "j1.ready").exists())
        a.client.reject_complete = True
        a.tick()
        self.assertEqual(self.rec(a, "j1")["status"], "done")
        full = self.full(j["id"])
        self.assertEqual(full["status"], "failed")
        self.assertIn("report_rejected", full["error"])

    def test_control_plane_unreachable_does_not_fail_work(self):
        self.set_mode("j1", "ondemand")
        a = self.worker("A")
        j = self.job()
        a.tick()
        ra = self.rec(a, "j1")
        a.client.down = True
        for _ in range(3):
            a.tick()
        self.assertEqual(self.rec(a, "j1")["status"], "running")
        self.assertTrue(self.alive(a, ra["handle"]))
        a.client.down = False
        a.tick()
        self.assertEqual(self.full(j["id"])["status"], "running")

    def test_does_not_launch_when_ownership_unconfirmed(self):
        a = self.worker("A")
        self.job()
        a.client.lose.add("heartbeat")  # the pre-launch ownership check gets no answer
        a.tick()
        rec = self.rec(a, "j1")
        self.assertEqual(rec["status"], "preparing")
        self.assertEqual(self.invocations("j1"), [])
        a.tick()
        self.assertEqual(self.rec(a, "j1")["status"], "running")


class TestSpineAdapter(WorkerEnv):
    def test_structured_result_preferred(self):
        self.set_mode("s", "result")
        a = self.worker("A")
        j = self.job("s")
        a.tick()
        wait_for(lambda: (self.ctl / "s.ready").exists())
        a.tick()
        res = self.full(j["id"])["result"]["engineering_result"]
        self.assertEqual(res["source"], "spine-result")
        self.assertEqual(res["run_id"], "run-s")
        self.assertEqual(res["summary"], "structured s")
        env = (self.ctl / "s.env").read_text()
        for var in ("GANGLION_JOB_ID", "GANGLION_ATTEMPT_ID", "GANGLION_EPOCH=1", "GANGLION_RESULT_FILE"):
            self.assertIn(var, env)

    def test_structured_failure_fails_the_job(self):
        self.set_mode("f", "failed")
        a = self.worker("A")
        j = self.job("f")
        a.tick()
        wait_for(lambda: (self.ctl / "f.ready").exists())
        a.tick()
        full = self.full(j["id"])
        self.assertEqual(full["status"], "failed")
        self.assertIn("spine_failed", full["error"])

    def test_protocol_selection(self):
        tmp = self.tmp / "ad"
        tmp.mkdir()
        sh("git", "init", "-q", str(tmp))
        rf = tmp / "r.json"
        rf.write_text(json.dumps({"schema": "ganglion.engineering-result/v1", "status": "completed", "summary": "s"}))
        (tmp / "DELIVERY.md").write_text("d\n")
        t0 = time.time() - 10
        self.assertEqual(SpineAdapter("auto").collect(tmp, rf, t0)["source"], "spine-result")
        self.assertEqual(SpineAdapter("1").collect(tmp, rf, t0)["source"], "delivery-md")
        rf.write_text("{half")
        self.assertIsNone(SpineAdapter("2").collect(tmp, rf, t0))
        self.assertEqual(SpineAdapter("auto").collect(tmp, rf, t0)["source"], "delivery-md")
        self.assertIsNone(SpineAdapter("auto").collect(tmp, rf, time.time() + 100))  # DELIVERY.md older than launch

    def test_launcher_exit_retryable_creates_new_attempt(self):
        self.set_mode("x", "exit")
        a = self.worker("A", retry_on_launcher_exit=True)
        j = self.job("x")
        a.tick()
        wait_for(lambda: (self.ctl / "x.ready").exists())
        time.sleep(0.3)
        a.tick()  # observes the exit -> fail(retryable) -> requeued -> claims epoch 2
        full = self.full(j["id"])
        self.assertEqual([x["status"] for x in full["attempts"]][:1], ["failed"])
        self.assertEqual(full["epoch"], 2)
        self.assertEqual(self.service.metrics()["launcher_failures"], 1)

    def test_timeout(self):
        a = self.worker("A")
        j = self.job("slow", timeout_seconds=0.5)
        a.tick()
        rec = self.rec(a, "slow")
        time.sleep(0.8)
        a.tick()
        full = self.full(j["id"])
        self.assertEqual(full["status"], "failed")
        self.assertIn("timeout", full["error"])
        self.assertFalse(self.alive(a, rec["handle"]))


class TestIsolation(WorkerEnv):
    def test_shared_checkout_runs_one_attempt_at_a_time(self):
        self.set_mode("s1", "ondemand")
        a = self.worker("A", isolation="shared")
        j1 = self.job("s1", repository={"name": "app"})
        j2 = self.job("s2", repository={"name": "app"})
        a.tick()
        self.assertEqual(self.full(j1["id"])["status"], "running")
        self.assertEqual(self.full(j2["id"])["status"], "queued")
        self.go("s1", self.rec(a, "s1")["attempt_id"])
        wait_for(lambda: (self.app / "DELIVERY.md").exists())
        a.tick()
        self.assertEqual(self.full(j1["id"])["status"], "completed")
        # Shared sessions are stopped after a terminal report so the next attempt can enter.
        self.assertFalse(self.alive(a, self.rec(a, "s1")["handle"]))
        self.assertEqual(self.full(j2["id"])["status"], "running")

    def test_done_session_keeps_shared_checkout_busy_until_stopped(self):
        """If a completed attempt's process is still alive, do not start another in the same checkout."""
        self.set_mode("s1", "ondemand")
        a = self.worker("A", isolation="shared")
        # Disable auto-stop so we can assert the busy-repo gate on its own.
        a._release_shared_checkout = lambda rec: None  # type: ignore[method-assign]
        j1 = self.job("s1", repository={"name": "app"})
        j2 = self.job("s2", repository={"name": "app"})
        a.tick()
        self.go("s1", self.rec(a, "s1")["attempt_id"])
        wait_for(lambda: (self.app / "DELIVERY.md").exists())
        a.tick()
        self.assertEqual(self.full(j1["id"])["status"], "completed")
        self.assertEqual(self.rec(a, "s1")["status"], "done")
        self.assertTrue(self.alive(a, self.rec(a, "s1")["handle"]))
        self.assertEqual(self.full(j2["id"])["status"], "queued")
        self.assertIn("app", a.busy_shared_repos())
        subprocess.run(["tmux", "-L", a.cfg.claude.tmux_socket, "kill-session",
                        "-t", f"={self.rec(a, 's1')['handle']}"], check=True)
        a.tick()
        self.assertEqual(self.full(j2["id"])["status"], "running")

    def test_prepare_cmderror_is_retryable(self):
        """CmdError subclasses RuntimeError; it must still be classified as retryable."""
        a = self.worker("A")
        j = self.job("boom", repository={"name": "app"})
        from ganglion.common import CmdError
        from ganglion.worker import repo as repo_mod

        def boom(*_a, **_k):
            raise CmdError("git lock raced")

        orig = repo_mod.deliver_requirement
        repo_mod.deliver_requirement = boom  # type: ignore[assignment]
        try:
            a.tick()
        finally:
            repo_mod.deliver_requirement = orig  # type: ignore[assignment]
        full = self.full(j["id"])
        self.assertEqual(full["status"], "queued")  # retryable fail requeues
        self.assertEqual(full["failures"], 1)
        self.assertIn("prepare_failed", full["attempts"][0]["error"])

    def test_lease_renewed_during_slow_prepare(self):
        """A prepare longer than lease_ttl must keep heartbeating or the attempt expires."""
        self.service.settings.lease_ttl = 2.0
        a = self.worker("A", heartbeat_interval=0.3)
        j = self.job("slowprep", repository={"name": "app"})
        from ganglion.worker import repo as repo_mod

        calls = {"n": 0}
        real_hb = a.client.heartbeat

        def counting_hb(*args, **kwargs):
            calls["n"] += 1
            return real_hb(*args, **kwargs)

        a.client.heartbeat = counting_hb  # type: ignore[method-assign]
        orig = repo_mod.prepare_workspace

        def slow_prepare(*args, **kwargs):
            time.sleep(2.6)  # > lease_ttl; background renewals must keep ownership
            return orig(*args, **kwargs)

        repo_mod.prepare_workspace = slow_prepare  # type: ignore[assignment]
        try:
            a.tick()
        finally:
            repo_mod.prepare_workspace = orig  # type: ignore[assignment]
        full = self.full(j["id"])
        self.assertEqual(full["status"], "running", full.get("error"))
        self.assertEqual(full["epoch"], 1)
        self.assertGreaterEqual(calls["n"], 3)  # initial + background renewals + pre-launch

    def test_stale_session_keeps_shared_checkout_busy(self):
        self.set_mode("s1", "ondemand")
        a = self.worker("A", isolation="shared")
        j = self.job("s1", repository={"name": "app"})
        a.tick()
        old = self.rec(a, "s1")
        self.clock.advance(self.lease_ttl + 1)
        a.tick()  # heartbeat rejected -> stale; the job is requeued
        self.assertEqual(self.rec(a, "s1")["status"], "stale")
        self.assertEqual(self.full(j["id"])["status"], "queued")  # not re-launched next to the live stale agent
        self.assertEqual(len(self.invocations("s1")), 1)
        subprocess.run(["tmux", "-L", a.cfg.claude.tmux_socket, "kill-session", "-t", f"={old['handle']}"], check=True)
        a.tick()
        self.assertEqual(self.full(j["id"])["epoch"], 2)
        self.assertEqual(self.rec(a, "s1")["status"], "running")

    def test_worktrees_run_concurrently_on_one_repo(self):
        a = self.worker("A")
        j1 = self.job("w1", repository={"name": "app"})
        j2 = self.job("w2", repository={"name": "app"})
        a.tick()
        self.assertEqual({self.full(j1["id"])["status"], self.full(j2["id"])["status"]}, {"running"})
        self.assertNotEqual(self.rec(a, "w1")["workspace"], self.rec(a, "w2")["workspace"])

    def test_unmapped_after_claim_releases_without_spending_budget(self):
        a = self.worker("A")
        j = self.job("gone", repository={"name": "app"})
        a.cfg.repos = {}  # config changed after registration
        a.claim_until_full()
        full = self.full(j["id"])
        self.assertEqual((full["status"], full["failures"]), ("queued", 0))
        self.assertEqual(full["attempts"][0]["status"], "released")
        self.assertFalse(a.registered)  # will re-register with its real repo list


if __name__ == "__main__":
    import unittest

    unittest.main()
