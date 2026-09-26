"""Single-machine mode with an unchanged Ganglion 0.1.x config (``[ganglion]`` table).

Ports every 0.1.x test's intent. Differences are deliberate and documented in DESIGN.md:
  * status lives in the embedded control plane (Job/Attempt) instead of state.json;
  * each attempt runs in its own git worktree (the user's checkout is not written to);
  * tmux sessions are named ``gl-<id>-<attempt>``; identity is the GANGLION_ATTEMPT_ID env;
  * completion is detected by the worker (the Stop hook only wakes it);
  * a session that ends without a result is reported as a failed attempt (0.1.x kept it as
    "launched" forever); it is still never relaunched automatically (max_attempts = 1).
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from support import BIN, ROOT, STOP_HOOK, Sandbox, sh, wait_for

from ganglion import cli
from ganglion.config import load_config
from ganglion.control.models import NotFound
from ganglion.inbox import Requirement, parse_frontmatter, submission_order
from ganglion.launchers.claude import ClaudeSettings, Tmux, build_claude_command, prewrite_trust
from ganglion.notify import NtfyNotifier, make_notifier, safe_notify
from ganglion.worker.state import WorkerState


class Legacy(Sandbox):
    max_depth = 2
    trust_wait = 1.5

    def setUp(self):
        super().setUp()
        self.app = self.make_project("app")
        self.state_dir = self.tmp / "state"
        self.tmux_socket = self.socket()
        self.repos = {"app": self.app}
        self.write_config()

    def write_config(self, **overrides):
        g = {
            "inbox_dir": str(self.inbox),
            "state_dir": str(self.state_dir),
            "poll_interval": 0.5,
            "max_depth": self.max_depth,
            "owner": "someone",
            "new_repo_root": str(self.tmp / "newrepos"),
            "req_path": "docs/requirements",
            "launch_prompt": "/spine-run --auto 需求见 {req_path}/{id}.md",
            "claude_json": str(self.tmp / "claude.json"),
            "trust_wait": self.trust_wait,
            "tmux_socket": self.tmux_socket,
        }
        g.update(overrides)

        def v(x):
            return json.dumps(x, ensure_ascii=False) if isinstance(x, str) else str(x).lower() if isinstance(x, bool) else str(x)

        lines = ["[ganglion]"] + [f"{k} = {v(x)}" for k, x in g.items()]
        lines += ["", "[repos]"] + [f"{k} = {json.dumps(str(p))}" for k, p in self.repos.items()]
        lines += ["", "[notify]", 'backend = "file"', f"file_path = {json.dumps(str(self.notif))}"]
        self.config_path = self.tmp / "config.toml"
        self.config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def once(self):
        self.assertEqual(cli.main(["--config", str(self.config_path), "once"]), 0)

    def client(self):
        return cli.make_client(load_config(self.config_path))

    def job(self, rid: str) -> dict | None:
        try:
            return self.client().get_job(f"inbox:{rid}")
        except NotFound:
            return None

    def status(self, rid: str) -> str | None:
        j = self.job(rid)
        return j and j["status"]

    def local(self, rid: str) -> dict | None:
        recs = [r for r in WorkerState(self.state_dir).snapshot()["attempts"].values() if r.get("name") == rid]
        return max(recs, key=lambda r: r["epoch"]) if recs else None

    def handle(self, rid: str) -> str:
        return self.local(rid)["handle"]

    def sessions(self, socket=None) -> set[str]:
        return Tmux(socket or self.tmux_socket).sessions()

    def local_events(self) -> list[dict]:
        p = self.state_dir / "events.jsonl"
        return [json.loads(l) for l in p.read_text().splitlines()] if p.exists() else []


# ---------------------------------------------------------------------------


class TestLaunch(Legacy):
    def test_existing_launches(self):
        self.submit("add-login", body="# 登录\n\n原样传递的正文 `$HOME` 'quote'\n")
        self.once()
        self.assertEqual(self.status("add-login"), "running")
        rec = self.local("add-login")
        self.assertEqual(rec["status"], "running")
        self.assertIn(rec["handle"], self.sessions())
        self.assertTrue(rec["handle"].startswith("gl-add-login-"))
        wait_for(lambda: self.invocations("add-login"), msg="fake claude start")

        # requirement delivered verbatim, committed on its own, in the attempt's worktree
        ws = Path(rec["workspace"])
        dest = ws / "docs/requirements/add-login.md"
        self.assertEqual(dest.read_bytes(), (self.inbox / "add-login.md").read_bytes())
        self.assertEqual(sh("git", "-C", str(ws), "log", "-1", "--format=%s").strip(), "req: add-login")
        self.assertEqual(sh("git", "-C", str(ws), "status", "--porcelain").strip(), "")
        self.assertEqual(rec["branch"], sh("git", "-C", str(ws), "branch", "--show-current").strip())
        # the user's own checkout is untouched
        self.assertEqual(sh("git", "-C", str(self.app), "log", "-1", "--format=%s").strip(), "init")
        self.assertEqual(sh("git", "-C", str(self.app), "status", "--porcelain").strip(), "")

        cwd, args = self.invocations("add-login")[0].split("\t")[1:]
        self.assertEqual(Path(cwd).resolve(), ws.resolve())
        self.assertEqual(args, "/spine-run --auto 需求见 docs/requirements/add-login.md")
        env = (self.ctl / "add-login.env").read_text()
        self.assertIn("GANGLION_ID=add-login", env)
        self.assertIn(f"GANGLION_STATE={self.state_dir}", env)
        self.assertIn(f"GANGLION_ATTEMPT_ID={rec['attempt_id']}", env)
        self.assertIn("GANGLION_EPOCH=1", env)

        cj = json.loads((self.tmp / "claude.json").read_text())
        self.assertTrue(cj["projects"][str(ws)]["hasTrustDialogAccepted"])
        self.assertFalse((self.ctl / "add-login.trust_accepted").exists())
        self.assertTrue(any("已启动" in t for t in self.titles()))

    def test_manual_waits_then_launches(self):
        self.submit("m1", target="manual", repo="later")
        self.once()
        self.assertEqual(self.status("m1"), "queued")
        self.assertIsNone(self.local("m1"))
        self.once()  # another round: no duplicate notification
        self.assertEqual(sum("等待" in t for t in self.titles()), 1)

        later = self.make_project("later")
        self.repos["later"] = later
        self.write_config()
        self.once()
        self.assertEqual(self.status("m1"), "running")
        self.assertTrue((Path(self.local("m1")["workspace"]) / "docs/requirements/m1.md").exists())

    def test_no_duplicate_launch(self):
        self.submit("dup")
        for _ in range(3):
            self.once()
        wait_for(lambda: self.invocations("dup"))
        time.sleep(0.5)
        self.assertEqual(len(self.invocations("dup")), 1)
        ws = self.local("dup")["workspace"]
        self.assertEqual(sh("git", "-C", ws, "log", "--format=%s").count("req: dup"), 1)

        # session ends without a result: reported as a failed attempt, never relaunched
        subprocess.run(["tmux", "-L", self.tmux_socket, "kill-session", "-t", f"={self.handle('dup')}"], check=True)
        self.once()
        self.once()
        self.assertEqual(len(self.invocations("dup")), 1)
        job = self.job("dup")
        self.assertEqual(job["status"], "failed")
        self.assertIn("launcher_exited", job["error"])

    def test_session_prefix_is_exact(self):
        t = Tmux(self.tmux_socket)
        t.new_session("gl-ab", self.tmp, {}, "sleep 60")
        self.assertTrue(t.has_session("gl-ab"))
        self.assertFalse(t.has_session("gl-a"))  # `tmux has-session -t gl-a` would say yes
        self.submit("ab")
        self.once()
        self.submit("a")
        self.once()
        wait_for(lambda: self.invocations("a"))
        self.assertIn(self.handle("a"), self.sessions())

    def test_error_isolated(self):
        self.submit("bad", repo="nope")
        self.submit("good")
        self.once()
        self.assertEqual(self.status("bad"), "failed")
        self.assertIn("nope", self.job("bad")["error"])
        self.assertEqual(self.status("good"), "running")
        self.assertTrue(any("出错" in t and "bad" in t for t in self.titles()))
        self.once()
        self.assertEqual(sum("出错" in t for t in self.titles()), 1)

    def test_invalid_id_rejected(self):
        self.submit("v1.2")
        self.once()
        self.assertEqual(self.status("v1.2"), "rejected")
        self.assertFalse(self.sessions())

    def test_invalid_frontmatter_reported_once(self):
        self.submit("weird", target="sideways")
        self.submit("badnum", max_attempts="lots")
        self.submit("fine")
        self.once()
        self.once()
        self.assertIsNone(self.job("weird"))
        self.assertIsNone(self.job("badnum"))
        self.assertEqual(sum("weird" in t for t in self.titles()), 1)
        self.assertEqual(sum("badnum" in t for t in self.titles()), 1)
        self.assertEqual(self.status("fine"), "running")  # one bad file never blocks the others

    def test_retry(self):
        self.submit("r1", repo="nope")
        self.once()
        self.assertEqual(self.status("r1"), "failed")
        self.repos["nope"] = self.make_project("nope")
        self.write_config()
        self.assertEqual(cli.main(["--config", str(self.config_path), "retry", "r1"]), 0)
        self.once()
        self.assertEqual(self.status("r1"), "running")
        self.assertEqual([a["status"] for a in self.job("r1")["attempts"]], ["active"])


class TestDepth(Legacy):
    def test_depth_rejection(self):
        self.submit("h", source="human")
        self.submit("a1", source="agent", parent="h")
        self.submit("a2", source="agent", parent="a1")
        self.submit("a3", source="agent", parent="a2")  # depth 3 > 2
        self.submit("orphan", source="agent", parent="missing")
        self.once()
        self.assertEqual(self.status("a2"), "running")
        self.assertEqual(self.job("a2")["depth"], 2)
        self.assertEqual(self.status("a3"), "rejected")
        self.assertEqual(self.status("orphan"), "rejected")
        self.assertIsNone(self.local("a3"))
        self.assertEqual(sum("拒绝" in t for t in self.titles()), 2)
        self.once()
        self.assertEqual(sum("拒绝" in t for t in self.titles()), 2)
        # lineage is recorded on the jobs themselves
        self.assertEqual(self.job("a2")["parent_job_id"], self.job("a1")["id"])

    def test_cycle(self):
        reqs = {
            "x": Requirement("x", Path("x.md"), {"source": "agent", "parent": "y"}, b""),
            "y": Requirement("y", Path("y.md"), {"source": "agent", "parent": "x"}, b""),
            "z": Requirement("z", Path("z.md"), {"source": "agent", "parent": "x"}, b""),
        }
        order, cyclic = submission_order(reqs)
        self.assertEqual(cyclic, {"x", "y"})
        self.assertLess(order.index("x"), order.index("z"))

    def test_cycle_rejected_end_to_end(self):
        self.submit("x", source="agent", parent="y")
        self.submit("y", source="agent", parent="x")
        self.submit("z", source="agent", parent="x")
        self.once()
        self.assertEqual({self.status(r) for r in "xyz"}, {"rejected"})
        self.assertFalse(self.sessions())


class TestTrust(Legacy):
    def test_trust_fallback_triggered(self):
        self.set_mode("t1", "trust")
        self.submit("t1")
        self.once()
        self.assertTrue((self.ctl / "t1.trust_accepted").exists())
        self.assertIn("trust_fallback", [e["kind"] for e in self.local_events()])
        wait_for(lambda: (self.ctl / "t1.ready").exists(), msg="claude continues after trust")

    def test_no_fallback_when_prewritten(self):
        self.submit("t2")
        self.once()
        self.assertNotIn("trust_fallback", [e["kind"] for e in self.local_events()])


class TestClaudeJson(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp()).resolve()
        self.cj = self.tmp / ".claude.json"
        self.proj = self.tmp / "proj"
        self.proj.mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_merge_keeps_existing_fields(self):
        orig = {
            "numStartups": 42,
            "oauthAccount": {"emailAddress": "x@y"},
            "projects": {
                "/other": {"hasTrustDialogAccepted": False, "allowedTools": ["Bash"]},
                str(self.proj): {"history": [{"display": "hi"}], "hasTrustDialogAccepted": False},
            },
        }
        self.cj.write_text(json.dumps(orig))
        os.chmod(self.cj, 0o600)
        prewrite_trust(self.cj, self.proj)
        d = json.loads(self.cj.read_text())
        self.assertEqual(d["numStartups"], 42)
        self.assertEqual(d["oauthAccount"], {"emailAddress": "x@y"})
        self.assertEqual(d["projects"]["/other"], orig["projects"]["/other"])
        self.assertEqual(d["projects"][str(self.proj)]["history"], [{"display": "hi"}])
        self.assertIs(d["projects"][str(self.proj)]["hasTrustDialogAccepted"], True)
        self.assertEqual(os.stat(self.cj).st_mode & 0o777, 0o600)
        self.assertEqual(list(self.tmp.glob(".*.tmp")), [])

    def test_missing_file_created(self):
        prewrite_trust(self.cj, self.proj)
        self.assertTrue(json.loads(self.cj.read_text())["projects"][str(self.proj)]["hasTrustDialogAccepted"])

    def test_symlinked_path_writes_both_keys(self):
        link = self.tmp / "link"
        link.symlink_to(self.proj)
        prewrite_trust(self.cj, link)
        p = json.loads(self.cj.read_text())["projects"]
        self.assertTrue(p[str(link)]["hasTrustDialogAccepted"])
        self.assertTrue(p[str(self.proj)]["hasTrustDialogAccepted"])

    def test_retries_while_other_process_writes(self):
        self.cj.write_text('{"numStartups": 1, "proj')

        def finish():
            time.sleep(0.4)
            self.cj.write_text(json.dumps({"numStartups": 2, "tipsHistory": {"a": 1}}))

        t = threading.Thread(target=finish)
        t.start()
        prewrite_trust(self.cj, self.proj, base_delay=0.1)
        t.join()
        d = json.loads(self.cj.read_text())
        self.assertEqual(d["numStartups"], 2)
        self.assertEqual(d["tipsHistory"], {"a": 1})
        self.assertTrue(d["projects"][str(self.proj)]["hasTrustDialogAccepted"])

    def test_gives_up_on_permanent_garbage(self):
        self.cj.write_text("not json")
        with self.assertRaises(RuntimeError):
            prewrite_trust(self.cj, self.proj, retries=3, base_delay=0.01)
        self.assertEqual(self.cj.read_text(), "not json")


class TestStopHook(Legacy):
    def run_hook(self, env_extra: dict | None, cwd: Path | None = None) -> subprocess.CompletedProcess:
        env = {k: v for k, v in os.environ.items() if not k.startswith("GANGLION_")}
        env.update(env_extra or {})
        return subprocess.run([str(STOP_HOOK)], input=json.dumps({"hook_event_name": "Stop", "cwd": str(cwd or self.app)}),
                              env=env, cwd=cwd or self.app, capture_output=True, text=True)

    def delivered(self) -> list[dict]:
        return [n for n in self.notifications() if "已交付" in n["title"]]

    def test_noop_without_env(self):
        self.submit("s0")
        self.once()
        before = WorkerState(self.state_dir).snapshot()
        p = self.run_hook(None)
        self.assertEqual((p.returncode, p.stdout), (0, ""))
        self.assertEqual(WorkerState(self.state_dir).snapshot(), before)
        self.assertFalse((self.state_dir / "wake").exists())

    def test_hook_only_wakes_the_worker(self):
        self.submit("s1")
        self.once()
        rec = self.local("s1")
        p = self.run_hook({"GANGLION_ID": "s1", "GANGLION_STATE": str(self.state_dir),
                           "GANGLION_ATTEMPT_ID": rec["attempt_id"]}, cwd=Path(rec["workspace"]))
        self.assertEqual((p.returncode, p.stdout), (0, ""))
        self.assertTrue((self.state_dir / "wake").exists())
        self.assertEqual(self.status("s1"), "running")  # the hook decides nothing

    def test_only_completes_when_delivery_exists_and_once(self):
        self.submit("s1")
        self.once()
        self.once()
        self.assertEqual(self.status("s1"), "running")
        ws = Path(self.local("s1")["workspace"])
        (ws / "DELIVERY.md").write_text("".join(f"line {i}\n" for i in range(1, 31)))
        self.once()
        self.once()
        d = self.delivered()
        self.assertEqual(len(d), 1)
        self.assertIn("line 20\n", d[0]["message"])
        self.assertNotIn("line 21", d[0]["message"])
        job = self.job("s1")
        self.assertEqual(job["status"], "completed")
        res = job["result"]["engineering_result"]
        self.assertEqual(res["source"], "delivery-md")
        self.assertEqual(res["final_sha"], sh("git", "-C", str(ws), "rev-parse", "HEAD").strip())

    def test_stale_delivery_ignored(self):
        self.write_config(isolation="shared")  # in-place checkout reuse is where this matters
        old = self.app / "DELIVERY.md"
        old.write_text("上一个需求的交付\n")
        t = time.time() - 3600
        os.utime(old, (t, t))
        self.submit("s2")
        self.once()
        self.once()
        self.assertEqual(self.delivered(), [])
        self.assertEqual(self.status("s2"), "running")

    def test_end_to_end_with_fake_claude(self):
        self.set_mode("e2e", "deliver")
        self.set_mode("mid", "stop")
        self.submit("e2e")
        self.once()
        wait_for(lambda: (self.ctl / "e2e.ready").exists(), msg="fake claude delivered")
        self.assertEqual((self.ctl / "e2e.hook").read_text().count("rc=0"), 2)
        self.once()
        self.assertEqual(len(self.delivered()), 1)
        self.assertEqual(self.status("e2e"), "completed")
        self.assertEqual(self.local("e2e")["status"], "done")

        self.repos["other"] = self.make_project("other")
        self.write_config()
        self.submit("mid", repo="other")
        self.once()
        wait_for(lambda: (self.ctl / "mid.ready").exists(), msg="fake claude stopped")
        self.once()
        self.assertEqual(len(self.delivered()), 1)
        self.assertEqual(self.status("mid"), "running")

    def test_legacy_session_keeps_legacy_hook_behaviour(self):
        proj = self.make_project("oldproj")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / "state.json").write_text(json.dumps({
            "version": 1, "config_path": str(self.config_path), "repos": {},
            "requests": {"old": {"status": "launched", "project": str(proj), "repo": "oldproj", "launched_at": time.time() - 5}},
        }))
        (proj / "DELIVERY.md").write_text("legacy done\n")
        env = {"GANGLION_ID": "old", "GANGLION_STATE": str(self.state_dir)}
        self.assertEqual(self.run_hook(env, cwd=proj).returncode, 0)
        self.assertEqual(self.run_hook(env, cwd=proj).returncode, 0)
        self.assertEqual(len(self.delivered()), 1)
        st = json.loads((self.state_dir / "state.json").read_text())
        self.assertEqual(st["requests"]["old"]["status"], "delivered")


class TestLegacyState(Legacy):
    def write_legacy(self, requests: dict):
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / "state.json").write_text(json.dumps({"version": 1, "requests": requests, "repos": {}}))

    def test_handled_legacy_requests_are_not_resubmitted(self):
        self.write_legacy({"old1": {"status": "launched"}, "old2": {"status": "error"}, "w": {"status": "waiting_repo"}})
        self.submit("old1")
        self.submit("old2")
        self.submit("w")
        self.once()
        self.assertIsNone(self.job("old1"))
        self.assertIsNone(self.job("old2"))
        self.assertEqual(self.status("w"), "running")  # 0.1.x never started it
        self.assertEqual(self.invocations("old1"), [])

    def test_retry_forgets_legacy_record(self):
        self.write_legacy({"old2": {"status": "error"}})
        self.submit("old2")
        self.once()
        self.assertIsNone(self.job("old2"))
        cli.main(["--config", str(self.config_path), "retry", "old2"])
        self.once()
        self.assertEqual(self.status("old2"), "running")


class TestNewRepo(Legacy):
    def test_gh_commands(self):
        from ganglion.worker.repo import gh_create_commands

        cmds = gh_create_commands("me", "proj", Path("/root/proj"))
        self.assertEqual(cmds[0], ["gh", "repo", "create", "me/proj", "--private"])
        self.assertEqual(cmds[1], ["gh", "repo", "clone", "me/proj", "/root/proj"])

    def test_new_path(self):
        self.submit("n1", target="new", repo="fresh")
        self.once()
        log = (self.ctl / "gh.log").read_text().splitlines()
        dest = self.tmp / "newrepos" / "fresh"
        self.assertEqual(log, ["repo create someone/fresh --private", f"repo clone someone/fresh {dest}"])
        self.assertEqual(WorkerState(self.state_dir).snapshot()["repos"]["fresh"], str(dest))
        self.assertEqual(self.status("n1"), "running")
        self.assertEqual(sh("git", "-C", str(dest), "log", "-1", "--format=%s").strip(), "req: n1")

        # a second `new` to the same name reuses the mapping (once the first is done: shared checkout)
        (dest / "DELIVERY.md").write_text("done\n")
        self.once()
        self.assertEqual(self.status("n1"), "completed")
        self.submit("n2", target="new", repo="fresh")
        self.once()
        self.assertEqual(len((self.ctl / "gh.log").read_text().splitlines()), 2)
        self.assertEqual(self.status("n2"), "running")

    def test_new_conflicts_with_config(self):
        self.submit("n3", target="new", repo="app")
        self.once()
        self.assertEqual(self.status("n3"), "failed")
        self.assertIn("use target: existing", self.job("n3")["error"])
        self.assertFalse((self.ctl / "gh.log").exists())


class TestRestart(Legacy):
    def spawn(self) -> subprocess.Popen:
        return subprocess.Popen([sys.executable, str(BIN), "--config", str(self.config_path), "run"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def test_kill_and_restart(self):
        self.submit("k1")
        p = self.spawn()
        try:
            wait_for(lambda: self.local("k1") and self.local("k1")["status"] == "running", msg="k1 running")
        finally:
            p.send_signal(signal.SIGKILL)
            p.wait()
        self.submit("k2")
        p = self.spawn()
        try:
            wait_for(lambda: self.local("k2") and self.local("k2")["status"] == "running", msg="k2 running")
            time.sleep(1.5)
        finally:
            p.terminate()
            p.wait(10)
        self.assertEqual(p.returncode, 0)
        self.assertEqual(len(self.invocations("k1")), 1)
        self.assertEqual(len(self.invocations("k2")), 1)
        self.assertEqual(self.status("k1"), "running")
        self.assertIn(self.handle("k1"), self.sessions())  # the killed worker's session survived

    def test_legacy_entry_point_still_works(self):
        # 0.1.x launchd plists run `ganglion.py --config <0.1.x config> run`
        self.submit("shim")
        q = subprocess.run([sys.executable, str(ROOT / "ganglion.py"), "--config", str(self.config_path), "once"],
                           capture_output=True, text=True)
        self.assertEqual(q.returncode, 0, q.stderr[-2000:])
        self.assertEqual(self.status("shim"), "running")

    def test_single_instance(self):
        p = self.spawn()
        try:
            wait_for(lambda: (self.state_dir / "ganglion.lock").exists())
            time.sleep(0.5)
            q = subprocess.run([sys.executable, str(BIN), "--config", str(self.config_path), "once"],
                               capture_output=True, text=True)
            self.assertNotEqual(q.returncode, 0)
            self.assertIn("already running", q.stderr)
        finally:
            p.terminate()
            p.wait(10)

    def test_crash_between_launching_and_running(self):
        self.submit("c1")
        self.once()
        wait_for(lambda: self.invocations("c1"))
        rec = self.local("c1")
        WorkerState(self.state_dir).update_attempt(rec["attempt_id"], status="launching")
        self.once()
        self.assertEqual(self.local("c1")["status"], "running")
        self.assertEqual(len(self.invocations("c1")), 1)

    def test_crash_before_tmux(self):
        # claimed and recorded, crashed before the session existed -> launched exactly once
        self.submit("c2")
        self.once()
        wait_for(lambda: self.invocations("c2"))
        rec = self.local("c2")
        subprocess.run(["tmux", "-L", self.tmux_socket, "kill-session", "-t", f"={rec['handle']}"], check=True)
        (self.ctl / "c2.invocations").unlink()
        WorkerState(self.state_dir).update_attempt(rec["attempt_id"], status="preparing")
        self.once()
        wait_for(lambda: self.invocations("c2"))
        self.assertEqual(len(self.invocations("c2")), 1)
        self.assertEqual(self.local("c2")["status"], "running")
        self.assertEqual(len(self.job("c2")["attempts"]), 1)

    def test_state_lost_adopts_sessions(self):
        self.submit("lost")
        self.once()
        wait_for(lambda: self.invocations("lost"))
        (self.state_dir / "worker.json").unlink()
        self.once()
        rec = self.local("lost")
        self.assertTrue(rec.get("adopted"))
        self.assertEqual(rec["status"], "running")
        self.assertEqual(len(self.invocations("lost")), 1)
        # and it still completes normally after adoption
        (Path(rec["workspace"]) / "DELIVERY.md").write_text("adopted and done\n")
        self.once()
        self.assertEqual(self.status("lost"), "completed")


class TestNtfy(unittest.TestCase):
    def test_json_publish_with_chinese_title(self):
        import http.server

        got = {}

        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                got["path"] = self.path
                got["auth"] = self.headers.get("Authorization")
                got["body"] = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *a):
                pass

        srv = http.server.HTTPServer(("127.0.0.1", 0), H)
        t = threading.Thread(target=srv.handle_request)
        t.start()
        n = make_notifier({"backend": "ntfy", "ntfy_topic": "tp", "ntfy_server": f"http://127.0.0.1:{srv.server_port}", "ntfy_token": "tk"})
        n.send("✅ 已交付：x", "正文", ["tada"])
        t.join(5)
        srv.server_close()
        self.assertEqual(got["path"], "/")
        self.assertEqual(got["auth"], "Bearer tk")
        self.assertEqual(got["body"]["topic"], "tp")
        self.assertEqual(got["body"]["title"], "✅ 已交付：x")
        self.assertEqual(got["body"]["tags"], ["tada"])

    def test_notify_failure_is_swallowed(self):
        self.assertFalse(safe_notify(NtfyNotifier("tp", server="http://127.0.0.1:9", timeout=1), "t", "m"))


class TestParsing(unittest.TestCase):
    def test_frontmatter(self):
        m = parse_frontmatter('---\ntarget: existing\nrepo: "app"  # c\nsource: agent\nparent: x\n---\nbody: no\n')
        self.assertEqual(m, {"target": "existing", "repo": "app", "source": "agent", "parent": "x"})
        self.assertEqual(parse_frontmatter("no fm\n"), {})
        self.assertEqual(parse_frontmatter("---\ntarget: new\n"), {})

    def test_default_source(self):
        self.assertEqual(Requirement("x", Path("x.md"), {}, b"").source, "human")

    def test_command_quoting(self):
        s = ClaudeSettings(claude_args="--permission-mode acceptEdits")
        cmd = build_claude_command(s, "it's r1 in docs/requirements $HOME")
        self.assertEqual(shlex.split(cmd), ["claude", "--permission-mode", "acceptEdits", "it's r1 in docs/requirements $HOME"])

    def test_example_configs_load(self):
        cfg = load_config(ROOT / "config.example.toml")
        self.assertEqual(cfg.notify["backend"], "ntfy")
        self.assertIn("{id}", cfg.launch_prompt)
        self.assertEqual(cfg.control_plane, "local")
        legacy = load_config(ROOT / "config.v0.1.example.toml")
        self.assertTrue(legacy.legacy_format)
        self.assertTrue(legacy.local)


if __name__ == "__main__":
    unittest.main()
