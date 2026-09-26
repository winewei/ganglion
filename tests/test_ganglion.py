"""Ganglion 自动化测试（Linux 沙箱可跑，只用标准库 unittest）。

    python3 -m unittest discover -s tests -v

- inbox 用本地 bare repo 模拟，"手机"是另一个 clone，负责 push 需求。
- claude / gh 用 tests/fakes 下的假脚本（放到 PATH 最前面）。
- 每个用例用独立的 tmux socket（-L），结束时 kill-server。
- 通知用 file 后端写 JSONL，断言其内容。
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

ROOT = Path(__file__).resolve().parent.parent
FAKES = ROOT / "tests" / "fakes"
STOP_HOOK = ROOT / "hooks" / "stop.sh"
sys.path.insert(0, str(ROOT))

import ganglion  # noqa: E402

_counter = 0


def sh(*args: str, cwd: Path | None = None, env: dict | None = None) -> str:
    return subprocess.run(args, cwd=cwd, env=env, check=True, capture_output=True, text=True).stdout


def wait_for(pred, timeout: float = 15.0, interval: float = 0.1, msg: str = "condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = pred()
        if v:
            return v
        time.sleep(interval)
    raise AssertionError(f"timeout waiting for {msg}")


class Env(unittest.TestCase):
    """搭一套完整的沙箱：inbox、目标工程、配置、假 CLI、独立 tmux。"""

    max_depth = 2
    trust_wait = 1.5

    def setUp(self):
        global _counter
        _counter += 1
        self.tmp = Path(tempfile.mkdtemp(prefix="ganglion-test-")).resolve()
        self.ctl = self.tmp / "ctl"
        self.ctl.mkdir()
        self.socket = f"gltest-{os.getpid()}-{_counter}"
        self._old_env = dict(os.environ)
        os.environ.update(
            {
                "PATH": f"{FAKES}{os.pathsep}{os.environ['PATH']}",
                "FAKE_CTL": str(self.ctl),
                "FAKE_STOP_HOOK": str(STOP_HOOK),
                "FAKE_CLAUDE_JSON": str(self.tmp / "claude.json"),
                "TMUX_TMPDIR": str(self.tmp),
                "GIT_AUTHOR_NAME": "t",
                "GIT_AUTHOR_EMAIL": "t@example.com",
                "GIT_COMMITTER_NAME": "t",
                "GIT_COMMITTER_EMAIL": "t@example.com",
                "GIT_CONFIG_NOSYSTEM": "1",
            }
        )
        os.environ.pop("TMUX", None)
        os.environ.pop("GANGLION_ID", None)
        os.environ.pop("GANGLION_STATE", None)

        # inbox: bare + 手机 clone + ganglion 的 clone
        self.bare = self.tmp / "inbox.git"
        sh("git", "init", "-q", "--bare", "-b", "main", str(self.bare))
        self.phone = self.tmp / "phone"
        sh("git", "clone", "-q", str(self.bare), str(self.phone))
        sh("git", "-C", str(self.phone), "checkout", "-q", "-b", "main")
        (self.phone / "README.md").write_text("inbox\n")
        sh("git", "-C", str(self.phone), "add", ".")
        sh("git", "-C", str(self.phone), "commit", "-q", "-m", "init")
        sh("git", "-C", str(self.phone), "push", "-q", "-u", "origin", "main")
        self.inbox = self.tmp / "inbox"
        sh("git", "clone", "-q", str(self.bare), str(self.inbox))

        # 已有工程
        self.app = self.make_project("app")
        self.state_dir = self.tmp / "state"
        self.notif = self.tmp / "notifications.jsonl"
        self.repos = {"app": self.app}
        self.write_config()

    def tearDown(self):
        subprocess.run(["tmux", "-L", self.socket, "kill-server"], capture_output=True)
        os.environ.clear()
        os.environ.update(self._old_env)
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers --
    def make_project(self, name: str) -> Path:
        p = self.tmp / "code" / name
        p.mkdir(parents=True)
        sh("git", "init", "-q", "-b", "main", str(p))
        (p / "README.md").write_text(name + "\n")
        sh("git", "-C", str(p), "add", ".")
        sh("git", "-C", str(p), "commit", "-q", "-m", "init")
        return p

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
            "tmux_socket": self.socket,
        }
        g.update(overrides)

        def v(x):
            return json.dumps(x, ensure_ascii=False) if isinstance(x, str) else str(x).lower() if isinstance(x, bool) else str(x)

        lines = ["[ganglion]"] + [f"{k} = {v(x)}" for k, x in g.items()]
        lines += ["", "[repos]"] + [f"{k} = {json.dumps(str(p))}" for k, p in self.repos.items()]
        lines += ["", "[notify]", 'backend = "file"', f"file_path = {json.dumps(str(self.notif))}"]
        self.config_path = self.tmp / "config.toml"
        self.config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.cfg = ganglion.load_config(self.config_path)

    def submit(self, rid: str, body: str = "做点事情\n", **meta):
        meta.setdefault("target", "existing")
        meta.setdefault("repo", "app")
        fm = "---\n" + "".join(f"{k}: {v}\n" for k, v in meta.items()) + "---\n"
        (self.phone / f"{rid}.md").write_text(fm + body, encoding="utf-8")
        sh("git", "-C", str(self.phone), "add", ".")
        sh("git", "-C", str(self.phone), "commit", "-q", "-m", f"add {rid}")
        sh("git", "-C", str(self.phone), "push", "-q")

    def set_mode(self, rid: str, mode: str):
        (self.ctl / f"{rid}.mode").write_text(mode)

    def ganglion(self) -> ganglion.Ganglion:
        return ganglion.Ganglion(ganglion.load_config(self.config_path))

    def once(self):
        g = self.ganglion()
        g._record_runtime()
        g.reconcile()
        g.poll_once()
        return g

    def state(self) -> dict:
        return json.loads((self.state_dir / "state.json").read_text())

    def status(self, rid: str):
        return self.state()["requests"].get(rid, {}).get("status")

    def sessions(self) -> set[str]:
        return ganglion.Tmux(self.socket).sessions()

    def invocations(self, rid: str) -> list[str]:
        p = self.ctl / f"{rid}.invocations"
        return p.read_text().splitlines() if p.exists() else []

    def notifications(self) -> list[dict]:
        if not self.notif.exists():
            return []
        return [json.loads(line) for line in self.notif.read_text().splitlines()]

    def titles(self) -> list[str]:
        return [n["title"] for n in self.notifications()]


# ---------------------------------------------------------------------------


class TestLaunch(Env):
    def test_existing_launches(self):
        self.submit("add-login", body="# 登录\n\n原样传递的正文 `$HOME` 'quote'\n")
        self.once()
        self.assertEqual(self.status("add-login"), "launched")
        self.assertIn("gl-add-login", self.sessions())
        wait_for(lambda: self.invocations("add-login"), msg="fake claude start")

        # 需求原样投递并单独提交
        dest = self.app / "docs/requirements/add-login.md"
        self.assertEqual(dest.read_bytes(), (self.inbox / "add-login.md").read_bytes())
        self.assertEqual(sh("git", "-C", str(self.app), "log", "-1", "--format=%s").strip(), "req: add-login")
        self.assertEqual(sh("git", "-C", str(self.app), "status", "--porcelain").strip(), "")

        # 启动参数与环境
        cwd, args = self.invocations("add-login")[0].split("\t")[1:]
        self.assertEqual(Path(cwd).resolve(), self.app)
        self.assertEqual(args, "/spine-run --auto 需求见 docs/requirements/add-login.md")
        env = (self.ctl / "add-login.env").read_text()
        self.assertIn("GANGLION_ID=add-login", env)
        self.assertIn(f"GANGLION_STATE={self.state_dir}", env)

        # trust 已预写，假 claude 没弹提示
        cj = json.loads((self.tmp / "claude.json").read_text())
        self.assertTrue(cj["projects"][str(self.app)]["hasTrustDialogAccepted"])
        self.assertFalse((self.ctl / "add-login.trust_accepted").exists())
        self.assertTrue(any("已启动" in t for t in self.titles()))

    def test_manual_waits_then_launches(self):
        self.submit("m1", target="manual", repo="later")
        self.once()
        self.assertEqual(self.status("m1"), "waiting_repo")
        self.assertNotIn("gl-m1", self.sessions())
        self.once()  # 再等一轮：不重复通知
        self.assertEqual(self.status("m1"), "waiting_repo")
        self.assertEqual(sum("等待" in t for t in self.titles()), 1)

        later = self.make_project("later")
        self.repos["later"] = later
        self.write_config()
        self.once()
        self.assertEqual(self.status("m1"), "launched")
        self.assertIn("gl-m1", self.sessions())
        self.assertTrue((later / "docs/requirements/m1.md").exists())

    def test_no_duplicate_launch(self):
        self.submit("dup")
        for _ in range(3):
            self.once()
        wait_for(lambda: self.invocations("dup"))
        time.sleep(0.5)
        self.assertEqual(len(self.invocations("dup")), 1)
        self.assertEqual(sh("git", "-C", str(self.app), "log", "--format=%s").count("req: dup"), 1)

        # session 结束后也不会再拉起
        subprocess.run(["tmux", "-L", self.socket, "kill-session", "-t", "=gl-dup"], check=True)
        self.once()
        self.assertEqual(len(self.invocations("dup")), 1)
        self.assertEqual(self.status("dup"), "launched")

    def test_session_prefix_is_exact(self):
        # 已有 gl-ab 时，gl-a 仍必须被启动（tmux has-session -t 会前缀匹配）
        self.submit("ab")
        self.once()
        self.submit("a")
        self.once()
        self.assertEqual({"gl-a", "gl-ab"} & self.sessions(), {"gl-a", "gl-ab"})
        wait_for(lambda: self.invocations("a"))

    def test_error_isolated(self):
        self.submit("bad", repo="nope")
        self.submit("good")
        self.once()
        self.assertEqual(self.status("bad"), "error")
        self.assertIn("nope", self.state()["requests"]["bad"]["error"])
        self.assertEqual(self.status("good"), "launched")
        self.assertTrue(any("出错" in t and "bad" in t for t in self.titles()))
        # error 不会每轮重复通知
        self.once()
        self.assertEqual(sum("出错" in t for t in self.titles()), 1)

    def test_invalid_id_rejected_as_error(self):
        self.submit("v1.2")
        self.once()
        self.assertEqual(self.status("v1.2"), "error")
        self.assertFalse(self.sessions())

    def test_retry(self):
        self.submit("r1", repo="nope")
        self.once()
        self.assertEqual(self.status("r1"), "error")
        self.repos["nope"] = self.make_project("nope")
        self.write_config()
        ganglion.main(["--config", str(self.config_path), "retry", "r1"])
        self.once()
        self.assertEqual(self.status("r1"), "launched")


class TestDepth(Env):
    max_depth = 2

    def test_depth_rejection(self):
        self.submit("h", source="human")
        self.submit("a1", source="agent", parent="h")
        self.submit("a2", source="agent", parent="a1")
        self.submit("a3", source="agent", parent="a2")  # 深度 3 > 2
        self.submit("orphan", source="agent", parent="missing")
        self.once()
        st = self.state()["requests"]
        self.assertEqual(st["a2"]["status"], "launched")
        self.assertEqual(st["a2"]["depth"], 2)
        self.assertEqual(st["a3"]["status"], "rejected")
        self.assertEqual(st["orphan"]["status"], "rejected")
        self.assertNotIn("gl-a3", self.sessions())
        self.assertFalse((self.app / "docs/requirements/a3.md").exists())
        self.assertEqual(sum("拒绝" in t for t in self.titles()), 2)
        self.once()  # 不重复通知
        self.assertEqual(sum("拒绝" in t for t in self.titles()), 2)

    def test_cycle(self):
        reqs = {
            "x": ganglion.Requirement("x", Path("x.md"), {"source": "agent", "parent": "y"}, b""),
            "y": ganglion.Requirement("y", Path("y.md"), {"source": "agent", "parent": "x"}, b""),
        }
        with self.assertRaises(ValueError):
            ganglion.compute_depth("x", reqs)


class TestTrust(Env):
    def test_trust_fallback_triggered(self):
        self.set_mode("t1", "trust")
        self.submit("t1")
        self.once()
        self.assertTrue((self.ctl / "t1.trust_accepted").exists())
        kinds = [e["kind"] for e in self.state()["requests"]["t1"]["events"]]
        self.assertIn("trust_fallback", kinds)
        wait_for(lambda: (self.ctl / "t1.ready").exists(), msg="claude continues after trust")

    def test_no_fallback_when_prewritten(self):
        self.submit("t2")
        self.once()
        kinds = [e["kind"] for e in self.state()["requests"]["t2"]["events"]]
        self.assertNotIn("trust_fallback", kinds)


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
        ganglion.prewrite_trust(self.cj, self.proj)
        d = json.loads(self.cj.read_text())
        self.assertEqual(d["numStartups"], 42)
        self.assertEqual(d["oauthAccount"], {"emailAddress": "x@y"})
        self.assertEqual(d["projects"]["/other"], orig["projects"]["/other"])
        self.assertEqual(d["projects"][str(self.proj)]["history"], [{"display": "hi"}])
        self.assertIs(d["projects"][str(self.proj)]["hasTrustDialogAccepted"], True)
        self.assertEqual(os.stat(self.cj).st_mode & 0o777, 0o600)
        self.assertEqual(list(self.tmp.glob(".*.tmp")), [])

    def test_missing_file_created(self):
        ganglion.prewrite_trust(self.cj, self.proj)
        self.assertTrue(json.loads(self.cj.read_text())["projects"][str(self.proj)]["hasTrustDialogAccepted"])

    def test_symlinked_path_writes_both_keys(self):
        link = self.tmp / "link"
        link.symlink_to(self.proj)
        ganglion.prewrite_trust(self.cj, link)
        p = json.loads(self.cj.read_text())["projects"]
        self.assertTrue(p[str(link)]["hasTrustDialogAccepted"])
        self.assertTrue(p[str(self.proj)]["hasTrustDialogAccepted"])

    def test_retries_while_other_process_writes(self):
        # 另一个 "claude" 正写到一半（非法 JSON），稍后写完整；我们要重试并保留它的字段。
        self.cj.write_text('{"numStartups": 1, "proj')

        def finish():
            time.sleep(0.4)
            self.cj.write_text(json.dumps({"numStartups": 2, "tipsHistory": {"a": 1}}))

        t = threading.Thread(target=finish)
        t.start()
        ganglion.prewrite_trust(self.cj, self.proj, base_delay=0.1)
        t.join()
        d = json.loads(self.cj.read_text())
        self.assertEqual(d["numStartups"], 2)
        self.assertEqual(d["tipsHistory"], {"a": 1})
        self.assertTrue(d["projects"][str(self.proj)]["hasTrustDialogAccepted"])

    def test_gives_up_on_permanent_garbage(self):
        self.cj.write_text("not json")
        with self.assertRaises(RuntimeError):
            ganglion.prewrite_trust(self.cj, self.proj, retries=3, base_delay=0.01)
        self.assertEqual(self.cj.read_text(), "not json")  # 绝不覆盖看不懂的文件


class TestStopHook(Env):
    def run_hook(self, rid: str | None, cwd: Path | None = None) -> subprocess.CompletedProcess:
        env = dict(os.environ)
        env.pop("GANGLION_ID", None)
        env.pop("GANGLION_STATE", None)
        if rid:
            env["GANGLION_ID"] = rid
            env["GANGLION_STATE"] = str(self.state_dir)
        return subprocess.run(
            [str(STOP_HOOK)],
            input=json.dumps({"hook_event_name": "Stop", "cwd": str(cwd or self.app)}),
            env=env,
            cwd=cwd or self.app,
            capture_output=True,
            text=True,
        )

    def delivered(self) -> list[dict]:
        return [n for n in self.notifications() if "已交付" in n["title"]]

    def test_noop_without_env(self):
        self.submit("s0")
        self.once()
        (self.app / "DELIVERY.md").write_text("done\n")
        before = self.state()
        p = self.run_hook(None)
        self.assertEqual(p.returncode, 0)
        self.assertEqual(p.stdout, "")
        self.assertEqual(self.state(), before)
        self.assertEqual(self.delivered(), [])

    def test_only_notifies_when_delivery_exists_and_once(self):
        self.submit("s1")
        self.once()
        self.assertEqual(self.run_hook("s1").returncode, 0)
        self.assertEqual(self.delivered(), [])
        self.assertEqual(self.status("s1"), "launched")

        (self.app / "DELIVERY.md").write_text("".join(f"line {i}\n" for i in range(1, 31)))
        self.run_hook("s1")
        self.run_hook("s1")
        d = self.delivered()
        self.assertEqual(len(d), 1)
        self.assertIn("line 20\n", d[0]["message"])
        self.assertNotIn("line 21", d[0]["message"])
        self.assertEqual(self.status("s1"), "delivered")
        self.once()  # 主循环也不会再碰它
        self.assertEqual(len(self.delivered()), 1)

    def test_stale_delivery_ignored(self):
        old = self.app / "DELIVERY.md"
        old.write_text("上一个需求的交付\n")
        t = time.time() - 3600
        os.utime(old, (t, t))
        self.submit("s2")
        self.once()
        self.run_hook("s2")
        self.assertEqual(self.delivered(), [])

    def test_end_to_end_with_fake_claude(self):
        self.set_mode("e2e", "deliver")
        self.set_mode("mid", "stop")
        self.submit("e2e")
        self.once()
        wait_for(lambda: (self.ctl / "e2e.ready").exists(), msg="fake claude delivered")
        self.assertEqual((self.ctl / "e2e.hook").read_text().count("rc=0"), 2)
        self.assertEqual(len(self.delivered()), 1)
        self.assertEqual(self.status("e2e"), "delivered")

        # 另一个工程里中途停下的 agent：不通知
        self.repos["other"] = self.make_project("other")
        self.write_config()
        self.submit("mid", repo="other")
        self.once()
        wait_for(lambda: (self.ctl / "mid.ready").exists(), msg="fake claude stopped")
        self.assertEqual(len(self.delivered()), 1)
        self.assertEqual(self.status("mid"), "launched")


class TestNewRepo(Env):
    def test_gh_commands(self):
        cmds = ganglion.gh_create_commands("me", "proj", Path("/root/proj"))
        self.assertEqual(cmds[0], ["gh", "repo", "create", "me/proj", "--private"])
        self.assertEqual(cmds[1], ["gh", "repo", "clone", "me/proj", "/root/proj"])

    def test_new_path(self):
        self.submit("n1", target="new", repo="fresh")
        self.once()
        log = (self.ctl / "gh.log").read_text().splitlines()
        dest = self.tmp / "newrepos" / "fresh"
        self.assertEqual(log, ["repo create someone/fresh --private", f"repo clone someone/fresh {dest}"])
        self.assertEqual(self.state()["repos"]["fresh"], str(dest))
        self.assertEqual(self.status("n1"), "launched")
        self.assertEqual(sh("git", "-C", str(dest), "log", "-1", "--format=%s").strip(), "req: n1")

        # 第二条 new 到同名仓库：复用映射，不再调 gh
        self.submit("n2", target="new", repo="fresh")
        self.once()
        self.assertEqual(len((self.ctl / "gh.log").read_text().splitlines()), 2)
        self.assertEqual(self.status("n2"), "launched")

    def test_new_conflicts_with_config(self):
        self.submit("n3", target="new", repo="app")
        self.once()
        self.assertEqual(self.status("n3"), "error")
        self.assertFalse((self.ctl / "gh.log").exists())


class TestRestart(Env):
    def spawn(self) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, str(ROOT / "ganglion.py"), "--config", str(self.config_path), "run"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def test_kill_and_restart(self):
        self.submit("k1")
        p = self.spawn()
        try:
            wait_for(lambda: (self.state_dir / "state.json").exists() and self.status("k1") == "launched", msg="launched")
        finally:
            p.send_signal(signal.SIGKILL)
            p.wait()
        self.submit("k2")
        p = self.spawn()
        try:
            wait_for(lambda: self.status("k2") == "launched", msg="k2 launched")
            time.sleep(1.5)  # 再跑几轮
        finally:
            p.terminate()
            p.wait(10)
        self.assertEqual(p.returncode, 0)
        self.assertEqual(len(self.invocations("k1")), 1)
        self.assertEqual(len(self.invocations("k2")), 1)

    def test_single_instance(self):
        p = self.spawn()
        try:
            wait_for(lambda: (self.state_dir / "ganglion.lock").exists())
            time.sleep(0.3)
            q = subprocess.run(
                [sys.executable, str(ROOT / "ganglion.py"), "--config", str(self.config_path), "once"],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(q.returncode, 0)
            self.assertIn("另一个", q.stderr)
        finally:
            p.terminate()
            p.wait(10)

    def test_crash_between_launching_and_launched(self):
        # 崩溃点 1：状态是 launching，session 已起 → 对账为 launched，不重启
        self.submit("c1")
        self.once()
        wait_for(lambda: self.invocations("c1"))
        with ganglion.State(self.state_dir).locked() as st:
            st["requests"]["c1"]["status"] = "launching"
        self.once()
        self.assertEqual(self.status("c1"), "launched")
        self.assertEqual(len(self.invocations("c1")), 1)

    def test_crash_before_tmux(self):
        # 崩溃点 2：状态是 launching，session 没起来 → 重新走一遍（投递幂等），只启动一次
        self.submit("c2")
        with ganglion.State(self.state_dir).locked() as st:
            st["requests"]["c2"] = {"status": "launching", "first_seen": time.time()}
        self.once()
        self.assertEqual(self.status("c2"), "launched")
        wait_for(lambda: self.invocations("c2"))
        self.assertEqual(len(self.invocations("c2")), 1)

    def test_state_lost_adopts_sessions(self):
        self.submit("lost")
        self.once()
        wait_for(lambda: self.invocations("lost"))
        (self.state_dir / "state.json").unlink()
        self.once()
        self.assertEqual(self.status("lost"), "launched")
        self.assertTrue(self.state()["requests"]["lost"].get("adopted"))
        self.assertEqual(len(self.invocations("lost")), 1)


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
        n = ganglion.make_notifier(
            {"backend": "ntfy", "ntfy_topic": "tp", "ntfy_server": f"http://127.0.0.1:{srv.server_port}", "ntfy_token": "tk"}
        )
        n.send("✅ 已交付：x", "正文", ["tada"])
        t.join(5)
        srv.server_close()
        self.assertEqual(got["path"], "/")
        self.assertEqual(got["auth"], "Bearer tk")
        self.assertEqual(got["body"]["topic"], "tp")
        self.assertEqual(got["body"]["title"], "✅ 已交付：x")
        self.assertEqual(got["body"]["tags"], ["tada"])

    def test_notify_failure_is_swallowed(self):
        n = ganglion.NtfyNotifier("tp", server="http://127.0.0.1:9", timeout=1)
        self.assertFalse(ganglion.safe_notify(n, "t", "m"))


class TestParsing(unittest.TestCase):
    def test_frontmatter(self):
        m = ganglion.parse_frontmatter('---\ntarget: existing\nrepo: "app"  # c\nsource: agent\nparent: x\n---\nbody: no\n')
        self.assertEqual(m, {"target": "existing", "repo": "app", "source": "agent", "parent": "x"})
        self.assertEqual(ganglion.parse_frontmatter("no fm\n"), {})
        self.assertEqual(ganglion.parse_frontmatter("---\ntarget: new\n"), {})

    def test_default_source(self):
        r = ganglion.Requirement("x", Path("x.md"), {}, b"")
        self.assertEqual(r.source, "human")

    def test_command_quoting(self):
        cfg = ganglion.Config(path=Path("c"), inbox_dir=Path("i"), state_dir=Path("s"))
        cfg.launch_prompt = "it's {id} in {req_path}"
        cfg.claude_args = "--permission-mode acceptEdits"
        cmd = ganglion.build_claude_command(cfg, "r1")
        self.assertEqual(shlex.split(cmd), ["claude", "--permission-mode", "acceptEdits", "it's r1 in docs/requirements"])

    def test_example_config_loads(self):
        cfg = ganglion.load_config(ROOT / "config.example.toml")
        self.assertEqual(cfg.notify["backend"], "ntfy")
        self.assertIn("{id}", cfg.launch_prompt)


if __name__ == "__main__":
    unittest.main()
