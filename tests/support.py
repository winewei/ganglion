"""Shared test sandbox (stdlib unittest; Linux or macOS with git + tmux >= 3.0).

- the inbox is a local bare repo; "the phone" is another clone that pushes requirements;
- claude / gh are fakes from tests/fakes (first on PATH);
- every test gets its own tmux socket(s) (-L) and kills them afterwards;
- notifications go to a JSONL file (file backend);
- nothing touches the real ~/.claude.json, ~/.ganglion or tmux server.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FAKES = ROOT / "tests" / "fakes"
STOP_HOOK = ROOT / "hooks" / "stop.sh"
BIN = ROOT / "bin" / "ganglion"
sys.path.insert(0, str(ROOT))

from ganglion.config import load_config  # noqa: E402
from ganglion.launchers.claude import Tmux  # noqa: E402

PG_DSN = os.environ.get("GANGLION_TEST_PG_DSN", "")

_counter = 0


def sh(*args: str, cwd: Path | None = None, env: dict | None = None) -> str:
    return subprocess.run(args, cwd=cwd, env=env, check=True, capture_output=True, text=True).stdout


def wait_for(pred, timeout: float = 20.0, interval: float = 0.1, msg: str = "condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = pred()
        if v:
            return v
        time.sleep(interval)
    raise AssertionError(f"timeout waiting for {msg}")


def toml_value(x) -> str:
    if isinstance(x, bool):
        return str(x).lower()
    if isinstance(x, str):
        return json.dumps(x, ensure_ascii=False)
    return str(x)


def toml(sections: dict[str, dict]) -> str:
    out = []
    for name, kv in sections.items():
        out.append(f"[{name}]")
        out += [f"{json.dumps(k) if ('-' in k or '.' in k) else k} = {toml_value(v)}" for k, v in kv.items()]
        out.append("")
    return "\n".join(out)


class Sandbox(unittest.TestCase):
    """A temp dir with an inbox, projects, fake CLIs and isolated tmux sockets."""

    def setUp(self):
        global _counter
        _counter += 1
        self.tmp = Path(tempfile.mkdtemp(prefix="ganglion-test-")).resolve()
        self.ctl = self.tmp / "ctl"
        self.ctl.mkdir()
        self.sockets: list[str] = []
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
        for k in ("TMUX", "GANGLION_ID", "GANGLION_STATE", "GANGLION_ATTEMPT_ID", "GANGLION_API_TOKEN", "GANGLION_CONFIG"):
            os.environ.pop(k, None)
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
        self.notif = self.tmp / "notifications.jsonl"

    def tearDown(self):
        for s in self.sockets:
            subprocess.run(["tmux", "-L", s, "kill-server"], capture_output=True)
        os.environ.clear()
        os.environ.update(self._old_env)
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers -------------------------------------------------------------------
    def socket(self, tag: str = "") -> str:
        s = f"gltest-{os.getpid()}-{_counter}{tag}"
        self.sockets.append(s)
        return s

    def make_project(self, name: str) -> Path:
        p = self.tmp / "code" / name
        p.mkdir(parents=True)
        sh("git", "init", "-q", "-b", "main", str(p))
        (p / "README.md").write_text(name + "\n")
        sh("git", "-C", str(p), "add", ".")
        sh("git", "-C", str(p), "commit", "-q", "-m", "init")
        return p

    def make_remote(self, name: str) -> Path:
        """A bare repo standing in for a remote Git host (for repository.url jobs)."""
        src = self.make_project(name + "-src")
        bare = self.tmp / "remotes" / f"{name}.git"
        bare.parent.mkdir(exist_ok=True)
        sh("git", "clone", "-q", "--bare", str(src), str(bare))
        return bare

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

    def invocations(self, rid: str) -> list[str]:
        p = self.ctl / f"{rid}.invocations"
        return p.read_text().splitlines() if p.exists() else []

    def notifications(self) -> list[dict]:
        if not self.notif.exists():
            return []
        return [json.loads(line) for line in self.notif.read_text().splitlines()]

    def titles(self) -> list[str]:
        return [n["title"] for n in self.notifications()]

    def sessions(self, socket: str) -> set[str]:
        return Tmux(socket).sessions()

    def worker_config(self, name: str, *, control_plane: str = "local", repos: dict | None = None, inbox: bool = True,
                      worker: dict | None = None, control: dict | None = None, claude: dict | None = None,
                      token_file: Path | None = None) -> Path:
        state = self.tmp / f"state-{name}"
        w = {
            "id": name,
            "control_plane": control_plane,
            "state_dir": str(state),
            "workspace_root": str(self.tmp / f"ws-{name}"),
            "poll_interval": 0.3,
            "heartbeat_interval": 0.2,
            "max_concurrent": 8,
            "isolation": "worktree",
            "req_path": "docs/requirements",
            "launch_prompt": "/spine-run --auto 需求见 {req_path}/{id}.md",
            "new_repo_root": str(self.tmp / f"newrepos-{name}"),
            "npc_cmd": "npc-not-installed",
        }
        if token_file:
            w["token_file"] = str(token_file)
        w.update(worker or {})
        c = {"claude_json": str(self.tmp / "claude.json"), "trust_wait": 1.0, "tmux_socket": self.socket(name)}
        c.update(claude or {})
        sections = {"worker": w, "claude": c, "repos": {k: str(v) for k, v in (repos or {}).items()}}
        if inbox:
            sections["inbox"] = {"dir": str(self.inbox), "owner": "someone"}
        ct = {"lease_ttl": 300, "max_depth": 2, "max_attempts": 3}
        ct.update(control or {})
        sections["control"] = ct
        sections["notify"] = {"backend": "file", "file_path": str(self.notif)}
        path = self.tmp / f"{name}.toml"
        path.write_text(toml(sections), encoding="utf-8")
        return path

    def load(self, path: Path):
        return load_config(path)
