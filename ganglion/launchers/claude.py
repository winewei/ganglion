"""Interactive Claude Code in tmux (the production launcher on macOS; also works on Linux).

Behaviour carried over from Ganglion 0.1.x, each item covered by tests:

* exact tmux session identity: ``list-sessions`` + string equality, and ``=name:`` targets,
  because ``has-session -t gl-a`` prefix-matches ``gl-ab``;
* trust pre-write into ``~/.claude.json`` (merge, keep mode, retry on half-written JSON,
  never overwrite an unparseable file, both absolute and realpath keys);
* trust fallback: watch the pane for ``trust_wait`` seconds and press Enter once;
* the command is built with ``shlex.join`` so quotes / ``$`` in the prompt stay literal;
* sessions carry ``GANGLION_*`` env vars; the Stop hook and re-adoption read them.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from ..common import CmdError, atomic_write_text, expand, now, run_cmd
from .base import Launcher, LaunchSpec

SESSION_PREFIX = "gl-"

TRUST_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in (r"do you trust the files in this folder", r"trust this folder", r"one you trust", r"yes,\s*proceed")
]


class Tmux:
    def __init__(self, socket: str = ""):
        self.base = ["tmux"] + (["-L", socket] if socket else [])

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return run_cmd([*self.base, *args], check=check, timeout=30)

    def sessions(self) -> set[str]:
        p = self._run("list-sessions", "-F", "#{session_name}", check=False)
        if p.returncode != 0:
            err = (p.stderr or "").lower()
            if "no server running" in err or "error connecting" in err or "no such file" in err or not err.strip():
                return set()  # no server == no sessions
            raise CmdError(f"tmux list-sessions failed: {p.stderr.strip()}")
        return {line.strip() for line in p.stdout.splitlines() if line.strip()}

    def has_session(self, name: str) -> bool:
        return name in self.sessions()  # exact; never `has-session -t` (prefix match)

    def new_session(self, name: str, cwd: Path, env: dict[str, str], command: str) -> None:
        args = ["new-session", "-d", "-s", name, "-c", str(cwd)]
        for k, v in env.items():
            args += ["-e", f"{k}={v}"]
        args.append(command)
        self._run(*args)

    def capture(self, name: str) -> str:
        p = self._run("capture-pane", "-p", "-t", f"={name}:", check=False)
        return p.stdout if p.returncode == 0 else ""

    def send_enter(self, name: str) -> None:
        self._run("send-keys", "-t", f"={name}:", "Enter")

    def kill(self, name: str) -> None:
        self._run("kill-session", "-t", f"={name}", check=False)

    def environment(self, name: str) -> dict[str, str]:
        p = self._run("show-environment", "-t", f"={name}", check=False)
        env = {}
        for line in p.stdout.splitlines() if p.returncode == 0 else []:
            k, sep, v = line.partition("=")
            if sep:
                env[k] = v
        return env


# ---------------------------------------------------------------------------- trust
def project_keys(project: Path) -> list[str]:
    keys = [str(project.absolute())]
    real = os.path.realpath(project)
    if real not in keys:  # macOS: /tmp -> /private/tmp etc.
        keys.append(real)
    return keys


def prewrite_trust(claude_json: Path, project: Path, retries: int = 8, base_delay: float = 0.2) -> None:
    """read -> merge -> tmp + rename -> read back; retry with backoff.

    Other claude processes rewrite this file at any time: a half-written read or being
    overwritten with stale content both show up as parse / verification failures.
    """
    keys = project_keys(project)
    last_err = ""
    for attempt in range(retries):
        try:
            try:
                st = os.stat(claude_json)
                mode = st.st_mode & 0o777
                data = json.loads(claude_json.read_text(encoding="utf-8") or "{}")
            except FileNotFoundError:
                mode, data = 0o600, {}
            if not isinstance(data, dict):
                raise ValueError("~/.claude.json top level is not an object")
            projects = data.setdefault("projects", {})
            if not isinstance(projects, dict):
                raise ValueError("projects is not an object")
            changed = False
            for k in keys:
                entry = projects.setdefault(k, {})
                if entry.get("hasTrustDialogAccepted") is not True:
                    entry["hasTrustDialogAccepted"] = True
                    changed = True
            if changed:
                atomic_write_text(claude_json, json.dumps(data, ensure_ascii=False, indent=2) + "\n", mode=mode)
            check = json.loads(claude_json.read_text(encoding="utf-8"))
            if all(check.get("projects", {}).get(k, {}).get("hasTrustDialogAccepted") is True for k in keys):
                return
            last_err = "read-back verification failed (overwritten by another process?)"
        except (json.JSONDecodeError, OSError, ValueError) as e:
            last_err = repr(e)
        time.sleep(base_delay * (2**attempt))
    raise RuntimeError(f"trust pre-write failed: {last_err}")


def trust_prompt_visible(screen: str) -> bool:
    return any(p.search(screen) for p in TRUST_PATTERNS)


# ---------------------------------------------------------------------------- launcher
@dataclass
class ClaudeSettings:
    claude_cmd: str = "claude"
    claude_args: str = ""
    claude_json: Path = expand("~/.claude.json")
    trust_wait: float = 8.0
    tmux_socket: str = ""


def build_claude_command(settings: ClaudeSettings, prompt: str) -> str:
    return shlex.join([settings.claude_cmd, *shlex.split(settings.claude_args), prompt])


class ClaudeCodeLauncher(Launcher):
    name = "claude-code"

    def __init__(self, settings: ClaudeSettings):
        self.s = settings
        self.tmux = Tmux(settings.tmux_socket)

    def available(self) -> bool:
        cmd = self.s.claude_cmd
        found = os.path.isfile(cmd) and os.access(cmd, os.X_OK) if os.sep in cmd else shutil.which(cmd) is not None
        return bool(found and shutil.which("tmux"))

    def prepare(self, spec: LaunchSpec) -> None:
        prewrite_trust(self.s.claude_json, spec.workspace)

    def start(self, spec: LaunchSpec) -> bool:
        if self.tmux.has_session(spec.handle):
            return False
        self.tmux.new_session(spec.handle, spec.workspace, spec.env, build_claude_command(self.s, spec.prompt))
        return True

    def after_start(self, spec: LaunchSpec) -> list[str]:
        deadline = now() + self.s.trust_wait
        while True:
            if trust_prompt_visible(self.tmux.capture(spec.handle)):
                self.tmux.send_enter(spec.handle)
                return ["trust_fallback"]
            if now() >= deadline:
                return []
            time.sleep(0.5)

    def is_alive(self, handle: str) -> bool | None:
        try:
            return self.tmux.has_session(handle)
        except CmdError:
            return None

    def stop(self, handle: str) -> None:
        self.tmux.kill(handle)

    def discover(self) -> dict[str, dict[str, str]]:
        try:
            names = self.tmux.sessions()
        except CmdError:
            return {}
        out = {}
        for name in names:
            if name.startswith(SESSION_PREFIX):
                env = {k: v for k, v in self.tmux.environment(name).items() if k.startswith("GANGLION_")}
                out[name] = env
        return out

    def attach_hint(self, handle: str) -> str:
        sock = f"-L {self.s.tmux_socket} " if self.s.tmux_socket else ""
        return f"tmux {sock}-CC attach -t {handle}"
