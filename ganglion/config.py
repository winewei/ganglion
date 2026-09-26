"""Configuration. One TOML file, separate scopes:

* ``[worker]`` / ``[claude]`` / ``[repos]`` -- worker-local: identity, control-plane URL,
  workspace root, launcher settings, local checkouts. Credentials stay here (or in env /
  files it points to), never in job payloads.
* ``[inbox]`` -- the Git inbox job source (optional; run it on one machine).
* ``[control]`` -- control plane: database, listen address, API token, lease policy. In
  single-machine mode (``control_plane = "local"``) the worker embeds it.
* ``[notify]`` -- notification backend (shared by whichever side notifies).

A Ganglion 1.x file (a ``[ganglion]`` table) is still accepted and mapped onto local mode,
so ``ganglion run`` keeps working with an unchanged config.
"""

from __future__ import annotations

import os
import socket
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .common import expand
from .launchers.claude import ClaudeSettings
from .spine import DEFAULT_LAUNCH_PROMPT


@dataclass
class Config:
    path: Path
    # worker
    worker_id: str = ""
    control_plane: str = "local"
    token: str = ""          # presented to a remote control plane (worker / CLI side)
    server_token: str = ""   # accepted by `ganglion server start`
    state_dir: Path = field(default_factory=lambda: expand("~/.ganglion"))
    workspace_root: Path = field(default_factory=lambda: expand("~/.ganglion/workspaces"))
    poll_interval: float = 10.0
    heartbeat_interval: float | None = None  # None: follow the control plane's advice
    max_concurrent: int = 4
    launcher: str = "claude-code"
    isolation: str = "worktree"              # worktree | shared  (for jobs on [repos] checkouts)
    publish: str = "none"                    # none | branch
    retry_on_launcher_exit: bool = False
    stale_policy: str = "keep"               # keep | stop  (what to do with a local process whose attempt went stale)
    new_repo_root: Path = field(default_factory=lambda: expand("~/code"))
    req_path: str = "docs/requirements"
    launch_prompt: str = DEFAULT_LAUNCH_PROMPT
    spine_protocol: str = "auto"
    npc_cmd: str = "npc"
    claude: ClaudeSettings = field(default_factory=ClaudeSettings)
    repos: dict[str, Path] = field(default_factory=dict)
    # inbox job source
    inbox_dir: Path | None = None
    inbox_git_pull: bool = True
    inbox_owner: str = ""
    legacy_state: Path | None = None
    # control plane
    database: str = ""
    listen: str = "127.0.0.1"
    port: int = 8700
    lease_ttl: float = 300.0
    server_heartbeat_interval: float = 30.0
    max_depth: int = 2
    max_attempts: int = 3
    worker_offline_after: float = 120.0
    sweep_interval: float = 5.0
    # shared
    notify: dict[str, Any] = field(default_factory=dict)
    legacy_format: bool = False

    @property
    def local(self) -> bool:
        return self.control_plane == "local"


def _token(section: dict[str, Any]) -> str:
    env = os.environ.get("GANGLION_API_TOKEN", "").strip()
    if env:
        return env
    tf = section.get("token_file")
    if tf:
        p = expand(tf)
        try:
            return p.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            raise ValueError(f"token_file {p} does not exist") from None
    return ""


def _claude(c: dict[str, Any]) -> ClaudeSettings:
    return ClaudeSettings(
        claude_cmd=str(c.get("claude_cmd", "claude")),
        claude_args=str(c.get("claude_args", "")),
        claude_json=expand(c.get("claude_json", "~/.claude.json")),
        trust_wait=float(c.get("trust_wait", 8)),
        tmux_socket=str(c.get("tmux_socket", "")),
    )


def default_worker_id() -> str:
    return (socket.gethostname().split(".")[0] or "worker").lower()


def load_config(path: str | os.PathLike[str]) -> Config:
    path = expand(path).resolve()
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    if "ganglion" in raw:
        return _load_legacy(path, raw)

    w = raw.get("worker", {})
    ib = raw.get("inbox", {})
    ct = raw.get("control", {})
    state_dir = expand(w.get("state_dir", "~/.ganglion"))
    cfg = Config(
        path=path,
        worker_id=str(w.get("id") or default_worker_id()),
        control_plane=str(w.get("control_plane", "local")).rstrip("/"),
        state_dir=state_dir,
        workspace_root=expand(w.get("workspace_root", state_dir / "workspaces")),
        poll_interval=float(w.get("poll_interval", 10)),
        heartbeat_interval=float(w["heartbeat_interval"]) if "heartbeat_interval" in w else None,
        max_concurrent=int(w.get("max_concurrent", 4)),
        launcher=str(w.get("launcher", "claude-code")),
        isolation=str(w.get("isolation", "worktree")),
        publish=str(w.get("publish", "none")),
        retry_on_launcher_exit=bool(w.get("retry_on_launcher_exit", False)),
        stale_policy=str(w.get("stale_policy", "keep")),
        new_repo_root=expand(w.get("new_repo_root", "~/code")),
        req_path=str(w.get("req_path", "docs/requirements")).strip("/"),
        launch_prompt=str(w.get("launch_prompt", DEFAULT_LAUNCH_PROMPT)),
        spine_protocol=str(w.get("spine_protocol", "auto")),
        npc_cmd=str(w.get("npc_cmd", "npc")),
        claude=_claude(raw.get("claude", {})),
        repos={k: expand(v) for k, v in raw.get("repos", {}).items()},
        inbox_dir=expand(ib["dir"]) if ib.get("dir") else None,
        inbox_git_pull=bool(ib.get("git_pull", True)),
        inbox_owner=str(ib.get("owner", "")),
        legacy_state=expand(ib["legacy_state"]) if ib.get("legacy_state") else state_dir / "state.json",
        database=str(ct.get("database") or f"sqlite:///{state_dir / 'control.db'}"),
        listen=str(ct.get("listen", "127.0.0.1")),
        port=int(ct.get("port", 8700)),
        lease_ttl=float(ct.get("lease_ttl", 300)),
        server_heartbeat_interval=float(ct.get("heartbeat_interval", 30)),
        max_depth=int(ct.get("max_depth", 2)),
        max_attempts=int(ct.get("max_attempts", 3)),
        worker_offline_after=float(ct.get("worker_offline_after", 120)),
        sweep_interval=float(ct.get("sweep_interval", 5)),
        notify=dict(raw.get("notify", {})),
    )
    # The worker's token authenticates it to a remote control plane; the server's token
    # is what it accepts. Same mechanism, possibly different files on different machines.
    cfg.server_token = _token(ct)
    cfg.token = _token(w) or cfg.server_token
    _validate(cfg)
    return cfg


def _load_legacy(path: Path, raw: dict[str, Any]) -> Config:
    """Ganglion 1.x ``[ganglion]`` config -> single-machine mode with 1.x semantics."""
    g = raw["ganglion"]
    if "inbox_dir" not in g or "state_dir" not in g:
        raise ValueError("config: [ganglion] must set inbox_dir and state_dir")
    state_dir = expand(g["state_dir"])
    cfg = Config(
        path=path,
        worker_id=default_worker_id(),
        control_plane="local",
        state_dir=state_dir,
        workspace_root=state_dir / "workspaces",
        poll_interval=float(g.get("poll_interval", 60)),
        max_concurrent=int(g.get("max_concurrent", 8)),
        # 1.x ran concurrent sessions inside the mapped checkout itself; 2.x gives each
        # attempt its own worktree instead (set isolation = "shared" for in-place, serialised).
        isolation=str(g.get("isolation", "worktree")),
        new_repo_root=expand(g.get("new_repo_root", "~/code")),
        req_path=str(g.get("req_path", "docs/requirements")).strip("/"),
        launch_prompt=str(g.get("launch_prompt", DEFAULT_LAUNCH_PROMPT)),
        claude=_claude(g),
        repos={k: expand(v) for k, v in raw.get("repos", {}).items()},
        inbox_dir=expand(g["inbox_dir"]),
        inbox_git_pull=bool(g.get("git_pull", True)),
        inbox_owner=str(g.get("owner", "")),
        legacy_state=state_dir / "state.json",
        database=f"sqlite:///{state_dir / 'control.db'}",
        max_depth=int(g.get("max_depth", 3)),
        # 1.x never relaunched automatically; keep a single attempt unless configured.
        max_attempts=int(g.get("max_attempts", 1)),
        notify=dict(raw.get("notify", {})),
        legacy_format=True,
    )
    _validate(cfg)
    return cfg


def _validate(cfg: Config) -> None:
    if cfg.isolation not in ("worktree", "shared"):
        raise ValueError("worker.isolation must be worktree | shared")
    if cfg.publish not in ("none", "branch"):
        raise ValueError("worker.publish must be none | branch")
    if cfg.stale_policy not in ("keep", "stop"):
        raise ValueError("worker.stale_policy must be keep | stop")
    if not cfg.local and not cfg.control_plane.startswith(("http://", "https://")):
        raise ValueError("worker.control_plane must be 'local' or an http(s):// URL")
