#!/usr/bin/env python3
"""Ganglion —— agent-spine 的自动启动器（MVP）。

只做机械动作：拉 inbox → 解析目标工程 → 投递需求 → 预写 trust → 在 tmux 里
以交互模式启动 ``claude "<prompt>"`` → 通知。不含任何 Agent 逻辑。

子命令：
  run         主循环（launchd 调用的就是它）
  once        只跑一轮（调试 / 测试）
  stop-hook   Claude Code Stop hook 的实现（由 hooks/stop.sh 调用）
  status      打印状态文件摘要
  retry ID    把 error / rejected 的需求重置，下一轮重新处理

只依赖标准库；外部命令只调用 git、gh、tmux（以及被启动的 claude）。
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import importlib
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
import tomllib
import traceback
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

SESSION_PREFIX = "gl-"
STATE_FILE = "state.json"
STATE_LOCK = "state.lock"
DAEMON_LOCK = "ganglion.lock"
EVENTS_FILE = "events.jsonl"
PYTHON_FILE = "python"  # stop.sh 从这里读到可用的 python 解释器路径
DELIVERY_FILE = "DELIVERY.md"
DELIVERY_PREVIEW_LINES = 20

# 状态机
S_WAITING_REPO = "waiting_repo"
S_LAUNCHING = "launching"
S_LAUNCHED = "launched"
S_DELIVERED = "delivered"
S_REJECTED = "rejected"
S_ERROR = "error"
TERMINAL = {S_LAUNCHED, S_DELIVERED, S_REJECTED, S_ERROR}

# tmux 会话名里 "." 和 ":" 有特殊含义；空白与引号只会带来麻烦。
ID_RE = re.compile(r"^[^\s.:'\"\\/`$-][^\s.:'\"\\/`$]*$")

# claude 的 trust 对话框文案随版本变化，这里宽松匹配。
TRUST_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in (
        r"do you trust the files in this folder",
        r"trust this folder",
        r"one you trust",
        r"yes,\s*proceed",
    )
]

DEFAULT_LAUNCH_PROMPT = (
    "/spine-run --auto 需求 {id}：详见 {req_path}/{id}.md（先完整阅读该文件）。"
    "全部完成后，在工程根目录写 DELIVERY.md 作为交付报告（前 20 行写结论摘要）。"
)


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[ganglion {ts}] {msg}", file=sys.stderr, flush=True)


def now() -> float:
    return time.time()


def expand(p: str | os.PathLike[str]) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(str(p))))


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------


@dataclass
class Config:
    path: Path
    inbox_dir: Path
    state_dir: Path
    poll_interval: float = 60.0
    max_depth: int = 3
    owner: str = ""
    new_repo_root: Path = field(default_factory=lambda: expand("~/code"))
    req_path: str = "docs/requirements"
    launch_prompt: str = DEFAULT_LAUNCH_PROMPT
    claude_cmd: str = "claude"
    claude_args: str = ""
    claude_json: Path = field(default_factory=lambda: expand("~/.claude.json"))
    trust_wait: float = 8.0
    tmux_socket: str = ""
    git_pull: bool = True
    repos: dict[str, Path] = field(default_factory=dict)
    notify: dict[str, Any] = field(default_factory=dict)


def load_config(path: str | os.PathLike[str]) -> Config:
    path = expand(path).resolve()
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    g = raw.get("ganglion", {})
    if "inbox_dir" not in g or "state_dir" not in g:
        raise ValueError("config: [ganglion] 必须配置 inbox_dir 和 state_dir")
    cfg = Config(
        path=path,
        inbox_dir=expand(g["inbox_dir"]),
        state_dir=expand(g["state_dir"]),
        poll_interval=float(g.get("poll_interval", 60)),
        max_depth=int(g.get("max_depth", 3)),
        owner=str(g.get("owner", "")),
        new_repo_root=expand(g.get("new_repo_root", "~/code")),
        req_path=str(g.get("req_path", "docs/requirements")).strip("/"),
        launch_prompt=str(g.get("launch_prompt", DEFAULT_LAUNCH_PROMPT)),
        claude_cmd=str(g.get("claude_cmd", "claude")),
        claude_args=str(g.get("claude_args", "")),
        claude_json=expand(g.get("claude_json", "~/.claude.json")),
        trust_wait=float(g.get("trust_wait", 8)),
        tmux_socket=str(g.get("tmux_socket", "")),
        git_pull=bool(g.get("git_pull", True)),
        repos={k: expand(v) for k, v in raw.get("repos", {}).items()},
        notify=dict(raw.get("notify", {})),
    )
    return cfg


# ---------------------------------------------------------------------------
# 原子 JSON 写入 + 状态文件（带 flock；主循环与 Stop hook 共用）
# ---------------------------------------------------------------------------


def atomic_write_text(path: Path, text: str, mode: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{int(now() * 1e6)}.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        if mode is not None:
            os.chmod(tmp, mode)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


class State:
    """state_dir/state.json 的读写。所有修改都在 ``locked()`` 内完成。"""

    def __init__(self, state_dir: Path):
        self.dir = state_dir
        self.path = state_dir / STATE_FILE
        self.lock_path = state_dir / STATE_LOCK
        self.data: dict[str, Any] = {}

    @contextlib.contextmanager
    def locked(self):
        self.dir.mkdir(parents=True, exist_ok=True)
        with open(self.lock_path, "a") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                self.data = self._read()
                yield self.data
                self._write()
            finally:
                fcntl.flock(lf, fcntl.LOCK_UN)

    def snapshot(self) -> dict[str, Any]:
        with self.locked() as d:
            return json.loads(json.dumps(d))

    def _read(self) -> dict[str, Any]:
        try:
            d = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            d = {}
        d.setdefault("version", 1)
        d.setdefault("requests", {})
        d.setdefault("repos", {})
        return d

    def _write(self) -> None:
        atomic_write_text(self.path, json.dumps(self.data, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def add_event(state_dir: Path, req: dict[str, Any] | None, rid: str, kind: str, detail: str = "") -> None:
    ev = {"ts": now(), "id": rid, "kind": kind, "detail": detail}
    if req is not None:
        req.setdefault("events", []).append(ev)
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        with open(state_dir / EVENTS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    except OSError:
        pass
    log(f"{rid}: {kind} {detail}".rstrip())


# ---------------------------------------------------------------------------
# 通知（可替换）
# ---------------------------------------------------------------------------


class Notifier:
    def send(self, title: str, message: str, tags: list[str] | None = None) -> None:  # pragma: no cover
        raise NotImplementedError


class NullNotifier(Notifier):
    def send(self, title, message, tags=None):
        log(f"notify(null): {title}")


class FileNotifier(Notifier):
    """把通知追加写进 JSONL 文件。调试和测试用。"""

    def __init__(self, path: str):
        self.path = expand(path)

    def send(self, title, message, tags=None):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": now(), "title": title, "message": message, "tags": tags or []}, ensure_ascii=False) + "\n")


class NtfyNotifier(Notifier):
    """用 ntfy 的 JSON 发布接口（标题走 body，避免中文进 HTTP header 触发 latin-1 编码错误）。"""

    def __init__(self, topic: str, server: str = "https://ntfy.sh", token: str = "", priority: int = 3, timeout: float = 10):
        if not topic:
            raise ValueError("notify.ntfy_topic 未配置")
        self.topic, self.server, self.token = topic, server.rstrip("/"), token
        self.priority, self.timeout = int(priority), float(timeout)

    def send(self, title, message, tags=None):
        body = {"topic": self.topic, "title": title, "message": message, "priority": self.priority}
        if tags:
            body["tags"] = tags
        req = urllib.request.Request(
            self.server + "/",
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            resp.read()


NOTIFIERS: dict[str, Callable[..., Notifier]] = {
    "ntfy": lambda c: NtfyNotifier(
        topic=c.get("ntfy_topic", ""),
        server=c.get("ntfy_server", "https://ntfy.sh"),
        token=c.get("ntfy_token", ""),
        priority=c.get("ntfy_priority", 3),
    ),
    "file": lambda c: FileNotifier(c.get("file_path", "~/.ganglion/notifications.jsonl")),
    "null": lambda c: NullNotifier(),
}


def make_notifier(conf: dict[str, Any]) -> Notifier:
    backend = conf.get("backend", "ntfy")
    if backend in NOTIFIERS:
        return NOTIFIERS[backend](conf)
    if ":" in backend:  # "some.module:factory" —— factory(conf) -> Notifier
        mod, _, attr = backend.partition(":")
        return getattr(importlib.import_module(mod), attr)(conf)
    raise ValueError(f"未知的通知后端: {backend}")


def safe_notify(notifier: Notifier, title: str, message: str, tags: list[str] | None = None) -> bool:
    try:
        notifier.send(title, message, tags)
        return True
    except Exception as e:  # 通知失败绝不影响主流程
        log(f"notify failed: {e!r}")
        return False


# ---------------------------------------------------------------------------
# 外部命令
# ---------------------------------------------------------------------------


class CmdError(RuntimeError):
    pass


def run_cmd(args: list[str], cwd: Path | None = None, check: bool = True, timeout: float = 120) -> subprocess.CompletedProcess:
    try:
        p = subprocess.run(args, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError as e:
        raise CmdError(f"找不到命令 {args[0]}（PATH={os.environ.get('PATH', '')}）") from e
    except subprocess.TimeoutExpired as e:
        raise CmdError(f"命令超时: {shlex.join(args)}") from e
    if check and p.returncode != 0:
        raise CmdError(f"{shlex.join(args)} 失败 (exit {p.returncode}): {(p.stderr or p.stdout).strip()}")
    return p


def git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return run_cmd(["git", "-C", str(repo), *args], check=check)


class Tmux:
    def __init__(self, socket: str = ""):
        self.base = ["tmux"] + (["-L", socket] if socket else [])

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return run_cmd([*self.base, *args], check=check, timeout=30)

    def sessions(self) -> set[str]:
        p = self._run("list-sessions", "-F", "#{session_name}", check=False)
        if p.returncode != 0:  # 没有 server = 没有 session
            return set()
        return {line.strip() for line in p.stdout.splitlines() if line.strip()}

    def has_session(self, name: str) -> bool:
        # 用 list-sessions 精确比较；`has-session -t name` 会做前缀匹配（gl-a 会命中 gl-ab）。
        return name in self.sessions()

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


# ---------------------------------------------------------------------------
# inbox 解析
# ---------------------------------------------------------------------------


@dataclass
class Requirement:
    id: str
    path: Path
    meta: dict[str, str]
    raw: bytes

    @property
    def target(self) -> str:
        return self.meta.get("target", "").strip().lower()

    @property
    def repo(self) -> str:
        return self.meta.get("repo", "").strip()

    @property
    def source(self) -> str:
        return (self.meta.get("source") or "human").strip().lower()

    @property
    def parent(self) -> str:
        return self.meta.get("parent", "").strip()


def parse_frontmatter(text: str) -> dict[str, str]:
    """极简 YAML frontmatter：只认 ``key: value`` 平铺字段。正文不解析。"""
    text = text.lstrip("﻿")
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}
    meta: dict[str, str] = {}
    for line in lines[1:]:
        if line.strip() in ("---", "..."):
            return meta
        if not line.strip() or line.lstrip().startswith("#") or ":" not in line:
            continue
        k, _, v = line.partition(":")
        v = v.split(" #", 1)[0].strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
            v = v[1:-1]
        meta[k.strip().lower()] = v
    return {}  # 没有闭合的 frontmatter 视为没有


def scan_inbox(inbox: Path) -> dict[str, Requirement]:
    reqs: dict[str, Requirement] = {}
    for p in sorted(inbox.glob("*.md")):
        if not p.is_file() or p.name.lower() == "readme.md":
            continue
        raw = p.read_bytes()
        meta = parse_frontmatter(raw.decode("utf-8", errors="replace"))
        reqs[p.stem] = Requirement(id=p.stem, path=p, meta=meta, raw=raw)
    return reqs


def compute_depth(rid: str, reqs: dict[str, Requirement]) -> int:
    """human 深度 0；agent 深度 = parent 深度 + 1。parent 缺失 / 成环抛 ValueError。"""
    seen: list[str] = []
    depth = 0
    cur = reqs[rid]
    while cur.source == "agent":
        if cur.id in seen:
            raise ValueError(f"parent 链成环: {' -> '.join(seen + [cur.id])}")
        seen.append(cur.id)
        depth += 1
        if not cur.parent:
            break  # 没写 parent 的 agent 需求：视为直接挂在某个 human 需求下
        if cur.parent not in reqs:
            raise ValueError(f"parent {cur.parent!r} 不在 inbox 中")
        cur = reqs[cur.parent]
    return depth


# ---------------------------------------------------------------------------
# ~/.claude.json trust 预写
# ---------------------------------------------------------------------------


def project_keys(project: Path) -> list[str]:
    keys = [str(project.absolute())]
    real = os.path.realpath(project)
    if real not in keys:  # macOS: /tmp -> /private/tmp 等
        keys.append(real)
    return keys


def prewrite_trust(claude_json: Path, project: Path, retries: int = 8, base_delay: float = 0.2) -> None:
    """读取 → 合并 → 写临时文件 → rename → 读回校验，失败重试。

    其它 claude 进程随时可能整体重写这个文件：读到半截 JSON、或我们写完后被别人
    用旧内容覆盖，都表现为解析失败 / 校验不通过，退避后重来即可。
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
                raise ValueError("~/.claude.json 顶层不是对象")
            projects = data.setdefault("projects", {})
            if not isinstance(projects, dict):
                raise ValueError("projects 字段不是对象")
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
            last_err = "读回校验失败（可能被其它进程覆盖）"
        except (json.JSONDecodeError, OSError, ValueError) as e:
            last_err = repr(e)
        time.sleep(base_delay * (2**attempt))
    raise RuntimeError(f"预写 trust 失败: {last_err}")


# ---------------------------------------------------------------------------
# 目标工程
# ---------------------------------------------------------------------------


class WaitingRepo(Exception):
    pass


def resolve_repo(cfg: Config, st: dict[str, Any], req: Requirement) -> Path:
    name = req.repo
    if req.target not in ("existing", "new", "manual"):
        raise ValueError(f"target 必须是 existing|new|manual，得到 {req.target!r}")
    if not name:
        raise ValueError("缺少 repo 字段")
    mapped = cfg.repos.get(name) or (expand(st["repos"][name]) if name in st["repos"] else None)
    if req.target == "existing":
        if not mapped:
            raise ValueError(f"[repos] 中没有 {name!r} 的映射")
        return check_git_repo(mapped)
    if req.target == "manual":
        if not mapped:
            raise WaitingRepo(name)
        return check_git_repo(mapped)
    # new
    if name in cfg.repos:
        raise ValueError(f"{name!r} 已在 [repos] 中映射，请用 target: existing")
    if mapped:  # 之前某条 new 需求（或崩溃前的本条）已创建
        return check_git_repo(mapped)
    return create_repo(cfg, st, name)


def check_git_repo(path: Path) -> Path:
    if not (path / ".git").exists():
        raise ValueError(f"{path} 不是 git 仓库")
    return path.absolute()


def gh_create_commands(owner: str, name: str, dest: Path) -> list[list[str]]:
    full = f"{owner}/{name}"
    return [
        ["gh", "repo", "create", full, "--private"],
        ["gh", "repo", "clone", full, str(dest)],
    ]


def create_repo(cfg: Config, st: dict[str, Any], name: str) -> Path:
    if not cfg.owner:
        raise ValueError("target: new 需要在 config 中配置 owner")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", name):
        raise ValueError(f"非法仓库名 {name!r}")
    dest = (cfg.new_repo_root / name).absolute()
    create, clone = gh_create_commands(cfg.owner, name, dest)
    if not (dest / ".git").exists():
        if dest.exists() and any(dest.iterdir()):
            raise ValueError(f"{dest} 已存在且不是 git 仓库")
        p = run_cmd(create, check=False)
        if p.returncode != 0:
            # 崩溃重启时远端可能已建好：能 view 就继续 clone
            if run_cmd(["gh", "repo", "view", f"{cfg.owner}/{name}"], check=False).returncode != 0:
                raise CmdError(f"{shlex.join(create)} 失败: {(p.stderr or p.stdout).strip()}")
        cfg.new_repo_root.mkdir(parents=True, exist_ok=True)
        run_cmd(clone)
    st["repos"][name] = str(dest)
    return dest


def deliver_requirement(cfg: Config, req: Requirement, project: Path) -> str:
    """把需求原样复制到 <工程>/<req_path>/<id>.md 并单独提交。幂等。返回 commit sha。"""
    rel = f"{cfg.req_path}/{req.id}.md" if cfg.req_path else f"{req.id}.md"
    dest = project / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists() or dest.read_bytes() != req.raw:
        dest.write_bytes(req.raw)
    git(project, "add", "--", rel)
    # 只提交这一个路径：不卷走用户暂存区里的其它改动
    pending = git(project, "status", "--porcelain", "--", rel).stdout.strip()
    if pending:
        git(project, "commit", "-q", "-m", f"req: {req.id}", "--", rel)
    return git(project, "rev-parse", "HEAD").stdout.strip()


# ---------------------------------------------------------------------------
# 启动
# ---------------------------------------------------------------------------


def session_name(rid: str) -> str:
    return SESSION_PREFIX + rid


def build_claude_command(cfg: Config, rid: str) -> str:
    prompt = cfg.launch_prompt.replace("{id}", rid).replace("{req_path}", cfg.req_path)
    parts = [cfg.claude_cmd, *shlex.split(cfg.claude_args), prompt]
    return shlex.join(parts)


def trust_prompt_visible(screen: str) -> bool:
    return any(p.search(screen) for p in TRUST_PATTERNS)


def launch(cfg: Config, tmux: Tmux, rid: str, project: Path) -> bool:
    """启动 tmux session。已存在则什么都不做并返回 False。"""
    name = session_name(rid)
    if tmux.has_session(name):
        return False
    env = {"GANGLION_ID": rid, "GANGLION_STATE": str(cfg.state_dir.absolute())}
    tmux.new_session(name, project, env, build_claude_command(cfg, rid))
    return True


def trust_fallback(cfg: Config, tmux: Tmux, rid: str) -> bool:
    """在 trust_wait 秒内轮询屏幕；看到 trust 提示就回车一次。返回是否触发。"""
    name = session_name(rid)
    deadline = now() + cfg.trust_wait
    while True:
        if trust_prompt_visible(tmux.capture(name)):
            tmux.send_enter(name)
            return True
        if now() >= deadline:
            return False
        time.sleep(0.5)


# ---------------------------------------------------------------------------
# 主循环
# ---------------------------------------------------------------------------


class Ganglion:
    def __init__(self, cfg: Config, notifier: Notifier | None = None):
        self.cfg = cfg
        self.state = State(cfg.state_dir)
        self.notifier = notifier or make_notifier(cfg.notify)
        self.tmux = Tmux(cfg.tmux_socket)

    # -- 工具 --
    def notify(self, title: str, message: str, tags: list[str] | None = None) -> None:
        safe_notify(self.notifier, title, message, tags)

    def _record_runtime(self) -> None:
        """让 Stop hook 能找到 config 和一个 ≥3.11 的 python。"""
        self.cfg.state_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_text(self.cfg.state_dir / PYTHON_FILE, sys.executable + "\n")
        with self.state.locked() as st:
            st["config_path"] = str(self.cfg.path)

    # -- 对账 --
    def reconcile(self) -> None:
        sessions = self.tmux.sessions()
        with self.state.locked() as st:
            reqs = st["requests"]
            for rid, r in reqs.items():
                live = session_name(rid) in sessions
                if r["status"] == S_LAUNCHING and live:
                    r["status"] = S_LAUNCHED
                    r.setdefault("launched_at", now())
                    add_event(self.cfg.state_dir, r, rid, "reconciled", "session 已存在，补记为 launched")
            for name in sessions:
                if not name.startswith(SESSION_PREFIX):
                    continue
                rid = name[len(SESSION_PREFIX):]
                if rid not in reqs:
                    reqs[rid] = {"status": S_LAUNCHED, "first_seen": now(), "launched_at": now(), "adopted": True}
                    add_event(self.cfg.state_dir, reqs[rid], rid, "adopted", "状态文件中没有记录，接管现存 session")

    # -- 一轮 --
    def poll_once(self) -> None:
        cfg = self.cfg
        if cfg.git_pull:
            try:
                git(cfg.inbox_dir, "pull", "--ff-only", "-q")
            except CmdError as e:
                log(f"inbox git pull 失败（继续用本地内容）: {e}")
        inbox = scan_inbox(cfg.inbox_dir)
        with self.state.locked() as st:
            known = dict(st["requests"])
        for rid in inbox:
            r = known.get(rid)
            if r is None or r["status"] in (S_WAITING_REPO, S_LAUNCHING):
                self.process(rid, inbox)

    def process(self, rid: str, inbox: dict[str, Requirement]) -> None:
        cfg = self.cfg
        req = inbox[rid]
        try:
            self._process(rid, req, inbox)
        except Exception as e:
            detail = f"{type(e).__name__}: {e}"
            with self.state.locked() as st:
                r = st["requests"].setdefault(rid, {"first_seen": now()})
                r["status"] = S_ERROR
                r["error"] = detail
                add_event(cfg.state_dir, r, rid, "error", detail)
            log(traceback.format_exc())
            self.notify(f"❌ Ganglion 出错：{rid}", detail, ["x"])

    def _process(self, rid: str, req: Requirement, inbox: dict[str, Requirement]) -> None:
        cfg = self.cfg
        with self.state.locked() as st:
            r = st["requests"].setdefault(rid, {"first_seen": now()})
            prev = r.get("status")
            r.update({"target": req.target, "repo": req.repo, "source": req.source, "parent": req.parent})

            # 0. id 合法性
            if not ID_RE.match(rid):
                raise ValueError(f"需求 id {rid!r} 不能含空白、'.'、':'、引号或以 '-' 开头（tmux 会话名限制）")

            # 1. 防繁殖
            if req.source == "agent":
                try:
                    depth = compute_depth(rid, inbox)
                    r["depth"] = depth
                    reason = f"深度 {depth} 超过 MAX_DEPTH={cfg.max_depth}" if depth > cfg.max_depth else ""
                except ValueError as e:
                    reason = str(e)
                if reason:
                    r["status"] = S_REJECTED
                    r["error"] = reason
                    add_event(cfg.state_dir, r, rid, "rejected", reason)
                    rejected = reason
                else:
                    rejected = ""
            else:
                rejected = ""
                r["depth"] = 0
        if rejected:
            self.notify(f"🚫 需求被拒绝：{rid}", rejected, ["no_entry"])
            return

        # 2. 目标工程
        with self.state.locked() as st:
            r = st["requests"][rid]
            try:
                project = resolve_repo(cfg, st, req)
            except WaitingRepo:
                if r.get("status") != S_WAITING_REPO:
                    r["status"] = S_WAITING_REPO
                    add_event(cfg.state_dir, r, rid, "waiting_repo", req.repo)
                    notify_wait = True
                else:
                    notify_wait = False
                project = None
            else:
                notify_wait = False
                r["project"] = str(project)
        if project is None:
            if notify_wait:
                self.notify(
                    f"⏳ 等待工程映射：{rid}",
                    f"请在 config 的 [repos] 中加入 {req.repo} = \"<本地路径>\"，Ganglion 会自动继续。",
                    ["hourglass"],
                )
            return

        # 3. 投递
        sha = deliver_requirement(cfg, req, project)
        # 4. trust
        prewrite_trust(cfg.claude_json, project)

        # 5. 启动（先落 launching，崩溃后可对账）
        with self.state.locked() as st:
            r = st["requests"][rid]
            r["status"] = S_LAUNCHING
            r["req_commit"] = sha
            r["launched_at"] = now()
            add_event(cfg.state_dir, r, rid, "launching", str(project))
        started = launch(cfg, self.tmux, rid, project)
        trusted = trust_fallback(cfg, self.tmux, rid) if started else False

        with self.state.locked() as st:
            r = st["requests"][rid]
            if trusted:
                add_event(cfg.state_dir, r, rid, "trust_fallback", "检测到 trust 提示，已发送 Enter")
            # trust_wait 期间 Stop hook 可能已经把它标成 delivered，不能倒退
            if r.get("status") == S_LAUNCHING:
                r["status"] = S_LAUNCHED
            add_event(cfg.state_dir, r, rid, "launched" if started else "session_exists", session_name(rid))
        if started or prev != S_LAUNCHING:
            self.notify(
                f"🚀 已启动：{rid}",
                f"{req.repo} @ {project}\ntmux -CC attach -t {session_name(rid)}",
                ["rocket"],
            )

    # -- 主循环 --
    def run_forever(self) -> None:
        stop = {"flag": False}

        def _sig(signum, _frame):
            stop["flag"] = True

        signal.signal(signal.SIGTERM, _sig)
        signal.signal(signal.SIGINT, _sig)
        self._record_runtime()
        self.reconcile()
        while not stop["flag"]:
            try:
                self.cfg = load_config(self.cfg.path)  # [repos] 的补充无需重启
                self.tmux = Tmux(self.cfg.tmux_socket)
            except Exception as e:
                log(f"重新加载 config 失败，沿用旧配置: {e!r}")
            try:
                self.poll_once()
            except Exception:
                log("poll 失败:\n" + traceback.format_exc())
            deadline = now() + self.cfg.poll_interval
            while not stop["flag"] and now() < deadline:
                time.sleep(min(1.0, max(0.0, deadline - now())))
        log("收到退出信号，主循环结束")


@contextlib.contextmanager
def daemon_lock(state_dir: Path):
    state_dir.mkdir(parents=True, exist_ok=True)
    f = open(state_dir / DAEMON_LOCK, "a+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        f.close()
        raise SystemExit("另一个 ganglion 实例正在运行（锁被占用）")
    try:
        f.seek(0)
        f.truncate()
        f.write(str(os.getpid()))
        f.flush()
        yield
    finally:
        fcntl.flock(f, fcntl.LOCK_UN)
        f.close()


# ---------------------------------------------------------------------------
# Stop hook
# ---------------------------------------------------------------------------


def stop_hook(stdin_text: str = "") -> int:
    """Claude Code Stop hook。任何情况都返回 0，绝不阻止 claude 停下。"""
    rid = os.environ.get("GANGLION_ID", "")
    state_dir = os.environ.get("GANGLION_STATE", "")
    if not rid or not state_dir:
        return 0
    state = State(expand(state_dir))
    try:
        hook_in = json.loads(stdin_text) if stdin_text.strip() else {}
    except json.JSONDecodeError:
        hook_in = {}

    with state.locked() as st:
        r = st["requests"].get(rid)
        if r is None:
            return 0
        root = r.get("project") or os.environ.get("CLAUDE_PROJECT_DIR") or hook_in.get("cwd") or os.getcwd()
        delivery = Path(root) / DELIVERY_FILE
        if r.get("status") == S_DELIVERED or r.get("delivered_at"):
            return 0
        try:
            mtime = delivery.stat().st_mtime
        except FileNotFoundError:
            return 0  # Agent 只是阶段性停下
        # 防御：目录被手工复用时，启动前就存在的旧 DELIVERY.md 不算
        if mtime + 1 < float(r.get("launched_at", 0)):
            return 0
        r["status"] = S_DELIVERED
        r["delivered_at"] = now()
        add_event(state.dir, r, rid, "delivered", str(delivery))
        config_path = st.get("config_path", "")
        repo = r.get("repo", "")

    try:
        with open(delivery, encoding="utf-8", errors="replace") as f:
            head = "".join(line for _, line in zip(range(DELIVERY_PREVIEW_LINES), f))
    except OSError:
        head = ""
    try:
        cfg = load_config(config_path)
        notifier = make_notifier(cfg.notify)
    except Exception as e:
        log(f"stop-hook: 无法加载通知配置: {e!r}")
        return 0
    safe_notify(notifier, f"✅ 已交付：{rid}（{repo}）", head or "(DELIVERY.md 为空)", ["white_check_mark"])
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ganglion")
    ap.add_argument("--config", default=os.environ.get("GANGLION_CONFIG", "~/.config/ganglion/config.toml"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run")
    sub.add_parser("once")
    sub.add_parser("stop-hook")
    sub.add_parser("status")
    rp = sub.add_parser("retry")
    rp.add_argument("id")
    a = ap.parse_args(argv)

    if a.cmd == "stop-hook":
        try:
            return stop_hook(sys.stdin.read() if not sys.stdin.isatty() else "")
        except Exception:
            log("stop-hook 异常:\n" + traceback.format_exc())
            return 0

    cfg = load_config(a.config)
    if a.cmd == "status":
        print(json.dumps(State(cfg.state_dir).snapshot(), ensure_ascii=False, indent=2))
        return 0
    if a.cmd == "retry":
        with State(cfg.state_dir).locked() as st:
            r = st["requests"].pop(a.id, None)
        print(f"{a.id}: 已清除（原状态 {r and r.get('status')}），下一轮重新处理")
        return 0

    with daemon_lock(cfg.state_dir):
        g = Ganglion(cfg)
        if a.cmd == "once":
            g._record_runtime()
            g.reconcile()
            g.poll_once()
        else:
            g.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
