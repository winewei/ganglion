"""Command line.

    ganglion server start|openapi          control plane (FastAPI + PostgreSQL/SQLite)
    ganglion worker start|once|status      worker runtime
    ganglion job submit|status|list|events|retry
    ganglion inbox sync                    one pass of the Git inbox job source
    ganglion metrics
    ganglion status                        local attempts + recent jobs
    ganglion retry ID                      = job retry (accepts a job id, key or inbox id)
    ganglion run | once                    0.1.x names for `worker start` / `worker once`
    ganglion stop-hook                     Claude Code Stop hook (called by hooks/stop.sh)

With ``control_plane = "local"`` (the default, and what a 0.1.x config maps to) the worker
embeds the control plane on SQLite: no server, no PostgreSQL, same Job/Attempt semantics.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import traceback
from pathlib import Path
from typing import Any

from .common import expand, log
from .config import Config, load_config
from .control.client import Client, HttpClient, LocalClient
from .control.models import ControlError, NotFound
from .notify import make_notifier

DEFAULT_CONFIG = os.environ.get("GANGLION_CONFIG", "~/.config/ganglion/config.toml")


# ---------------------------------------------------------------------------- wiring
def make_service(cfg: Config, notifier=None):
    from .control.service import ControlPlane, Settings
    from .control.store import open_store

    settings = Settings(
        lease_ttl=cfg.lease_ttl,
        heartbeat_interval=cfg.server_heartbeat_interval,
        max_depth=cfg.max_depth,
        max_attempts=cfg.max_attempts,
        worker_offline_after=cfg.worker_offline_after,
    )
    return ControlPlane(open_store(cfg.database), settings, notifier or make_notifier(cfg.notify))


def make_client(cfg: Config, owner_process: bool = False) -> Client:
    """``owner_process``: this process *is* the embedded control plane (worker in local mode),
    so it runs startup (restart grace). Short-lived CLI commands only migrate the schema."""
    if cfg.local:
        svc = make_service(cfg)
        if owner_process:
            svc.start()
        else:
            svc.store.migrate()
        return LocalClient(svc, cfg.worker_id)
    return HttpClient(cfg.control_plane, cfg.token, cfg.worker_id)


def make_inbox(cfg: Config, client: Client):
    if cfg.inbox_dir is None:
        return None
    from .inbox import InboxSource

    return InboxSource(cfg.inbox_dir, client, cfg.state_dir, owner=cfg.inbox_owner, git_pull=cfg.inbox_git_pull,
                       legacy_state=cfg.legacy_state, notifier=make_notifier(cfg.notify))


def make_worker(cfg: Config, client: Client | None = None):
    from .worker.runtime import Worker

    client = client or make_client(cfg, owner_process=True)
    return Worker(cfg, client, job_source=make_inbox(cfg, client))


def _print(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True, default=str))


def _brief(job: dict[str, Any]) -> dict[str, Any]:
    keep = ("id", "key", "name", "status", "target", "repository", "source", "depth", "epoch", "attempt_count",
            "failures", "current_attempt_id", "error", "result", "created_at", "updated_at")
    return {k: job.get(k) for k in keep}


# ---------------------------------------------------------------------------- commands
def cmd_worker_start(cfg: Config) -> int:
    from .worker.state import daemon_lock

    stop = threading.Event()

    def _sig(_signum, _frame):
        stop.set()

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)
    with daemon_lock(cfg.state_dir):
        w = make_worker(cfg)
        w.run_forever(stop, reload=lambda: load_config(cfg.path))
    return 0


def cmd_worker_once(cfg: Config) -> int:
    from .worker.state import daemon_lock

    with daemon_lock(cfg.state_dir):
        w = make_worker(cfg)
        w.start()
        w.tick()
    return 0


def cmd_status(cfg: Config) -> int:
    from .worker.state import WorkerState

    local = WorkerState(cfg.state_dir).snapshot()
    attempts = {k: {f: v.get(f) for f in ("name", "job_id", "epoch", "status", "handle", "workspace", "stale_reason", "outcome")}
                for k, v in local["attempts"].items()}
    out: dict[str, Any] = {"worker_id": cfg.worker_id, "control_plane": cfg.control_plane, "local_attempts": attempts}
    try:
        out["jobs"] = [_brief(j) for j in make_client(cfg).list_jobs(limit=50)]
    except ControlError as e:
        out["jobs_error"] = f"{e.code}: {e.detail}"
    _print(out)
    return 0


def cmd_retry(cfg: Config, ident: str) -> int:
    client = make_client(cfg)
    for candidate in (ident, f"inbox:{ident}"):
        try:
            job = client.retry_job(candidate)
            print(f"{ident}: requeued job {job['id']} (was failed); the next claim creates a new attempt")
            return 0
        except NotFound:
            continue
        except ControlError as e:
            print(f"{ident}: {e.detail}", file=sys.stderr)
            return 1
    from . import legacy

    old = legacy.forget(cfg.state_dir, ident)
    if cfg.inbox_dir is not None:
        make_inbox(cfg, client).forget(ident)
    print(f"{ident}: no job found; cleared 0.1.x record (previous status {old}); the inbox will resubmit it")
    return 0


def cmd_job_submit(cfg: Config, a: argparse.Namespace) -> int:
    goal = Path(a.goal_file).read_text(encoding="utf-8") if a.goal_file else (a.goal or "")
    repo = {"name": a.repo, "url": a.url, "ref": a.ref, "owner": a.owner}
    spec = {
        "key": a.key, "name": a.name, "repository": {k: v for k, v in repo.items() if v}, "target": a.target, "goal": goal,
        "launcher": a.launcher, "source": a.source, "parent_job_id": a.parent, "max_attempts": a.max_attempts,
        "timeout_seconds": a.timeout,
    }
    job, created = make_client(cfg).create_job({k: v for k, v in spec.items() if v is not None})
    _print({"created": created, "job": _brief(job)})
    return 0


def stop_hook(stdin_text: str = "") -> int:
    """Claude Code Stop hook. Always 0 and silent: a hook must never keep claude from stopping.

    0.2.x sessions: nudge the worker (touch ``<state>/wake``) so it looks for the result now;
    the worker -- not the hook -- decides and reports. 0.1.x sessions: 0.1.x behaviour.
    """
    state_dir = os.environ.get("GANGLION_STATE", "")
    if not state_dir:
        return 0
    sd = expand(state_dir)
    if os.environ.get("GANGLION_ATTEMPT_ID"):
        from .worker.state import WAKE_FILE, local_event

        (sd / WAKE_FILE).touch()
        local_event(sd, "stop_hook", attempt=os.environ["GANGLION_ATTEMPT_ID"])
        return 0
    rid = os.environ.get("GANGLION_ID", "")
    if rid:
        from .legacy import stop_hook_legacy

        try:
            hook_in = json.loads(stdin_text) if stdin_text.strip() else {}
        except json.JSONDecodeError:
            hook_in = {}
        return stop_hook_legacy(rid, sd, hook_in)
    return 0


def cmd_server_start(cfg: Config, a: argparse.Namespace) -> int:
    import uvicorn

    from .control.api import create_app

    host = a.host or cfg.listen
    port = a.port or cfg.port
    if a.insecure_no_auth and host not in ("127.0.0.1", "::1", "localhost"):
        raise SystemExit("--insecure-no-auth is only allowed on a loopback address")
    if cfg.database.startswith("sqlite") and not a.allow_sqlite:
        log("note: serving from SQLite; use PostgreSQL ([control] database = \"postgresql://...\") for a shared deployment")
    app = create_app(make_service(cfg), [cfg.server_token], allow_unauthenticated=a.insecure_no_auth, sweep_interval=cfg.sweep_interval)
    uvicorn.run(app, host=host, port=port, log_level="info")
    return 0


def cmd_openapi() -> int:
    from .control.api import create_app
    from .control.service import ControlPlane

    app = create_app(ControlPlane(store=None), [], allow_unauthenticated=True)  # type: ignore[arg-type]
    print(json.dumps(app.openapi(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


# ---------------------------------------------------------------------------- argparse
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="ganglion")
    ap.add_argument("--config", default=DEFAULT_CONFIG)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("run", help="alias of `worker start` (0.1.x name)")
    sub.add_parser("once", help="alias of `worker once` (0.1.x name)")
    sub.add_parser("status")
    sub.add_parser("stop-hook")
    sub.add_parser("metrics")
    rp = sub.add_parser("retry")
    rp.add_argument("id")

    sp = sub.add_parser("server").add_subparsers(dest="sub", required=True)
    ss = sp.add_parser("start")
    ss.add_argument("--host")
    ss.add_argument("--port", type=int)
    ss.add_argument("--insecure-no-auth", action="store_true", help="loopback only; for local experiments")
    ss.add_argument("--allow-sqlite", action="store_true", help="silence the SQLite note")
    sp.add_parser("openapi")

    wp = sub.add_parser("worker").add_subparsers(dest="sub", required=True)
    wp.add_parser("start")
    wp.add_parser("once")
    wp.add_parser("status")

    jp = sub.add_parser("job").add_subparsers(dest="sub", required=True)
    js = jp.add_parser("submit")
    js.add_argument("--repo", required=True, help="repository name (global identity; workers map it locally)")
    js.add_argument("--url", help="clone URL (lets any worker serve it)")
    js.add_argument("--ref")
    js.add_argument("--owner", help="for --target new")
    js.add_argument("--target", default="existing", choices=["existing", "new", "manual"])
    js.add_argument("--name")
    js.add_argument("--key", help="idempotency key")
    g = js.add_mutually_exclusive_group()
    g.add_argument("--goal")
    g.add_argument("--goal-file")
    js.add_argument("--launcher", default="claude-code")
    js.add_argument("--source", default="human", choices=["human", "agent"])
    js.add_argument("--parent", help="parent job id")
    js.add_argument("--max-attempts", type=int)
    js.add_argument("--timeout", type=float, help="seconds")
    for name in ("status", "events", "retry"):
        p = jp.add_parser(name)
        p.add_argument("id")
    jl = jp.add_parser("list")
    jl.add_argument("--status")

    ip = sub.add_parser("inbox").add_subparsers(dest="sub", required=True)
    ip.add_parser("sync")
    return ap


def main(argv: list[str] | None = None) -> int:
    a = build_parser().parse_args(argv)

    if a.cmd == "stop-hook":
        try:
            return stop_hook(sys.stdin.read() if not sys.stdin.isatty() else "")
        except Exception:
            log("stop-hook error:\n" + traceback.format_exc())
            return 0
    if a.cmd == "server" and a.sub == "openapi":
        return cmd_openapi()

    cfg = load_config(a.config)
    try:
        if a.cmd in ("run",) or (a.cmd == "worker" and a.sub == "start"):
            return cmd_worker_start(cfg)
        if a.cmd == "once" or (a.cmd == "worker" and a.sub == "once"):
            return cmd_worker_once(cfg)
        if a.cmd == "status" or (a.cmd == "worker" and a.sub == "status"):
            return cmd_status(cfg)
        if a.cmd == "retry" or (a.cmd == "job" and a.sub == "retry"):
            return cmd_retry(cfg, a.id)
        if a.cmd == "server":
            return cmd_server_start(cfg, a)
        if a.cmd == "metrics":
            _print(make_client(cfg).metrics())
            return 0
        if a.cmd == "inbox":
            client = make_client(cfg)
            src = make_inbox(cfg, client)
            if src is None:
                raise SystemExit("no [inbox] dir configured")
            _print([{"id": rid, "job": j["id"], "status": j["status"], "created": c} for rid, j, c in src.sync()])
            return 0
        if a.cmd == "job":
            client = make_client(cfg)
            if a.sub == "submit":
                return cmd_job_submit(cfg, a)
            if a.sub == "status":
                _print(client.get_job(a.id))
            elif a.sub == "events":
                job = client.get_job(a.id)
                _print(client.events(job_id=job["id"]))
            elif a.sub == "list":
                _print([_brief(j) for j in client.list_jobs(a.status)])
            return 0
    except ControlError as e:
        print(f"error: {e.code}: {e.detail}", file=sys.stderr)
        return 1
    return 2
