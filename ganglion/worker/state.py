"""Worker-local state: ``<state_dir>/worker.json`` under ``flock``.

This is launcher metadata and pending-report bookkeeping, NOT authoritative job state: the
control plane owns ownership. Locally we remember which attempts this machine runs, where
their workspaces and processes are, and any result we still owe the control plane (so a
crash between "agent finished" and "report accepted" loses nothing).

Local attempt record ``status``:
    preparing -> launching -> running -> reporting -> done
    and the side exits ``stale`` (control plane says someone else owns the job; we stop
    acting but leave the process alone) and ``dropped`` (claim replay for a dead attempt).
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
from pathlib import Path
from typing import Any, Iterator

from ..common import atomic_write_text, log, now

WORKER_STATE = "worker.json"
WORKER_LOCK = "worker.lock"
DAEMON_LOCK = "ganglion.lock"
EVENTS_FILE = "events.jsonl"
PYTHON_FILE = "python"
WAKE_FILE = "wake"

L_PREPARING = "preparing"
L_LAUNCHING = "launching"
L_RUNNING = "running"
L_REPORTING = "reporting"
L_DONE = "done"
L_STALE = "stale"
L_DROPPED = "dropped"
LOCAL_ACTIVE = (L_PREPARING, L_LAUNCHING, L_RUNNING)
LOCAL_OPEN = LOCAL_ACTIVE + (L_REPORTING,)


class WorkerState:
    def __init__(self, state_dir: Path):
        self.dir = state_dir
        self.path = state_dir / WORKER_STATE
        self.lock_path = state_dir / WORKER_LOCK

    @contextlib.contextmanager
    def locked(self) -> Iterator[dict[str, Any]]:
        self.dir.mkdir(parents=True, exist_ok=True)
        with open(self.lock_path, "a") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                data = self._read()
                yield data
                atomic_write_text(self.path, json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
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
        d.setdefault("version", 2)
        d.setdefault("attempts", {})
        d.setdefault("repos", {})       # name -> path for repos this worker created (target: new)
        d.setdefault("pending_claim", None)
        return d

    def update_attempt(self, attempt_id: str, **fields: Any) -> dict[str, Any]:
        with self.locked() as st:
            rec = st["attempts"].setdefault(attempt_id, {"attempt_id": attempt_id, "created": now()})
            rec.update(fields)
            return dict(rec)


def local_event(state_dir: Path, kind: str, detail: str = "", **fields: Any) -> None:
    """Worker-local diagnostics log (``events.jsonl``), as in 1.x. Not the audit trail."""
    ev = {"ts": now(), "kind": kind, "detail": detail, **fields}
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        with open(state_dir / EVENTS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    except OSError:
        pass
    who = fields.get("job") or fields.get("attempt") or ""
    log(f"{who}: {kind} {detail}".strip())


@contextlib.contextmanager
def daemon_lock(state_dir: Path):
    """One worker process per state directory."""
    state_dir.mkdir(parents=True, exist_ok=True)
    f = open(state_dir / DAEMON_LOCK, "a+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        f.close()
        raise SystemExit("another ganglion worker is already running on this state_dir (lock held)")
    try:
        f.seek(0)
        f.truncate()
        f.write(str(os.getpid()))
        f.flush()
        yield
    finally:
        fcntl.flock(f, fcntl.LOCK_UN)
        f.close()
