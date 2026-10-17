"""Ganglion 0.1.x compatibility (``<state_dir>/state.json``).

0.2.x never treats 0.1.x state as authoritative distributed state. It is only used to:

* keep the inbox adapter from resubmitting requirements 0.1.x already handled
  (see ``inbox.legacy_handled``);
* let ``ganglion retry <id>`` forget a 0.1.x entry, exactly like 0.1.x ``retry`` did;
* keep the Stop hook working for claude sessions that 0.1.x launched before the upgrade
  (they carry ``GANGLION_ID`` but no ``GANGLION_ATTEMPT_ID``): same DELIVERY.md check,
  same once-only "delivered" notification, same state update.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
from pathlib import Path
from typing import Any, Iterator

from .common import atomic_write_text, expand, log, now
from .notify import make_notifier, safe_notify
from .spine import DELIVERY_FILE, DELIVERY_PREVIEW_LINES

STATE_FILE = "state.json"
STATE_LOCK = "state.lock"


@contextlib.contextmanager
def locked_legacy_state(state_dir: Path) -> Iterator[dict[str, Any]]:
    state_dir.mkdir(parents=True, exist_ok=True)
    with open(state_dir / STATE_LOCK, "a") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            try:
                data = json.loads((state_dir / STATE_FILE).read_text(encoding="utf-8"))
            except FileNotFoundError:
                data = None
            yield data if data is not None else {}
            if data is not None:
                atomic_write_text(state_dir / STATE_FILE, json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def forget(state_dir: Path, rid: str) -> str | None:
    """Remove a 0.1.x request record. Returns its old status (None if absent)."""
    with locked_legacy_state(state_dir) as st:
        r = (st.get("requests") or {}).pop(rid, None)
    return r and r.get("status")


def stop_hook_legacy(rid: str, state_dir: Path, hook_in: dict[str, Any]) -> int:
    """0.1.x Stop hook semantics for sessions launched by 0.1.x. Always returns 0."""
    with locked_legacy_state(state_dir) as st:
        r = (st.get("requests") or {}).get(rid)
        if r is None:
            return 0
        root = r.get("project") or os.environ.get("CLAUDE_PROJECT_DIR") or hook_in.get("cwd") or os.getcwd()
        delivery = Path(root) / DELIVERY_FILE
        if r.get("status") == "delivered" or r.get("delivered_at"):
            return 0
        try:
            mtime = delivery.stat().st_mtime
        except FileNotFoundError:
            return 0
        if mtime + 1 < float(r.get("launched_at", 0)):
            return 0
        r["status"] = "delivered"
        r["delivered_at"] = now()
        r.setdefault("events", []).append({"ts": now(), "id": rid, "kind": "delivered", "detail": str(delivery)})
        config_path = st.get("config_path", "")
        repo = r.get("repo", "")
    try:
        with open(delivery, encoding="utf-8", errors="replace") as f:
            head = "".join(line for _, line in zip(range(DELIVERY_PREVIEW_LINES), f))
    except OSError:
        head = ""
    try:
        from .config import load_config

        notifier = make_notifier(load_config(expand(config_path)).notify)
    except Exception as e:
        log(f"stop-hook: cannot load notification config: {e!r}")
        return 0
    safe_notify(notifier, f"✅ 已交付：{rid}（{repo}）", head or "(DELIVERY.md 为空)", ["white_check_mark"])
    return 0
