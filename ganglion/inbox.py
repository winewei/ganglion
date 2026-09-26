"""Git inbox job source: Markdown requirement files in a git repo -> control-plane jobs.

One ``<id>.md`` per requirement (frontmatter: target, repo, source, parent; optional
repo_url, ref, max_attempts, timeout). The whole file is the job goal, delivered verbatim.

Ingestion is idempotent: each file becomes ``POST /v1/jobs`` with ``key = "inbox:<id>"``,
so rediscovering it (every poll, after a crash, from two adapters) never creates a second
job. Parents are submitted before children so the control plane can compute depth; a parent
that is missing from the inbox or part of a cycle is submitted as an unresolvable reference
and the control plane rejects the child (recursion protection is enforced there, not here).

The control plane never needs this clone -- only the machine running the adapter does.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .common import CmdError, atomic_write_text, git, log
from .control.client import Client
from .control.models import ControlError, Unavailable
from .notify import Notifier, safe_notify

KEY_PREFIX = "inbox:"
MEMO_FILE = "inbox.json"
# 1.x statuses meaning "already handled; do not submit again"
LEGACY_HANDLED = {"launched", "delivered", "rejected", "error"}


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
    """Minimal YAML frontmatter: flat ``key: value`` only. The body is not parsed."""
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
    return {}  # unterminated frontmatter == none


def scan_inbox(inbox: Path) -> dict[str, Requirement]:
    reqs: dict[str, Requirement] = {}
    for p in sorted(inbox.glob("*.md")):
        if not p.is_file() or p.name.lower() == "readme.md":
            continue
        raw = p.read_bytes()
        reqs[p.stem] = Requirement(id=p.stem, path=p, meta=parse_frontmatter(raw.decode("utf-8", errors="replace")), raw=raw)
    return reqs


def submission_order(reqs: dict[str, Requirement]) -> tuple[list[str], set[str]]:
    """Parents before children. Returns (order, ids on a parent cycle)."""
    order: list[str] = []
    state: dict[str, int] = {}  # 1 = visiting, 2 = done
    cyclic: set[str] = set()

    def visit(rid: str, path: list[str]) -> None:
        if state.get(rid) == 2:
            return
        if state.get(rid) == 1:
            cyclic.update(path[path.index(rid):])
            return
        state[rid] = 1
        r = reqs[rid]
        if r.source == "agent" and r.parent in reqs:
            visit(r.parent, path + [rid])
        state[rid] = 2
        order.append(rid)

    for rid in sorted(reqs):
        visit(rid, [])
    return order, cyclic


def legacy_handled(legacy_state: Path | None) -> dict[str, str]:
    """Compatibility read path for Ganglion 1.x ``state.json``: ids it already handled."""
    if not legacy_state or not legacy_state.exists():
        return {}
    try:
        d = json.loads(legacy_state.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        log(f"legacy state {legacy_state} unreadable; ignoring it")
        return {}
    return {rid: r.get("status", "") for rid, r in (d.get("requests") or {}).items() if r.get("status") in LEGACY_HANDLED}


class InboxSource:
    def __init__(self, inbox_dir: Path, client: Client, state_dir: Path, owner: str = "", git_pull: bool = True,
                 legacy_state: Path | None = None, notifier: Notifier | None = None):
        self.dir = inbox_dir
        self.client = client
        self.memo_path = state_dir / MEMO_FILE
        self.owner = owner
        self.git_pull = git_pull
        self.legacy_state = legacy_state
        self.notifier = notifier

    def _memo(self) -> dict[str, Any]:
        try:
            return json.loads(self.memo_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"jobs": {}, "invalid": {}}

    def __call__(self) -> list[tuple[str, dict[str, Any], bool]]:
        return self.sync()

    def sync(self) -> list[tuple[str, dict[str, Any], bool]]:
        if self.git_pull:
            try:
                git(self.dir, "pull", "--ff-only", "-q", timeout=120)
            except CmdError as e:
                log(f"inbox git pull failed (using local content): {e}")
        reqs = scan_inbox(self.dir)
        memo = self._memo()
        memo.setdefault("jobs", {})
        memo.setdefault("invalid", {})
        legacy = legacy_handled(self.legacy_state)
        order, cyclic = submission_order(reqs)
        out = []
        try:
            for rid in order:
                if rid in memo["jobs"] or rid in legacy:
                    continue
                r = reqs[rid]
                problem = self._invalid(r)
                if problem:
                    digest = hashlib.sha256(r.raw).hexdigest()
                    if memo["invalid"].get(rid) != digest:  # report each version of a bad file once
                        memo["invalid"][rid] = digest
                        log(f"inbox {rid}: {problem}")
                        if self.notifier:
                            safe_notify(self.notifier, f"❌ Ganglion 出错：{rid}", problem, ["x"])
                    continue
                parent_id = None
                if r.source == "agent" and r.parent:
                    if rid in cyclic:
                        parent_id = f"inbox-cycle:{r.parent}"
                    elif r.parent in memo["jobs"]:
                        parent_id = memo["jobs"][r.parent]
                    else:
                        parent_id = f"inbox-missing:{r.parent}"  # control plane rejects: parent not found
                repository = {"name": r.repo}
                if r.meta.get("repo_url"):
                    repository["url"] = r.meta["repo_url"]
                if r.meta.get("ref"):
                    repository["ref"] = r.meta["ref"]
                if r.target == "new":
                    repository["owner"] = r.meta.get("owner") or self.owner
                spec = {
                    "key": KEY_PREFIX + rid,
                    "name": rid,
                    "repository": repository,
                    "target": r.target,
                    "goal": r.raw.decode("utf-8", errors="replace"),
                    "source": r.source,
                    "parent_job_id": parent_id,
                }
                if r.meta.get("max_attempts"):
                    spec["max_attempts"] = int(r.meta["max_attempts"])
                if r.meta.get("timeout"):
                    spec["timeout_seconds"] = float(r.meta["timeout"])
                try:
                    job, created = self.client.create_job(spec)
                except Unavailable:
                    raise
                except ControlError as e:
                    log(f"inbox {rid}: control plane refused it: {e.detail}")
                    continue
                memo["jobs"][rid] = job["id"]
                memo["invalid"].pop(rid, None)
                out.append((rid, job, created))
        except Unavailable as e:
            log(f"inbox sync paused: {e}")
        finally:
            atomic_write_text(self.memo_path, json.dumps(memo, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        return out

    @staticmethod
    def _invalid(r: Requirement) -> str:
        if r.target not in ("existing", "new", "manual"):
            return f"target must be existing|new|manual, got {r.target!r}"
        if not r.repo:
            return "missing repo field"
        if r.source not in ("human", "agent"):
            return f"source must be human|agent, got {r.source!r}"
        for field, conv in (("max_attempts", int), ("timeout", float)):
            if r.meta.get(field):
                try:
                    if conv(r.meta[field]) <= 0:
                        raise ValueError
                except ValueError:
                    return f"{field} must be a positive number, got {r.meta[field]!r}"
        return ""

    def forget(self, rid: str) -> None:
        """Drop the local memo entry so the next sync re-reads the file (after `retry`)."""
        memo = self._memo()
        memo.get("jobs", {}).pop(rid, None)
        memo.get("invalid", {}).pop(rid, None)
        atomic_write_text(self.memo_path, json.dumps(memo, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
