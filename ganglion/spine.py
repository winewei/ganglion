"""Agent Spine adapter: the only place that knows how a Spine run is started and how its
outcome is recognised. Runs on the worker; the control plane never reads Spine files.

Two result channels, chosen by ``protocol``:

* ``structured`` -- Ganglion exports ``GANGLION_RESULT_FILE`` (plus job / attempt / epoch
  ids) into the runtime's environment. A Spine that supports external orchestration writes
  a small JSON document there (schema below). Nothing in Agent Spine 1.9.x does this yet;
  this is the contract Ganglion offers to Spine 2.x.
* ``delivery-md`` -- compatibility with current Spine (1.x): the launch prompt asks the
  agent to write ``DELIVERY.md`` at the workspace root when it is done; its first lines
  become the summary. Only a DELIVERY.md written after this attempt was launched counts.

``protocol = "auto"`` (default) prefers a structured result and falls back to DELIVERY.md.
``"1"`` = DELIVERY.md only, ``"2"`` = structured only. Version checks stay in this module.

Structured result document (``GANGLION_RESULT_FILE``)::

    {"schema": "ganglion.engineering-result/v1",
     "status": "completed" | "completed-with-issues" | "failed",
     "run_id": "...", "final_sha": "...", "summary": "...", "result_uri": "..."}
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Any

from .common import CmdError, git, run_cmd

DELIVERY_FILE = "DELIVERY.md"
DELIVERY_PREVIEW_LINES = 20
RESULT_SCHEMA = "ganglion.engineering-result/v1"
FAILED_STATUSES = {"failed", "aborted", "error"}
SUMMARY_MAX = 16000  # below the API limit; the control plane stores at most 4000

DEFAULT_LAUNCH_PROMPT = (
    "/spine-run --auto 需求 {id}：详见 {req_path}/{id}.md（先完整阅读该文件）。"
    "全部完成后，在工程根目录写 DELIVERY.md 作为交付报告（前 20 行写结论摘要）。"
)


def detect_spine_version(npc_cmd: str = "npc") -> str | None:
    """``npc --version`` -> "1.9.1"; None when Spine's CLI is not installed."""
    if not shutil.which(npc_cmd):
        return None
    try:
        out = run_cmd([npc_cmd, "--version"], check=False, timeout=15).stdout
    except CmdError:
        return None
    m = re.search(r"(\d+\.\d+(?:\.\d+)?)", out or "")
    return m.group(1) if m else None


class SpineAdapter:
    def __init__(self, protocol: str = "auto", launch_prompt: str = DEFAULT_LAUNCH_PROMPT, req_path: str = "docs/requirements",
                 version: str | None = None):
        if protocol not in ("auto", "1", "2"):
            raise ValueError("spine_protocol must be auto | 1 | 2")
        self.protocol = protocol
        self.launch_prompt = launch_prompt
        self.req_path = req_path
        self.version = version

    def prompt(self, name: str) -> str:
        return self.launch_prompt.replace("{id}", name).replace("{req_path}", self.req_path)

    def env(self, *, job_id: str, attempt_id: str, epoch: int, name: str, result_file: Path) -> dict[str, str]:
        return {
            "GANGLION_JOB_ID": job_id,
            "GANGLION_ATTEMPT_ID": attempt_id,
            "GANGLION_EPOCH": str(epoch),
            "GANGLION_ID": name,  # 1.x name, kept for hooks/scripts that used it
            "GANGLION_RESULT_FILE": str(result_file),
        }

    def collect(self, workspace: Path, result_file: Path, launched_at: float) -> dict[str, Any] | None:
        """The engineering result if the run has finished, else None. Never raises on bad input."""
        res = None
        if self.protocol in ("auto", "2"):
            res = self._structured(result_file)
        if res is None and self.protocol in ("auto", "1"):
            res = self._delivery(workspace, launched_at)
        if res is None:
            return None
        if not res.get("final_sha"):
            try:
                res["final_sha"] = git(workspace, "rev-parse", "HEAD", check=True).stdout.strip() or None
            except CmdError:
                pass
        if self.version:
            res.setdefault("spine_version", self.version)
        return {k: v for k, v in res.items() if v is not None}

    @staticmethod
    def is_failure(result: dict[str, Any]) -> bool:
        return str(result.get("status") or "").lower() in FAILED_STATUSES

    def _structured(self, result_file: Path) -> dict[str, Any] | None:
        try:
            doc = json.loads(result_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None  # absent or half-written: not finished yet
        if not isinstance(doc, dict) or doc.get("schema") != RESULT_SCHEMA:
            return None
        def s(key: str, limit: int) -> str | None:  # the agent wrote this file: bound every field
            v = doc.get(key)
            return None if v is None else str(v)[:limit]

        return {
            "status": s("status", 64) or "completed",
            "run_id": s("run_id", 200),
            "final_sha": s("final_sha", 64),
            "summary": s("summary", SUMMARY_MAX),
            "result_uri": s("result_uri", 2000),
            "source": "spine-result",
        }

    @staticmethod
    def _delivery(workspace: Path, launched_at: float) -> dict[str, Any] | None:
        delivery = workspace / DELIVERY_FILE
        try:
            mtime = delivery.stat().st_mtime
        except OSError:
            return None
        if mtime + 1 < launched_at:
            return None  # left over from an earlier run in a reused directory
        try:
            with open(delivery, encoding="utf-8", errors="replace") as f:
                head = "".join(line for _, line in zip(range(DELIVERY_PREVIEW_LINES), f))
        except OSError:
            return None
        return {"status": "completed", "summary": head or "(DELIVERY.md is empty)", "source": "delivery-md"}
