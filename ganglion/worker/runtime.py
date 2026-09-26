"""Ganglion worker runtime.

    register -> reconcile -> loop { heartbeat, observe/report own attempts, claim, prepare,
                                    launch Agent Spine }

Two layers of truth, never confused:

* the **control plane** decides ownership (attempt id + epoch + lease). The worker only
  acts on an attempt the control plane still confirms, and every report is fenced
  server-side anyway;
* the **local machine** knows which processes exist. The worker never kills or relaunches
  something just because one side cannot be observed right now: "unknown" stays unknown
  until it can be resolved.

Crash safety carried over from 1.x: state is written *before* each side effect (claim
request id before the claim, ``launching`` before the tmux session, the report and its
request id before sending it), and every side effect is idempotent, so a crash at any
point is resumed by simply doing the step again.
"""

from __future__ import annotations

import platform as _platform
import shutil
import sys
import threading
import traceback
from pathlib import Path
from typing import Any, Callable

from .. import __version__
from ..common import CmdError, log, log_metric, new_id, now
from ..config import Config
from ..control.client import Client
from ..control.models import Conflict, ControlError, InvalidRequest, NotFound, StaleAttempt, Unavailable
from ..launchers.base import Launcher, LaunchSpec
from ..launchers.claude import SESSION_PREFIX, ClaudeCodeLauncher
from ..notify import Notifier, make_notifier, safe_notify
from ..spine import SpineAdapter, detect_spine_version
from . import repo as repo_mod
from .state import (
    L_DONE,
    L_DROPPED,
    L_LAUNCHING,
    L_PREPARING,
    L_REPORTING,
    L_RUNNING,
    L_STALE,
    LOCAL_ACTIVE,
    LOCAL_OPEN,
    PYTHON_FILE,
    WAKE_FILE,
    WorkerState,
    local_event,
)


def make_launcher(cfg: Config) -> Launcher:
    if cfg.launcher == "claude-code":
        return ClaudeCodeLauncher(cfg.claude)
    raise ValueError(f"unknown launcher {cfg.launcher!r} (available: claude-code)")


class Worker:
    def __init__(
        self,
        cfg: Config,
        client: Client,
        launcher: Launcher | None = None,
        notifier: Notifier | None = None,
        adapter: SpineAdapter | None = None,
        job_source: Callable[[], Any] | None = None,
    ):
        self.cfg = cfg
        self.client = client
        self.launcher = launcher or make_launcher(cfg)
        self.notifier = notifier or make_notifier(cfg.notify)
        self.state = WorkerState(cfg.state_dir)
        self.spine_version = detect_spine_version(cfg.npc_cmd)
        self.adapter = adapter or SpineAdapter(cfg.spine_protocol, cfg.launch_prompt, cfg.req_path, self.spine_version)
        self.job_source = job_source
        self.server_settings: dict[str, Any] = {"heartbeat_interval": 30.0, "lease_ttl": 300.0}
        self.registered = False
        self._advertised: Any = None

    # ------------------------------------------------------------------ identity
    def info(self) -> dict[str, Any]:
        """What we tell the control plane. Deliberately minimal: no hostname, user or paths."""
        have = {b: shutil.which(b) is not None for b in ("git", "gh", "tmux", "docker", "codex")}
        launcher_ok = self.launcher.available()
        with self.state.locked() as st:
            created = list(st["repos"])
        return {
            "platform": sys.platform,
            "arch": _platform.machine() or "unknown",
            "launchers": [self.launcher.name] if launcher_ok else [],
            "capabilities": {**have, self.launcher.name: launcher_ok, "agent-spine": self.spine_version is not None},
            "repos": sorted(set(self.cfg.repos) | set(created)),
            "info": {"ganglion": __version__, "agent_spine": self.spine_version or "absent"},
        }

    def register(self) -> dict[str, Any]:
        info = self.info()
        resp = self.client.register(info)
        self.server_settings = resp.get("settings") or self.server_settings
        self._advertised = info
        self.registered = True
        return resp

    @property
    def heartbeat_interval(self) -> float:
        return float(self.cfg.heartbeat_interval or self.server_settings.get("heartbeat_interval") or 30.0)

    def record_runtime(self) -> None:
        """Let the Stop hook find a Python >= 3.11 (macOS hook environments often have 3.9)."""
        from ..common import atomic_write_text

        atomic_write_text(self.cfg.state_dir / PYTHON_FILE, sys.executable + "\n")

    # ------------------------------------------------------------------ startup reconciliation
    def start(self) -> None:
        """Register and reconcile local processes with the control plane's view."""
        self.record_runtime()
        resp = self.register()
        self.reconcile(resp.get("active_attempts") or [])

    def reconcile(self, owned: list[dict[str, Any]]) -> None:
        remote = {o["attempt"]["id"]: o for o in owned}
        live = self.launcher.discover()
        live_by_attempt = {env["GANGLION_ATTEMPT_ID"]: (h, env) for h, env in live.items() if env.get("GANGLION_ATTEMPT_ID")}
        went_stale = []
        with self.state.locked() as st:
            for att_id, rec in st["attempts"].items():
                if rec["status"] in LOCAL_ACTIVE and att_id not in remote:
                    # The control plane no longer lists it as ours (lease expired while we were
                    # away, or it was reassigned). Its process, if any, is left alone.
                    rec["status"] = L_STALE
                    rec["stale_reason"] = "not owned according to the control plane"
                    went_stale.append(dict(rec))
                # reporting records are resolved by re-sending the report (idempotent / fenced)
            for att_id, o in remote.items():
                if att_id in st["attempts"]:
                    continue
                job, att = o["job"], o["attempt"]
                rec = self._new_record(job, att)
                if att_id in live_by_attempt:
                    handle, env = live_by_attempt[att_id]
                    rec.update(
                        status=L_RUNNING, handle=handle, adopted=True,
                        workspace=env.get("GANGLION_WORKSPACE"), mode=env.get("GANGLION_WS_MODE"),
                        launched_at=float(env.get("GANGLION_LAUNCHED_AT") or now()),
                        result_file=env.get("GANGLION_RESULT_FILE"),
                    )
                    local_event(self.cfg.state_dir, "adopted", f"live session {handle}", job=job["name"], attempt=att_id)
                else:
                    local_event(self.cfg.state_dir, "recovered", "owned but not running here; will prepare and launch",
                                job=job["name"], attempt=att_id)
                st["attempts"][att_id] = rec
            known = set(st["attempts"])
        for handle, env in live.items():
            att = env.get("GANGLION_ATTEMPT_ID")
            if att and att not in known:
                local_event(self.cfg.state_dir, "orphan_session", f"{handle} belongs to unknown attempt {att}; left alone")
        for rec in went_stale:
            self._stale(rec, "not owned according to the control plane", update=False)

    # ------------------------------------------------------------------ main loop
    def tick(self) -> None:
        if self.job_source is not None:
            try:
                self.job_source()
            except Exception:
                log("job source failed:\n" + traceback.format_exc())
        try:
            if not self.registered or self.info() != self._advertised:
                self.register()
            else:
                self.client.worker_heartbeat()
        except NotFound:
            self.registered = False
        except Unavailable as e:
            log(f"control plane unavailable: {e}")
        for att_id, rec in self._open_records().items():
            try:
                self.step(att_id)
            except Exception:
                log(f"{att_id}: step failed:\n" + traceback.format_exc())
        if self.registered:
            self.claim_until_full()

    def run_forever(self, stop: threading.Event, reload: Callable[[], Config] | None = None) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                self.start()
                break
            except (Unavailable, NotFound) as e:
                log(f"register failed ({e}); retrying in {backoff:.0f}s")
                stop.wait(backoff)
                backoff = min(backoff * 2, 60)
        wake = self.cfg.state_dir / WAKE_FILE
        while not stop.is_set():
            if reload is not None:
                try:
                    self.cfg = reload()
                    self.launcher = make_launcher(self.cfg)
                except Exception as e:
                    log(f"config reload failed, keeping the previous one: {e!r}")
            try:
                self.tick()
            except Exception:
                log("tick failed:\n" + traceback.format_exc())
            mark = _mtime(wake)
            deadline = now() + min(self.cfg.poll_interval, self.heartbeat_interval)
            while not stop.is_set() and now() < deadline and _mtime(wake) == mark:
                stop.wait(min(0.5, max(0.0, deadline - now())))
        log("worker stopping; running sessions are left alone")

    def _open_records(self) -> dict[str, dict[str, Any]]:
        return {k: v for k, v in self.state.snapshot()["attempts"].items() if v["status"] in LOCAL_OPEN}

    # ------------------------------------------------------------------ claim
    def capacity(self) -> int:
        active = sum(1 for r in self._open_records().values() if r["status"] in LOCAL_ACTIVE)
        return max(0, self.cfg.max_concurrent - active)

    def busy_shared_repos(self) -> list[str]:
        """Repo names whose shared checkout must not receive another attempt right now: an
        attempt of ours is working in it, or a stale one's process may still be (its
        DELIVERY.md would otherwise be mistaken for the next attempt's result)."""
        busy = set()
        for r in self.state.snapshot()["attempts"].values():
            if not r.get("repo_name") or (r.get("job") or {}).get("repository", {}).get("url"):
                continue
            if r.get("mode") not in (repo_mod.MODE_SHARED, None):
                continue
            if r["status"] in LOCAL_ACTIVE:
                busy.add(r["repo_name"])
            elif r["status"] == L_STALE and r.get("handle") and self.launcher.is_alive(r["handle"]) is not False:
                busy.add(r["repo_name"])
        return sorted(busy)

    def claim_until_full(self) -> int:
        claimed = 0
        while self.registered and self.capacity() > 0:
            with self.state.locked() as st:
                # persisted before sending: a lost response is retried with the same id
                request_id = st["pending_claim"] or new_id("clm")
                st["pending_claim"] = request_id
            try:
                resp = self.client.claim(request_id, self.busy_shared_repos())
            except Unavailable as e:
                log(f"claim: control plane unavailable ({e}); will retry with the same request id")
                return claimed
            except NotFound:
                self.registered = False
                return claimed
            with self.state.locked() as st:
                st["pending_claim"] = None
            if resp is None:
                return claimed
            job, att = resp["job"], resp["attempt"]
            with self.state.locked() as st:
                if att["id"] in st["attempts"]:
                    continue
                rec = self._new_record(job, att)
                if att["status"] != "active":
                    rec["status"] = L_DROPPED  # replay of a claim whose attempt has since ended
                st["attempts"][att["id"]] = rec
            if att["status"] != "active":
                local_event(self.cfg.state_dir, "claim_replay_dropped", f"attempt is {att['status']}", attempt=att["id"])
                continue
            local_event(self.cfg.state_dir, "claimed", f"epoch {att['epoch']}", job=job["name"], attempt=att["id"])
            claimed += 1
            self.step(att["id"])
            if self.state.snapshot()["attempts"][att["id"]]["status"] != L_RUNNING:
                break  # it failed or was released: don't burn the retry budget in one tick
        return claimed

    def _new_record(self, job: dict[str, Any], att: dict[str, Any]) -> dict[str, Any]:
        repo = job.get("repository") or {}
        return {
            "attempt_id": att["id"], "job_id": job["id"], "epoch": att["epoch"], "name": job["name"],
            "repo_name": repo.get("name"), "status": L_PREPARING, "created": now(), "last_heartbeat": 0.0,
            "job": {k: job.get(k) for k in ("id", "name", "repository", "target", "goal", "timeout_seconds")},
        }

    # ------------------------------------------------------------------ per-attempt state machine
    def step(self, att_id: str) -> None:
        rec = self.state.snapshot()["attempts"].get(att_id)
        if rec is None:
            return
        if rec["status"] == L_REPORTING:
            # Keep the lease while the report is pending (a slow report must not cost the work),
            # but let the report itself decide ownership: if our outcome was already committed
            # and only the reply was lost, the heartbeat is refused while the report replays.
            try:
                self.client.heartbeat(rec["attempt_id"], rec["epoch"])
            except (StaleAttempt, NotFound, Unavailable):
                pass
            self._flush(rec)
        elif rec["status"] in (L_PREPARING, L_LAUNCHING):
            self._launch(rec)
        elif rec["status"] == L_RUNNING:
            self._observe(rec)

    def _launch(self, rec: dict[str, Any]) -> None:
        att_id, job = rec["attempt_id"], rec["job"]
        cfg = self.cfg
        # Only start work we still own. If we cannot tell right now, wait.
        if self._heartbeat(rec, force=True) is not True:
            return
        try:
            with self.state.locked() as st:
                local_repos = dict(st["repos"])
            tag = repo_mod.attempt_tag(job, att_id)
            ws = repo_mod.prepare_workspace(cfg, local_repos, job, tag)
            if job["target"] == "new":
                with self.state.locked() as st:
                    st["repos"][ws.repo_name] = str(ws.path)
            rec = self.state.update_attempt(att_id, workspace=str(ws.path), mode=ws.mode, branch=ws.branch)
            sha = repo_mod.deliver_requirement(ws.path, cfg.req_path, job["name"], job.get("goal") or "")
            result_file = cfg.state_dir / "results" / f"{att_id}.json"
            result_file.parent.mkdir(parents=True, exist_ok=True)
            launched_at = rec.get("launched_at") or now()
            spec = LaunchSpec(
                handle=f"{SESSION_PREFIX}{tag}",
                workspace=ws.path,
                prompt=self.adapter.prompt(job["name"]),
                attempt_id=att_id,
                env={
                    **self.adapter.env(job_id=job["id"], attempt_id=att_id, epoch=rec["epoch"], name=job["name"], result_file=result_file),
                    "GANGLION_STATE": str(cfg.state_dir.absolute()),
                    "GANGLION_WORKSPACE": str(ws.path),
                    "GANGLION_WS_MODE": ws.mode,
                    "GANGLION_LAUNCHED_AT": repr(launched_at),
                },
            )
            self.launcher.prepare(spec)
        except repo_mod.RepoNotMapped as e:
            # our registration is out of date; give the job back without spending its budget
            self.registered = False
            self._report(rec, "release", reason=str(e))
            return
        except (repo_mod.RepoConfigError, RuntimeError) as e:  # config problem / trust pre-write gave up
            self._report(rec, "fail", reason="prepare_failed", message=str(e), retryable=False)
            return
        except (CmdError, OSError) as e:  # git/gh/network/disk: may succeed on another attempt
            self._report(rec, "fail", reason="prepare_failed", message=str(e), retryable=True)
            return
        except Exception as e:  # a bug: surface it once instead of retrying silently every tick
            log(f"{att_id}: prepare crashed:\n" + traceback.format_exc())
            self._report(rec, "fail", reason="prepare_failed", message=f"internal error: {e!r}", retryable=False)
            return

        if self._heartbeat(rec, force=True) is not True:  # re-confirm right before starting a process
            return
        self.state.update_attempt(att_id, status=L_LAUNCHING, handle=spec.handle, launched_at=launched_at,
                                  req_commit=sha, result_file=str(result_file))
        try:
            started = self.launcher.start(spec)
        except CmdError as e:
            self._report(rec, "fail", reason="launch_failed", message=str(e), retryable=True)
            log_metric("launcher_failures", job=job["id"], reason="launch_failed")
            return
        kinds = self.launcher.after_start(spec) if started else []
        with self.state.locked() as st:
            r = st["attempts"][att_id]
            if r["status"] == L_LAUNCHING:  # never move a record backwards
                r["status"] = L_RUNNING
        for k in kinds:
            local_event(cfg.state_dir, k, spec.handle, job=job["name"], attempt=att_id)
        local_event(cfg.state_dir, "launched" if started else "session_exists", spec.handle, job=job["name"], attempt=att_id)
        if started:
            safe_notify(self.notifier, f"🚀 已启动：{job['name']}",
                        f"{rec.get('repo_name')} @ {ws.path}\n{self.launcher.attach_hint(spec.handle)}", ["rocket"])

    def _observe(self, rec: dict[str, Any]) -> None:
        ws = Path(rec["workspace"])
        result_file = Path(rec.get("result_file") or self.cfg.state_dir / "results" / f"{rec['attempt_id']}.json")
        launched_at = float(rec.get("launched_at") or 0)
        res = self.adapter.collect(ws, result_file, launched_at)
        if res is not None:
            self._finish(rec, res)
            return
        alive = self.launcher.is_alive(rec["handle"])
        if alive is False:
            res = self.adapter.collect(ws, result_file, launched_at)  # finished just before exiting?
            if res is not None:
                self._finish(rec, res)
                return
            log_metric("launcher_failures", job=rec["job_id"], reason="launcher_exited")
            self._report(rec, "fail", reason="launcher_exited", message=f"{rec['handle']} exited without a result",
                         retryable=self.cfg.retry_on_launcher_exit)
            return
        timeout = (rec.get("job") or {}).get("timeout_seconds")
        if timeout and now() - launched_at > float(timeout):
            self.launcher.stop(rec["handle"])
            self._report(rec, "fail", reason="timeout", message=f"no result after {timeout}s", retryable=False)
            return
        self._heartbeat(rec)  # alive or unknown: keep the lease

    def _finish(self, rec: dict[str, Any], res: dict[str, Any]) -> None:
        if self.cfg.publish == "branch":
            try:
                ws = repo_mod.Workspace(Path(rec["workspace"]), rec.get("mode") or "", rec.get("repo_name") or "", rec.get("branch"))
                branch, remote = repo_mod.publish_branch(ws)
                if branch:
                    res["branch"] = branch
                    res.setdefault("result_uri", f"{remote}#{branch}")
            except CmdError as e:
                log(f"{rec['attempt_id']}: publish failed (result still reported): {e}")
        elif rec.get("branch"):
            res.setdefault("branch", rec["branch"])
        if self.adapter.is_failure(res):
            self._report(rec, "fail", reason="spine_failed", message=(res.get("summary") or "")[:4000], retryable=False)
        else:
            self._report(rec, "complete", result=res)

    # ------------------------------------------------------------------ talking to the control plane
    def _heartbeat(self, rec: dict[str, Any], force: bool = False) -> bool | None:
        """True: still ours. False: stale (handled). None: cannot tell right now."""
        if not force and now() - float(rec.get("last_heartbeat") or 0) < self.heartbeat_interval:
            return True
        try:
            out = self.client.heartbeat(rec["attempt_id"], rec["epoch"])
        except (StaleAttempt, NotFound) as e:
            self._stale(rec, getattr(e, "extra", {}).get("reason") or e.detail)
            return False
        except Unavailable as e:
            log(f"{rec['attempt_id']}: heartbeat not delivered ({e}); keeping local work running")
            return None
        self.state.update_attempt(rec["attempt_id"], last_heartbeat=now(), lease_until=out.get("lease_until"))
        return True

    def _report(self, rec: dict[str, Any], kind: str, **payload: Any) -> None:
        """Persist the report (with a stable request id) before sending it."""
        with self.state.locked() as st:
            r = st["attempts"][rec["attempt_id"]]
            if r["status"] in (L_DONE, L_STALE, L_DROPPED):
                return
            if r["status"] != L_REPORTING:
                r["status"] = L_REPORTING
                r["report"] = {"kind": kind, "payload": payload, "request_id": new_id("rpt"), "attempts": 0}
            rec = dict(r)
        self._flush(rec)

    def _flush(self, rec: dict[str, Any]) -> None:
        att_id = rec["attempt_id"]
        with self.state.locked() as st:
            r = st["attempts"][att_id]
            r["report"]["attempts"] += 1
            rpt = dict(r["report"])
        kind, p, rid, n = rpt["kind"], rpt["payload"], rpt["request_id"], rpt["attempts"]
        try:
            if kind == "complete":
                self.client.complete(att_id, rec["epoch"], rid, p["result"], report_attempts=n)
            elif kind == "fail":
                self.client.fail(att_id, rec["epoch"], rid, p["reason"], p.get("message", ""), p.get("retryable", False),
                                 report_attempts=n)
            else:
                self.client.release(att_id, rec["epoch"], rid, p.get("reason", ""))
        except Unavailable as e:
            log(f"{att_id}: {kind} report not delivered ({e}); will retry with request id {rid}")
            log_metric("result_report_retries", attempt=att_id, kind=kind)
            return
        except StaleAttempt as e:
            # Ownership moved on. We must not claim this outcome; keep it locally for humans.
            self._stale(rec, e.extra.get("reason") or e.detail)
            return
        except (Conflict, NotFound) as e:
            self.state.update_attempt(att_id, status=L_DONE, outcome=f"{kind}: {e.code}: {e.detail}")
            local_event(self.cfg.state_dir, "report_conflict", e.detail, job=rec.get("name"), attempt=att_id)
            return
        except InvalidRequest as e:
            # The control plane will never accept this payload; resending it forever would
            # strand the attempt. Report the rejection itself as the outcome instead.
            local_event(self.cfg.state_dir, "report_rejected", e.detail, job=rec.get("name"), attempt=att_id)
            if kind == "fail" and p.get("reason") == "report_rejected":
                self.state.update_attempt(att_id, status=L_DONE, outcome=f"report rejected: {e.detail}")
                return
            with self.state.locked() as st:
                st["attempts"][att_id]["report"] = {
                    "kind": "fail", "request_id": new_id("rpt"), "attempts": 0,
                    "payload": {"reason": "report_rejected", "message": f"{kind} rejected: {e.detail}"[:3000], "retryable": False},
                }
            self._flush(self.state.snapshot()["attempts"][att_id])
            return
        except ControlError as e:  # unexpected (auth, protocol): keep the report and retry later
            log(f"{att_id}: {kind} report failed ({e.code}: {e.detail}); will retry")
            return
        self.state.update_attempt(att_id, status=L_DONE, outcome=kind, reported_at=now())
        local_event(self.cfg.state_dir, f"reported_{kind}", p.get("reason", "") if kind != "complete" else "",
                    job=rec.get("name"), attempt=att_id)

    def _stale(self, rec: dict[str, Any], reason: str, update: bool = True) -> None:
        """The control plane says we are not the owner. Stop acting; do not destroy work."""
        if update:
            with self.state.locked() as st:
                r = st["attempts"].get(rec["attempt_id"])
                if r is None or r["status"] == L_STALE:
                    return
                r["status"] = L_STALE
                r["stale_reason"] = reason
        handle = rec.get("handle")
        action = "left running for inspection"
        if handle and self.cfg.stale_policy == "stop":
            self.launcher.stop(handle)
            action = "stopped (stale_policy = stop)"
        local_event(self.cfg.state_dir, "stale", f"{reason}; session {handle or '-'} {action}",
                    job=rec.get("name"), attempt=rec["attempt_id"])
        safe_notify(
            self.notifier,
            f"⚠️ 执行权已转移：{rec.get('name')}",
            f"attempt {rec['attempt_id']} (epoch {rec.get('epoch')}) is no longer authoritative ({reason}). "
            f"Its result will not be reported. Session {handle or '-'} {action}.",
            ["warning"],
        )


def _mtime(p: Path) -> float:
    try:
        return p.stat().st_mtime
    except OSError:
        return 0.0
