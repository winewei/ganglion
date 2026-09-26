"""Ganglion control plane: jobs, attempts, leases, fencing, workers, events.

Pure Python, no web framework: the FastAPI app (``api.py``) and the embedded single-machine
mode (``client.LocalClient``) both call this class, so there is one implementation of the
ownership rules.

The invariant this module exists to keep:

    A job may have many attempts, but at most one current authoritative attempt. A stale
    attempt can never mutate authoritative job state.

How it is kept:

* ``jobs.epoch`` is a fencing token. Every claim bumps it and creates a new attempt row
  carrying the new epoch. Old attempt rows are never reused or overwritten.
* ``jobs.current_attempt_id`` / ``jobs.lease_until`` name the owner and its lease.
* Every authoritative change (claim, heartbeat, complete, fail, release, expiry) is ONE
  compare-and-set on the job row, conditioned on the (status, epoch, current attempt, lease)
  the caller believes in. Whoever's compare-and-set matches wins; everyone else gets
  ``StaleAttempt``. Nothing is decided by a read followed by an unconditional write.
* A lease that has passed ``lease_until`` is dead: the owner can no longer heartbeat or
  report, and the job is claimable. The one exception is control-plane downtime: on startup
  running leases are extended by one TTL, because workers could not heartbeat while the
  server was down (``Settings.restart_grace``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable

from ..common import NAME_MAX, NAME_RE, log, log_metric, new_id, now
from ..notify import Notifier, NullNotifier, safe_notify
from .models import (
    ATT_ACTIVE,
    ATT_COMPLETED,
    ATT_EXPIRED,
    ATT_FAILED,
    ATT_RELEASED,
    DEFAULT_LAUNCHER,
    JOB_COMPLETED,
    JOB_FAILED,
    JOB_QUEUED,
    JOB_REJECTED,
    JOB_RUNNING,
    JOB_STATUSES,
    SOURCES,
    TARGETS,
    WORKER_OFFLINE,
    WORKER_ONLINE,
    Conflict,
    InvalidRequest,
    NotFound,
    StaleAttempt,
)
from .store import Increment, IntegrityError, Store, Tx

WORKER_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
REPO_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")


@dataclass
class Settings:
    lease_ttl: float = 300.0             # seconds an attempt stays authoritative without a heartbeat
    heartbeat_interval: float = 30.0     # advice to workers (must be well below lease_ttl)
    max_depth: int = 2                   # recursion protection for source=agent jobs
    max_attempts: int = 3                # expiries + retryable failures before a job fails
    worker_offline_after: float = 120.0  # observability only; ownership is decided by leases
    restart_grace: bool = True
    max_goal_bytes: int = 256 * 1024
    max_summary_chars: int = 4000

    def public(self) -> dict[str, Any]:
        return {
            "lease_ttl": self.lease_ttl,
            "heartbeat_interval": self.heartbeat_interval,
            "max_depth": self.max_depth,
            "max_attempts": self.max_attempts,
        }


class ControlPlane:
    def __init__(
        self,
        store: Store,
        settings: Settings | None = None,
        notifier: Notifier | None = None,
        clock: Callable[[], float] = now,
    ):
        self.store = store
        self.settings = settings or Settings()
        self.notifier = notifier or NullNotifier()
        self.clock = clock

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        self.store.migrate()
        extended = 0
        t0 = self.clock()
        with self.store.tx() as t:
            if self.settings.restart_grace:
                for job in t.all("SELECT * FROM jobs WHERE status = 'running'"):
                    new_lease = max(job["lease_until"] or 0, t0 + self.settings.lease_ttl)
                    if t.cas("jobs", job["id"], {"status": JOB_RUNNING, "epoch": job["epoch"]}, {"lease_until": new_lease}):
                        t.cas("attempts", job["current_attempt_id"], {"status": ATT_ACTIVE}, {"lease_until": new_lease})
                        extended += 1
            t.add_event(t0, "control.started", data={"leases_extended": extended})
        log(f"control plane started ({extended} running lease(s) extended by restart grace)")

    def _notify(self, notes: list[tuple[str, str, list[str]]]) -> None:
        for title, message, tags in notes:
            safe_notify(self.notifier, title, message, tags)

    # ------------------------------------------------------------------ jobs
    def create_job(self, spec: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """Admit a job. Idempotent on ``key``: the same key always returns the same job.

        Admission failures that are properties of the request (recursion depth, missing
        parent, invalid name) are *stored* as ``rejected`` jobs so a repeatedly-discovered
        request is answered consistently and notified once. Returns ``(job, created)``.
        """
        s = self.settings
        key = spec.get("key") or None
        repo = dict(spec.get("repository") or {})
        target = spec.get("target") or "existing"
        source = spec.get("source") or "human"
        launcher = spec.get("launcher") or DEFAULT_LAUNCHER
        goal = spec.get("goal") or ""
        if target not in TARGETS:
            raise InvalidRequest(f"target must be one of {TARGETS}, got {target!r}")
        if source not in SOURCES:
            raise InvalidRequest(f"source must be one of {SOURCES}, got {source!r}")
        if not repo.get("name"):
            raise InvalidRequest("repository.name is required")
        if len(goal.encode("utf-8")) > s.max_goal_bytes:
            raise InvalidRequest(f"goal exceeds {s.max_goal_bytes} bytes; link large inputs instead")
        max_attempts = int(spec.get("max_attempts") or s.max_attempts)

        for _ in range(3):
            notes: list[tuple[str, str, list[str]]] = []
            t0 = self.clock()
            with self.store.tx() as t:
                if key:
                    existing = t.job_by_key(key)
                    if existing:
                        return self._job_view(t, existing), False
                job_id = new_id("job")
                name = spec.get("name") or (key.split(":", 1)[-1] if key else job_id)
                status, error, waiting = JOB_QUEUED, None, False
                depth = 0
                parent_id = spec.get("parent_job_id") or None

                if not NAME_RE.match(name) or len(name) > NAME_MAX:
                    status, error = JOB_REJECTED, (
                        f"job name {name!r} must not contain whitespace, '.', ':', quotes, '/', '$' "
                        "or start with '-' (it becomes a tmux session name)"
                    )
                if status == JOB_QUEUED and source == "agent":
                    if parent_id:
                        parent = t.get("jobs", parent_id)
                        if parent is None:
                            status, error = JOB_REJECTED, f"parent job {parent_id!r} not found"
                        else:
                            depth = parent["depth"] + 1
                            if parent["status"] == JOB_REJECTED:
                                status, error = JOB_REJECTED, f"parent job {parent['name']!r} was rejected"
                    else:
                        depth = 1  # agent job with no declared parent: hangs directly under a human job
                    if status == JOB_QUEUED and depth > s.max_depth:
                        status, error = JOB_REJECTED, f"depth {depth} exceeds max_depth={s.max_depth}"
                elif source == "human" and parent_id and t.get("jobs", parent_id) is None:
                    status, error = JOB_REJECTED, f"parent job {parent_id!r} not found"

                if status == JOB_QUEUED and not repo.get("url"):
                    serving = [w["id"] for w in t.workers_serving_repo(repo["name"])]
                    if target == "existing" and not serving:
                        status, error = JOB_FAILED, (
                            f"no registered worker maps repository {repo['name']!r} "
                            "(add it to a worker's [repos], then `ganglion job retry`)"
                        )
                    elif target == "new":
                        if not repo.get("owner"):
                            status, error = JOB_FAILED, "target: new needs repository.owner"
                        elif not REPO_NAME_RE.match(repo["name"]):
                            status, error = JOB_REJECTED, f"invalid repository name {repo['name']!r}"
                    elif target == "manual" and not serving:
                        waiting = True

                row = {
                    "id": job_id,
                    "key": key,
                    "name": name,
                    "status": status,
                    "target": target,
                    "repository": repo,
                    "goal": goal,
                    "launcher": launcher,
                    "requirements": dict(spec.get("requirements") or {}),
                    "source": source,
                    "parent_job_id": parent_id,
                    "depth": depth,
                    "max_attempts": max_attempts,
                    "timeout_seconds": spec.get("timeout_seconds"),
                    "error": error,
                    "created_at": t0,
                    "updated_at": t0,
                    "finished_at": t0 if status in (JOB_REJECTED, JOB_FAILED) else None,
                }
                if not t.insert("jobs", row, on_conflict_ignore="key" if key else None):
                    continue  # lost a race on the same key: the next round returns the winner
                t.add_event(t0, "job.created", job_id, data={"key": key, "status": status, "source": source, "depth": depth})
                if status == JOB_REJECTED:
                    t.add_event(t0, "job.rejected", job_id, data={"reason": error})
                    notes.append((f"🚫 需求被拒绝：{name}", error, ["no_entry"]))
                elif status == JOB_FAILED:
                    t.add_event(t0, "job.failed", job_id, data={"reason": error})
                    notes.append((f"❌ Ganglion 出错：{name}", error, ["x"]))
                elif waiting:
                    t.add_event(t0, "job.waiting", job_id, data={"repository": repo["name"]})
                    notes.append(
                        (
                            f"⏳ 等待工程映射：{name}",
                            f"请在某个 worker 配置的 [repos] 中加入 {repo['name']} = \"<本地路径>\"，Ganglion 会自动继续。",
                            ["hourglass"],
                        )
                    )
                view = self._job_view(t, row)
            log_metric("jobs_created", status=status)
            self._notify(notes)
            return view, True
        raise Conflict("could not create job (key contention)")  # pragma: no cover

    @staticmethod
    def _resolve(t: Tx, ident: str) -> dict[str, Any] | None:
        """Job id, else idempotency key, else the most recent job with that name."""
        return t.get("jobs", ident) or t.job_by_key(ident) or t.latest_job_by_name(ident)

    def get_job(self, job_id: str) -> dict[str, Any]:
        with self.store.tx() as t:
            job = self._resolve(t, job_id)
            if job is None:
                raise NotFound(f"job {job_id!r} not found")
            return self._job_view(t, job, attempts=True)

    def list_jobs(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if status and status not in JOB_STATUSES:
            raise InvalidRequest(f"unknown status {status!r}")
        with self.store.tx() as t:
            return [self._job_view(t, j) for j in t.list_jobs(status, limit)]

    def retry_job(self, job_id: str) -> dict[str, Any]:
        """Manual retry: ``failed`` -> ``queued`` with a fresh attempt budget.

        Rejected jobs cannot be retried (that would bypass recursion protection). History is
        kept: the next claim creates a new attempt with a higher epoch.
        """
        t0 = self.clock()
        with self.store.tx() as t:
            job = self._resolve(t, job_id)
            if job is None:
                raise NotFound(f"job {job_id!r} not found")
            if job["status"] == JOB_REJECTED:
                raise InvalidRequest("rejected jobs cannot be retried (admission/recursion protection); submit a new job")
            if job["status"] != JOB_FAILED:
                raise InvalidRequest(f"only failed jobs can be retried; job is {job['status']}")
            ok = t.cas(
                "jobs",
                job["id"],
                {"status": JOB_FAILED, "epoch": job["epoch"]},
                {"status": JOB_QUEUED, "failures": 0, "error": None, "finished_at": None, "result": None, "updated_at": t0},
            )
            if not ok:
                raise Conflict("job changed concurrently; try again")
            t.add_event(t0, "job.retried", job["id"])
            return self._job_view(t, t.get("jobs", job["id"]), attempts=True)

    # ------------------------------------------------------------------ workers
    def register_worker(self, worker_id: str, info: dict[str, Any]) -> dict[str, Any]:
        """Upsert a worker and return the attempts the control plane believes it owns.

        The worker reconciles its local processes against that list (see worker.runtime).
        """
        if not WORKER_ID_RE.match(worker_id or ""):
            raise InvalidRequest("worker_id must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}")
        t0 = self.clock()
        with self.store.tx() as t:
            prev = t.get("workers", worker_id)
            row = {
                "id": worker_id,
                "status": WORKER_ONLINE,
                "platform": str(info.get("platform") or "unknown"),
                "arch": str(info.get("arch") or "unknown"),
                "launchers": list(info.get("launchers") or []),
                "capabilities": dict(info.get("capabilities") or {}),
                "repos": sorted(set(info.get("repos") or [])),
                "info": {k: v for k, v in (info.get("info") or {}).items()},
                "registered_at": t0,
                "last_seen": t0,
            }
            t.upsert("workers", row, keep=("registered_at",))
            t.add_event(
                t0, "worker.registered", worker_id=worker_id,
                data={"launchers": row["launchers"], "repos": row["repos"], "re_registered": prev is not None},
            )
            owned = [
                {"attempt": a, "job": self._job_view(t, t.get("jobs", a["job_id"]))}
                for a in t.active_attempts_for_worker(worker_id)
            ]
            return {"worker": t.get("workers", worker_id), "active_attempts": owned, "settings": self.settings.public()}

    def worker_heartbeat(self, worker_id: str) -> dict[str, Any]:
        t0 = self.clock()
        with self.store.tx() as t:
            w = t.get("workers", worker_id)
            if w is None:
                raise NotFound(f"worker {worker_id!r} is not registered")
            t.cas("workers", worker_id, {}, {"last_seen": t0, "status": WORKER_ONLINE})
            if w["status"] == WORKER_OFFLINE:
                t.add_event(t0, "worker.online", worker_id=worker_id)
            active = [a["id"] for a in t.active_attempts_for_worker(worker_id)]
        return {"server_time": t0, "active_attempt_ids": active}

    def list_workers(self) -> list[dict[str, Any]]:
        with self.store.tx() as t:
            return t.list_workers()

    # ------------------------------------------------------------------ claim
    def claim(self, worker_id: str, request_id: str | None = None, exclude_repos: list[str] | None = None) -> dict[str, Any] | None:
        """Atomically take the oldest eligible job. Returns ``{job, attempt, replay}`` or None.

        ``request_id`` makes the call idempotent: a retry after a lost response returns the
        attempt created by the first call (whatever its status is now) instead of claiming a
        second job. The worker must check ``attempt.status``.
        """
        for _ in range(3):
            try:
                return self._claim(worker_id, request_id, set(exclude_repos or ()))
            except IntegrityError:
                # Two identical requests raced on claim_request_id (or a (job, epoch) pair);
                # the loser's transaction rolled back entirely. Re-run: it will replay.
                continue
        raise Conflict("claim contention; retry")  # pragma: no cover

    def _claim(self, worker_id: str, request_id: str | None, exclude: set[str]) -> dict[str, Any] | None:
        t0 = self.clock()
        notes: list = []
        result = None
        conflicts = 0
        with self.store.tx() as t:
            if request_id:
                prior = t.attempt_by_claim_request(request_id)
                if prior is not None:
                    if prior["worker_id"] != worker_id:
                        raise InvalidRequest("request_id already used by another worker")
                    return {"job": self._job_view(t, t.get("jobs", prior["job_id"])), "attempt": prior, "replay": True}
            worker = t.get("workers", worker_id)
            if worker is None:
                raise NotFound(f"worker {worker_id!r} is not registered")
            t.cas("workers", worker_id, {}, {"last_seen": t0, "status": WORKER_ONLINE})

            after = None
            while result is None:
                cands = t.claim_candidates(t0, after, 50)
                if not cands:
                    break
                for job in cands:
                    after = (job["created_at"], job["id"])
                    if not self._eligible(job, worker, exclude):
                        continue
                    if job["status"] == JOB_RUNNING:  # lease lapsed: retire the old attempt first
                        if not self._expire(t, job, t0, notes):
                            conflicts += 1
                            t.add_event(t0, "claim.conflict", job["id"], worker_id=worker_id)
                            continue
                        job = t.get("jobs", job["id"])
                        if job["status"] != JOB_QUEUED:
                            continue  # attempts exhausted -> failed
                    epoch = job["epoch"] + 1
                    att_id = new_id("att")
                    lease = t0 + self.settings.lease_ttl
                    won = t.cas(
                        "jobs",
                        job["id"],
                        {"status": JOB_QUEUED, "epoch": job["epoch"]},
                        {
                            "status": JOB_RUNNING, "epoch": epoch, "current_attempt_id": att_id, "lease_until": lease,
                            "attempt_count": Increment(), "updated_at": t0, "error": None,
                        },
                    )
                    if not won:
                        conflicts += 1
                        t.add_event(t0, "claim.conflict", job["id"], worker_id=worker_id)
                        continue
                    attempt = {
                        "id": att_id, "job_id": job["id"], "worker_id": worker_id, "epoch": epoch, "status": ATT_ACTIVE,
                        "claim_request_id": request_id, "lease_until": lease, "started_at": t0, "heartbeat_at": t0,
                    }
                    t.insert("attempts", attempt)
                    t.add_event(t0, "job.claimed", job["id"], att_id, worker_id, epoch,
                                data={"queue_time": t0 - job["created_at"] if epoch == 1 else None})
                    t.add_event(t0, "attempt.started", job["id"], att_id, worker_id, epoch)
                    result = {
                        "job": self._job_view(t, t.get("jobs", job["id"])),
                        "attempt": t.get("attempts", att_id),
                        "replay": False,
                    }
                    break
        if conflicts:
            log_metric("claim_conflicts", conflicts, worker=worker_id)
        if result:
            log_metric("jobs_claimed", worker=worker_id, job=result["job"]["id"], epoch=result["attempt"]["epoch"])
        self._notify(notes)
        return result

    @staticmethod
    def _eligible(job: dict[str, Any], worker: dict[str, Any], exclude: set[str]) -> bool:
        """Basic matching only: launcher, required capabilities, repository reachability."""
        if job["launcher"] not in (worker.get("launchers") or []):
            return False
        caps = worker.get("capabilities") or {}
        for cap, want in ((job.get("requirements") or {}).get("capabilities") or {}).items():
            if want and not caps.get(cap):
                return False
        repo = job.get("repository") or {}
        name = repo.get("name")
        if repo.get("url"):
            return bool(caps.get("git", True))
        if name in exclude:
            return False  # worker already runs an attempt in that shared local checkout
        if job["target"] == "new":
            # a worker that already has it (created by an earlier `new` job) reuses it
            return bool(caps.get("gh")) or name in (worker.get("repos") or [])
        return name in (worker.get("repos") or [])

    # ------------------------------------------------------------------ expiry
    def _expire(self, t: Tx, job: dict[str, Any], t0: float, notes: list) -> bool:
        """Retire a running job's lapsed attempt. One compare-and-set; False if someone beat us."""
        exhausted = job["failures"] + 1 >= job["max_attempts"]
        set_: dict[str, Any] = {
            "status": JOB_FAILED if exhausted else JOB_QUEUED,
            "failures": Increment(),
            "lease_until": None,
            "updated_at": t0,
        }
        if exhausted:
            set_["finished_at"] = t0
            set_["error"] = f"lease expired; attempts exhausted ({job['failures'] + 1}/{job['max_attempts']})"
        ok = t.cas(
            "jobs",
            job["id"],
            {"status": JOB_RUNNING, "epoch": job["epoch"], "current_attempt_id": job["current_attempt_id"], "lease_until__lt": t0},
            set_,
        )
        if not ok:
            return False
        att = t.get("attempts", job["current_attempt_id"])
        t.cas("attempts", job["current_attempt_id"], {"status": ATT_ACTIVE}, {"status": ATT_EXPIRED, "finished_at": t0, "error": "lease expired"})
        t.add_event(t0, "attempt.expired", job["id"], job["current_attempt_id"], att and att["worker_id"], job["epoch"])
        if exhausted:
            t.add_event(t0, "job.failed", job["id"], data={"reason": set_["error"]})
            notes.append((f"❌ Ganglion 出错：{job['name']}", set_["error"], ["x"]))
        else:
            t.add_event(t0, "job.requeued", job["id"], data={"reason": "lease_expired"})
        log_metric("lease_expirations", job=job["id"], epoch=job["epoch"])
        return True

    def sweep(self) -> dict[str, int]:
        """Housekeeping: expire lapsed leases and mark silent workers offline.

        Not needed for correctness (claims and fenced calls expire lazily), but it makes
        state and events timely for observers. Safe to run from any number of places.
        """
        t0 = self.clock()
        notes: list = []
        expired = offline = 0
        with self.store.tx() as t:
            for job in t.expired_running(t0):
                expired += self._expire(t, job, t0, notes)
            for w in t.stale_workers(t0 - self.settings.worker_offline_after):
                if t.cas("workers", w["id"], {"status": WORKER_ONLINE, "last_seen": w["last_seen"]}, {"status": WORKER_OFFLINE}):
                    t.add_event(t0, "worker.offline", worker_id=w["id"], data={"last_seen": w["last_seen"]})
                    offline += 1
        self._notify(notes)
        return {"expired": expired, "offline": offline}

    # ------------------------------------------------------------------ fenced attempt operations
    def _fence(self, t: Tx, attempt_id: str, worker_id: str, epoch: int, op: str, t0: float, notes: list):
        """Return ``(attempt, job, error)``. ``error`` is None only for the current owner."""
        att = t.get("attempts", attempt_id)
        if att is None:
            return None, None, NotFound(f"attempt {attempt_id!r} not found")
        job = t.get("jobs", att["job_id"])
        err = None
        if att["worker_id"] != worker_id:
            err = StaleAttempt("attempt belongs to another worker", reason="worker_mismatch")
        elif att["epoch"] != epoch:
            err = StaleAttempt("epoch does not match this attempt", reason="epoch_mismatch")
        elif att["status"] != ATT_ACTIVE:
            err = StaleAttempt(f"attempt is {att['status']}", reason=att["status"])
        elif job["current_attempt_id"] != att["id"] or job["epoch"] != epoch or job["status"] != JOB_RUNNING:
            err = StaleAttempt("job is owned by another attempt", reason="not_current", current_epoch=job["epoch"])
        elif (job["lease_until"] or 0) < t0:
            self._expire(t, job, t0, notes)
            err = StaleAttempt("lease expired", reason="lease_expired")
        if err is not None:
            t.add_event(t0, "attempt.rejected", att["job_id"], attempt_id, worker_id, epoch,
                        data={"op": op, "reason": err.extra.get("reason")})
            log_metric("stale_attempt_rejections", op=op, reason=err.extra.get("reason"), worker=worker_id)
        return att, job, err

    def _owner_expect(self, att: dict[str, Any], t0: float) -> dict[str, Any]:
        return {"status": JOB_RUNNING, "current_attempt_id": att["id"], "epoch": att["epoch"], "lease_until__gte": t0}

    def heartbeat(self, attempt_id: str, worker_id: str, epoch: int) -> dict[str, Any]:
        """Renew the lease. Naturally idempotent."""
        t0 = self.clock()
        notes: list = []
        err = None
        with self.store.tx() as t:
            att, job, err = self._fence(t, attempt_id, worker_id, epoch, "heartbeat", t0, notes)
            if err is None:
                lease = t0 + self.settings.lease_ttl
                if t.cas("jobs", job["id"], self._owner_expect(att, t0), {"lease_until": lease, "updated_at": t0}):
                    t.cas("attempts", att["id"], {"status": ATT_ACTIVE}, {"lease_until": lease, "heartbeat_at": t0})
                    t.add_event(t0, "attempt.heartbeat", job["id"], att["id"], worker_id, epoch)
                    out = {"attempt_id": att["id"], "epoch": epoch, "lease_until": lease, "job_status": JOB_RUNNING}
                else:
                    err = StaleAttempt("job is owned by another attempt", reason="not_current")
        self._notify(notes)
        if err is not None:
            raise err
        return out

    def complete(self, attempt_id: str, worker_id: str, epoch: int, request_id: str | None,
                 result: dict[str, Any], report_attempts: int = 1) -> dict[str, Any]:
        """Record the engineering result and finish the job. Idempotent for the same attempt."""
        t0 = self.clock()
        result = self._compact_result(result)
        notes: list = []
        with self.store.tx() as t:
            att = t.get("attempts", attempt_id)
            if att and att["worker_id"] == worker_id and att["epoch"] == epoch:
                if att["status"] == ATT_COMPLETED:
                    return self._attempt_outcome(t, att, replay=True)
                if att["status"] in (ATT_FAILED, ATT_RELEASED):
                    raise Conflict(f"attempt already {att['status']}; cannot complete it")
            att, job, err = self._fence(t, attempt_id, worker_id, epoch, "complete", t0, notes)
            if err is None:
                job_result = {"attempt_id": att["id"], "epoch": epoch, "worker_id": worker_id, "engineering_result": result}
                if t.cas("jobs", job["id"], self._owner_expect(att, t0),
                         {"status": JOB_COMPLETED, "result": job_result, "finished_at": t0, "updated_at": t0,
                          "lease_until": None, "error": None}):
                    t.cas("attempts", att["id"], {"status": ATT_ACTIVE},
                          {"status": ATT_COMPLETED, "finished_at": t0, "final_request_id": request_id, "result": result})
                    duration = t0 - att["started_at"]
                    t.add_event(t0, "attempt.completed", job["id"], att["id"], worker_id, epoch,
                                data={"duration": duration, "report_attempts": report_attempts})
                    t.add_event(t0, "result.received", job["id"], att["id"], worker_id, epoch,
                                data={k: result.get(k) for k in ("run_id", "final_sha", "result_uri", "source")})
                    t.add_event(t0, "job.completed", job["id"], att["id"], worker_id, epoch)
                    repo = (job.get("repository") or {}).get("name", "")
                    notes.append((f"✅ 已交付：{job['name']}（{repo}）", result.get("summary") or "(no summary)", ["white_check_mark"]))
                    out = self._attempt_outcome(t, t.get("attempts", att["id"]), replay=False)
                    log_metric("attempt_duration", duration, job=job["id"], outcome="completed")
                else:
                    err = StaleAttempt("job is owned by another attempt", reason="not_current")
        self._notify(notes)
        if err is not None:
            raise err
        return out

    def fail(self, attempt_id: str, worker_id: str, epoch: int, request_id: str | None, reason: str,
             message: str = "", retryable: bool = False, report_attempts: int = 1) -> dict[str, Any]:
        """Record an attempt failure. Retryable failures requeue the job while budget remains."""
        t0 = self.clock()
        notes: list = []
        with self.store.tx() as t:
            att = t.get("attempts", attempt_id)
            if att and att["worker_id"] == worker_id and att["epoch"] == epoch:
                if att["status"] == ATT_FAILED:
                    return self._attempt_outcome(t, att, replay=True)
                if att["status"] in (ATT_COMPLETED, ATT_RELEASED):
                    raise Conflict(f"attempt already {att['status']}; cannot fail it")
            att, job, err = self._fence(t, attempt_id, worker_id, epoch, "fail", t0, notes)
            if err is None:
                text = f"{reason}: {message}" if message else reason
                requeue = retryable and job["failures"] + 1 < job["max_attempts"]
                set_: dict[str, Any] = {
                    "status": JOB_QUEUED if requeue else JOB_FAILED, "failures": Increment(), "lease_until": None,
                    "updated_at": t0, "error": text[:2000],
                }
                if not requeue:
                    set_["finished_at"] = t0
                if t.cas("jobs", job["id"], self._owner_expect(att, t0), set_):
                    t.cas("attempts", att["id"], {"status": ATT_ACTIVE},
                          {"status": ATT_FAILED, "finished_at": t0, "final_request_id": request_id, "error": text[:2000]})
                    t.add_event(t0, "attempt.failed", job["id"], att["id"], worker_id, epoch,
                                data={"reason": reason, "retryable": retryable, "report_attempts": report_attempts})
                    if requeue:
                        t.add_event(t0, "job.requeued", job["id"], data={"reason": reason})
                    else:
                        t.add_event(t0, "job.failed", job["id"], att["id"], worker_id, epoch, data={"reason": reason})
                        notes.append((f"❌ Ganglion 出错：{job['name']}", text[:2000], ["x"]))
                    out = self._attempt_outcome(t, t.get("attempts", att["id"]), replay=False)
                else:
                    err = StaleAttempt("job is owned by another attempt", reason="not_current")
        self._notify(notes)
        if err is not None:
            raise err
        return out

    def release(self, attempt_id: str, worker_id: str, epoch: int, request_id: str | None, reason: str = "") -> dict[str, Any]:
        """Give the job back without spending its failure budget (e.g. graceful shutdown before launch)."""
        t0 = self.clock()
        notes: list = []
        with self.store.tx() as t:
            att = t.get("attempts", attempt_id)
            if att and att["worker_id"] == worker_id and att["epoch"] == epoch:
                if att["status"] == ATT_RELEASED:
                    return self._attempt_outcome(t, att, replay=True)
                if att["status"] in (ATT_COMPLETED, ATT_FAILED):
                    raise Conflict(f"attempt already {att['status']}; cannot release it")
            att, job, err = self._fence(t, attempt_id, worker_id, epoch, "release", t0, notes)
            if err is None:
                if t.cas("jobs", job["id"], self._owner_expect(att, t0), {"status": JOB_QUEUED, "lease_until": None, "updated_at": t0}):
                    t.cas("attempts", att["id"], {"status": ATT_ACTIVE},
                          {"status": ATT_RELEASED, "finished_at": t0, "final_request_id": request_id, "error": reason or None})
                    t.add_event(t0, "attempt.released", job["id"], att["id"], worker_id, epoch, data={"reason": reason})
                    t.add_event(t0, "job.requeued", job["id"], data={"reason": "released"})
                    out = self._attempt_outcome(t, t.get("attempts", att["id"]), replay=False)
                else:
                    err = StaleAttempt("job is owned by another attempt", reason="not_current")
        self._notify(notes)
        if err is not None:
            raise err
        return out

    # ------------------------------------------------------------------ read models
    def events(self, job_id: str | None = None, after_id: int = 0, limit: int = 200) -> list[dict[str, Any]]:
        with self.store.tx() as t:
            return t.events(job_id, after_id, min(max(limit, 1), 1000))

    def metrics(self) -> dict[str, Any]:
        with self.store.tx() as t:
            ev = t.count_by("events", "type")
            jobs = t.count_by("jobs", "status")
            workers = t.count_by("workers", "status")
            attempts = t.count_by("attempts", "status")
            launcher_failures = sum(
                1 for e in t.all("SELECT data FROM events WHERE type = 'attempt.failed'")
                if (e["data"] or {}).get("reason") in ("launcher_exited", "launch_failed")
            )
            retries = sum(
                max(0, int((e["data"] or {}).get("report_attempts") or 1) - 1)
                for e in t.all("SELECT data FROM events WHERE type IN ('attempt.completed', 'attempt.failed')")
            )
            avg_duration = t.avg(
                "SELECT AVG(finished_at - started_at) AS v FROM attempts WHERE status = 'completed' AND finished_at IS NOT NULL"
            )
            avg_queue = t.avg(
                "SELECT AVG(a.started_at - j.created_at) AS v FROM attempts a JOIN jobs j ON j.id = a.job_id WHERE a.epoch = 1"
            )
        return {
            "jobs_created": ev.get("job.created", 0),
            "jobs_claimed": ev.get("job.claimed", 0),
            "claim_conflicts": ev.get("claim.conflict", 0),
            "active_workers": workers.get(WORKER_ONLINE, 0),
            "active_attempts": attempts.get(ATT_ACTIVE, 0),
            "lease_expirations": ev.get("attempt.expired", 0),
            "stale_attempt_rejections": ev.get("attempt.rejected", 0),
            "launcher_failures": launcher_failures,
            "result_report_retries": retries,
            "attempt_duration_avg_seconds": avg_duration,
            "job_queue_time_avg_seconds": avg_queue,
            "jobs_by_status": jobs,
            "attempts_by_status": attempts,
        }

    # ------------------------------------------------------------------ helpers
    def _compact_result(self, result: dict[str, Any]) -> dict[str, Any]:
        """Keep the stored result small: pointers and a bounded summary, never logs."""
        allowed = ("status", "run_id", "final_sha", "summary", "result_uri", "branch", "source", "spine_version")
        out = {k: result.get(k) for k in allowed if result.get(k) is not None}
        if isinstance(out.get("summary"), str) and len(out["summary"]) > self.settings.max_summary_chars:
            out["summary"] = out["summary"][: self.settings.max_summary_chars] + "\n…(truncated)"
        return out

    def _attempt_outcome(self, t: Tx, att: dict[str, Any], replay: bool) -> dict[str, Any]:
        return {"attempt": att, "job": self._job_view(t, t.get("jobs", att["job_id"])), "replay": replay}

    @staticmethod
    def _job_view(t: Tx, job: dict[str, Any], attempts: bool = False) -> dict[str, Any]:
        v = dict(job)
        if attempts:
            v["attempts"] = t.attempts_for_job(job["id"])
        return v
