"""HTTP API for the control plane (FastAPI). The generated OpenAPI document is the protocol.

    ganglion server openapi > openapi.json      # the committed copy is checked by tests

This module only translates HTTP <-> ``ControlPlane`` calls. All rules live in service.py.

Authentication: static bearer token(s), compared in constant time. The app refuses to start
without a token unless explicitly told to run unauthenticated (see server.py, which also
refuses to do that on a non-loopback address).
"""

from __future__ import annotations

import hmac
import threading
from contextlib import asynccontextmanager
from typing import Any, Literal, Optional

from fastapi import Depends, FastAPI, Header, Query, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, ConfigDict, Field

from .. import __version__
from ..common import log
from .models import ControlError, Unauthorized
from .service import ControlPlane

API_PREFIX = "/v1"

JobStatus = Literal["queued", "running", "completed", "failed", "rejected"]
AttemptStatus = Literal["active", "completed", "failed", "released", "expired"]


# ---------------------------------------------------------------------------- schemas
class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Repository(_M):
    """Global repository identity. Never a worker-local path."""

    name: str = Field(min_length=1, max_length=200, description="Logical name; workers map it to a local checkout via [repos].")
    url: Optional[str] = Field(None, max_length=2000, description="Clone URL. When set, any worker with git can serve the job.")
    ref: Optional[str] = Field(None, max_length=200, description="Branch/ref to check out (clone path only).")
    owner: Optional[str] = Field(None, max_length=200, description="Account/org that owns the repo; required for target=new.")


class Requirements(_M):
    capabilities: dict[str, bool] = Field(default_factory=dict, description="Worker capabilities that must be true.")


class JobCreate(_M):
    key: Optional[str] = Field(None, max_length=300, description="Idempotency key. Same key => same job (e.g. 'inbox:<id>').")
    name: Optional[str] = Field(None, max_length=100, description="Human label; used in tmux session and requirement file names.")
    repository: Repository
    target: Literal["existing", "new", "manual"] = "existing"
    goal: str = Field("", description="Requirement text, delivered verbatim into the repository.")
    launcher: str = Field("claude-code", max_length=64)
    requirements: Requirements = Field(default_factory=Requirements)
    source: Literal["human", "agent"] = "human"
    parent_job_id: Optional[str] = Field(None, max_length=100)
    max_attempts: Optional[int] = Field(None, ge=1, le=20)
    timeout_seconds: Optional[float] = Field(None, gt=0)


class EngineeringResult(_M):
    """Compact pointer-style result. Large logs/artifacts stay outside the control plane."""

    status: Optional[str] = Field(None, max_length=64)
    run_id: Optional[str] = Field(None, max_length=200)
    final_sha: Optional[str] = Field(None, max_length=64)
    summary: Optional[str] = Field(None, max_length=20000)
    result_uri: Optional[str] = Field(None, max_length=2000)
    branch: Optional[str] = Field(None, max_length=300)
    source: Optional[str] = Field(None, max_length=64, description="How the result was obtained, e.g. 'spine-result' or 'delivery-md'.")
    spine_version: Optional[str] = Field(None, max_length=64)


class Attempt(BaseModel):
    id: str
    job_id: str
    worker_id: str
    epoch: int
    status: AttemptStatus
    claim_request_id: Optional[str] = None
    final_request_id: Optional[str] = None
    lease_until: float
    started_at: float
    heartbeat_at: Optional[float] = None
    finished_at: Optional[float] = None
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None


class Job(BaseModel):
    id: str
    key: Optional[str] = None
    name: str
    status: JobStatus
    target: str
    repository: dict[str, Any]
    goal: str
    launcher: str
    requirements: dict[str, Any]
    source: str
    parent_job_id: Optional[str] = None
    depth: int
    max_attempts: int
    timeout_seconds: Optional[float] = None
    attempt_count: int = 0
    failures: int = 0
    epoch: int = 0
    current_attempt_id: Optional[str] = None
    lease_until: Optional[float] = None
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    created_at: float
    updated_at: float
    finished_at: Optional[float] = None
    attempts: Optional[list[Attempt]] = None


class JobCreated(BaseModel):
    job: Job
    created: bool


class WorkerRegister(_M):
    worker_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    platform: str = Field(max_length=32, description="e.g. darwin, linux")
    arch: str = Field(max_length=32, description="e.g. arm64, x86_64")
    launchers: list[str] = Field(default_factory=list)
    capabilities: dict[str, bool] = Field(default_factory=dict)
    repos: list[str] = Field(default_factory=list, description="Repository names this worker maps to local checkouts.")
    info: dict[str, str] = Field(default_factory=dict, description="Versions etc. (ganglion, agent_spine). No secrets.")


class Worker(BaseModel):
    id: str
    status: str
    platform: str
    arch: str
    launchers: list[str]
    capabilities: dict[str, Any]
    repos: list[str]
    info: dict[str, Any]
    registered_at: float
    last_seen: float


class OwnedAttempt(BaseModel):
    attempt: Attempt
    job: Job


class SettingsView(BaseModel):
    lease_ttl: float
    heartbeat_interval: float
    max_depth: int
    max_attempts: int


class Registered(BaseModel):
    worker: Worker
    active_attempts: list[OwnedAttempt] = Field(description="Attempts the control plane believes this worker currently owns.")
    settings: SettingsView


class WorkerHeartbeatResult(BaseModel):
    server_time: float
    active_attempt_ids: list[str]


class ClaimRequest(_M):
    worker_id: str
    request_id: Optional[str] = Field(None, max_length=100, description="Retry with the same id after a lost response.")
    exclude_repos: list[str] = Field(default_factory=list, description="Repo names whose shared local checkout is busy on this worker.")


class Claimed(BaseModel):
    job: Optional[Job] = None
    attempt: Optional[Attempt] = None
    replay: bool = False


class Fence(_M):
    worker_id: str
    epoch: int = Field(ge=1)


class HeartbeatResult(BaseModel):
    attempt_id: str
    epoch: int
    lease_until: float
    job_status: str


class CompleteRequest(Fence):
    request_id: Optional[str] = Field(None, max_length=100)
    result: EngineeringResult
    report_attempts: int = Field(1, ge=1)


class FailRequest(Fence):
    request_id: Optional[str] = Field(None, max_length=100)
    reason: str = Field(max_length=64, description="Machine-readable, e.g. launcher_exited, prepare_failed, timeout.")
    message: str = Field("", max_length=4000)
    retryable: bool = False
    report_attempts: int = Field(1, ge=1)


class ReleaseRequest(Fence):
    request_id: Optional[str] = Field(None, max_length=100)
    reason: str = Field("", max_length=400)


class AttemptOutcome(BaseModel):
    attempt: Attempt
    job: Job
    replay: bool = Field(description="True when this response replays an already-recorded outcome.")


class Event(BaseModel):
    id: int
    ts: float
    type: str
    job_id: Optional[str] = None
    attempt_id: Optional[str] = None
    worker_id: Optional[str] = None
    epoch: Optional[int] = None
    data: dict[str, Any]


class ErrorBody(BaseModel):
    error: Literal["not_found", "invalid", "stale_attempt", "conflict", "unavailable", "unauthorized"]
    detail: str
    reason: Optional[str] = Field(None, description="For stale_attempt: why the caller is not the owner.")
    current_epoch: Optional[int] = None


ERR = {s: {"model": ErrorBody} for s in (401, 404, 409, 422, 503)}


# ---------------------------------------------------------------------------- app
def create_app(
    service: ControlPlane,
    tokens: list[str],
    allow_unauthenticated: bool = False,
    sweep_interval: float = 5.0,
) -> FastAPI:
    tokens = [t for t in tokens if t]
    if not tokens and not allow_unauthenticated:
        raise RuntimeError("refusing to start without an API token (set [control] token_file / GANGLION_API_TOKEN)")
    stop = threading.Event()

    def _sweeper():
        while not stop.wait(sweep_interval):
            try:
                service.sweep()
            except Exception as e:  # transient DB issues must not kill the loop
                log(f"sweep failed: {e!r}")

    @asynccontextmanager
    async def lifespan(_app):
        service.start()
        th = threading.Thread(target=_sweeper, name="ganglion-sweeper", daemon=True)
        if sweep_interval > 0:
            th.start()
        try:
            yield
        finally:
            stop.set()

    app = FastAPI(
        title="Ganglion Control Plane",
        version=__version__,
        description=(
            "Job intake, worker registration, atomic claim, lease + epoch fencing, results and events. "
            "Every attempt mutation carries (attempt_id, worker_id, epoch); the server rejects any caller "
            "that is not the job's current, unexpired owner with 409 stale_attempt."
        ),
        lifespan=lifespan,
    )

    def auth(authorization: Optional[str] = Header(None)) -> None:
        if not tokens:
            return
        given = (authorization or "").removeprefix("Bearer ").strip().encode()
        # compare against every token so timing does not reveal which one matched
        ok = False
        for tok in tokens:
            ok |= hmac.compare_digest(given, tok.encode())
        if not ok:
            raise Unauthorized("missing or invalid bearer token")

    @app.exception_handler(ControlError)
    async def _control_error(_req: Request, e: ControlError):
        return JSONResponse(status_code=e.http_status, content=e.body())

    A = [Depends(auth)]

    @app.get("/healthz", include_in_schema=False)
    def healthz():
        return {"ok": True}

    @app.post(f"{API_PREFIX}/jobs", response_model=JobCreated, status_code=201, dependencies=A, responses=ERR)
    def create_job(body: JobCreate, response: Response):
        job, created = service.create_job(body.model_dump())
        if not created:
            response.status_code = 200
        return {"job": job, "created": created}

    @app.get(f"{API_PREFIX}/jobs", response_model=list[Job], dependencies=A, responses=ERR)
    def list_jobs(status: Optional[JobStatus] = None, limit: int = Query(100, ge=1, le=1000)):
        return service.list_jobs(status, limit)

    @app.get(f"{API_PREFIX}/jobs/{{job_id}}", response_model=Job, dependencies=A, responses=ERR,
             description="Look up by job id, idempotency key, or name (most recent). Includes the full attempt history.")
    def get_job(job_id: str):
        return service.get_job(job_id)

    @app.post(f"{API_PREFIX}/jobs/{{job_id}}/retry", response_model=Job, dependencies=A, responses=ERR)
    def retry_job(job_id: str):
        return service.retry_job(job_id)

    @app.post(f"{API_PREFIX}/jobs/claim", response_model=Claimed, dependencies=A, responses=ERR,
              description="Atomically claim the oldest eligible job. `job` is null when nothing is eligible.")
    def claim(body: ClaimRequest):
        return service.claim(body.worker_id, body.request_id, body.exclude_repos) or {"job": None, "attempt": None}

    @app.post(f"{API_PREFIX}/workers/register", response_model=Registered, dependencies=A, responses=ERR)
    def register(body: WorkerRegister):
        return service.register_worker(body.worker_id, body.model_dump(exclude={"worker_id"}))

    @app.post(f"{API_PREFIX}/workers/{{worker_id}}/heartbeat", response_model=WorkerHeartbeatResult, dependencies=A, responses=ERR)
    def worker_heartbeat(worker_id: str):
        return service.worker_heartbeat(worker_id)

    @app.get(f"{API_PREFIX}/workers", response_model=list[Worker], dependencies=A, responses=ERR)
    def list_workers():
        return service.list_workers()

    @app.post(f"{API_PREFIX}/attempts/{{attempt_id}}/heartbeat", response_model=HeartbeatResult, dependencies=A, responses=ERR)
    def attempt_heartbeat(attempt_id: str, body: Fence):
        return service.heartbeat(attempt_id, body.worker_id, body.epoch)

    @app.post(f"{API_PREFIX}/attempts/{{attempt_id}}/complete", response_model=AttemptOutcome, dependencies=A, responses=ERR)
    def complete(attempt_id: str, body: CompleteRequest):
        return service.complete(attempt_id, body.worker_id, body.epoch, body.request_id,
                                body.result.model_dump(exclude_none=True), body.report_attempts)

    @app.post(f"{API_PREFIX}/attempts/{{attempt_id}}/fail", response_model=AttemptOutcome, dependencies=A, responses=ERR)
    def fail(attempt_id: str, body: FailRequest):
        return service.fail(attempt_id, body.worker_id, body.epoch, body.request_id, body.reason, body.message,
                            body.retryable, body.report_attempts)

    @app.post(f"{API_PREFIX}/attempts/{{attempt_id}}/release", response_model=AttemptOutcome, dependencies=A, responses=ERR)
    def release(attempt_id: str, body: ReleaseRequest):
        return service.release(attempt_id, body.worker_id, body.epoch, body.request_id, body.reason)

    @app.get(f"{API_PREFIX}/events", response_model=list[Event], dependencies=A, responses=ERR)
    def events(job_id: Optional[str] = None, after_id: int = 0, limit: int = Query(200, ge=1, le=1000)):
        return service.events(job_id, after_id, limit)

    @app.get(f"{API_PREFIX}/config", response_model=SettingsView, dependencies=A, responses=ERR,
             description="Server-side policy workers should follow (lease TTL, heartbeat interval, ...).")
    def config():
        return service.settings.public()

    @app.get(f"{API_PREFIX}/metrics", dependencies=A, responses=ERR,
             description="Counters and gauges derived from the database (restart-safe). ?format=prometheus for text.")
    def metrics(format: Literal["json", "prometheus"] = "json"):
        m = service.metrics()
        if format == "json":
            return m
        lines = []
        for k, v in m.items():
            if isinstance(v, dict):
                for label, n in v.items():
                    lines.append(f'ganglion_{k}{{status="{label}"}} {n}')
            elif v is not None:
                lines.append(f"ganglion_{k} {v}")
        return PlainTextResponse("\n".join(lines) + "\n")

    return app
