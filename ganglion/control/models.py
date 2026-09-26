"""Domain vocabulary shared by the control plane service, its HTTP layer and the clients.

Standard library only: the worker imports this module (for the error types) and must not
need FastAPI / pydantic. The HTTP request/response *schemas* live in ``api.py`` and are
published as OpenAPI -- that schema, not this file, is the wire contract.
"""

from __future__ import annotations

# --- Job: the requested engineering outcome ---------------------------------------------
JOB_QUEUED = "queued"        # eligible for a claim
JOB_RUNNING = "running"      # owned by exactly one current attempt (current_attempt_id, epoch, lease_until)
JOB_COMPLETED = "completed"  # terminal: an attempt reported a result
JOB_FAILED = "failed"        # terminal until `retry`: attempts exhausted / non-retryable failure / no worker can serve it
JOB_REJECTED = "rejected"    # terminal forever: failed admission (recursion depth, missing parent, invalid name)
JOB_IMPORTED = "imported"    # terminal: placeholder for a Ganglion 1.x request, keeps the inbox from relaunching it
JOB_STATUSES = (JOB_QUEUED, JOB_RUNNING, JOB_COMPLETED, JOB_FAILED, JOB_REJECTED, JOB_IMPORTED)
JOB_TERMINAL = (JOB_COMPLETED, JOB_FAILED, JOB_REJECTED, JOB_IMPORTED)

# --- Attempt: one worker's execution of a job. Never overwritten, never reused. ----------
ATT_ACTIVE = "active"
ATT_COMPLETED = "completed"
ATT_FAILED = "failed"
ATT_RELEASED = "released"
ATT_EXPIRED = "expired"
ATT_STATUSES = (ATT_ACTIVE, ATT_COMPLETED, ATT_FAILED, ATT_RELEASED, ATT_EXPIRED)

TARGETS = ("existing", "new", "manual")
SOURCES = ("human", "agent")
DEFAULT_LAUNCHER = "claude-code"

WORKER_ONLINE = "online"
WORKER_OFFLINE = "offline"


class ControlError(Exception):
    """Base class. ``code`` is stable and machine-readable (it is the HTTP error body's ``error``)."""

    code = "error"
    http_status = 400

    def __init__(self, detail: str = "", **extra):
        super().__init__(detail or self.code)
        self.detail = detail or self.code
        self.extra = extra

    def body(self) -> dict:
        return {"error": self.code, "detail": self.detail, **self.extra}


class NotFound(ControlError):
    code = "not_found"
    http_status = 404


class InvalidRequest(ControlError):
    code = "invalid"
    http_status = 422


class StaleAttempt(ControlError):
    """The caller is not (or no longer) the authoritative owner of the job.

    ``reason`` is one of: worker_mismatch, epoch_mismatch, not_current, lease_expired,
    expired, released, failed, completed. The worker must stop acting on the attempt.
    """

    code = "stale_attempt"
    http_status = 409


class Conflict(ControlError):
    """A retry that contradicts what was already recorded (e.g. complete after fail)."""

    code = "conflict"
    http_status = 409


class Unavailable(ControlError):
    """Transient: database or network. Callers retry; nothing was decided."""

    code = "unavailable"
    http_status = 503


class Unauthorized(ControlError):
    code = "unauthorized"
    http_status = 401


ERRORS = {c.code: c for c in (NotFound, InvalidRequest, StaleAttempt, Conflict, Unavailable, Unauthorized)}
