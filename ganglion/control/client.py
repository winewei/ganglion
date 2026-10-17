"""Control-plane clients used by workers, the inbox adapter and the CLI.

* :class:`HttpClient` -- talks to a remote control plane (``ganglion server start``). stdlib
  ``urllib`` only, so a worker needs no third-party packages.
* :class:`LocalClient` -- embeds the control plane in-process on SQLite. This is the
  single-machine mode: same Job/Attempt model, same fencing code, no server, no PostgreSQL.

Both raise the error types from ``models``; transport problems are ``Unavailable``.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .models import ERRORS, ControlError, Unavailable


class Client:
    worker_id: str = ""

    # worker protocol
    def register(self, info: dict[str, Any]) -> dict[str, Any]: ...
    def worker_heartbeat(self) -> dict[str, Any]: ...
    def claim(self, request_id: str | None, exclude_repos: list[str]) -> dict[str, Any] | None: ...
    def heartbeat(self, attempt_id: str, epoch: int) -> dict[str, Any]: ...
    def complete(self, attempt_id: str, epoch: int, request_id: str, result: dict[str, Any], report_attempts: int = 1) -> dict[str, Any]: ...
    def fail(self, attempt_id: str, epoch: int, request_id: str, reason: str, message: str = "", retryable: bool = False,
             report_attempts: int = 1) -> dict[str, Any]: ...
    def release(self, attempt_id: str, epoch: int, request_id: str, reason: str = "") -> dict[str, Any]: ...

    # job sources / operators
    def create_job(self, spec: dict[str, Any]) -> tuple[dict[str, Any], bool]: ...
    def get_job(self, job_id: str) -> dict[str, Any]: ...
    def list_jobs(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]: ...
    def retry_job(self, job_id: str) -> dict[str, Any]: ...
    def list_workers(self) -> list[dict[str, Any]]: ...
    def events(self, job_id: str | None = None, after_id: int = 0, limit: int = 200) -> list[dict[str, Any]]: ...
    def metrics(self) -> dict[str, Any]: ...
    def config(self) -> dict[str, Any]: ...


class LocalClient(Client):
    """In-process control plane. ``service.start()`` must have been called."""

    def __init__(self, service, worker_id: str = ""):
        self.service = service
        self.worker_id = worker_id

    def register(self, info):
        return self.service.register_worker(self.worker_id, info)

    def worker_heartbeat(self):
        return self.service.worker_heartbeat(self.worker_id)

    def claim(self, request_id, exclude_repos):
        self.service.sweep()  # no server thread in embedded mode; keep state timely
        return self.service.claim(self.worker_id, request_id, exclude_repos)

    def heartbeat(self, attempt_id, epoch):
        return self.service.heartbeat(attempt_id, self.worker_id, epoch)

    def complete(self, attempt_id, epoch, request_id, result, report_attempts=1):
        return self.service.complete(attempt_id, self.worker_id, epoch, request_id, result, report_attempts)

    def fail(self, attempt_id, epoch, request_id, reason, message="", retryable=False, report_attempts=1):
        return self.service.fail(attempt_id, self.worker_id, epoch, request_id, reason, message, retryable, report_attempts)

    def release(self, attempt_id, epoch, request_id, reason=""):
        return self.service.release(attempt_id, self.worker_id, epoch, request_id, reason)

    def create_job(self, spec):
        return self.service.create_job(spec)

    def get_job(self, job_id):
        return self.service.get_job(job_id)

    def list_jobs(self, status=None, limit=100):
        return self.service.list_jobs(status, limit)

    def retry_job(self, job_id):
        return self.service.retry_job(job_id)

    def list_workers(self):
        return self.service.list_workers()

    def events(self, job_id=None, after_id=0, limit=200):
        return self.service.events(job_id, after_id, limit)

    def metrics(self):
        return self.service.metrics()

    def config(self):
        return self.service.settings.public()


class HttpClient(Client):
    def __init__(self, base_url: str, token: str = "", worker_id: str = "", timeout: float = 30.0):
        self.base = base_url.rstrip("/")
        self.token = token
        self.worker_id = worker_id
        self.timeout = timeout

    def _call(self, method: str, path: str, body: dict[str, Any] | None = None, query: dict[str, Any] | None = None):
        url = self.base + path
        if query:
            q = {k: v for k, v in query.items() if v is not None}
            if q:
                url += "?" + urllib.parse.urlencode(q)
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                payload = json.loads(raw)
            except Exception:
                payload = {}
            code = payload.get("error") if isinstance(payload, dict) else None
            if code in ERRORS:
                extra = {k: v for k, v in payload.items() if k not in ("error", "detail")}
                raise ERRORS[code](payload.get("detail", ""), **extra) from None
            if e.code >= 500:
                raise Unavailable(f"control plane HTTP {e.code}") from None
            if e.code == 422:
                raise ERRORS["invalid"](str(payload.get("detail", payload))[:2000]) from None
            raise ControlError(f"HTTP {e.code}: {raw[:500]!r}") from None
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            raise Unavailable(f"control plane unreachable: {e}") from None

    # worker protocol
    def register(self, info):
        return self._call("POST", "/v1/workers/register", {"worker_id": self.worker_id, **info})[1]

    def worker_heartbeat(self):
        return self._call("POST", f"/v1/workers/{urllib.parse.quote(self.worker_id)}/heartbeat")[1]

    def claim(self, request_id, exclude_repos):
        out = self._call("POST", "/v1/jobs/claim",
                         {"worker_id": self.worker_id, "request_id": request_id, "exclude_repos": list(exclude_repos)})[1]
        return out if out and out.get("job") else None

    def _att(self, attempt_id: str, op: str, body: dict[str, Any]):
        body = {"worker_id": self.worker_id, **body}
        return self._call("POST", f"/v1/attempts/{urllib.parse.quote(attempt_id)}/{op}", body)[1]

    def heartbeat(self, attempt_id, epoch):
        return self._att(attempt_id, "heartbeat", {"epoch": epoch})

    def complete(self, attempt_id, epoch, request_id, result, report_attempts=1):
        return self._att(attempt_id, "complete", {"epoch": epoch, "request_id": request_id, "result": result,
                                                  "report_attempts": report_attempts})

    def fail(self, attempt_id, epoch, request_id, reason, message="", retryable=False, report_attempts=1):
        return self._att(attempt_id, "fail", {"epoch": epoch, "request_id": request_id, "reason": reason,
                                              "message": message[:4000], "retryable": retryable,
                                              "report_attempts": report_attempts})

    def release(self, attempt_id, epoch, request_id, reason=""):
        return self._att(attempt_id, "release", {"epoch": epoch, "request_id": request_id, "reason": reason[:400]})

    # job sources / operators
    def create_job(self, spec):
        status, out = self._call("POST", "/v1/jobs", {k: v for k, v in spec.items() if v is not None})
        return out["job"], out["created"]

    def get_job(self, job_id):
        return self._call("GET", f"/v1/jobs/{urllib.parse.quote(job_id, safe='')}")[1]

    def list_jobs(self, status=None, limit=100):
        return self._call("GET", "/v1/jobs", query={"status": status, "limit": limit})[1]

    def retry_job(self, job_id):
        return self._call("POST", f"/v1/jobs/{urllib.parse.quote(job_id, safe='')}/retry")[1]

    def list_workers(self):
        return self._call("GET", "/v1/workers")[1]

    def events(self, job_id=None, after_id=0, limit=200):
        return self._call("GET", "/v1/events", query={"job_id": job_id, "after_id": after_id, "limit": limit})[1]

    def metrics(self):
        return self._call("GET", "/v1/metrics")[1]

    def config(self):
        return self._call("GET", "/v1/config")[1]
