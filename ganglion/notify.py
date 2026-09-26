"""Notification backends (unchanged behaviour from Ganglion 1.x).

Notification is presentation only: a failed send is logged and never affects job state.
Who sends what (see DESIGN.md "Notifications"):
  - control plane: job lifecycle (rejected / waiting for a repo / completed / failed), exactly
    once per committed state transition;
  - worker: operational facts only it knows (session started + attach command, stale attempt).
"""

from __future__ import annotations

import importlib
import json
import urllib.request
from typing import Any, Callable

from .common import expand, log, now


class Notifier:
    def send(self, title: str, message: str, tags: list[str] | None = None) -> None:  # pragma: no cover
        raise NotImplementedError


class NullNotifier(Notifier):
    def send(self, title, message, tags=None):
        log(f"notify(null): {title}")


class FileNotifier(Notifier):
    """Append notifications to a JSONL file. For debugging and tests."""

    def __init__(self, path: str):
        self.path = expand(path)

    def send(self, title, message, tags=None):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": now(), "title": title, "message": message, "tags": tags or []}, ensure_ascii=False) + "\n")


class NtfyNotifier(Notifier):
    """ntfy JSON publish API (title goes in the body: a non-latin-1 Title header breaks urllib)."""

    def __init__(self, topic: str, server: str = "https://ntfy.sh", token: str = "", priority: int = 3, timeout: float = 10):
        if not topic:
            raise ValueError("notify.ntfy_topic is not configured")
        self.topic, self.server, self.token = topic, server.rstrip("/"), token
        self.priority, self.timeout = int(priority), float(timeout)

    def send(self, title, message, tags=None):
        body: dict[str, Any] = {"topic": self.topic, "title": title, "message": message, "priority": self.priority}
        if tags:
            body["tags"] = tags
        req = urllib.request.Request(
            self.server + "/",
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            resp.read()


NOTIFIERS: dict[str, Callable[..., Notifier]] = {
    "ntfy": lambda c: NtfyNotifier(
        topic=c.get("ntfy_topic", ""),
        server=c.get("ntfy_server", "https://ntfy.sh"),
        token=c.get("ntfy_token", ""),
        priority=c.get("ntfy_priority", 3),
    ),
    "file": lambda c: FileNotifier(c.get("file_path", "~/.ganglion/notifications.jsonl")),
    "null": lambda c: NullNotifier(),
}


def make_notifier(conf: dict[str, Any] | None) -> Notifier:
    conf = conf or {"backend": "null"}
    backend = conf.get("backend", "ntfy")
    if backend in NOTIFIERS:
        return NOTIFIERS[backend](conf)
    if ":" in backend:  # "some.module:factory" -- factory(conf) -> Notifier
        mod, _, attr = backend.partition(":")
        return getattr(importlib.import_module(mod), attr)(conf)
    raise ValueError(f"unknown notification backend: {backend}")


def safe_notify(notifier: Notifier, title: str, message: str, tags: list[str] | None = None) -> bool:
    try:
        notifier.send(title, message, tags)
        return True
    except Exception as e:  # a notification failure never affects the main flow
        log(f"notify failed: {e!r}")
        return False
