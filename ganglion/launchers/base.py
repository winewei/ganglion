"""Launcher interface: how a worker starts and observes an engineering runtime process.

A launcher knows nothing about jobs, leases or control planes. The worker hands it a
:class:`LaunchSpec` and later asks "is it still alive?". Results are not the launcher's
business either -- they are collected by the Spine adapter (``ganglion.spine``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class LaunchSpec:
    handle: str                 # launcher-specific identity, e.g. a tmux session name
    workspace: Path             # directory the runtime runs in
    prompt: str                 # initial instruction (e.g. "/spine-run --auto ...")
    attempt_id: str
    env: dict[str, str] = field(default_factory=dict)


class Launcher:
    name = "base"

    def available(self) -> bool:
        """Can this machine run it at all (binaries present)? Used for registration."""
        raise NotImplementedError

    def prepare(self, spec: LaunchSpec) -> None:
        """Make the workspace launchable (e.g. pre-accept trust). Idempotent."""

    def start(self, spec: LaunchSpec) -> bool:
        """Start the process. Idempotent: returns False if ``spec.handle`` is already running."""
        raise NotImplementedError

    def after_start(self, spec: LaunchSpec) -> list[str]:
        """Post-start nudges (e.g. answer a trust prompt). Returns diagnostic event kinds."""
        return []

    def is_alive(self, handle: str) -> bool | None:
        """True/False, or None when it cannot be determined right now (never guess)."""
        raise NotImplementedError

    def stop(self, handle: str) -> None:
        raise NotImplementedError

    def discover(self) -> dict[str, dict[str, str]]:
        """Live processes started by Ganglion on this machine: handle -> their GANGLION_* env.

        Used to re-adopt work after the worker lost its local state.
        """
        return {}

    def attach_hint(self, handle: str) -> str:
        return ""
