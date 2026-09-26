"""Small shared helpers. Standard library only (the worker must run without extra deps)."""

from __future__ import annotations

import json
import os
import re
import secrets
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

# A job name ends up in a tmux session name and a file name. tmux rewrites "." and ":";
# whitespace, quotes and shell metacharacters only cause trouble. Chinese is fine.
NAME_RE = re.compile(r"^[^\s.:'\"\\/`$-][^\s.:'\"\\/`$]*$")
NAME_MAX = 100


def now() -> float:
    return time.time()


def new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(8)}"


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[ganglion {ts}] {msg}", file=sys.stderr, flush=True)


def log_metric(name: str, value: float = 1, **labels: Any) -> None:
    """One JSON line per metric sample on stderr. Enough to grep / ship to a log pipeline."""
    rec = {"ts": round(now(), 3), "metric": name, "value": value, **labels}
    print(json.dumps(rec, ensure_ascii=False, sort_keys=True), file=sys.stderr, flush=True)


def expand(p: str | os.PathLike[str]) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(str(p))))


def atomic_write_text(path: Path, text: str, mode: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{int(now() * 1e6)}.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        if mode is not None:
            os.chmod(tmp, mode)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


class CmdError(RuntimeError):
    pass


def run_cmd(args: list[str], cwd: Path | None = None, check: bool = True, timeout: float = 120) -> subprocess.CompletedProcess:
    try:
        p = subprocess.run(args, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError as e:
        raise CmdError(f"command not found: {args[0]} (PATH={os.environ.get('PATH', '')})") from e
    except subprocess.TimeoutExpired as e:
        raise CmdError(f"command timed out: {shlex.join(args)}") from e
    if check and p.returncode != 0:
        raise CmdError(f"{shlex.join(args)} failed (exit {p.returncode}): {(p.stderr or p.stdout).strip()}")
    return p


def git(repo: Path, *args: str, check: bool = True, timeout: float = 600) -> subprocess.CompletedProcess:
    return run_cmd(["git", "-C", str(repo), *args], check=check, timeout=timeout)
