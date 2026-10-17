"""Environment preparation on the worker: turn a job's *global* repository identity into a
private, writable workspace on this machine, and deliver the requirement into it.

Workspace strategies (the job never carries a local path):

* ``repository.url`` set -> a cached mirror under ``<workspace_root>/cache`` is cloned or
  fetched, and every attempt gets its own clone under ``<workspace_root>/attempts``.
* ``target: existing|manual`` -> the worker's own ``[repos]`` mapping. With
  ``isolation = "worktree"`` (default) each attempt gets a ``git worktree`` on its own branch
  ``ganglion/<name>-<attempt>``; with ``"shared"`` (0.1.x behaviour) the attempt runs in the
  checkout itself and the worker never runs two attempts in it at once.
* ``target: new`` -> ``gh repo create`` + ``gh repo clone`` into ``new_repo_root`` (0.1.x
  behaviour, crash-safe: an existing remote is adopted). A brand-new repo has no commit to
  branch from, so it is used in place (shared).

Agent Spine creates its own worktrees inside the workspace; Ganglion never touches them.
"""

from __future__ import annotations

import hashlib
import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..common import CmdError, git, run_cmd
from ..config import Config

MODE_CLONE = "clone"
MODE_WORKTREE = "worktree"
MODE_SHARED = "shared"


class RepoConfigError(ValueError):
    """This worker cannot serve the job as configured (not transient)."""


class RepoNotMapped(RepoConfigError):
    """The repository is not (or no longer) mapped on this worker."""


@dataclass
class Workspace:
    path: Path
    mode: str
    repo_name: str
    branch: str | None = None


def check_git_repo(path: Path) -> Path:
    if not (path / ".git").exists():
        raise RepoConfigError(f"{path} is not a git repository")
    return path.absolute()


def gh_create_commands(owner: str, name: str, dest: Path) -> list[list[str]]:
    full = f"{owner}/{name}"
    return [["gh", "repo", "create", full, "--private"], ["gh", "repo", "clone", full, str(dest)]]


def create_repo(cfg: Config, owner: str, name: str) -> Path:
    if not owner:
        raise RepoConfigError("target: new needs repository.owner")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", name):
        raise RepoConfigError(f"invalid repository name {name!r}")
    dest = (cfg.new_repo_root / name).absolute()
    create, clone = gh_create_commands(owner, name, dest)
    if not (dest / ".git").exists():
        if dest.exists() and any(dest.iterdir()):
            raise RepoConfigError(f"{dest} exists and is not a git repository")
        p = run_cmd(create, check=False)
        if p.returncode != 0:
            # after a crash (or on another worker) the remote may already exist: adopt it
            if run_cmd(["gh", "repo", "view", f"{owner}/{name}"], check=False).returncode != 0:
                raise CmdError(f"{shlex.join(create)} failed: {(p.stderr or p.stdout).strip()}")
        cfg.new_repo_root.mkdir(parents=True, exist_ok=True)
        run_cmd(clone, timeout=600)
    return dest


def _slug(url: str) -> str:
    tail = re.sub(r"[^A-Za-z0-9._-]+", "-", url.rstrip("/").rsplit("/", 1)[-1].removesuffix(".git"))[:40]
    return f"{tail}-{hashlib.sha256(url.encode()).hexdigest()[:10]}"


def attempt_tag(job: dict[str, Any], attempt_id: str) -> str:
    """Unique, readable per-attempt label: ``<job name>-<8 hex of the attempt id>``."""
    return f"{job['name']}-{attempt_id.removeprefix('att_')[:8]}"


def prepare_workspace(cfg: Config, local_repos: dict[str, str], job: dict[str, Any], tag: str) -> Workspace:
    """Idempotent: re-running after a crash reuses what already exists."""
    repo = job.get("repository") or {}
    name = repo.get("name", "")
    if repo.get("url"):
        return _clone_workspace(cfg, tag, repo)
    if job["target"] == "new":
        if name in cfg.repos:
            raise RepoConfigError(f"{name!r} is already mapped in [repos]; use target: existing")
        path = Path(local_repos[name]) if name in local_repos else create_repo(cfg, repo.get("owner", ""), name)
        return Workspace(check_git_repo(path), MODE_SHARED, name, _current_branch(path))
    mapped = cfg.repos.get(name) or (Path(local_repos[name]) if name in local_repos else None)
    if mapped is None:
        raise RepoNotMapped(f"[repos] has no mapping for {name!r} on this worker")
    mapped = check_git_repo(mapped)
    if cfg.isolation == MODE_SHARED:
        return Workspace(mapped, MODE_SHARED, name, _current_branch(mapped))
    return _worktree_workspace(cfg, tag, mapped, name)


def _current_branch(path: Path) -> str | None:
    p = git(path, "symbolic-ref", "--quiet", "--short", "HEAD", check=False)
    return p.stdout.strip() or None


def _worktree_workspace(cfg: Config, tag: str, mapped: Path, name: str) -> Workspace:
    ws = cfg.workspace_root / "attempts" / tag
    branch = f"ganglion/{tag}"
    if (ws / ".git").exists():
        return Workspace(ws.absolute(), MODE_WORKTREE, name, branch)
    ws.parent.mkdir(parents=True, exist_ok=True)
    git(mapped, "worktree", "prune", check=False)
    exists = git(mapped, "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}", check=False).returncode == 0
    if exists:
        git(mapped, "worktree", "add", str(ws), branch)
    else:
        git(mapped, "worktree", "add", "-b", branch, str(ws), "HEAD")
    return Workspace(ws.absolute(), MODE_WORKTREE, name, branch)


def _clone_workspace(cfg: Config, tag: str, repo: dict[str, Any]) -> Workspace:
    url = repo["url"]
    cache = cfg.workspace_root / "cache" / f"{_slug(url)}.git"
    if (cache / "HEAD").exists():
        git(cache, "remote", "update", "--prune")
    else:
        cache.parent.mkdir(parents=True, exist_ok=True)
        tmp = cache.with_name(cache.name + ".tmp")
        if tmp.exists():
            run_cmd(["rm", "-rf", str(tmp)])
        run_cmd(["git", "clone", "--mirror", "--quiet", url, str(tmp)], timeout=1800)
        tmp.rename(cache)
    ws = cfg.workspace_root / "attempts" / tag
    branch = f"ganglion/{tag}"
    if not (ws / ".git").exists():
        ws.parent.mkdir(parents=True, exist_ok=True)
        if ws.exists():
            run_cmd(["rm", "-rf", str(ws)])  # half-made clone from a crash: never contains work
        args = ["git", "clone", "--quiet", "--reference-if-able", str(cache), "--dissociate"]
        if repo.get("ref"):
            args += ["--branch", repo["ref"]]
        run_cmd(args + [url, str(ws)], timeout=1800)
        if git(ws, "rev-parse", "--verify", "--quiet", "HEAD", check=False).returncode == 0:
            git(ws, "checkout", "-q", "-b", branch)
        else:  # empty remote
            git(ws, "checkout", "-q", "--orphan", branch)
    return Workspace(ws.absolute(), MODE_CLONE, repo.get("name", ""), branch)


def deliver_requirement(workspace: Path, req_path: str, name: str, goal: str) -> str:
    """Write the goal verbatim to ``<req_path>/<name>.md`` and commit it alone. Idempotent."""
    rel = f"{req_path}/{name}.md" if req_path else f"{name}.md"
    dest = workspace / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    raw = goal.encode("utf-8")
    if not dest.exists() or dest.read_bytes() != raw:
        dest.write_bytes(raw)
    git(workspace, "add", "--", rel)
    if git(workspace, "status", "--porcelain", "--", rel).stdout.strip():
        git(workspace, "commit", "-q", "-m", f"req: {name}", "--", rel)  # only this path
    return git(workspace, "rev-parse", "HEAD").stdout.strip()


def publish_branch(ws: Workspace) -> tuple[str | None, str | None]:
    """Push the attempt's branch to ``origin`` so the result survives this machine.

    Returns ``(branch, remote_url)``; ``(None, None)`` when there is nothing to push to.
    """
    if not ws.branch:
        return None, None
    remote = git(ws.path, "remote", "get-url", "origin", check=False)
    if remote.returncode != 0:
        return None, None
    git(ws.path, "push", "--quiet", "origin", f"HEAD:refs/heads/{ws.branch}")
    return ws.branch, remote.stdout.strip()
