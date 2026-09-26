"""Transactional persistence for the control plane.

The service talks to a :class:`Tx` (one database transaction) through a handful of
table-level operations: ``get``, ``insert``, ``cas`` (compare-and-set update), and a few
named queries. All SQL lives in this module.

Correctness does not depend on a particular locking dialect: every ownership change is a
single conditional ``UPDATE ... WHERE <expected state>`` whose affected row count decides the
winner. That is atomic on both backends:

* PostgreSQL (production): under READ COMMITTED a concurrent ``UPDATE`` of the same row
  blocks on the row lock and then re-evaluates its ``WHERE`` against the committed row, so
  exactly one of N racing compare-and-sets matches.
* SQLite (single-machine mode and tests): write transactions start with ``BEGIN IMMEDIATE``,
  so writers are serialised and a compare-and-set sees the previous writer's commit.

Transient backend failures surface as :class:`Unavailable` -- never as a job failure.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterator

from .models import Unavailable

SCHEMA_VERSION = 1

_DDL = """
CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    key TEXT UNIQUE,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    target TEXT NOT NULL,
    repository TEXT NOT NULL,
    goal TEXT NOT NULL,
    launcher TEXT NOT NULL,
    requirements TEXT NOT NULL,
    source TEXT NOT NULL,
    parent_job_id TEXT,
    depth INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    timeout_seconds {float},
    attempt_count INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0,
    epoch INTEGER NOT NULL DEFAULT 0,
    current_attempt_id TEXT,
    lease_until {float},
    result TEXT,
    error TEXT,
    created_at {float} NOT NULL,
    updated_at {float} NOT NULL,
    finished_at {float}
);
CREATE INDEX IF NOT EXISTS jobs_by_status ON jobs (status, created_at, id);
CREATE TABLE IF NOT EXISTS attempts (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs (id),
    worker_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    status TEXT NOT NULL,
    claim_request_id TEXT UNIQUE,
    final_request_id TEXT,
    lease_until {float} NOT NULL,
    started_at {float} NOT NULL,
    heartbeat_at {float},
    finished_at {float},
    result TEXT,
    error TEXT,
    UNIQUE (job_id, epoch)
);
CREATE INDEX IF NOT EXISTS attempts_by_worker ON attempts (worker_id, status);
CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    platform TEXT NOT NULL,
    arch TEXT NOT NULL,
    launchers TEXT NOT NULL,
    capabilities TEXT NOT NULL,
    repos TEXT NOT NULL,
    info TEXT NOT NULL,
    registered_at {float} NOT NULL,
    last_seen {float} NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id {serial},
    ts {float} NOT NULL,
    type TEXT NOT NULL,
    job_id TEXT,
    attempt_id TEXT,
    worker_id TEXT,
    epoch INTEGER,
    data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_by_job ON events (job_id, id);
CREATE INDEX IF NOT EXISTS events_by_type ON events (type);
"""

COLUMNS = {
    "jobs": {
        "id", "key", "name", "status", "target", "repository", "goal", "launcher", "requirements", "source",
        "parent_job_id", "depth", "max_attempts", "timeout_seconds", "attempt_count", "failures", "epoch",
        "current_attempt_id", "lease_until", "result", "error", "created_at", "updated_at", "finished_at",
    },
    "attempts": {
        "id", "job_id", "worker_id", "epoch", "status", "claim_request_id", "final_request_id", "lease_until",
        "started_at", "heartbeat_at", "finished_at", "result", "error",
    },
    "workers": {
        "id", "status", "platform", "arch", "launchers", "capabilities", "repos", "info", "registered_at", "last_seen",
    },
}
JSON_COLUMNS = {"repository", "requirements", "result", "launchers", "capabilities", "repos", "info", "data"}


class IntegrityError(Exception):
    """A uniqueness constraint fired (e.g. a duplicate claim request id)."""


def _enc(col: str, v: Any) -> Any:
    if col in JSON_COLUMNS and v is not None:
        return json.dumps(v, ensure_ascii=False, sort_keys=True)
    return v


def _dec(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    out = dict(row)
    for k in JSON_COLUMNS & out.keys():
        if isinstance(out[k], str):
            out[k] = json.loads(out[k])
    return out


def _check_cols(table: str, cols) -> None:
    bad = set(cols) - COLUMNS[table]
    if bad:
        raise ValueError(f"unknown column(s) for {table}: {sorted(bad)}")


class Tx:
    """One open transaction. Only valid inside ``Store.tx()``."""

    def __init__(self, store: "Store", cur):
        self.store = store
        self.cur = cur

    # -- primitives --------------------------------------------------------------------
    def _exec(self, sql: str, params: tuple | list = ()):
        return self.store._execute(self.cur, sql, tuple(params))

    def all(self, sql: str, params: tuple | list = ()) -> list[dict[str, Any]]:
        self._exec(sql, params)
        return [_dec(r) for r in self.store._fetchall(self.cur)]

    def one(self, sql: str, params: tuple | list = ()) -> dict[str, Any] | None:
        rows = self.all(sql, params)
        return rows[0] if rows else None

    # -- generic table operations --------------------------------------------------------
    def get(self, table: str, id_: str) -> dict[str, Any] | None:
        _check_cols(table, ())
        return self.one(f"SELECT * FROM {table} WHERE id = ?", (id_,))

    def insert(self, table: str, row: dict[str, Any], on_conflict_ignore: str | None = None) -> bool:
        """Insert; with ``on_conflict_ignore=<column>`` a duplicate is a no-op returning False."""
        _check_cols(table, row)
        cols = list(row)
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})"
        if on_conflict_ignore:
            sql += f" ON CONFLICT ({on_conflict_ignore}) DO NOTHING"
        self._exec(sql, [_enc(c, row[c]) for c in cols])
        return self.store._rowcount(self.cur) == 1

    def upsert(self, table: str, row: dict[str, Any], keep: tuple[str, ...] = ()) -> None:
        """Insert or update by id. Columns in ``keep`` are only written on insert."""
        _check_cols(table, row)
        cols = list(row)
        upd = [c for c in cols if c != "id" and c not in keep]
        sql = (
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)}) "
            f"ON CONFLICT (id) DO UPDATE SET {', '.join(f'{c} = excluded.{c}' for c in upd)}"
        )
        self._exec(sql, [_enc(c, row[c]) for c in cols])

    def cas(self, table: str, id_: str, expect: dict[str, Any], set_: dict[str, Any]) -> bool:
        """Compare-and-set one row. True iff the row matched ``expect`` and was updated.

        ``expect`` keys are columns (equality, ``None`` = IS NULL) or ``<col>__lt`` /
        ``<col>__gte`` / ``<col>__in``.
        """
        where, params = ["id = ?"], [id_]
        for k, v in expect.items():
            col, _, op = k.partition("__")
            _check_cols(table, [col])
            if op == "lt":
                where.append(f"{col} < ?")
                params.append(v)
            elif op == "gte":
                where.append(f"{col} >= ?")
                params.append(v)
            elif op == "in":
                where.append(f"{col} IN ({', '.join('?' for _ in v)})")
                params.extend(v)
            elif v is None:
                where.append(f"{col} IS NULL")
            else:
                where.append(f"{col} = ?")
                params.append(v)
        _check_cols(table, set_)
        sets, sparams = [], []
        for c, v in set_.items():
            if isinstance(v, Increment):
                sets.append(f"{c} = {c} + ?")
                sparams.append(v.by)
            else:
                sets.append(f"{c} = ?")
                sparams.append(_enc(c, v))
        self._exec(f"UPDATE {table} SET {', '.join(sets)} WHERE {' AND '.join(where)}", sparams + params)
        return self.store._rowcount(self.cur) == 1

    # -- named queries -------------------------------------------------------------------
    def get_meta_float(self, k: str) -> float | None:
        row = self.one("SELECT v FROM meta WHERE k = ?", (k,))
        return float(row["v"]) if row else None

    def set_meta(self, k: str, v: Any) -> None:
        self._exec("INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v", (k, str(v)))

    def job_by_key(self, key: str) -> dict[str, Any] | None:
        return self.one("SELECT * FROM jobs WHERE key = ?", (key,))

    def latest_job_by_name(self, name: str) -> dict[str, Any] | None:
        return self.one("SELECT * FROM jobs WHERE name = ? ORDER BY created_at DESC, id LIMIT 1", (name,))

    def claim_candidates(self, now: float, after: tuple[float, str] | None, limit: int) -> list[dict[str, Any]]:
        """Claimable jobs in FIFO order: queued, or running with a lapsed lease."""
        sql = "SELECT * FROM jobs WHERE (status = 'queued' OR (status = 'running' AND lease_until < ?))"
        params: list[Any] = [now]
        if after:
            sql += " AND (created_at > ? OR (created_at = ? AND id > ?))"
            params += [after[0], after[0], after[1]]
        sql += " ORDER BY created_at, id LIMIT ?"
        params.append(limit)
        return self.all(sql, params)

    def expired_running(self, now: float, limit: int = 100) -> list[dict[str, Any]]:
        return self.all(
            "SELECT * FROM jobs WHERE status = 'running' AND lease_until < ? ORDER BY lease_until LIMIT ?", (now, limit)
        )

    def attempts_for_job(self, job_id: str) -> list[dict[str, Any]]:
        return self.all("SELECT * FROM attempts WHERE job_id = ? ORDER BY epoch", (job_id,))

    def attempt_by_claim_request(self, request_id: str) -> dict[str, Any] | None:
        return self.one("SELECT * FROM attempts WHERE claim_request_id = ?", (request_id,))

    def active_attempts_for_worker(self, worker_id: str) -> list[dict[str, Any]]:
        return self.all(
            "SELECT * FROM attempts WHERE worker_id = ? AND status = 'active' ORDER BY started_at", (worker_id,)
        )

    def list_jobs(self, status: str | None, limit: int) -> list[dict[str, Any]]:
        if status:
            return self.all("SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC, id LIMIT ?", (status, limit))
        return self.all("SELECT * FROM jobs ORDER BY created_at DESC, id LIMIT ?", (limit,))

    def list_workers(self) -> list[dict[str, Any]]:
        return self.all("SELECT * FROM workers ORDER BY id")

    def workers_serving_repo(self, name: str) -> list[dict[str, Any]]:
        # Small table; filter in Python to stay portable (no JSON operators).
        return [w for w in self.list_workers() if name in (w.get("repos") or [])]

    def stale_workers(self, cutoff: float) -> list[dict[str, Any]]:
        return self.all("SELECT * FROM workers WHERE status = 'online' AND last_seen < ?", (cutoff,))

    def add_event(self, ts: float, type_: str, job_id=None, attempt_id=None, worker_id=None, epoch=None, data=None) -> None:
        self._exec(
            "INSERT INTO events (ts, type, job_id, attempt_id, worker_id, epoch, data) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ts, type_, job_id, attempt_id, worker_id, epoch, json.dumps(data or {}, ensure_ascii=False, sort_keys=True)),
        )

    def events(self, job_id: str | None, after_id: int, limit: int) -> list[dict[str, Any]]:
        if job_id:
            return self.all(
                "SELECT * FROM events WHERE job_id = ? AND id > ? ORDER BY id LIMIT ?", (job_id, after_id, limit)
            )
        return self.all("SELECT * FROM events WHERE id > ? ORDER BY id LIMIT ?", (after_id, limit))

    def count_by(self, table: str, col: str) -> dict[str, int]:
        if table != "events":
            _check_cols(table, [col])
        rows = self.all(f"SELECT {col} AS k, COUNT(*) AS n FROM {table} GROUP BY {col}")
        return {r["k"]: int(r["n"]) for r in rows}

    def avg(self, sql: str, params: tuple = ()) -> float | None:
        r = self.one(sql, params)
        v = r and list(r.values())[0]
        return float(v) if v is not None else None

    def sum_event_field(self, type_: str, field: str) -> int:
        total = 0
        for r in self.all("SELECT data FROM events WHERE type = ?", (type_,)):
            total += int((r["data"] or {}).get(field) or 0)
        return total


class Increment:
    def __init__(self, by: int = 1):
        self.by = by


class Store:
    """Backend-neutral transaction factory. Subclasses supply connections and dialect."""

    float_type = "DOUBLE PRECISION"
    serial_type = "BIGSERIAL PRIMARY KEY"

    def __init__(self):
        self._local = threading.local()

    # subclass hooks
    def _connect(self):  # pragma: no cover
        raise NotImplementedError

    def _begin(self, conn):  # pragma: no cover
        raise NotImplementedError

    def _execute(self, cur, sql: str, params: tuple):  # pragma: no cover
        raise NotImplementedError

    def _fetchall(self, cur) -> list[dict]:
        return [dict(r) for r in cur.fetchall()]

    def _rowcount(self, cur) -> int:
        return cur.rowcount

    def _is_transient(self, e: Exception) -> bool:  # pragma: no cover
        return False

    def _is_integrity(self, e: Exception) -> bool:  # pragma: no cover
        return False

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            with contextlib.suppress(Exception):
                conn.close()
            self._local.conn = None

    def _conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is None:
            try:
                conn = self._connect()
            except Exception as e:
                if self._is_transient(e):
                    raise Unavailable(f"database unavailable: {e}") from e
                raise
            self._local.conn = conn
        return conn

    @contextlib.contextmanager
    def tx(self) -> Iterator[Tx]:
        conn = self._conn()
        try:
            cur = self._begin(conn)
            try:
                yield Tx(self, cur)
            except BaseException:
                with contextlib.suppress(Exception):
                    conn.rollback()
                raise
            conn.commit()
        except (IntegrityError, Unavailable):
            raise
        except Exception as e:
            if self._is_integrity(e):
                raise IntegrityError(str(e)) from e
            if self._is_transient(e):
                # The connection may be dead and the commit's fate unknown. Drop it; callers
                # retry with the same request id, which every mutation treats idempotently.
                self.close()
                raise Unavailable(f"database unavailable: {e}") from e
            raise

    def migrate(self) -> None:
        ddl = _DDL.format(float=self.float_type, serial=self.serial_type)
        with self.tx() as t:
            for stmt in (s.strip() for s in ddl.split(";")):
                if stmt:
                    t._exec(stmt)
            row = t.one("SELECT v FROM meta WHERE k = 'schema_version'")
            if row is None:
                t._exec("INSERT INTO meta (k, v) VALUES ('schema_version', ?)", (str(SCHEMA_VERSION),))
            elif int(row["v"]) > SCHEMA_VERSION:
                raise RuntimeError(f"database schema v{row['v']} is newer than this Ganglion (v{SCHEMA_VERSION})")


class SqliteStore(Store):
    float_type = "REAL"
    serial_type = "INTEGER PRIMARY KEY AUTOINCREMENT"

    def __init__(self, path: str | Path):
        super().__init__()
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _begin(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")  # take the write lock up front: serialises writers
        return cur

    def _execute(self, cur, sql, params):
        return cur.execute(sql, params)

    def _is_transient(self, e):
        return isinstance(e, sqlite3.OperationalError) and ("locked" in str(e) or "busy" in str(e) or "disk I/O" in str(e))

    def _is_integrity(self, e):
        return isinstance(e, sqlite3.IntegrityError)


class PostgresStore(Store):
    def __init__(self, dsn: str):
        super().__init__()
        import psycopg  # noqa: F401  (server extra; fail early with a clear ImportError)

        self.dsn = dsn

    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row

        return psycopg.connect(self.dsn, row_factory=dict_row, connect_timeout=10)

    def _begin(self, conn):
        # psycopg opens a transaction implicitly on first execute (autocommit off);
        # default isolation is READ COMMITTED, which the compare-and-set design relies on.
        return conn.cursor()

    def _execute(self, cur, sql, params):
        return cur.execute(sql.replace("?", "%s"), params)

    def _is_transient(self, e):
        import psycopg

        return isinstance(e, (psycopg.OperationalError, psycopg.InterfaceError)) and not isinstance(
            e, psycopg.errors.IntegrityError
        )

    def _is_integrity(self, e):
        import psycopg

        return isinstance(e, psycopg.errors.IntegrityError)


def open_store(url: str) -> Store:
    """``sqlite:///abs/path.db`` | ``sqlite:///~/x.db`` | ``postgresql://...``."""
    if url.startswith(("postgres://", "postgresql://")):
        return PostgresStore(url)
    if url == "sqlite://:memory:":
        return SqliteStore(":memory:")
    if url.startswith("sqlite:///"):
        from ..common import expand

        rest = url[len("sqlite://"):]          # "/abs/path.db" (extra leading slashes tolerated)
        while rest.startswith("//"):
            rest = rest[1:]
        if rest.startswith(("/~", "/$")):      # sqlite:///~/x.db
            rest = rest[1:]
        return SqliteStore(expand(rest))
    raise ValueError(f"unsupported database url: {url!r} (use sqlite:///path or postgresql://...)")
