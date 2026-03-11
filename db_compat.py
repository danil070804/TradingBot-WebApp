import os
import re
from typing import Any

import aiosqlite as sqlite_driver

try:
    import asyncpg
except Exception:  # pragma: no cover - optional dependency in local env
    asyncpg = None


Row = sqlite_driver.Row


def _is_postgres_dsn(dsn: str) -> bool:
    return dsn.startswith("postgres://") or dsn.startswith("postgresql://")


def using_postgres() -> bool:
    dsn = os.getenv("DATABASE_URL", "")
    return _is_postgres_dsn(dsn)


def _convert_placeholders(query: str) -> str:
    i = 0
    out = []
    for ch in query:
        if ch == "?":
            i += 1
            out.append(f"${i}")
        else:
            out.append(ch)
    return "".join(out)


def _normalize_query_for_pg(query: str) -> str:
    q = query.strip()
    q = q.replace("IFNULL(", "COALESCE(")
    if q.upper().startswith("INSERT OR IGNORE INTO"):
        q = re.sub(r"(?i)^INSERT OR IGNORE INTO", "INSERT INTO", q, count=1)
        if "ON CONFLICT" not in q.upper():
            q += " ON CONFLICT DO NOTHING"
    return _convert_placeholders(q)


class _PGCursor:
    def __init__(self, rows: list[Any] | None = None, lastrowid: int | None = None, rowcount: int = 0):
        self._rows = rows or []
        self.lastrowid = lastrowid
        self.rowcount = rowcount

    async def fetchone(self):
        if not self._rows:
            return None
        return self._rows[0]

    async def fetchall(self):
        return self._rows


class _PGConnectionCtx:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = None
        self.row_factory = None

    async def __aenter__(self):
        if asyncpg is None:
            raise RuntimeError("asyncpg is not installed, but PostgreSQL DATABASE_URL is set.")
        self.conn = await asyncpg.connect(self.dsn)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.conn is not None:
            await self.conn.close()

    async def execute(self, query: str, params: tuple = ()):
        q = _normalize_query_for_pg(query)
        q_upper = q.lstrip().upper()

        if q_upper.startswith("SELECT") or q_upper.startswith("WITH"):
            rows = await self.conn.fetch(q, *params)
            return _PGCursor(rows=rows, rowcount=len(rows))

        if q_upper.startswith("INSERT") and "RETURNING" not in q_upper:
            try:
                row = await self.conn.fetchrow(q + " RETURNING id", *params)
                return _PGCursor(
                    lastrowid=int(row["id"]) if row and "id" in row else None,
                    rowcount=1 if row else 0,
                )
            except Exception:
                status = await self.conn.execute(q, *params)
                return _PGCursor(rowcount=_parse_pg_rowcount(status))

        status = await self.conn.execute(q, *params)
        return _PGCursor(rowcount=_parse_pg_rowcount(status))

    async def executemany(self, query: str, seq_of_params):
        q = _normalize_query_for_pg(query)
        await self.conn.executemany(q, seq_of_params)

    async def commit(self):
        return


def connect(path_or_dsn: str):
    dsn = os.getenv("DATABASE_URL", "").strip()
    if dsn and _is_postgres_dsn(dsn):
        return _PGConnectionCtx(dsn)
    return sqlite_driver.connect(path_or_dsn)


def _parse_pg_rowcount(status: str) -> int:
    parts = str(status or "").strip().split()
    if not parts:
        return 0
    try:
        return int(parts[-1])
    except (TypeError, ValueError):
        return 0
