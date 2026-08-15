import os
import re
import sqlite3
import aiosqlite
from pathlib import Path
from contextlib import asynccontextmanager
from app.config import settings

try:
    import asyncpg
except ImportError:
    asyncpg = None

SCHEMA_PATH = Path(__file__).parent.parent / "data" / "schema.sql"


class PostgresCursorWrapper:
    def __init__(self, records, rowcount=0):
        self._records = records
        self.rowcount = rowcount

    async def fetchone(self):
        if self._records:
            return self._records[0]
        return None

    async def fetchall(self):
        return self._records


class PostgresConnectionWrapper:
    def __init__(self, conn):
        self._conn = conn
        self._tx = None

    async def execute(self, sql: str, parameters=()):
        # Convert SQLite ? parameters to PostgreSQL $1, $2, ...
        param_count = 0

        def replace_param(match):
            nonlocal param_count
            param_count += 1
            return f"${param_count}"

        # Translate SQLite-specific syntax to PostgreSQL compatible syntax
        pg_sql = re.sub(r'\?', replace_param, sql)
        pg_sql = pg_sql.replace("INSERT OR IGNORE INTO", "INSERT INTO")
        if "INSERT INTO events" in sql or "INSERT INTO counters" in sql or "INSERT INTO duplicate_events" in sql:
            if "ON CONFLICT" not in pg_sql:
                if "events" in sql:
                    pg_sql += " ON CONFLICT (event_id) DO NOTHING"
                elif "counters" in sql:
                    pg_sql += " ON CONFLICT (name) DO NOTHING"

        try:
            if parameters:
                status_str = await self._conn.execute(pg_sql, *parameters)
            else:
                status_str = await self._conn.execute(pg_sql)

            rowcount = 0
            if status_str:
                parts = status_str.split()
                if len(parts) >= 2 and parts[-1].isdigit():
                    rowcount = int(parts[-1])

            # If it's a SELECT query, fetch records
            if pg_sql.strip().upper().startswith("SELECT"):
                if parameters:
                    records = await self._conn.fetch(pg_sql, *parameters)
                else:
                    records = await self._conn.fetch(pg_sql)
                return PostgresCursorWrapper(records, rowcount=rowcount)

            return PostgresCursorWrapper([], rowcount=rowcount)
        except asyncpg.UniqueViolationError as exc:
            raise sqlite3.IntegrityError(str(exc)) from exc

    async def executescript(self, sql: str):
        # Convert SQLite DDL schema to PostgreSQL compatible schema
        pg_sql = sql.replace("PRAGMA journal_mode=WAL;", "")
        pg_sql = pg_sql.replace("PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
        pg_sql = pg_sql.replace("INSERT OR IGNORE INTO", "INSERT INTO")
        pg_sql = pg_sql.replace("VALUES ('duplicates_blocked', 0);", "VALUES ('duplicates_blocked', 0) ON CONFLICT (name) DO NOTHING;")
        await self._conn.execute(pg_sql)

    async def commit(self):
        # asyncpg executes auto-commit by default outside transaction blocks
        pass


@asynccontextmanager
async def get_db():
    """
    Async context manager for database connections.
    Supports PostgreSQL via asyncpg if DATABASE_URL is configured,
    otherwise uses single-file SQLite via aiosqlite with WAL mode.
    """
    db_url = settings.database_url or os.environ.get("DATABASE_URL", "")

    if db_url.startswith("postgres://") or db_url.startswith("postgresql://"):
        if not asyncpg:
            raise RuntimeError("asyncpg is required for PostgreSQL support")
        # Standardize postgresql:// protocol prefix
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)

        conn = await asyncpg.connect(db_url)
        try:
            yield PostgresConnectionWrapper(conn)
        finally:
            await conn.close()
    else:
        db_path = settings.db_path
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            await db.execute("PRAGMA busy_timeout=4000;")
            yield db


async def init_db():
    """
    Execute plain SQL schema setup at startup.
    Ensures tables exist and counters are pre-seeded.
    """
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    async with get_db() as db:
        await db.executescript(schema_sql)
        try:
            await db.execute("ALTER TABLE dm_jobs ADD COLUMN reconcile_attempts INTEGER NOT NULL DEFAULT 0;")
        except Exception:
            pass
        await db.commit()
