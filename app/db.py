import os
import aiosqlite
from pathlib import Path
from contextlib import asynccontextmanager
from app.config import settings

SCHEMA_PATH = Path(__file__).parent.parent / "data" / "schema.sql"


@asynccontextmanager
async def get_db():
    """
    Async context manager for aiosqlite connection with WAL mode enabled.
    Yields an active aiosqlite.Connection object and automatically closes it on exit.
    """
    db_path = settings.db_path
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
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
        await db.commit()
