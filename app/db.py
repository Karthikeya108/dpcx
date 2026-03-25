"""Database connection layer.

In Databricks App: uses SQLite for metadata storage (persists within container).
On startup/sync, data is populated from Unity Catalog via SQL Statement API.

In local dev: connects to Lakebase PG via CLI-generated credentials.

Both environments use the same SQLAlchemy ORM models.
"""

import json
import logging
import os
import subprocess
import sys
import threading

from fastapi import Request
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from app.config import settings

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

_thread_local = threading.local()
_engine = None

SQLITE_PATH = "/tmp/data_products.db"


def _is_app_environment():
    return os.getenv("DATABRICKS_CLIENT_ID") is not None


def _get_workspace_host():
    host = os.getenv("DATABRICKS_HOST", settings.workspace_url)
    if not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/")


def _create_lakebase_engine():
    """Create SQLAlchemy engine for Lakebase PG (local dev)."""
    import psycopg2

    def _create_pg_connection():
        profile = settings.databricks_profile
        endpoint = (
            f"projects/{settings.lakebase_project}"
            f"/branches/production/endpoints/primary"
        )
        result = subprocess.run(
            ["databricks", "postgres", "generate-database-credential",
             endpoint, "--profile", profile, "--output", "json"],
            capture_output=True, text=True,
        )
        if result.returncode != 0 or not result.stdout.strip():
            raise RuntimeError(f"CLI failed: {result.stderr}")
        token = json.loads(result.stdout)["token"]

        result = subprocess.run(
            ["databricks", "current-user", "me",
             "--profile", profile, "--output", "json"],
            capture_output=True, text=True,
        )
        email = json.loads(result.stdout)["userName"]

        return psycopg2.connect(
            host=settings.lakebase_host,
            port=settings.lakebase_port,
            database=settings.lakebase_database,
            user=email,
            password=token,
            sslmode="require",
            connect_timeout=10,
        )

    return create_engine(
        "postgresql://",
        creator=_create_pg_connection,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=300,
    )


def _create_sqlite_engine():
    """Create SQLAlchemy engine for SQLite (app environment)."""
    import os as _os
    import sqlite3
    from app.models.database import Base

    # Check if existing DB has the latest schema — if not, recreate
    if _os.path.exists(SQLITE_PATH):
        try:
            conn = sqlite3.connect(SQLITE_PATH)
            cur = conn.execute("PRAGMA table_info(data_products)")
            cols = {row[1] for row in cur.fetchall()}
            conn.close()
            if "current_version" not in cols:
                logger.info("SQLite schema outdated — recreating database")
                _os.remove(SQLITE_PATH)
        except Exception:
            _os.remove(SQLITE_PATH)

    engine = create_engine(
        f"sqlite:///{SQLITE_PATH}",
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )

    # Create all tables if they don't exist
    Base.metadata.create_all(engine)
    logger.info(f"SQLite database initialized at {SQLITE_PATH}")
    return engine


def _get_engine():
    global _engine
    if _engine is not None:
        return _engine

    if _is_app_environment():
        _engine = _create_sqlite_engine()
    else:
        try:
            _engine = _create_lakebase_engine()
        except Exception as e:
            logger.warning(f"Lakebase connection failed, falling back to SQLite: {e}")
            _engine = _create_sqlite_engine()

    return _engine


def get_session(request: Request):
    """FastAPI dependency: get a DB session."""
    # Store user token in thread-local for UC API calls (used by unity_catalog service)
    _thread_local.user_token = _extract_user_token(request)
    _thread_local.user_email = request.headers.get("x-forwarded-email")

    engine = _get_engine()
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()


def _extract_user_token(request: Request) -> str | None:
    """Extract the user's OAuth token from the request."""
    forwarded = request.headers.get("x-forwarded-access-token")
    if forwarded:
        return forwarded
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:]
    return None
