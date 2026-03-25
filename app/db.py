"""Database connection layer.

In Databricks App: connects to Lakebase PG via SDK-generated credentials (SP).
In local dev: connects to Lakebase PG via CLI-generated credentials.

Both environments use the same SQLAlchemy ORM and Lakebase PostgreSQL.
"""

import json
import logging
import os
import subprocess
import sys
import threading

import psycopg2
from fastapi import Request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import settings

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

_thread_local = threading.local()
_engine = None


def _is_app_environment():
    return os.getenv("DATABRICKS_CLIENT_ID") is not None


def _generate_pg_credential_sdk():
    """Generate Lakebase PG credential via Databricks SDK (App SP)."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    username = w.current_user.me().user_name
    endpoint = (
        f"projects/{settings.lakebase_project}"
        f"/branches/production/endpoints/primary"
    )
    cred = w.postgres.generate_database_credential(endpoint=endpoint)
    logger.info(f"SDK PG credential for {username}")
    return username, cred.token


def _generate_pg_credential_cli():
    """Generate Lakebase PG credential via Databricks CLI (local dev)."""
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
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(f"CLI current-user failed: {result.stderr}")
    email = json.loads(result.stdout)["userName"]
    return email, token


def _create_pg_connection():
    """Create a PG connection with fresh credentials."""
    if _is_app_environment():
        username, token = _generate_pg_credential_sdk()
    else:
        username, token = _generate_pg_credential_cli()

    return psycopg2.connect(
        host=settings.lakebase_host,
        port=settings.lakebase_port,
        database=settings.lakebase_database,
        user=username,
        password=token,
        sslmode="require",
        connect_timeout=10,
    )


def _get_engine():
    global _engine
    if _engine is not None:
        return _engine

    _engine = create_engine(
        "postgresql://",
        creator=_create_pg_connection,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=300,
    )
    logger.info("Lakebase PG engine created")
    return _engine


def _ensure_tables():
    """Create tables in Lakebase if they don't exist (first deploy)."""
    from app.models.database import Base
    engine = _get_engine()
    Base.metadata.create_all(engine)
    logger.info("Database tables ensured")


def get_session(request: Request):
    """FastAPI dependency: get a DB session."""
    # Store user token in thread-local for UC API calls
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
