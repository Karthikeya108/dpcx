"""Data Products Manager - FastAPI Application."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import OperationalError

from app.db import _ensure_tables, _get_engine
from app.routers import contracts, products, settings

logger = logging.getLogger(__name__)


def _seed_default_settings():
    """Insert default app settings if the database is empty."""
    from sqlalchemy.orm import sessionmaker
    from app.models.database import AppSetting

    engine = _get_engine()
    session = sessionmaker(bind=engine)()
    try:
        if session.query(AppSetting).count() == 0:
            defaults = [
                ("workspace_url", "https://adb-4116661263058619.19.azuredatabricks.net", "Databricks workspace URL"),
                ("warehouse_id", "fe12763ffa92c9b5", "SQL warehouse ID"),
                ("lakebase_host", "ep-square-bird-ea8u2b9t.database.northeurope.azuredatabricks.net", "Lakebase endpoint"),
                ("lakebase_project", "data-products-metadata", "Lakebase project name"),
                ("lakebase_database", "data_products_metadata", "Lakebase database name"),
                ("scan_tag_key", "data_product", "Governed tag key for data products"),
            ]
            for key, value, desc in defaults:
                session.add(AppSetting(key=key, value=value, description=desc))
            session.commit()
            logger.info("Default settings seeded")
    except Exception as e:
        logger.warning(f"Failed to seed settings: {e}")
        session.rollback()
    finally:
        session.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        _ensure_tables()
        _seed_default_settings()
    except Exception as e:
        logger.error(f"Startup failed: {e}")
    yield


app = FastAPI(
    title="Data Products Manager",
    description="Manage data products, data contracts, and lineage on Databricks",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(OperationalError)
async def db_connection_error_handler(request: Request, exc: OperationalError):
    return JSONResponse(
        status_code=503,
        content={"error": "Database connection failed", "detail": str(exc)[:300]},
    )


@app.exception_handler(RuntimeError)
async def runtime_error_handler(request: Request, exc: RuntimeError):
    return JSONResponse(
        status_code=500,
        content={"error": "Runtime error", "detail": str(exc)[:500]},
    )


app.include_router(products.router)
app.include_router(contracts.router)
app.include_router(settings.router)


@app.get("/api/health")
def health():
    return {"status": "ok"}


# Serve React frontend in production
frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"
if frontend_dist.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="frontend")
