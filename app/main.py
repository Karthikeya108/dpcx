"""Data Products Manager - FastAPI Application."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import OperationalError

from app.db import _get_engine, _is_app_environment
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


def _auto_sync_on_startup():
    """Auto-sync data products from UC on app startup if database is empty."""
    from sqlalchemy.orm import sessionmaker
    from app.models.database import DataProduct

    engine = _get_engine()
    session = sessionmaker(bind=engine)()
    try:
        count = session.query(DataProduct).count()
        if count == 0 and _is_app_environment():
            logger.info("Database empty — triggering auto-sync from Unity Catalog...")
            from app.services.unity_catalog import sync_products_from_uc
            scan = sync_products_from_uc(session)
            logger.info(f"Auto-sync: {scan.products_found} products, {scan.tables_found} tables")

            if scan.products_found > 0:
                # Auto-generate contracts
                from app.services.odcs import generate_odcs_for_asset
                from app.models.database import DataContract, DataContractVersion
                products = session.query(DataProduct).all()
                contract_count = 0
                for product in products:
                    for table in product.tables:
                        contract_name = f"{product.tag_value}.{table.table_name}"
                        domain_short = product.domain.replace("ins_", "")
                        contract = DataContract(
                            product_id=product.id,
                            name=contract_name,
                            version="1.0.0",
                            description=table.description or f"Output port contract for {table.full_name}",
                            status="draft",
                            contract_type="output",
                            owner=f"{domain_short.title()} Data Engineering",
                        )
                        session.add(contract)
                        session.flush()
                        contract.odcs_yaml = generate_odcs_for_asset(contract, product, table)
                        session.add(DataContractVersion(
                            contract_id=contract.id, version="1.0.0",
                            odcs_yaml=contract.odcs_yaml,
                            change_summary=f"Auto-generated for {table.full_name}",
                        ))
                        contract_count += 1
                session.commit()
                logger.info(f"Auto-generated {contract_count} contracts")

                # Seed lineage for risk_analytics
                _seed_lineage(session)
    except Exception as e:
        logger.error(f"Auto-sync failed: {e}")
        session.rollback()
    finally:
        session.close()


def _seed_lineage(session):
    """Seed lineage for the risk_analytics derived product."""
    from app.models.database import DataProduct, DataProductLineage

    risk = session.query(DataProduct).filter_by(tag_value="risk_analytics").first()
    if not risk:
        return

    sources = ["customer_360", "claims_financial", "underwriting_risk", "policy_lifecycle", "claims_management"]
    for tag in sources:
        src = session.query(DataProduct).filter_by(tag_value=tag).first()
        if src:
            existing = session.query(DataProductLineage).filter_by(
                source_product_id=src.id, target_product_id=risk.id
            ).first()
            if not existing:
                lineage = DataProductLineage(
                    source_product_id=src.id,
                    target_product_id=risk.id,
                )
                lineage.source_tables = [f"{tag}.*"]
                lineage.target_tables = ["ins_policy.risk_analytics.*"]
                session.add(lineage)
    session.commit()
    logger.info("Lineage seeded for risk_analytics")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        _seed_default_settings()
    except Exception as e:
        logger.error(f"Startup seed failed: {e}")
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
