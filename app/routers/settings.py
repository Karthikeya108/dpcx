"""Settings and Scan API routes."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_session
from app.models.database import AppSetting, ScanJob
from app.models.schemas import (
    AppSettingOut, AppSettingUpdate, DashboardStats,
    ScanJobOut, ScanTriggerIn,
)
from app.models.database import DataContract, DataProduct
from app.services.unity_catalog import sync_products_from_uc

router = APIRouter(tags=["Settings & Stats"])


# ─── Dashboard Stats ───

@router.get("/api/stats", response_model=DashboardStats)
def get_stats(db: Session = Depends(get_session)):
    total_products = db.query(DataProduct).count()
    total_tables = 0
    pii_count = 0
    domain_counts = {}

    products = db.query(DataProduct).all()
    for p in products:
        total_tables += p.table_count
        if p.has_pii:
            pii_count += 1
        domain_counts[p.domain] = domain_counts.get(p.domain, 0) + 1

    total_contracts = db.query(DataContract).count()
    contracts = db.query(DataContract).all()
    status_counts = {}
    for c in contracts:
        status_counts[c.status] = status_counts.get(c.status, 0) + 1

    recent_scans = (
        db.query(ScanJob)
        .order_by(ScanJob.created_at.desc())
        .limit(5)
        .all()
    )

    return DashboardStats(
        total_products=total_products,
        total_tables=total_tables,
        total_contracts=total_contracts,
        products_by_domain=domain_counts,
        contracts_by_status=status_counts,
        pii_product_count=pii_count,
        recent_scans=[ScanJobOut.model_validate(s) for s in recent_scans],
    )


# ─── Settings ───

@router.get("/api/settings", response_model=list[AppSettingOut])
def get_settings(db: Session = Depends(get_session)):
    return db.query(AppSetting).order_by(AppSetting.key).all()


@router.put("/api/settings/{key}", response_model=AppSettingOut)
def update_setting(key: str, payload: AppSettingUpdate, db: Session = Depends(get_session)):
    setting = db.query(AppSetting).filter_by(key=key).first()
    if not setting:
        setting = AppSetting(key=key, value=payload.value, description=payload.description)
        db.add(setting)
    else:
        setting.value = payload.value
        if payload.description is not None:
            setting.description = payload.description
    db.commit()
    db.refresh(setting)
    return setting


# ─── Scan ───

@router.post("/api/scan/trigger", response_model=ScanJobOut)
def trigger_scan(payload: ScanTriggerIn, db: Session = Depends(get_session)):
    scan_job = sync_products_from_uc(
        db,
        tag_prefix=payload.tag_prefix,
        tag_suffix=payload.tag_suffix,
    )
    return scan_job


@router.get("/api/scan/history", response_model=list[ScanJobOut])
def scan_history(limit: int = 20, db: Session = Depends(get_session)):
    return (
        db.query(ScanJob)
        .order_by(ScanJob.created_at.desc())
        .limit(limit)
        .all()
    )
