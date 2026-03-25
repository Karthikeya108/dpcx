"""Data Products API routes."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_session
from app.models.database import DataContract, DataContractVersion, DataProduct
from app.models.schemas import (
    DataContractOut, DataProductDetailOut, DataProductOut,
    DataProductVersionOut, DetectChangesOut, GenerateContractsOut,
    LineageGraphOut, PublishVersionIn, ScanJobOut, VersionDiffOut,
)
from app.services.odcs import generate_odcs_for_asset
from app.services.unity_catalog import get_product_lineage, sync_products_from_uc
from app.services import versioning

router = APIRouter(prefix="/api/data-products", tags=["Data Products"])


@router.get("", response_model=list[DataProductOut])
def list_products(
    domain: str = None,
    status: str = None,
    db: Session = Depends(get_session),
):
    query = db.query(DataProduct)
    if domain:
        query = query.filter(DataProduct.domain == domain)
    if status:
        query = query.filter(DataProduct.status == status)
    return query.order_by(DataProduct.name).all()


@router.get("/{product_id}", response_model=DataProductDetailOut)
def get_product(product_id: str, db: Session = Depends(get_session)):
    product = db.query(DataProduct).filter_by(id=product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Data product not found")

    result = DataProductDetailOut.model_validate(product)
    result.contract_count = len(product.contracts)
    return result


@router.post("/sync", response_model=ScanJobOut)
def sync_from_uc(
    tag_prefix: str = None,
    tag_suffix: str = None,
    db: Session = Depends(get_session),
):
    scan_job = sync_products_from_uc(db, tag_prefix=tag_prefix, tag_suffix=tag_suffix)
    return scan_job


@router.post("/{product_id}/generate-contracts", response_model=GenerateContractsOut)
def generate_contracts(product_id: str, db: Session = Depends(get_session)):
    """Auto-generate one output port ODCS contract per asset (table) in a data product.

    Each contract is fully populated with:
    - Table name, full path, description
    - All column names, types, descriptions, nullability
    - PII classification and compliance tags
    - Data quality rules (completeness, PII handling)
    - Technical owner, business owner, data governance officer
    - Domain and subdomain tags
    - Service level agreements
    - Output port metadata
    """
    product = db.query(DataProduct).filter_by(id=product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Data product not found")

    if not product.tables:
        raise HTTPException(
            status_code=400,
            detail="Data product has no tables. Sync from Unity Catalog first.",
        )

    created_contracts = []

    for table in product.tables:
        # Check if a contract already exists for this table
        contract_name = f"{product.tag_value}.{table.table_name}"
        existing = db.query(DataContract).filter_by(
            name=contract_name, product_id=product.id
        ).first()
        if existing:
            out = DataContractOut.model_validate(existing)
            out.product_name = product.display_name or product.name
            created_contracts.append(out)
            continue

        # Derive owner from domain
        domain_short = product.domain.replace("ins_", "")
        technical_owner = f"{domain_short.title()} Data Engineering"

        contract = DataContract(
            product_id=product.id,
            name=contract_name,
            version="1.0.0",
            description=table.description or f"Output port contract for {table.full_name}",
            status="draft",
            contract_type="output",
            owner=technical_owner,
        )
        db.add(contract)
        db.flush()

        # Generate the full ODCS YAML for this single asset
        contract.odcs_yaml = generate_odcs_for_asset(contract, product, table)

        db.add(contract)
        db.flush()

        # Create initial version record
        version = DataContractVersion(
            contract_id=contract.id,
            version="1.0.0",
            odcs_yaml=contract.odcs_yaml,
            change_summary=f"Auto-generated output port contract for {table.full_name}",
        )
        db.add(version)

        out = DataContractOut.model_validate(contract)
        out.product_name = product.display_name or product.name
        created_contracts.append(out)

    db.commit()

    return GenerateContractsOut(
        product_id=product.id,
        product_name=product.display_name or product.name,
        contracts_created=len(created_contracts),
        contracts=created_contracts,
    )


@router.get("/{product_id}/lineage", response_model=LineageGraphOut)
def get_lineage(product_id: str, db: Session = Depends(get_session)):
    result = get_product_lineage(db, str(product_id))
    return result


# ─── Versioning ───

@router.get("/{product_id}/versions", response_model=list[DataProductVersionOut])
def list_versions(product_id: str, db: Session = Depends(get_session)):
    from app.models.database import DataProductVersion
    return (
        db.query(DataProductVersion)
        .filter_by(product_id=product_id)
        .order_by(DataProductVersion.created_at.desc())
        .all()
    )


@router.post("/{product_id}/versions/detect", response_model=DetectChangesOut)
def detect_version_changes(product_id: str, db: Session = Depends(get_session)):
    """Detect schema changes between current UC state and last published version."""
    try:
        result = versioning.detect_changes(db, product_id)
        return DetectChangesOut(**result)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{product_id}/versions", response_model=DataProductVersionOut)
def create_version(product_id: str, db: Session = Depends(get_session)):
    """Create a new draft version from current product state."""
    try:
        pv = versioning.create_version(db, product_id)
        return DataProductVersionOut.model_validate(pv)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{product_id}/versions/{version_id}/publish", response_model=DataProductVersionOut)
def publish_version(
    product_id: str,
    version_id: str,
    payload: PublishVersionIn = None,
    db: Session = Depends(get_session),
):
    """Publish a draft version — freezes the schema snapshot."""
    try:
        published_by = payload.published_by if payload else None
        pv = versioning.publish_version(db, version_id, published_by)
        return DataProductVersionOut.model_validate(pv)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{product_id}/versions/{version_id}/deprecate", response_model=DataProductVersionOut)
def deprecate_version(product_id: str, version_id: str, db: Session = Depends(get_session)):
    """Deprecate a published version."""
    try:
        pv = versioning.deprecate_version(db, version_id)
        return DataProductVersionOut.model_validate(pv)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{product_id}/versions/{va_id}/diff/{vb_id}", response_model=VersionDiffOut)
def diff_versions(product_id: str, va_id: str, vb_id: str, db: Session = Depends(get_session)):
    """Compute the diff between two versions."""
    try:
        result = versioning.diff_versions(db, va_id, vb_id)
        return VersionDiffOut(**result)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
