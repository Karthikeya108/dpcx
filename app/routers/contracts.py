"""Data Contracts API routes."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import Response
from sqlalchemy.orm import Session

from app.db import get_session
from app.models.database import DataContract, DataContractVersion, DataProduct
from app.models.schemas import (
    DataContractCreate, DataContractDetailOut, DataContractOut, DataContractUpdate,
)
from app.services.odcs import generate_odcs_yaml, parse_odcs_yaml

router = APIRouter(prefix="/api/data-contracts", tags=["Data Contracts"])


@router.get("", response_model=list[DataContractOut])
def list_contracts(
    status: str = None,
    product_id: str = None,
    db: Session = Depends(get_session),
):
    query = db.query(DataContract)
    if status:
        query = query.filter(DataContract.status == status)
    if product_id:
        query = query.filter(DataContract.product_id == product_id)

    contracts = query.order_by(DataContract.name).all()
    results = []
    for c in contracts:
        out = DataContractOut.model_validate(c)
        if c.product:
            out.product_name = c.product.display_name or c.product.name
        results.append(out)
    return results


@router.get("/{contract_id}", response_model=DataContractDetailOut)
def get_contract(contract_id: str, db: Session = Depends(get_session)):
    contract = db.query(DataContract).filter_by(id=contract_id).first()
    if not contract:
        raise HTTPException(status_code=404, detail="Data contract not found")

    result = DataContractDetailOut.model_validate(contract)
    if contract.product:
        result.product_name = contract.product.display_name or contract.product.name
    return result


@router.post("", response_model=DataContractOut)
def create_contract(payload: DataContractCreate, db: Session = Depends(get_session)):
    product = None
    if payload.product_id:
        product = db.query(DataProduct).filter_by(id=payload.product_id).first()
        if not product:
            raise HTTPException(status_code=404, detail="Data product not found")

    contract = DataContract(
        product_id=payload.product_id,
        name=payload.name,
        version=payload.version,
        description=payload.description,
        contract_type=payload.contract_type,
        owner=payload.owner,
    )

    # Generate ODCS YAML if not provided
    if payload.odcs_yaml:
        contract.odcs_yaml = payload.odcs_yaml
    elif product:
        contract.odcs_yaml = generate_odcs_yaml(contract, product)

    db.add(contract)
    db.commit()
    db.refresh(contract)

    # Create initial version
    version = DataContractVersion(
        contract_id=contract.id,
        version=contract.version,
        odcs_yaml=contract.odcs_yaml,
        change_summary="Initial creation",
    )
    db.add(version)
    db.commit()

    out = DataContractOut.model_validate(contract)
    if product:
        out.product_name = product.display_name or product.name
    return out


@router.put("/{contract_id}", response_model=DataContractOut)
def update_contract(
    contract_id: str,
    payload: DataContractUpdate,
    db: Session = Depends(get_session),
):
    contract = db.query(DataContract).filter_by(id=contract_id).first()
    if not contract:
        raise HTTPException(status_code=404, detail="Data contract not found")

    old_version = contract.version
    update_data = payload.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(contract, key, value)

    # If version changed, create new version record
    if payload.version and payload.version != old_version:
        version = DataContractVersion(
            contract_id=contract.id,
            version=payload.version,
            odcs_yaml=contract.odcs_yaml,
            change_summary=f"Updated from {old_version} to {payload.version}",
        )
        db.add(version)

    db.commit()
    db.refresh(contract)

    out = DataContractOut.model_validate(contract)
    if contract.product:
        out.product_name = contract.product.display_name or contract.product.name
    return out


@router.get("/{contract_id}/odcs")
def download_odcs(contract_id: str, db: Session = Depends(get_session)):
    contract = db.query(DataContract).filter_by(id=contract_id).first()
    if not contract:
        raise HTTPException(status_code=404, detail="Data contract not found")

    yaml_content = contract.odcs_yaml
    if not yaml_content and contract.product:
        yaml_content = generate_odcs_yaml(contract, contract.product)
        contract.odcs_yaml = yaml_content
        db.commit()

    if not yaml_content:
        raise HTTPException(status_code=404, detail="No ODCS YAML available")

    filename = f"{contract.name}-v{contract.version}.odcs.yaml"
    return Response(
        content=yaml_content,
        media_type="application/x-yaml",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/upload", response_model=DataContractOut)
async def upload_odcs(
    file: UploadFile = File(...),
    product_id: str = None,
    db: Session = Depends(get_session),
):
    content = await file.read()
    yaml_content = content.decode("utf-8")

    parsed = parse_odcs_yaml(yaml_content)

    contract = DataContract(
        product_id=product_id,
        name=parsed["name"],
        version=parsed["version"],
        description=parsed["description"],
        status=parsed["status"],
        contract_type=parsed["contract_type"],
        owner=parsed["owner"],
        odcs_yaml=yaml_content,
    )
    db.add(contract)
    db.commit()
    db.refresh(contract)

    version = DataContractVersion(
        contract_id=contract.id,
        version=contract.version,
        odcs_yaml=yaml_content,
        change_summary="Uploaded from ODCS YAML file",
    )
    db.add(version)
    db.commit()

    return DataContractOut.model_validate(contract)
