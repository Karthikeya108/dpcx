"""Pydantic schemas for API request/response models."""

from datetime import datetime
from typing import Optional


from pydantic import BaseModel


# ─── Data Products ───

class DataProductColumnOut(BaseModel):
    id: str
    column_name: str
    data_type: str
    description: Optional[str] = None
    is_pii: bool = False
    is_nullable: bool = True
    ordinal_position: Optional[int] = None

    model_config = {"from_attributes": True}


class DataProductTableOut(BaseModel):
    id: str
    catalog_name: str
    schema_name: str
    table_name: str
    full_name: str
    table_type: Optional[str] = None
    description: Optional[str] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    columns: list[DataProductColumnOut] = []

    model_config = {"from_attributes": True}


class DataProductOut(BaseModel):
    id: str
    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    domain: str
    subdomain: Optional[str] = None
    tag_value: str
    status: str = "active"
    current_version: str = "1.0.0"
    table_count: int = 0
    total_row_count: int = 0
    has_pii: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class DataProductDetailOut(DataProductOut):
    tables: list[DataProductTableOut] = []
    contract_count: int = 0


# ─── Data Product Versions ───

class DataProductVersionOut(BaseModel):
    id: str
    product_id: str
    version: str
    status: str = "draft"
    change_type: Optional[str] = None
    change_summary: Optional[str] = None
    published_by: Optional[str] = None
    published_at: Optional[datetime] = None
    deprecated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class DetectChangesOut(BaseModel):
    change_type: Optional[str] = None
    diff: dict = {}
    change_summary: str = ""
    current_version: str = ""
    new_version: Optional[str] = None


class VersionDiffOut(BaseModel):
    version_a: str
    version_b: str
    change_type: Optional[str] = None
    diff: dict = {}
    change_summary: str = ""


class PublishVersionIn(BaseModel):
    published_by: Optional[str] = None


# ─── Data Contracts ───

class DataContractCreate(BaseModel):
    product_id: Optional[str] = None
    name: str
    version: str = "1.0.0"
    description: Optional[str] = None
    contract_type: str = "output"
    odcs_yaml: Optional[str] = None
    owner: Optional[str] = None


class DataContractUpdate(BaseModel):
    name: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    contract_type: Optional[str] = None
    odcs_yaml: Optional[str] = None
    owner: Optional[str] = None


class DataContractVersionOut(BaseModel):
    id: str
    version: str
    change_summary: Optional[str] = None
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class DataContractOut(BaseModel):
    id: str
    product_id: Optional[str] = None
    name: str
    version: str
    description: Optional[str] = None
    status: str = "draft"
    contract_type: str = "output"
    odcs_yaml: Optional[str] = None
    owner: Optional[str] = None
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    product_name: Optional[str] = None

    model_config = {"from_attributes": True}


class DataContractDetailOut(DataContractOut):
    versions: list[DataContractVersionOut] = []


class GenerateContractsOut(BaseModel):
    product_id: str
    product_name: str
    contracts_created: int
    contracts: list[DataContractOut]


# ─── Lineage ───

class LineageNodeOut(BaseModel):
    id: str
    name: str
    domain: str
    status: str = "active"
    input_contracts: list[dict] = []
    output_contracts: list[dict] = []


class LineageEdgeOut(BaseModel):
    source: str
    target: str
    source_tables: list[str] = []
    target_tables: list[str] = []


class LineageGraphOut(BaseModel):
    nodes: list[LineageNodeOut]
    edges: list[LineageEdgeOut]


# ─── Scan Jobs ───

class ScanTriggerIn(BaseModel):
    tag_prefix: Optional[str] = None
    tag_suffix: Optional[str] = None
    metastore_id: Optional[str] = None


class ScanJobOut(BaseModel):
    id: str
    job_type: str
    tag_prefix: Optional[str] = None
    tag_suffix: Optional[str] = None
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    tables_found: int = 0
    products_found: int = 0
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


# ─── Settings ───

class AppSettingOut(BaseModel):
    key: str
    value: str
    description: Optional[str] = None

    model_config = {"from_attributes": True}


class AppSettingUpdate(BaseModel):
    value: str
    description: Optional[str] = None


# ─── Stats ───

class DashboardStats(BaseModel):
    total_products: int = 0
    total_tables: int = 0
    total_contracts: int = 0
    products_by_domain: dict[str, int] = {}
    contracts_by_status: dict[str, int] = {}
    pii_product_count: int = 0
    recent_scans: list[ScanJobOut] = []
