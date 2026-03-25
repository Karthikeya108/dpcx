"""SQLAlchemy ORM models — compatible with both PostgreSQL (Lakebase) and SQLite."""

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Integer, BigInteger,
    String, Text, UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship


def _new_uuid():
    return str(uuid.uuid4())


class Base(DeclarativeBase):
    pass


class DataProduct(Base):
    __tablename__ = "data_products"

    id = Column(String(36), primary_key=True, default=_new_uuid)
    name = Column(String(255), nullable=False)
    display_name = Column(String(255))
    description = Column(Text)
    domain = Column(String(255), nullable=False)
    subdomain = Column(String(255))
    tag_value = Column(String(255), nullable=False, unique=True)
    status = Column(String(50), default="active")
    current_version = Column(String(50), default="1.0.0")
    table_count = Column(Integer, default=0)
    total_row_count = Column(BigInteger, default=0)
    has_pii = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    tables = relationship("DataProductTable", back_populates="product", cascade="all, delete-orphan")
    contracts = relationship("DataContract", back_populates="product")
    versions = relationship("DataProductVersion", back_populates="product", cascade="all, delete-orphan")
    source_lineage = relationship(
        "DataProductLineage",
        foreign_keys="DataProductLineage.source_product_id",
        back_populates="source_product",
    )
    target_lineage = relationship(
        "DataProductLineage",
        foreign_keys="DataProductLineage.target_product_id",
        back_populates="target_product",
    )


class DataProductTable(Base):
    __tablename__ = "data_product_tables"

    id = Column(String(36), primary_key=True, default=_new_uuid)
    product_id = Column(String(36), ForeignKey("data_products.id", ondelete="CASCADE"), nullable=False)
    catalog_name = Column(String(255), nullable=False)
    schema_name = Column(String(255), nullable=False)
    table_name = Column(String(255), nullable=False)
    full_name = Column(String(768), nullable=False, unique=True)
    table_type = Column(String(50))
    description = Column(Text)
    row_count = Column(BigInteger)
    column_count = Column(Integer)
    size_bytes = Column(BigInteger)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    product = relationship("DataProduct", back_populates="tables")
    columns = relationship("DataProductColumn", back_populates="table", cascade="all, delete-orphan")


class DataProductColumn(Base):
    __tablename__ = "data_product_columns"

    id = Column(String(36), primary_key=True, default=_new_uuid)
    table_id = Column(String(36), ForeignKey("data_product_tables.id", ondelete="CASCADE"), nullable=False)
    column_name = Column(String(255), nullable=False)
    data_type = Column(String(100), nullable=False)
    description = Column(Text)
    is_pii = Column(Boolean, default=False)
    is_nullable = Column(Boolean, default=True)
    ordinal_position = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)

    table = relationship("DataProductTable", back_populates="columns")


class DataProductVersion(Base):
    __tablename__ = "data_product_versions"

    id = Column(String(36), primary_key=True, default=_new_uuid)
    product_id = Column(String(36), ForeignKey("data_products.id", ondelete="CASCADE"), nullable=False)
    version = Column(String(50), nullable=False)
    status = Column(String(50), default="draft")  # draft, published, deprecated, retired
    change_type = Column(String(20))  # major, minor, patch
    change_summary = Column(Text)
    schema_snapshot_json = Column(Text, default="{}")
    published_by = Column(String(255))
    published_at = Column(DateTime)
    deprecated_at = Column(DateTime)
    parent_version_id = Column(String(36), ForeignKey("data_product_versions.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    product = relationship("DataProduct", back_populates="versions")
    parent_version = relationship("DataProductVersion", remote_side="DataProductVersion.id")

    __table_args__ = (UniqueConstraint("product_id", "version", name="uq_product_version"),)

    @property
    def schema_snapshot(self):
        import json as _json
        return _json.loads(self.schema_snapshot_json or "{}")

    @schema_snapshot.setter
    def schema_snapshot(self, val):
        import json as _json
        self.schema_snapshot_json = _json.dumps(val or {})


class DataContract(Base):
    __tablename__ = "data_contracts"

    id = Column(String(36), primary_key=True, default=_new_uuid)
    product_id = Column(String(36), ForeignKey("data_products.id", ondelete="SET NULL"))
    name = Column(String(255), nullable=False)
    version = Column(String(50), nullable=False, default="1.0.0")
    description = Column(Text)
    status = Column(String(50), default="draft")
    contract_type = Column(String(50), default="output")
    odcs_yaml = Column(Text)
    owner = Column(String(255))
    created_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    product = relationship("DataProduct", back_populates="contracts")
    versions = relationship("DataContractVersion", back_populates="contract", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint("name", "version", name="uq_contract_name_version"),)


class DataContractVersion(Base):
    __tablename__ = "data_contract_versions"

    id = Column(String(36), primary_key=True, default=_new_uuid)
    contract_id = Column(String(36), ForeignKey("data_contracts.id", ondelete="CASCADE"), nullable=False)
    version = Column(String(50), nullable=False)
    odcs_yaml = Column(Text)
    change_summary = Column(Text)
    created_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)

    contract = relationship("DataContract", back_populates="versions")


class ScanJob(Base):
    __tablename__ = "scan_jobs"

    id = Column(String(36), primary_key=True, default=_new_uuid)
    job_type = Column(String(50), nullable=False, default="tag_scan")
    tag_prefix = Column(String(255))
    tag_suffix = Column(String(255))
    metastore_id = Column(String(255))
    status = Column(String(50), default="pending")
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    tables_found = Column(Integer, default=0)
    products_found = Column(Integer, default=0)
    error_message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class AppSetting(Base):
    __tablename__ = "app_settings"

    key = Column(String(255), primary_key=True)
    value = Column(Text, nullable=False)
    description = Column(Text)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DataProductLineage(Base):
    __tablename__ = "data_product_lineage"

    id = Column(String(36), primary_key=True, default=_new_uuid)
    source_product_id = Column(String(36), ForeignKey("data_products.id", ondelete="CASCADE"), nullable=False)
    target_product_id = Column(String(36), ForeignKey("data_products.id", ondelete="CASCADE"), nullable=False)
    source_tables_json = Column(Text, default="[]")
    target_tables_json = Column(Text, default="[]")
    last_refreshed = Column(DateTime, default=datetime.utcnow)

    source_product = relationship("DataProduct", foreign_keys=[source_product_id], back_populates="source_lineage")
    target_product = relationship("DataProduct", foreign_keys=[target_product_id], back_populates="target_lineage")

    __table_args__ = (UniqueConstraint("source_product_id", "target_product_id", name="uq_lineage"),)

    @property
    def source_tables(self):
        import json
        return json.loads(self.source_tables_json or "[]")

    @source_tables.setter
    def source_tables(self, val):
        import json
        self.source_tables_json = json.dumps(val or [])

    @property
    def target_tables(self):
        import json
        return json.loads(self.target_tables_json or "[]")

    @target_tables.setter
    def target_tables(self, val):
        import json
        self.target_tables_json = json.dumps(val or [])
