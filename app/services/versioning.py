"""Data product versioning service.

Versions track schema changes over time. A version is a frozen snapshot of the
product's tables, columns, types, descriptions, PII flags, and tags.

Change detection compares the current UC metadata against the latest published
version's snapshot to auto-classify changes as major, minor, or patch.
"""

import json
import logging
from datetime import datetime

from sqlalchemy.orm import Session

from app.models.database import DataProduct, DataProductVersion

logger = logging.getLogger(__name__)


def _build_schema_snapshot(product: DataProduct) -> dict:
    """Build a schema snapshot dict from the current product state."""
    tables = {}
    for t in product.tables:
        columns = {}
        for c in sorted(t.columns, key=lambda x: x.ordinal_position or 0):
            columns[c.column_name] = {
                "data_type": c.data_type,
                "description": c.description or "",
                "is_pii": c.is_pii,
                "is_nullable": c.is_nullable,
            }
        tables[t.full_name] = {
            "table_name": t.table_name,
            "description": t.description or "",
            "table_type": t.table_type or "MANAGED",
            "columns": columns,
        }

    return {
        "product_name": product.display_name or product.name,
        "domain": product.domain,
        "subdomain": product.subdomain or "",
        "tag_value": product.tag_value,
        "has_pii": product.has_pii,
        "tables": tables,
    }


def _compute_diff(old_snapshot: dict, new_snapshot: dict) -> dict:
    """Compare two schema snapshots and return a structured diff."""
    old_tables = set(old_snapshot.get("tables", {}).keys())
    new_tables = set(new_snapshot.get("tables", {}).keys())

    added_tables = new_tables - old_tables
    removed_tables = old_tables - new_tables
    common_tables = old_tables & new_tables

    added_columns = []
    removed_columns = []
    type_changes = []
    description_changes = []
    pii_changes = []

    for table_name in common_tables:
        old_cols = old_snapshot["tables"][table_name].get("columns", {})
        new_cols = new_snapshot["tables"][table_name].get("columns", {})

        for col in set(new_cols.keys()) - set(old_cols.keys()):
            added_columns.append(f"{table_name}.{col}")

        for col in set(old_cols.keys()) - set(new_cols.keys()):
            removed_columns.append(f"{table_name}.{col}")

        for col in set(old_cols.keys()) & set(new_cols.keys()):
            if old_cols[col]["data_type"] != new_cols[col]["data_type"]:
                type_changes.append(
                    f"{table_name}.{col}: {old_cols[col]['data_type']} -> {new_cols[col]['data_type']}"
                )
            if old_cols[col].get("description", "") != new_cols[col].get("description", ""):
                description_changes.append(f"{table_name}.{col}")
            if old_cols[col].get("is_pii") != new_cols[col].get("is_pii"):
                pii_changes.append(f"{table_name}.{col}")

    return {
        "added_tables": sorted(added_tables),
        "removed_tables": sorted(removed_tables),
        "added_columns": sorted(added_columns),
        "removed_columns": sorted(removed_columns),
        "type_changes": sorted(type_changes),
        "description_changes": sorted(description_changes),
        "pii_changes": sorted(pii_changes),
    }


def _classify_change(diff: dict) -> str | None:
    """Classify a diff as major, minor, patch, or None (no change)."""
    if diff["removed_tables"] or diff["removed_columns"] or diff["type_changes"]:
        return "major"
    if diff["added_tables"] or diff["added_columns"]:
        return "minor"
    if diff["description_changes"] or diff["pii_changes"]:
        return "patch"
    return None


def _format_change_summary(diff: dict, change_type: str) -> str:
    """Build a human-readable change summary from a diff."""
    parts = []
    if diff["added_tables"]:
        parts.append(f"Added tables: {', '.join(diff['added_tables'])}")
    if diff["removed_tables"]:
        parts.append(f"Removed tables: {', '.join(diff['removed_tables'])}")
    if diff["added_columns"]:
        parts.append(f"Added columns: {', '.join(diff['added_columns'])}")
    if diff["removed_columns"]:
        parts.append(f"Removed columns: {', '.join(diff['removed_columns'])}")
    if diff["type_changes"]:
        parts.append(f"Type changes: {', '.join(diff['type_changes'])}")
    if diff["description_changes"]:
        parts.append(f"Description changes: {len(diff['description_changes'])} columns")
    if diff["pii_changes"]:
        parts.append(f"PII classification changes: {', '.join(diff['pii_changes'])}")
    return "; ".join(parts) if parts else "No changes detected"


def _bump_version(current: str, change_type: str) -> str:
    """Bump a semver version string based on change type."""
    parts = current.split(".")
    if len(parts) != 3:
        parts = ["1", "0", "0"]
    major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])

    if change_type == "major":
        return f"{major + 1}.0.0"
    elif change_type == "minor":
        return f"{major}.{minor + 1}.0"
    else:
        return f"{major}.{minor}.{patch + 1}"


def get_latest_published_version(db: Session, product_id: str) -> DataProductVersion | None:
    """Get the latest published version for a product."""
    return (
        db.query(DataProductVersion)
        .filter_by(product_id=product_id, status="published")
        .order_by(DataProductVersion.created_at.desc())
        .first()
    )


def detect_changes(db: Session, product_id: str) -> dict:
    """Detect schema changes between current UC state and last published version.

    Returns a dict with: change_type, diff, current_version, new_version, snapshot.
    """
    product = db.query(DataProduct).filter_by(id=product_id).first()
    if not product:
        raise ValueError("Product not found")

    current_snapshot = _build_schema_snapshot(product)
    latest = get_latest_published_version(db, product_id)

    if not latest:
        return {
            "change_type": "major",
            "diff": {
                "added_tables": list(current_snapshot["tables"].keys()),
                "removed_tables": [], "added_columns": [], "removed_columns": [],
                "type_changes": [], "description_changes": [], "pii_changes": [],
            },
            "change_summary": "Initial version — no previous published version exists",
            "current_version": product.current_version,
            "new_version": "1.0.0",
            "snapshot": current_snapshot,
        }

    old_snapshot = latest.schema_snapshot
    diff = _compute_diff(old_snapshot, current_snapshot)
    change_type = _classify_change(diff)

    if not change_type:
        return {
            "change_type": None,
            "diff": diff,
            "change_summary": "No changes detected",
            "current_version": product.current_version,
            "new_version": None,
            "snapshot": current_snapshot,
        }

    new_version = _bump_version(latest.version, change_type)

    return {
        "change_type": change_type,
        "diff": diff,
        "change_summary": _format_change_summary(diff, change_type),
        "current_version": product.current_version,
        "new_version": new_version,
        "snapshot": current_snapshot,
    }


def create_version(
    db: Session,
    product_id: str,
    version: str = None,
    change_type: str = None,
    change_summary: str = None,
    status: str = "draft",
) -> DataProductVersion:
    """Create a new version for a product from current state."""
    product = db.query(DataProduct).filter_by(id=product_id).first()
    if not product:
        raise ValueError("Product not found")

    snapshot = _build_schema_snapshot(product)
    latest = get_latest_published_version(db, product_id)

    if not version:
        if latest:
            diff = _compute_diff(latest.schema_snapshot, snapshot)
            change_type = change_type or _classify_change(diff) or "patch"
            version = _bump_version(latest.version, change_type)
            change_summary = change_summary or _format_change_summary(diff, change_type)
        else:
            version = "1.0.0"
            change_type = "major"
            change_summary = change_summary or "Initial version"

    pv = DataProductVersion(
        product_id=product_id,
        version=version,
        status=status,
        change_type=change_type,
        change_summary=change_summary,
        parent_version_id=latest.id if latest else None,
    )
    pv.schema_snapshot = snapshot
    db.add(pv)
    db.flush()

    product.current_version = version
    product.updated_at = datetime.utcnow()
    db.commit()

    logger.info(f"Version {version} ({change_type}) created for {product.name}")
    return pv


def publish_version(db: Session, version_id: str, published_by: str = None) -> DataProductVersion:
    """Publish a draft version — freezes the schema snapshot."""
    pv = db.query(DataProductVersion).filter_by(id=version_id).first()
    if not pv:
        raise ValueError("Version not found")
    if pv.status != "draft":
        raise ValueError(f"Cannot publish version in '{pv.status}' status")

    pv.status = "published"
    pv.published_by = published_by
    pv.published_at = datetime.utcnow()
    db.commit()

    logger.info(f"Version {pv.version} published for product {pv.product_id}")
    return pv


def deprecate_version(db: Session, version_id: str) -> DataProductVersion:
    """Deprecate a published version."""
    pv = db.query(DataProductVersion).filter_by(id=version_id).first()
    if not pv:
        raise ValueError("Version not found")
    if pv.status != "published":
        raise ValueError(f"Cannot deprecate version in '{pv.status}' status")

    pv.status = "deprecated"
    pv.deprecated_at = datetime.utcnow()
    db.commit()
    return pv


def diff_versions(db: Session, version_id_a: str, version_id_b: str) -> dict:
    """Compute the diff between two versions."""
    va = db.query(DataProductVersion).filter_by(id=version_id_a).first()
    vb = db.query(DataProductVersion).filter_by(id=version_id_b).first()
    if not va or not vb:
        raise ValueError("Version not found")

    diff = _compute_diff(va.schema_snapshot, vb.schema_snapshot)
    change_type = _classify_change(diff)

    return {
        "version_a": va.version,
        "version_b": vb.version,
        "change_type": change_type,
        "diff": diff,
        "change_summary": _format_change_summary(diff, change_type or "patch"),
    }
