"""ODCS (Open Data Contract Standard) service for data contract YAML generation."""

import yaml
from datetime import datetime

from app.models.database import DataContract, DataProduct, DataProductTable


def generate_odcs_for_asset(
    contract: DataContract,
    product: DataProduct,
    table: DataProductTable,
) -> str:
    """Generate ODCS v3.0 YAML for a single asset (table) output port contract.

    Populates all available metadata: table name, description, column names,
    column types, column descriptions, PII classification, tags, and ownership.
    """
    has_pii = any(col.is_pii for col in table.columns)

    # Build the schema elements from columns
    elements = []
    for col in sorted(table.columns, key=lambda c: c.ordinal_position or 0):
        element = {
            "name": col.column_name,
            "logicalType": col.data_type,
            "physicalType": col.data_type,
            "description": col.description or "",
            "isNullable": col.is_nullable,
        }
        if col.is_pii:
            element["classification"] = "PII"
            element["tags"] = ["sensitive", "pii"]
            element["isUnique"] = False
        elements.append(element)

    # Determine tags for this asset
    tags = [f"data_product:{product.tag_value}"]
    if has_pii:
        tags.append("contains_pii")
    tags.append(f"domain:{product.domain}")
    if product.subdomain:
        tags.append(f"subdomain:{product.subdomain}")

    odcs = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": str(contract.id),
        "name": contract.name,
        "version": contract.version,
        "status": contract.status,
        "description": {
            "purpose": (
                table.description
                or f"Output port contract for {table.full_name} "
                   f"in the {product.display_name or product.name} data product"
            ),
            "limitations": "",
            "usage": f"This contract governs the {table.table_name} asset "
                     f"within the {product.display_name or product.name} data product.",
        },
        "type": "output",
        "domain": product.domain,
        "subdomain": product.subdomain or "",
        "tags": tags,
        "dataProduct": {
            "name": product.display_name or product.name,
            "id": str(product.id),
            "tagValue": product.tag_value,
            "domain": product.domain,
        },
        "outputPort": {
            "name": table.table_name,
            "fullName": table.full_name,
            "description": table.description or "",
            "type": table.table_type or "MANAGED",
            "catalog": table.catalog_name,
            "schema": table.schema_name,
            "table": table.table_name,
        },
        "team": [],
        "schema": [
            {
                "name": table.full_name,
                "physicalName": table.full_name,
                "description": table.description or "",
                "type": "table",
                "physicalType": table.table_type or "MANAGED",
                "catalog": table.catalog_name,
                "database": table.schema_name,
                "containsPii": has_pii,
                "elements": elements,
            }
        ],
        "quality": _build_quality_section(table),
        "serviceLevel": {
            "availability": {
                "percentage": "99.9%",
            },
            "freshness": {
                "frequency": "daily",
                "timestampField": "updated_at",
            },
            "retention": {
                "period": "3 years",
                "unlimited": False,
            },
        },
        "support": {
            "channel": "email",
            "url": "mailto:data-team@insurance.com",
            "responseTime": "24h",
        },
        "customProperties": {
            "generatedAt": datetime.utcnow().isoformat(),
            "generator": "data-products-manager",
            "odcsVersion": "3.0.0",
            "assetFullName": table.full_name,
            "columnCount": len(table.columns),
            "containsPii": has_pii,
        },
    }

    # Add team members
    if contract.owner:
        odcs["team"].append({
            "name": contract.owner,
            "role": "technicalOwner",
            "email": f"{contract.owner.replace(' ', '.').lower()}@insurance.com",
        })
    odcs["team"].append({
        "name": f"{product.domain} Data Team",
        "role": "businessOwner",
        "email": f"{product.domain.replace('ins_', '')}-data@insurance.com",
    })
    odcs["team"].append({
        "name": "Data Governance Office",
        "role": "dataGovernanceOfficer",
        "email": "data-governance@insurance.com",
    })

    return yaml.dump(odcs, default_flow_style=False, sort_keys=False, allow_unicode=True)


def _build_quality_section(table: DataProductTable) -> list:
    """Build data quality rules based on column metadata."""
    rules = []

    for col in table.columns:
        # NOT NULL check for non-nullable columns
        if not col.is_nullable:
            rules.append({
                "type": "completeness",
                "field": col.column_name,
                "description": f"{col.column_name} must not be null",
                "operator": "isNotNull",
                "threshold": 100,
            })

        # PII masking rule
        if col.is_pii:
            rules.append({
                "type": "compliance",
                "field": col.column_name,
                "description": f"{col.column_name} contains PII and must be handled per data classification policy",
                "classification": "PII",
            })

    return rules


def generate_odcs_yaml(contract: DataContract, product: DataProduct = None) -> str:
    """Generate ODCS v3.0 YAML for a contract covering all tables in a product.

    Legacy function for backward compatibility. For per-asset contracts,
    use generate_odcs_for_asset instead.
    """
    if product and product.tables and len(product.tables) == 1:
        return generate_odcs_for_asset(contract, product, product.tables[0])

    odcs = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": str(contract.id),
        "name": contract.name,
        "version": contract.version,
        "status": contract.status,
        "description": {
            "purpose": contract.description or "Data contract for " + contract.name,
        },
        "type": contract.contract_type or "output",
        "team": [],
        "schema": [],
        "support": {
            "channel": "email",
            "url": "mailto:data-team@insurance.com",
        },
    }

    if contract.owner:
        odcs["team"].append({"name": contract.owner, "role": "technicalOwner"})

    if product:
        odcs["domain"] = product.domain
        odcs["dataProduct"] = {
            "name": product.display_name or product.name,
            "id": str(product.id),
            "tagValue": product.tag_value,
        }

        for table in product.tables:
            schema_obj = {
                "name": table.full_name,
                "description": table.description or "",
                "type": "table",
                "elements": [],
            }
            for col in table.columns:
                element = {
                    "name": col.column_name,
                    "type": col.data_type,
                    "description": col.description or "",
                    "nullable": col.is_nullable,
                }
                if col.is_pii:
                    element["classification"] = "PII"
                    element["tags"] = ["sensitive", "pii"]
                schema_obj["elements"].append(element)
            odcs["schema"].append(schema_obj)

    odcs["serviceLevel"] = {
        "availability": {"percentage": "99.9%"},
        "freshness": {"frequency": "daily", "timestampField": "updated_at"},
    }
    odcs["customProperties"] = {
        "generatedAt": datetime.utcnow().isoformat(),
        "generator": "data-products-manager",
        "odcsVersion": "3.0.0",
    }

    return yaml.dump(odcs, default_flow_style=False, sort_keys=False, allow_unicode=True)


def parse_odcs_yaml(yaml_content: str) -> dict:
    """Parse an ODCS YAML file and extract contract fields."""
    data = yaml.safe_load(yaml_content)

    result = {
        "name": data.get("name", ""),
        "version": data.get("version", "1.0.0"),
        "description": "",
        "status": data.get("status", "draft"),
        "contract_type": data.get("type", "output"),
        "owner": None,
    }

    desc = data.get("description", {})
    if isinstance(desc, dict):
        result["description"] = desc.get("purpose", "")
    elif isinstance(desc, str):
        result["description"] = desc

    team = data.get("team", [])
    for member in team:
        if member.get("role") in ("owner", "technicalOwner"):
            result["owner"] = member.get("name")
            break

    return result
