"""Unity Catalog service for syncing data products from Databricks."""

import json
import os
import subprocess
import time
from datetime import datetime

import requests
from sqlalchemy.orm import Session

from app.config import settings
from app.models.database import (
    DataProduct, DataProductColumn, DataProductLineage, DataProductTable, ScanJob,
)

PII_KEYWORDS = {"pii", "ssn", "social_security", "date_of_birth", "dob", "email",
                "phone", "bank_account", "credit_card", "encrypted", "sensitive"}


def _get_auth():
    """Get workspace host and token. Uses OBO user token if available."""
    host = os.getenv("DATABRICKS_HOST", settings.workspace_url)
    if not host.startswith("http"):
        host = f"https://{host}"
    host = host.rstrip("/")

    # Option 1: OBO — use the current user's token from thread-local
    from app.db import _thread_local
    user_token = getattr(_thread_local, "user_token", None)
    if user_token:
        return host, user_token

    # Option 2: Databricks SDK (App SP token)
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        token = w.config.token
        sdk_host = w.config.host
        if sdk_host and token:
            if not sdk_host.startswith("http"):
                sdk_host = f"https://{sdk_host}"
            return sdk_host.rstrip("/"), token
    except Exception:
        pass

    # Option 3: Local dev via CLI
    try:
        result = subprocess.run(
            ["databricks", "auth", "token", "--profile", settings.databricks_profile],
            capture_output=True, text=True,
        )
        if result.returncode != 0 or not result.stdout.strip():
            raise RuntimeError(f"CLI auth failed: {result.stderr}")
        token = json.loads(result.stdout)["access_token"]
        return host, token
    except Exception as e:
        raise RuntimeError(f"Failed to get auth token: {e}")


def _sql_query(sql: str) -> list[list]:
    """Execute SQL via Statement Execution API and return data_array."""
    host, token = _get_auth()
    resp = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "warehouse_id": settings.warehouse_id,
            "statement": sql,
            "wait_timeout": "50s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        },
    )
    if resp.status_code >= 400:
        raise Exception(f"SQL API error {resp.status_code}: {resp.text[:300]}")

    result = resp.json()
    status = result.get("status", {}).get("state")

    # Poll if needed
    statement_id = result.get("statement_id")
    while status in ("PENDING", "RUNNING"):
        time.sleep(2)
        poll = requests.get(
            f"{host}/api/2.0/sql/statements/{statement_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        result = poll.json()
        status = result.get("status", {}).get("state")

    if status == "FAILED":
        error = result.get("status", {}).get("error", {})
        raise Exception(f"SQL failed: {error.get('message', 'Unknown')}")

    return result.get("result", {}).get("data_array", [])


def _api_get(path: str, params: dict = None):
    """Make authenticated GET request to Databricks REST API."""
    host, token = _get_auth()
    resp = requests.get(
        f"{host}/api/2.1/unity-catalog{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params or {},
    )
    resp.raise_for_status()
    return resp.json()


def _has_pii(columns: list[dict]) -> bool:
    """Check if any column appears to contain PII based on name/comment."""
    for col in columns:
        name_lower = col.get("name", "").lower()
        comment_lower = (col.get("comment") or "").lower()
        combined = name_lower + " " + comment_lower
        if any(kw in combined for kw in PII_KEYWORDS):
            return True
    return False


def sync_products_from_uc(db: Session, tag_prefix: str = None, tag_suffix: str = None) -> ScanJob:
    """Scan Unity Catalog for tables with data_product tags and sync to Lakebase.

    Uses system.information_schema.table_tags to discover tagged tables,
    then fetches table/column metadata via UC REST API.
    """
    scan_job = ScanJob(
        job_type="tag_scan",
        tag_prefix=tag_prefix,
        tag_suffix=tag_suffix,
        status="running",
        started_at=datetime.utcnow(),
    )
    db.add(scan_job)
    db.commit()

    try:
        tag_key = settings.scan_tag_key
        tables_found = 0
        products_found_set = set()

        # Query all tables with the data_product tag from information_schema
        tag_filter = f"tag_name = '{tag_key}'"
        if tag_prefix:
            tag_filter += f" AND tag_value LIKE '{tag_prefix}%'"
        if tag_suffix:
            tag_filter += f" AND tag_value LIKE '%{tag_suffix}'"

        # Query across all catalogs using system.information_schema
        tag_sql = f"""
            SELECT catalog_name, schema_name, table_name, tag_value
            FROM system.information_schema.table_tags
            WHERE {tag_filter}
            ORDER BY catalog_name, schema_name, table_name
        """
        tag_rows = _sql_query(tag_sql)

        for row in tag_rows:
            catalog_name, schema_name, table_name, product_tag = row
            full_name = f"{catalog_name}.{schema_name}.{table_name}"
            tables_found += 1
            products_found_set.add(product_tag)

            # Get table metadata from UC REST API
            try:
                table_info = _api_get(f"/tables/{full_name}")
            except Exception as table_err:
                import logging
                logging.getLogger(__name__).debug(f"UC API for {full_name}: {table_err}")
                table_info = {
                    "name": table_name,
                    "full_name": full_name,
                    "comment": None,
                    "table_type": "MANAGED",
                    "columns": [],
                }

            columns = table_info.get("columns", [])

            # Fallback: get columns via SQL if UC REST API didn't return them
            if not columns:
                try:
                    col_rows = _sql_query(
                        f"SELECT column_name, full_data_type, comment, is_nullable "
                        f"FROM system.information_schema.columns "
                        f"WHERE table_catalog='{catalog_name}' "
                        f"AND table_schema='{schema_name}' "
                        f"AND table_name='{table_name}' "
                        f"ORDER BY ordinal_position"
                    )
                    columns = [
                        {
                            "name": r[0],
                            "type_text": r[1] or "STRING",
                            "comment": r[2],
                            "nullable": r[3] == "YES",
                        }
                        for r in col_rows
                    ]
                except Exception:
                    pass

            # Get table comment via SQL if not from REST API
            table_comment = table_info.get("comment")
            if not table_comment:
                try:
                    comment_rows = _sql_query(
                        f"SELECT comment FROM system.information_schema.tables "
                        f"WHERE table_catalog='{catalog_name}' "
                        f"AND table_schema='{schema_name}' "
                        f"AND table_name='{table_name}'"
                    )
                    if comment_rows and comment_rows[0][0]:
                        table_comment = comment_rows[0][0]
                except Exception:
                    pass

            # Upsert data product
            product = db.query(DataProduct).filter_by(tag_value=product_tag).first()
            if not product:
                product = DataProduct(
                    name=product_tag,
                    display_name=product_tag.replace("_", " ").title(),
                    description=f"Data product: {product_tag}",
                    domain=catalog_name,
                    subdomain=schema_name,
                    tag_value=product_tag,
                )
                db.add(product)
                db.flush()

            # Upsert table
            dp_table = db.query(DataProductTable).filter_by(full_name=full_name).first()
            if not dp_table:
                dp_table = DataProductTable(
                    product_id=product.id,
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    table_name=table_name,
                    full_name=full_name,
                    table_type=table_info.get("table_type", "MANAGED"),
                    description=table_comment,
                    column_count=len(columns),
                )
                db.add(dp_table)
                db.flush()
            else:
                dp_table.description = table_comment or dp_table.description
                dp_table.column_count = len(columns)

            # Sync columns
            existing_cols = {c.column_name for c in dp_table.columns}
            for idx, col in enumerate(columns):
                col_name = col["name"]
                if col_name not in existing_cols:
                    dp_col = DataProductColumn(
                        table_id=dp_table.id,
                        column_name=col_name,
                        data_type=col.get("type_text", col.get("type_name", "STRING")),
                        description=col.get("comment"),
                        is_pii=any(kw in col_name.lower() for kw in PII_KEYWORDS),
                        is_nullable=col.get("nullable", True),
                        ordinal_position=idx,
                    )
                    db.add(dp_col)

            # Update product PII flag
            product.has_pii = product.has_pii or _has_pii(columns)

        # Update product table counts
        for tag_val in products_found_set:
            product = db.query(DataProduct).filter_by(tag_value=tag_val).first()
            if product:
                product.table_count = len(product.tables)
                product.updated_at = datetime.utcnow()

        scan_job.status = "completed"
        scan_job.completed_at = datetime.utcnow()
        scan_job.tables_found = tables_found
        scan_job.products_found = len(products_found_set)
        db.commit()

        # Sync lineage from UC system tables
        try:
            _sync_lineage_from_system_tables(db)
        except Exception as le:
            import logging
            logging.getLogger(__name__).warning(f"Lineage sync failed: {le}")

    except Exception as e:
        scan_job.status = "failed"
        scan_job.completed_at = datetime.utcnow()
        scan_job.error_message = str(e)
        db.commit()

    return scan_job


def _sync_lineage_from_system_tables(db: Session):
    """Query system.access.table_lineage to build product-level lineage.

    Maps each table-level lineage row to its parent data product (via tag),
    then aggregates into product-to-product edges.
    """
    # Build table -> product mapping
    all_products = db.query(DataProduct).all()
    table_to_product = {}
    for p in all_products:
        for t in p.tables:
            table_to_product[t.full_name] = p

    if not table_to_product:
        return

    # Query UC table-level lineage for all known tables
    lineage_sql = """
        SELECT DISTINCT
            source_table_full_name,
            target_table_full_name
        FROM system.access.table_lineage
        WHERE source_table_full_name IS NOT NULL
          AND target_table_full_name IS NOT NULL
    """
    try:
        rows = _sql_query(lineage_sql)
    except Exception:
        return

    # Aggregate table lineage into product lineage
    product_edges: dict[tuple[str, str], dict] = {}

    for row in rows:
        src_table, tgt_table = row[0], row[1]
        src_product = table_to_product.get(src_table)
        tgt_product = table_to_product.get(tgt_table)

        if not src_product or not tgt_product:
            continue
        if src_product.id == tgt_product.id:
            continue

        key = (str(src_product.id), str(tgt_product.id))
        if key not in product_edges:
            product_edges[key] = {
                "source_tables": set(),
                "target_tables": set(),
            }
        product_edges[key]["source_tables"].add(src_table)
        product_edges[key]["target_tables"].add(tgt_table)

    # Upsert lineage records
    for (src_id, tgt_id), edge in product_edges.items():
        existing = db.query(DataProductLineage).filter_by(
            source_product_id=src_id,
            target_product_id=tgt_id,
        ).first()
        if existing:
            existing.source_tables = list(edge["source_tables"])
            existing.target_tables = list(edge["target_tables"])
            existing.last_refreshed = datetime.utcnow()
        else:
            lineage = DataProductLineage(
                source_product_id=src_id,
                target_product_id=tgt_id,
            )
            lineage.source_tables = list(edge["source_tables"])
            lineage.target_tables = list(edge["target_tables"])
            db.add(lineage)

    db.commit()
    import logging
    logging.getLogger(__name__).info(
        f"Lineage synced: {len(product_edges)} product-level edges"
    )


def get_product_lineage(db: Session, product_id: str) -> dict:
    """Get product-level lineage by aggregating table-level lineage from UC."""
    product = db.query(DataProduct).filter_by(id=product_id).first()
    if not product:
        return {"nodes": [], "edges": []}

    # Get all products for mapping tables back to products
    all_products = db.query(DataProduct).all()
    table_to_product = {}
    for p in all_products:
        for t in p.tables:
            table_to_product[t.full_name] = p

    # Get contracts for port labels
    product_contracts = {}
    for p in all_products:
        input_contracts = []
        output_contracts = []
        for c in p.contracts:
            entry = {"id": str(c.id), "name": c.name, "version": c.version}
            if c.contract_type == "input":
                input_contracts.append(entry)
            else:
                output_contracts.append(entry)
        product_contracts[str(p.id)] = {
            "input": input_contracts,
            "output": output_contracts,
        }

    # Build nodes and edges
    nodes = {}
    edges = {}

    for p in all_products:
        contracts = product_contracts.get(str(p.id), {"input": [], "output": []})
        nodes[str(p.id)] = {
            "id": str(p.id),
            "name": p.display_name or p.name,
            "domain": p.domain,
            "status": p.status,
            "input_contracts": contracts["input"],
            "output_contracts": contracts["output"],
        }

    # Check cached lineage
    cached = db.query(DataProductLineage).filter(
        (DataProductLineage.source_product_id == product_id) |
        (DataProductLineage.target_product_id == product_id)
    ).all()

    for link in cached:
        key = (str(link.source_product_id), str(link.target_product_id))
        if key not in edges:
            edges[key] = {
                "source": str(link.source_product_id),
                "target": str(link.target_product_id),
                "source_tables": link.source_tables or [],
                "target_tables": link.target_tables or [],
            }

    # Try to fetch fresh lineage from UC if no cache
    if not cached:
        try:
            _refresh_lineage_from_uc(db, product, table_to_product, nodes, edges)
        except Exception:
            pass

    # Filter to only include connected nodes
    connected_ids = {str(product.id)}
    for edge in edges.values():
        connected_ids.add(edge["source"])
        connected_ids.add(edge["target"])

    filtered_nodes = [v for k, v in nodes.items() if k in connected_ids]

    return {
        "nodes": filtered_nodes,
        "edges": list(edges.values()),
    }


def _refresh_lineage_from_uc(db, product, table_to_product, nodes, edges):
    """Fetch table-level lineage from UC and aggregate to product level."""
    for table in product.tables:
        try:
            lineage_resp = _api_get("/lineage/table-lineage", {
                "table_name": table.full_name,
            })
        except Exception:
            continue

        for upstream in lineage_resp.get("upstreams", []):
            up_table = upstream.get("tableInfo", {}).get("name", "")
            if up_table in table_to_product:
                source_product = table_to_product[up_table]
                if str(source_product.id) != str(product.id):
                    key = (str(source_product.id), str(product.id))
                    if key not in edges:
                        edges[key] = {
                            "source": str(source_product.id),
                            "target": str(product.id),
                            "source_tables": [],
                            "target_tables": [],
                        }
                    if up_table not in edges[key]["source_tables"]:
                        edges[key]["source_tables"].append(up_table)
                    if table.full_name not in edges[key]["target_tables"]:
                        edges[key]["target_tables"].append(table.full_name)

        for downstream in lineage_resp.get("downstreams", []):
            down_table = downstream.get("tableInfo", {}).get("name", "")
            if down_table in table_to_product:
                target_product = table_to_product[down_table]
                if str(target_product.id) != str(product.id):
                    key = (str(product.id), str(target_product.id))
                    if key not in edges:
                        edges[key] = {
                            "source": str(product.id),
                            "target": str(target_product.id),
                            "source_tables": [],
                            "target_tables": [],
                        }
                    if table.full_name not in edges[key]["source_tables"]:
                        edges[key]["source_tables"].append(table.full_name)
                    if down_table not in edges[key]["target_tables"]:
                        edges[key]["target_tables"].append(down_table)

    # Cache the lineage
    for key, edge_data in edges.items():
        existing = db.query(DataProductLineage).filter_by(
            source_product_id=edge_data["source"],
            target_product_id=edge_data["target"],
        ).first()
        if not existing:
            lineage = DataProductLineage(
                source_product_id=edge_data["source"],
                target_product_id=edge_data["target"],
                source_tables=edge_data["source_tables"],
                target_tables=edge_data["target_tables"],
            )
            db.add(lineage)
    db.commit()
