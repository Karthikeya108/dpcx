"""Create a derived data product that reads from existing products to establish lineage.

Creates a 'risk_analytics' data product in ins_policy catalog that derives from:
- customer_360 (customer risk scores)
- claims_financial (claim reserves, adjudications)
- underwriting_risk (risk assessments)
- policy_lifecycle (policies, coverage details)

This creates real table-level lineage in Unity Catalog that the app can aggregate
into product-level lineage.
"""

import json
import os
import subprocess
import configparser
import time
import requests

PROFILE = "uc-demo-ws-ne"
WAREHOUSE_ID = "fe12763ffa92c9b5"


def get_connection_params():
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", PROFILE],
        capture_output=True, text=True,
    )
    token_info = json.loads(result.stdout)
    token = token_info["access_token"]
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser("~/.databrickscfg"))
    host = cfg[PROFILE]["host"].rstrip("/")
    return host, token


class DatabricksSQL:
    def __init__(self, host, token, warehouse_id):
        self.host = host
        self.warehouse_id = warehouse_id
        self._token_time = time.time()
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    def execute(self, sql, wait_timeout="50s"):
        if time.time() - self._token_time > 1800:
            _, new_token = get_connection_params()
            self.session.headers["Authorization"] = f"Bearer {new_token}"
            self._token_time = time.time()

        url = f"{self.host}/api/2.0/sql/statements"
        payload = {
            "warehouse_id": self.warehouse_id,
            "statement": sql,
            "wait_timeout": wait_timeout,
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        }
        resp = self.session.post(url, json=payload)
        if resp.status_code >= 400:
            raise Exception(f"HTTP {resp.status_code}: {resp.text[:500]}")
        result = resp.json()
        status = result.get("status", {}).get("state")
        statement_id = result.get("statement_id")
        while status in ("PENDING", "RUNNING"):
            time.sleep(2)
            poll = self.session.get(f"{url}/{statement_id}")
            result = poll.json()
            status = result.get("status", {}).get("state")
        if status == "FAILED":
            error = result.get("status", {}).get("error", {})
            raise Exception(f"SQL failed: {error.get('message', 'Unknown')}")
        return result

    def run(self, sql, desc=""):
        try:
            self.execute(sql)
            print(f"  OK: {desc}")
        except Exception as e:
            print(f"  FAIL: {desc}: {e}")


def main():
    host, token = get_connection_params()
    db = DatabricksSQL(host, token, WAREHOUSE_ID)
    print("Connected.\n")

    # Step 1: Create schema for derived products
    print("Creating derived analytics schema...")
    db.run(
        "CREATE SCHEMA IF NOT EXISTS ins_policy.risk_analytics "
        "COMMENT 'Derived risk analytics sub-domain - combines data from multiple source products'",
        "Schema: ins_policy.risk_analytics"
    )

    # Step 2: Create derived tables using CTAS from source products
    print("\nCreating derived tables (CTAS from source products)...")

    # Table 1: Customer Risk Summary — derives from customer_360 + policy_lifecycle
    db.run("""
        CREATE OR REPLACE TABLE ins_policy.risk_analytics.customer_risk_summary
        COMMENT 'Derived: Customer risk summary combining risk scores with policy data'
        AS
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            c.customer_segment,
            rs.overall_risk_score,
            rs.claims_frequency_score,
            rs.claims_severity_score,
            rs.fraud_indicator_score,
            rs.model_version,
            p.policy_type,
            p.premium_amount,
            p.status as policy_status,
            rs.score_date,
            CURRENT_TIMESTAMP() as derived_at
        FROM ins_customer.customer_master.customers c
        INNER JOIN ins_customer.crm.customer_risk_scores rs
            ON c.customer_id = rs.customer_id
        INNER JOIN ins_policy.policy_admin.policies p
            ON c.customer_id = p.customer_id
        WHERE rs.score_date >= '2024-01-01'
        LIMIT 100000
    """, "Table: customer_risk_summary (from customer_360 + policy_lifecycle)")

    # Table 2: Claims Risk Analysis — derives from claims_financial + underwriting_risk
    db.run("""
        CREATE OR REPLACE TABLE ins_policy.risk_analytics.claims_risk_analysis
        COMMENT 'Derived: Claims risk analysis combining adjudication outcomes with risk assessments'
        AS
        SELECT
            ca.claim_id,
            ca.decision,
            ca.approved_amount,
            cr.reserve_type,
            cr.initial_amount as reserve_initial,
            cr.current_amount as reserve_current,
            cr.status as reserve_status,
            ra.overall_risk_score,
            ra.location_risk_factor,
            ra.recommendation as risk_recommendation,
            ca.review_date,
            CURRENT_TIMESTAMP() as derived_at
        FROM ins_claims.adjudication.claim_adjudications ca
        INNER JOIN ins_claims.adjudication.claim_reserves cr
            ON ca.claim_id = cr.claim_id
        CROSS JOIN (
            SELECT overall_risk_score, location_risk_factor, recommendation
            FROM ins_policy.underwriting.risk_assessments
            LIMIT 100000
        ) ra
        LIMIT 100000
    """, "Table: claims_risk_analysis (from claims_financial + underwriting_risk)")

    # Table 3: Portfolio Risk Dashboard — derives from all 4 source products
    db.run("""
        CREATE OR REPLACE TABLE ins_policy.risk_analytics.portfolio_risk_dashboard
        COMMENT 'Derived: Portfolio-level risk metrics for executive dashboards'
        AS
        SELECT
            p.policy_type,
            p.status as policy_status,
            COUNT(DISTINCT p.policy_id) as policy_count,
            AVG(p.premium_amount) as avg_premium,
            SUM(p.premium_amount) as total_premium,
            AVG(cd.coverage_limit) as avg_coverage_limit,
            COUNT(DISTINCT cl.claim_id) as claim_count,
            AVG(cl.estimated_amount) as avg_claim_amount,
            AVG(rs.overall_risk_score) as avg_risk_score,
            CURRENT_TIMESTAMP() as derived_at
        FROM ins_policy.policy_admin.policies p
        LEFT JOIN ins_policy.policy_admin.coverage_details cd
            ON p.policy_id = cd.policy_id
        LEFT JOIN ins_claims.claims_processing.claims cl
            ON p.policy_id = cl.policy_id
        LEFT JOIN ins_customer.crm.customer_risk_scores rs
            ON p.customer_id = rs.customer_id
        GROUP BY p.policy_type, p.status
    """, "Table: portfolio_risk_dashboard (from all 4 source products)")

    # Step 3: Apply governed tag
    print("\nApplying governed tags...")
    for table in [
        "ins_policy.risk_analytics.customer_risk_summary",
        "ins_policy.risk_analytics.claims_risk_analysis",
        "ins_policy.risk_analytics.portfolio_risk_dashboard",
    ]:
        db.run(
            f"ALTER TABLE {table} SET TAGS ('data_product' = 'risk_analytics')",
            f"Tag {table} -> data_product=risk_analytics"
        )

    # Step 4: Add 'risk_analytics' as allowed value to the governed tag
    print("\nAdding 'risk_analytics' to governed tag allowed values...")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Get current tag policy
    resp = requests.get(f"{host}/api/2.0/tag-policies", headers=headers)
    policies = resp.json().get("tag_policies", [])
    dp_policy = None
    for p in policies:
        if p["key"] == "data_product":
            dp_policy = p
            break

    if dp_policy:
        existing_values = [v["name"] for v in dp_policy.get("values", [])]
        if "risk_analytics" not in existing_values:
            new_values = [{"name": v} for v in existing_values] + [{"name": "risk_analytics"}]
            resp = requests.patch(
                f"{host}/api/2.0/tag-policies/{dp_policy['id']}",
                headers=headers,
                json={
                    "tag_policy": {"values": new_values},
                    "update_mask": "values",
                },
            )
            print(f"  Update tag policy: {resp.status_code}")
        else:
            print("  risk_analytics already in allowed values")
    else:
        print("  WARN: data_product tag policy not found")

    print("\nDone! The 'risk_analytics' data product derives from:")
    print("  - customer_360 (customer risk scores)")
    print("  - claims_financial (claim adjudications, reserves)")
    print("  - underwriting_risk (risk assessments)")
    print("  - policy_lifecycle (policies, coverage details)")
    print("  - claims_management (claims)")
    print("\nSync the app to discover the new product and its lineage.")


if __name__ == "__main__":
    main()
