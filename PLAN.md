# Data Products Implementation Plan

## Context
Build a comprehensive Data Products management platform on Azure Databricks to demonstrate data product governance using Unity Catalog governed tags, ODCS data contracts, and a web application backed by Lakebase. The project uses the insurance industry as the domain, with realistic datasets across multiple catalogs/schemas.

**Workspace**: `uc-demo-ws-ne` (Azure Databricks: `adb-4116661263058619.19.azuredatabricks.net`)
**SQL Warehouse**: `data-products-poc` (ID: `fe12763ffa92c9b5`, X-Large PRO, currently STOPPED)

---

## Phase 1: Insurance Dataset Creation (Unity Catalog)

### Domain Architecture (3 Catalogs + Sub-domain Schemas)

**Catalog 1: `ins_policy`** — Policy lifecycle management
- Schema: `underwriting` — Risk assessment & selection
  - `risk_applications` — Insurance applications with risk details
  - `underwriting_decisions` — Approval/denial decisions with rationale
  - `risk_assessments` — Risk scoring and assessment records
- Schema: `policy_admin` — Policy lifecycle
  - `policies` — Master policy records (core entity)
  - `policy_versions` — Version history of policy changes
  - `coverage_details` — Coverage types, limits, deductibles
  - `endorsements` — Mid-term policy modifications
  - `renewals` — Renewal records and history

**Catalog 2: `ins_claims`** — Claims processing & settlement
- Schema: `claims_processing` — FNOL and claim intake
  - `claims` — Master claim records (core entity)
  - `claim_incidents` — Incident details (date, location, description)
  - `claimant_information` — Claimant PII (name, SSN, DOB) ⚠️ SENSITIVE
- Schema: `adjudication` — Claim review & decision
  - `claim_adjudications` — Review decisions with rationale
  - `evidence_documents` — Supporting documentation metadata
  - `claim_reserves` — Financial reserves per claim
  - `reserve_adjustments` — Reserve change history
- Schema: `payments` — Claim payments & recovery
  - `claim_payments` — Payment transactions ⚠️ SENSITIVE (bank info)
  - `subrogation_cases` — Recovery from third parties

**Catalog 3: `ins_customer`** — Customer & agent master data
- Schema: `customer_master` — Customer profiles
  - `customers` — Master customer records ⚠️ SENSITIVE (SSN, DOB)
  - `customer_addresses` — Address history
  - `customer_contacts` — Phone, email, preferences ⚠️ SENSITIVE
- Schema: `agent_management` — Producer/agent data
  - `agents` — Agent profiles and licensing
  - `agent_assignments` — Agent-to-policy assignments
  - `agent_commissions` — Commission records
- Schema: `crm` — Customer interactions
  - `customer_interactions` — Interaction/touchpoint log
  - `customer_risk_scores` — Risk scoring history

**Total: 3 catalogs, 8 schemas, 24 tables, ~100K records each**

### Data Generation Approach
- Use Python with `Faker` library for realistic data generation
- Execute via Databricks notebook or local Databricks Connect
- Proper data types: dates, decimals, UUIDs, enums
- Foreign key relationships maintained across catalogs
- PII fields clearly identified (SSN, DOB, bank accounts, email)
- Every table and column gets descriptive names + descriptions via `COMMENT` SQL

### Governed Tags for Data Products
Apply Unity Catalog governed tags to group tables into logical data products:
- Tag key: `data_product`, values:
  - `policy_lifecycle` — policies, policy_versions, coverage_details, endorsements, renewals
  - `underwriting_risk` — risk_applications, underwriting_decisions, risk_assessments
  - `claims_management` — claims, claim_incidents, claimant_information
  - `claims_financial` — claim_adjudications, claim_reserves, reserve_adjustments, claim_payments, subrogation_cases
  - `customer_360` — customers, customer_addresses, customer_contacts, customer_interactions, customer_risk_scores
  - `agent_network` — agents, agent_assignments, agent_commissions
  - `evidence_docs` — evidence_documents

---

## Phase 2: Lakebase Autoscaling Instance

### Setup
- Create a Lakebase Autoscaling instance via Databricks CLI/API
- Branch: production, 4-8 CUs autoscaling
- Database name: `data_products_metadata`

### Schema (PostgreSQL tables for metadata & contracts)

**Core tables:**
- `data_products` — Cached metadata about discovered data products (name, description, domain, status, tag_value, table_count, created_at, updated_at)
- `data_product_tables` — Tables belonging to each data product (product_id FK, catalog, schema, table_name, description, row_count, column_count)
- `data_product_columns` — Column metadata per table (table_id FK, column_name, data_type, description, is_pii, is_nullable)
- `data_contracts` — ODCS data contracts (product_id FK, name, version, status, odcs_yaml TEXT, created_by, created_at, updated_at)
- `data_contract_versions` — Version history of contracts
- `scan_jobs` — Scan job history (job_id, status, started_at, completed_at, tables_found, products_found)
- `app_settings` — Application configuration (key-value pairs for workspace URL, warehouse ID, etc.)

### SQLAlchemy ORM
- Models defined in Python using SQLAlchemy 2.0 declarative style
- OAuth token-based auth with automatic refresh (pattern from lakebase_demos)
- Connection pooling with `pool_pre_ping=True`

---

## Phase 3: Web Application (APX Framework — FastAPI + React)

### Scaffolding
- Use `apx init` to bootstrap the project structure
- FastAPI backend + React frontend with Tailwind CSS + shadcn/ui
- App deployed as a Databricks App

### Backend (FastAPI)

**API Routes:**
```
GET  /api/health                          — Health check
GET  /api/data-products                   — List all data products
GET  /api/data-products/{id}              — Get product details (tables, columns, lineage)
POST /api/data-products/sync              — Trigger sync from Unity Catalog to Lakebase
GET  /api/data-products/{id}/lineage      — Get product-level lineage (aggregated from table-level UC lineage)
GET  /api/data-contracts                  — List all contracts
GET  /api/data-contracts/{id}             — Get contract detail
POST /api/data-contracts                  — Create new contract (from product)
PUT  /api/data-contracts/{id}             — Update contract
GET  /api/data-contracts/{id}/odcs        — Download ODCS YAML
POST /api/data-contracts/upload           — Upload ODCS YAML
GET  /api/settings                        — Get app settings
PUT  /api/settings                        — Update app settings
POST /api/scan/trigger                    — Trigger scan job (by tag prefix/suffix)
GET  /api/scan/history                    — Get scan job history
GET  /api/stats                           — Dashboard statistics
```

**Backend Architecture:**
- `app/main.py` — FastAPI app setup, CORS, lifespan
- `app/models/` — SQLAlchemy ORM models
- `app/routers/` — API route handlers (products, contracts, settings, scan)
- `app/services/` — Business logic (UC sync, ODCS conversion, product-level lineage aggregation)
- `app/db.py` — Lakebase connection with OAuth token refresh
- `app/config.py` — Settings management

**Key integrations:**
- Databricks SDK for Unity Catalog API (list catalogs, schemas, tables, tags, lineage)
- SQLAlchemy ORM for Lakebase CRUD
- ODCS YAML serialization/deserialization (PyYAML)
- Databricks Jobs API for triggering scan jobs

### Frontend (React + shadcn/ui)

**Pages:**
1. **Home/Dashboard** (`/`)
   - Data product count, domain breakdown, contract count
   - Recent activity, scan status
   - Charts: products by domain, contracts by status

2. **Data Products List** (`/data-products`)
   - Searchable, filterable table of all products
   - Columns: name, domain, tables count, contract status, last synced
   - Filter by domain (catalog), status

3. **Data Product Detail** (`/data-products/:id`)
   - Product metadata, description, domain
   - Tables list with column details
   - PII/sensitive data indicators
   - "Create Data Contract" button
   - Link to lineage view

4. **Data Product Lineage** (`/data-products/:id/lineage`)
   - **Interactive visual graph** using React Flow showing product-level lineage
   - Each **data product node** displays: product name, domain, status badge
   - **Input ports** on the left of each node — labeled with the data contract name(s) that govern incoming data
   - **Output ports** on the right of each node — labeled with the data contract name(s) that govern outgoing data
   - Edges connect output ports of upstream products to input ports of downstream products
   - If a port has no associated data contract, show "No Contract" in muted style
   - Backend aggregates table-level lineage from UC API across all tables in the product, maps each upstream/downstream table to its parent data product (via governed tag), and attaches contract names from Lakebase to each port
   - Tables not belonging to any data product shown as standalone nodes (dimmed)
   - Click a node to navigate to that product's detail page
   - Click a contract label on a port to navigate to the contract detail page
   - Hover on an edge to see which specific tables drive the relationship

5. **Data Contracts List** (`/data-contracts`)
   - Searchable table of all contracts
   - Columns: name, version, status, product, created, updated
   - Upload ODCS button

6. **Data Contract Detail/Edit** (`/data-contracts/:id`)
   - View/edit contract fields (name, version, description, schema, quality, SLA)
   - ODCS YAML preview
   - Download ODCS button
   - Version history

7. **Settings** (`/settings`)
   - Workspace URL configuration
   - Lakebase endpoint configuration
   - SQL Warehouse JDBC/ODBC connection string
   - Scan configuration (tag prefix/suffix, metastore)
   - Trigger scan button + scan history

### app.yaml Configuration
```yaml
command:
  - uvicorn
  - app.main:app
  - --host=0.0.0.0
  - --port=8000
resources:
  - name: sql-warehouse
    sql_warehouse:
      id: fe12763ffa92c9b5
      permission: CAN_USE
  - name: lakebase
    lakebase:
      endpoint: <lakebase-endpoint>
      permission: read_write
      catalog: <catalog>
      database: data_products_metadata
env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: fe12763ffa92c9b5
```

---

## Implementation Order

### Step 1: Create catalogs, schemas, and generate data
- Create 3 catalogs and 8 schemas
- Generate and load ~100K records per table (24 tables)
- Add table/column descriptions via ALTER TABLE/COMMENT
- Apply governed tags (`data_product` key)

### Step 2: Create Lakebase Autoscaling instance
- Provision Lakebase instance (4-8 CUs)
- Create metadata schema tables
- Verify connectivity

### Step 3: Build web application
- Scaffold with APX (`apx init`)
- Implement backend (FastAPI + SQLAlchemy + Databricks SDK)
- Implement frontend (React + shadcn/ui)
- Wire up all pages and API routes

### Step 4: Deploy as Databricks App
- Configure `app.yaml`
- Deploy and test end-to-end

---

## Verification Plan
1. **Data**: Query each table to verify ~100K records, descriptions, and governed tags
2. **Lakebase**: Connect to Lakebase and verify schema tables exist
3. **App**: Start local dev server, test each page and API endpoint
4. **ODCS**: Create a contract, download as ODCS YAML, re-upload and verify
5. **Scan**: Trigger a scan job, verify products sync from UC to Lakebase
6. **Lineage**: View lineage for a data product and verify graph renders
