# Data Products Manager

A comprehensive Data Products governance platform built on Azure Databricks, demonstrating data product discovery, ODCS data contracts, product-level lineage visualization, and schema versioning.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Databricks App (FastAPI + React)          │
│  ┌──────────────┐  ┌──────────────────────────────────────┐ │
│  │  React UI    │  │  FastAPI Backend                     │ │
│  │  shadcn/ui   │──│  - Products & Versioning API         │ │
│  │  React Flow  │  │  - Contracts API (ODCS)              │ │
│  │  Tailwind    │  │  - Lineage, Settings & Scan API      │ │
│  └──────────────┘  └───────────┬──────────────────────────┘ │
└─────────────────────────────────┼────────────────────────────┘
                                  │
           ┌──────────────────────┼───────────────────────┐
           │                      │                       │
  ┌────────▼─────────┐  ┌────────▼───────────┐  ┌────────▼──────┐
  │  SQLite / Lakebase│  │  Unity Catalog     │  │  SQL Statement│
  │  (Metadata)      │  │  (Governed Tags)   │  │  API (OBO)    │
  │  - Products      │  │  - 3 Catalogs      │  │  - Tag scan   │
  │  - Contracts     │  │  - 9 Schemas       │  │  - Column     │
  │  - Versions      │  │  - 28 Tables       │  │    metadata   │
  │  - Lineage       │  │  - ~2.4M Records   │  │  - Lineage    │
  └──────────────────┘  └────────────────────┘  └───────────────┘
```

## Components

### 1. Insurance Dataset (Unity Catalog)

Three domain catalogs modeled after the insurance industry:

| Catalog | Domain | Schemas | Tables | Description |
|---------|--------|---------|--------|-------------|
| `ins_policy` | Insurance Policy | `underwriting`, `policy_admin`, `risk_analytics` | 11 | Policy lifecycle, underwriting, and derived risk analytics |
| `ins_claims` | Insurance Claims | `claims_processing`, `adjudication`, `payments` | 10 | Claim submission through settlement and recovery |
| `ins_customer` | Insurance Customer | `customer_master`, `agent_management`, `crm` | 7 | Customer and agent master data |

Each table has ~100K records with realistic data generated via Faker. Tables containing PII/sensitive data (SSN, DOB, bank accounts, email) are auto-detected and flagged.

### 2. Governed Tags (Data Products)

Tables are grouped into **8 data products** using the Unity Catalog **governed tag** `data_product`. The governed tag policy enforces only allowed values can be assigned.

| Data Product | Tag Value | Tables | Domain | Type |
|---|---|---|---|---|
| Policy Lifecycle | `policy_lifecycle` | 5 | Insurance Policy | Source |
| Underwriting Risk | `underwriting_risk` | 3 | Insurance Policy | Source |
| Claims Management | `claims_management` | 3 | Insurance Claims | Source |
| Claims Financial | `claims_financial` | 5 | Insurance Claims | Source |
| Customer 360 | `customer_360` | 5 | Insurance Customer | Source |
| Agent Network | `agent_network` | 3 | Insurance Customer | Source |
| Evidence Docs | `evidence_docs` | 1 | Insurance Claims | Source |
| **Risk Analytics** | `risk_analytics` | 3 | Insurance Policy | **Derived** |

**Risk Analytics** is a derived data product created via CTAS (CREATE TABLE AS SELECT) from 5 source products, establishing real Unity Catalog table-level lineage.

### 3. Metadata Storage

The app uses a dual storage strategy:
- **Databricks App**: SQLite within the container (auto-populated from UC on first scan)
- **Local dev**: Lakebase Autoscaling PostgreSQL (`data-products-metadata`, 4-8 CU)

Both use the same SQLAlchemy ORM models. Metadata tables:
- `data_products` — Product metadata synced from Unity Catalog
- `data_product_tables` — Tables belonging to each product with descriptions
- `data_product_columns` — Column metadata with PII flags and descriptions
- `data_product_versions` — Schema version snapshots with diff classification
- `data_contracts` — ODCS-compliant output port contracts
- `data_contract_versions` — Contract version history
- `data_product_lineage` — Cached product-level lineage from UC
- `scan_jobs` — Scan history
- `app_settings` — Application configuration

### 4. Auto-Generated ODCS Contracts

When "Generate Contracts" is clicked on a data product, **one output port contract per table** is auto-created with fully populated ODCS v3.0 YAML:

- Table name, catalog, schema, full path, description
- All column names, logical/physical types, descriptions, nullability
- PII classification tags and compliance quality rules
- Completeness rules for NOT NULL columns
- Technical owner (derived from domain), business owner, data governance officer
- Domain/subdomain/data_product governed tags
- Service level agreements (availability, freshness, retention)

### 5. Product-Level Lineage

Lineage is aggregated from Unity Catalog's `system.access.table_lineage` into product-level edges. The visualization shows:
- Data product nodes with domain labels
- **Output port contracts** as small blue squares on the right border (hover for contract name, click to navigate)
- Animated edges showing data flow direction
- Source products on the left, derived products in the center/right

### 6. Data Product Versioning

Versions track **schema changes** over time using frozen snapshots. Each version captures the full state of tables, columns, types, descriptions, and PII flags.

#### Version Lifecycle

```
draft → published → deprecated → retired
         │
         └── immutable once published (schema snapshot frozen)
```

#### Change Detection & Classification

On "Detect Changes", the system compares current UC metadata against the latest published version's snapshot:

| Change Type | Trigger | Version Bump |
|---|---|---|
| **Major** | Column removed, column type changed, table removed | 1.x → 2.0.0 |
| **Minor** | Column added, table added | 1.0 → 1.1.0 |
| **Patch** | Description changed, PII classification changed | 1.0.0 → 1.0.1 |

#### Diff Detection Logic

The diff engine compares two schema snapshots and identifies:
- Added/removed tables
- Added/removed columns
- Column type changes (breaking)
- Description and PII classification changes (non-breaking)

Each change is auto-classified and a human-readable summary is generated.

#### API Endpoints

```
GET  /api/data-products/{id}/versions              — List all versions
POST /api/data-products/{id}/versions/detect       — Detect changes vs last published
POST /api/data-products/{id}/versions              — Create draft version from current state
PUT  /api/data-products/{id}/versions/{v}/publish   — Publish a draft (freeze snapshot)
PUT  /api/data-products/{id}/versions/{v}/deprecate — Deprecate a published version
GET  /api/data-products/{id}/versions/{a}/diff/{b}  — Diff two versions
```

### 7. Web Application

**Backend** (FastAPI) — 22 REST API endpoints:
- Products: list, detail, sync from UC, generate contracts, lineage
- Versioning: detect changes, create/publish/deprecate versions, diff
- Contracts: CRUD, ODCS YAML download/upload
- Settings: configuration, scan trigger/history, dashboard stats

**Frontend** (React + TypeScript):
- **Dashboard** — Product/contract counts, domain breakdown, PII indicators, recent scans
- **Data Products** — Searchable list with domain labels ("Insurance Policy" etc.)
- **Product Detail** — Tables with expandable column metadata, PII badges, output port contracts, version history with detect/create/publish/deprecate actions
- **Lineage** — Interactive React Flow graph with product nodes, contract port squares, animated edges
- **Data Contracts** — CRUD with ODCS YAML preview/edit, download/upload, red cancel button on edit
- **Settings** — Workspace configuration, scan trigger with tag prefix/suffix filters

**Authentication**: On-behalf-of (OBO) — the app uses the logged-in user's `x-forwarded-access-token` for Unity Catalog API calls and SQL Statement execution.

## Project Structure

```
dp_implementation/
├── app/                          # FastAPI backend
│   ├── main.py                   # App entrypoint, startup sync
│   ├── config.py                 # Settings
│   ├── db.py                     # SQLite / Lakebase connection (OBO)
│   ├── models/
│   │   ├── database.py           # ORM models (SQLite + PG compatible)
│   │   └── schemas.py            # Pydantic request/response schemas
│   ├── routers/
│   │   ├── products.py           # Products, versioning, lineage API
│   │   ├── contracts.py          # Contracts API
│   │   └── settings.py           # Settings, stats, scan API
│   └── services/
│       ├── unity_catalog.py      # UC sync, lineage from system tables
│       ├── odcs.py               # ODCS YAML generation/parsing
│       └── versioning.py         # Schema diff, version lifecycle
├── frontend/                     # React frontend
│   ├── src/
│   │   ├── main.tsx              # App entrypoint with routes
│   │   ├── components/Layout.tsx # Sidebar navigation layout
│   │   ├── lib/
│   │   │   ├── api.ts            # API client + TypeScript types
│   │   │   └── utils.ts          # Domain labels, cn() helper
│   │   └── pages/                # 7 page components
│   ├── dist/                     # Production build
│   └── package.json
├── scripts/
│   ├── generate_insurance_data.py  # Data generation (24 source tables)
│   └── create_derived_product.py   # Derived risk_analytics product
├── app.yaml                      # Databricks App configuration
├── requirements.txt              # Python dependencies
├── PLAN.md                       # Implementation plan
└── README.md
```

## Workspace Details

| Resource | Value |
|---|---|
| Workspace | `adb-4116661263058619.19.azuredatabricks.net` |
| Profile | `<PROFILE_NAME>` |
| SQL Warehouse | `data-products-poc` (ID: `fe12763ffa92c9b5`) |
| Lakebase Project | `data-products-metadata` |
| Lakebase Endpoint | `ep-square-bird-ea8u2b9t.database.northeurope.azuredatabricks.net` |
| Lakebase Database | `data_products_metadata` |
| App URL | https://data-products-manager-4116661263058619.19.azure.databricksapps.com |

## Local Development

### Backend

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Requires authenticated Databricks CLI profile. Uses Lakebase PG locally.

### Frontend

```bash
cd frontend
npm install
npm run dev
```

Runs on `http://localhost:3000` with API proxy to `http://localhost:8000`.

### Data Generation

```bash
# Generate 24 source tables (~100K records each)
python scripts/generate_insurance_data.py

# Create derived risk_analytics product (3 CTAS tables)
python scripts/create_derived_product.py
```

Supports resuming — skips tables that already have data.

## Deployment

The app is deployed as a Databricks App:

```bash
# Upload backend + pre-built frontend dist to workspace
for f in app/*.py app/models/*.py app/routers/*.py app/services/*.py \
         requirements.txt app.yaml; do
  databricks workspace import "/Users/<user>/apps/data-products-manager/$f" \
    --file "$f" --format AUTO --overwrite --profile=<PROFILE_NAME>
done

# Upload pre-built frontend
for f in frontend/dist/index.html frontend/dist/assets/*; do
  databricks workspace import "/Users/<user>/apps/data-products-manager/$f" \
    --file "$f" --format AUTO --overwrite --profile=<PROFILE_NAME>
done

# Deploy
databricks apps deploy data-products-manager \
  --source-code-path "/Workspace/Users/<user>/apps/data-products-manager" \
  --mode SNAPSHOT --no-wait --profile=<PROFILE_NAME>
```

After deployment, open the app URL and click **"Trigger Scan"** in Settings to populate data from Unity Catalog, then **"Generate Contracts"** on each product.

## Key Technologies

- **Azure Databricks** — Unity Catalog, Governed Tags, SQL Warehouse, Lakebase, Apps
- **FastAPI** — Python REST API backend
- **SQLAlchemy 2.0** — ORM (SQLite in App, PostgreSQL in local dev)
- **React 19** + **TypeScript** — Frontend SPA
- **Tailwind CSS** + **shadcn/ui** — UI styling
- **React Flow** — Lineage graph visualization
- **ODCS v3.0** — Open Data Contract Standard for contract YAML
- **Faker** — Realistic synthetic data generation
- **Semantic Versioning** — Schema change tracking with major/minor/patch classification
