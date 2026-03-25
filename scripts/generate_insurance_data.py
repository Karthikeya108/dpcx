"""
Insurance dataset generator for Data Products POC.
Creates 3 catalogs, 8 schemas, 24 tables with ~100K records each.
Uses Databricks SQL Statement Execution API (REST).
"""

import os
import uuid
import random
import json
import time
import subprocess
import requests
from datetime import datetime, timedelta, date
from faker import Faker

PROFILE = "uc-demo-ws-ne"
WAREHOUSE_ID = "fe12763ffa92c9b5"


def get_connection_params():
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", PROFILE],
        capture_output=True, text=True
    )
    token_info = json.loads(result.stdout)
    token = token_info["access_token"]

    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser("~/.databrickscfg"))
    host = cfg[PROFILE]["host"].rstrip("/")
    return host, token


class DatabricksSQL:
    """Execute SQL via the Statement Execution API with automatic token refresh."""

    def __init__(self, host, token, warehouse_id):
        self.host = host
        self.warehouse_id = warehouse_id
        self._token_time = time.time()
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    def _refresh_token_if_needed(self):
        """Refresh token every 30 minutes to avoid expiry."""
        if time.time() - self._token_time > 1800:
            _, new_token = get_connection_params()
            self.session.headers["Authorization"] = f"Bearer {new_token}"
            self._token_time = time.time()
            print("    [Token refreshed]")

    def execute(self, sql, wait_timeout="50s"):
        self._refresh_token_if_needed()
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
        if status == "FAILED":
            error = result.get("status", {}).get("error", {})
            raise Exception(f"SQL failed: {error.get('message', 'Unknown error')}")

        # Poll if PENDING or RUNNING
        statement_id = result.get("statement_id")
        while status in ("PENDING", "RUNNING"):
            time.sleep(2)
            poll_resp = self.session.get(f"{url}/{statement_id}")
            poll_resp.raise_for_status()
            result = poll_resp.json()
            status = result.get("status", {}).get("state")

        if status == "FAILED":
            error = result.get("status", {}).get("error", {})
            raise Exception(f"SQL failed: {error.get('message', 'Unknown error')}")

        return result

    def execute_quiet(self, sql, description=""):
        try:
            self.execute(sql)
            if description:
                print(f"  OK: {description}")
            return True
        except Exception as e:
            print(f"  FAIL: {description}: {e}")
            return False

    def fetch_one(self, sql):
        result = self.execute(sql)
        data = result.get("result", {}).get("data_array", [])
        if data:
            return data[0]
        return None


fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_RECORDS = 100_000
BATCH_SIZE = 2000  # Rows per INSERT statement

# Shared reference IDs
CUSTOMER_IDS = [str(uuid.uuid4()) for _ in range(NUM_RECORDS)]
POLICY_IDS = [str(uuid.uuid4()) for _ in range(NUM_RECORDS)]
CLAIM_IDS = [str(uuid.uuid4()) for _ in range(NUM_RECORDS)]
AGENT_IDS = [str(uuid.uuid4()) for _ in range(50_000)]

US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY"
]
POLICY_TYPES = ["Auto", "Home", "Life", "Health", "Commercial", "Umbrella", "Renters", "Travel"]
COVERAGE_TYPES = ["Liability", "Collision", "Comprehensive", "Medical", "Uninsured Motorist", "Property Damage", "Personal Injury"]
CLAIM_TYPES = ["Auto Collision", "Property Damage", "Medical", "Theft", "Natural Disaster", "Liability", "Workers Comp"]
RISK_LEVELS = ["Low", "Medium", "High", "Very High"]
STATUSES_POLICY = ["Active", "Expired", "Cancelled", "Pending", "Suspended"]
STATUSES_CLAIM = ["Open", "Under Review", "Approved", "Denied", "Closed", "Reopened"]
DECISION_OUTCOMES = ["Approved", "Denied", "Pending Review", "Escalated"]
PAYMENT_METHODS = ["ACH", "Check", "Wire Transfer", "Credit Card"]
ENDORSEMENT_TYPES = ["Coverage Change", "Address Change", "Vehicle Change", "Name Change", "Premium Adjustment"]
EVIDENCE_TYPES = ["Photo", "Police Report", "Medical Record", "Witness Statement", "Video", "Invoice", "Estimate"]
INTERACTION_TYPES = ["Phone Call", "Email", "Chat", "In-Person", "Letter", "Mobile App"]
LICENSE_TYPES = ["Property & Casualty", "Life & Health", "Surplus Lines", "Adjuster"]


def random_date(start_year=2020, end_year=2025):
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def random_datetime(start_year=2020, end_year=2025):
    d = random_date(start_year, end_year)
    return datetime.combine(d, datetime.min.time()) + timedelta(
        hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59)
    )


def random_money(low=100, high=100000):
    return round(random.uniform(low, high), 2)


def esc(val):
    """Escape a value for SQL INSERT."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)):
        return str(val)
    s = str(val).replace("'", "''")
    return f"'{s}'"


# ─── TABLE DEFINITIONS ───────────────────────────────────────────────

CATALOGS_SCHEMAS = {
    "ins_policy": {
        "comment": "Insurance Policy domain - manages policy lifecycle from underwriting through renewal",
        "schemas": {
            "underwriting": "Risk assessment and underwriting decision sub-domain",
            "policy_admin": "Policy administration and lifecycle management sub-domain",
        }
    },
    "ins_claims": {
        "comment": "Insurance Claims domain - handles claim submission through settlement and recovery",
        "schemas": {
            "claims_processing": "First Notice of Loss (FNOL) and claim intake sub-domain",
            "adjudication": "Claim review, adjudication, and reserve management sub-domain",
            "payments": "Claim payments processing and subrogation recovery sub-domain",
        }
    },
    "ins_customer": {
        "comment": "Insurance Customer domain - single source of truth for customer and agent data",
        "schemas": {
            "customer_master": "Customer master data and profiles sub-domain",
            "agent_management": "Insurance agent and producer management sub-domain",
            "crm": "Customer relationship management and interaction tracking sub-domain",
        }
    },
}


# Each table: (full_name, ddl, col_names, gen_fn)
# gen_fn(i) returns a tuple of Python values

TABLE_DEFS = []

# ─── ins_policy.underwriting.risk_applications ───
TABLE_DEFS.append({
    "name": "ins_policy.underwriting.risk_applications",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_policy.underwriting.risk_applications (
    application_id STRING NOT NULL COMMENT 'Unique identifier for the insurance application',
    customer_id STRING NOT NULL COMMENT 'Reference to the customer submitting the application',
    application_date DATE NOT NULL COMMENT 'Date the application was submitted',
    policy_type STRING NOT NULL COMMENT 'Type of insurance being applied for',
    requested_coverage_amount DECIMAL(15,2) COMMENT 'Requested coverage amount in USD',
    risk_level STRING COMMENT 'Assessed risk level',
    medical_history_flag BOOLEAN COMMENT 'Whether applicant disclosed medical history',
    prior_claims_count INT COMMENT 'Number of prior insurance claims',
    credit_score INT COMMENT 'Applicant credit score at time of application',
    status STRING NOT NULL COMMENT 'Current application status',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
) COMMENT 'Insurance applications submitted by customers with risk assessment details'""",
    "cols": ["application_id","customer_id","application_date","policy_type","requested_coverage_amount","risk_level","medical_history_flag","prior_claims_count","credit_score","status","created_at","updated_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), CUSTOMER_IDS[i % NUM_RECORDS], str(random_date()),
        random.choice(POLICY_TYPES), random_money(10000,1000000), random.choice(RISK_LEVELS),
        random.choice([True, False]), random.randint(0,10), random.randint(300,850),
        random.choice(["Submitted","Under Review","Approved","Denied"]),
        str(random_datetime()), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_policy.underwriting.underwriting_decisions",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_policy.underwriting.underwriting_decisions (
    decision_id STRING NOT NULL COMMENT 'Unique identifier for the underwriting decision',
    application_id STRING NOT NULL COMMENT 'Reference to the associated risk application',
    underwriter_id STRING NOT NULL COMMENT 'Identifier of the underwriter',
    decision_date DATE NOT NULL COMMENT 'Date the underwriting decision was made',
    decision_outcome STRING NOT NULL COMMENT 'Outcome of the decision',
    premium_amount DECIMAL(12,2) COMMENT 'Calculated premium amount in USD',
    conditions STRING COMMENT 'Special conditions or exclusions applied',
    denial_reason STRING COMMENT 'Reason for denial if applicable',
    risk_score DECIMAL(5,2) COMMENT 'Calculated risk score (0-100)',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Underwriting decisions with rationale for insurance application approvals and denials'""",
    "cols": ["decision_id","application_id","underwriter_id","decision_date","decision_outcome","premium_amount","conditions","denial_reason","risk_score","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4()), str(random_date()),
        random.choice(DECISION_OUTCOMES), random_money(500,50000),
        random.choice(["None","Exclusion: pre-existing conditions","Higher deductible required","Inspection required"]),
        random.choice(["N/A","High risk score","Incomplete documentation","Prior fraud flag","Exceeds coverage limits"]),
        round(random.uniform(0,100),2), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_policy.underwriting.risk_assessments",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_policy.underwriting.risk_assessments (
    assessment_id STRING NOT NULL COMMENT 'Unique identifier for the risk assessment',
    application_id STRING NOT NULL COMMENT 'Reference to the associated risk application',
    assessment_date DATE NOT NULL COMMENT 'Date the risk assessment was performed',
    assessor_name STRING COMMENT 'Name of the risk assessor',
    property_value DECIMAL(15,2) COMMENT 'Estimated property value',
    location_risk_factor DECIMAL(5,2) COMMENT 'Location-based risk factor (0-10)',
    claims_history_score DECIMAL(5,2) COMMENT 'Score based on prior claims history (0-100)',
    overall_risk_score DECIMAL(5,2) NOT NULL COMMENT 'Composite risk score (0-100)',
    recommendation STRING COMMENT 'Risk assessment recommendation',
    notes STRING COMMENT 'Additional assessment notes',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Detailed risk scoring and assessment records for insurance applications'""",
    "cols": ["assessment_id","application_id","assessment_date","assessor_name","property_value","location_risk_factor","claims_history_score","overall_risk_score","recommendation","notes","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), str(uuid.uuid4()), str(random_date()), fake.name(),
        random_money(50000,2000000), round(random.uniform(0,10),2),
        round(random.uniform(0,100),2), round(random.uniform(0,100),2),
        random.choice(["Accept","Accept with conditions","Decline","Refer to senior underwriter"]),
        fake.sentence(), str(random_datetime())
    ),
})

# ─── ins_policy.policy_admin ─────
TABLE_DEFS.append({
    "name": "ins_policy.policy_admin.policies",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_policy.policy_admin.policies (
    policy_id STRING NOT NULL COMMENT 'Unique identifier for the insurance policy',
    customer_id STRING NOT NULL COMMENT 'Reference to the policyholder customer',
    agent_id STRING COMMENT 'Reference to the servicing insurance agent',
    policy_number STRING NOT NULL COMMENT 'Human-readable policy number',
    policy_type STRING NOT NULL COMMENT 'Type of insurance policy',
    effective_date DATE NOT NULL COMMENT 'Date the policy coverage begins',
    expiration_date DATE NOT NULL COMMENT 'Date the policy coverage ends',
    premium_amount DECIMAL(12,2) NOT NULL COMMENT 'Annual premium amount in USD',
    deductible_amount DECIMAL(10,2) COMMENT 'Policy deductible amount in USD',
    status STRING NOT NULL COMMENT 'Current policy status',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
) COMMENT 'Master policy records containing core insurance policy information'""",
    "cols": ["policy_id","customer_id","agent_id","policy_number","policy_type","effective_date","expiration_date","premium_amount","deductible_amount","status","created_at","updated_at"],
    "gen": lambda i: (
        POLICY_IDS[i], CUSTOMER_IDS[i % NUM_RECORDS], AGENT_IDS[i % len(AGENT_IDS)],
        f"POL-{random.randint(100000,999999)}", random.choice(POLICY_TYPES),
        str(random_date(2020,2024)), str(random_date(2024,2026)),
        random_money(500,25000), random_money(250,5000), random.choice(STATUSES_POLICY),
        str(random_datetime()), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_policy.policy_admin.policy_versions",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_policy.policy_admin.policy_versions (
    version_id STRING NOT NULL COMMENT 'Unique identifier for the policy version',
    policy_id STRING NOT NULL COMMENT 'Reference to the parent policy',
    version_number INT NOT NULL COMMENT 'Sequential version number',
    effective_date DATE NOT NULL COMMENT 'Date this version became effective',
    change_reason STRING COMMENT 'Reason for the policy version change',
    premium_amount DECIMAL(12,2) COMMENT 'Premium amount for this version',
    changed_by STRING COMMENT 'User who made the change',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Version history tracking all changes made to insurance policies over time'""",
    "cols": ["version_id","policy_id","version_number","effective_date","change_reason","premium_amount","changed_by","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), POLICY_IDS[i % NUM_RECORDS], random.randint(1,5),
        str(random_date()), random.choice(["Initial creation","Coverage update","Premium adjustment","Renewal","Endorsement"]),
        random_money(500,25000), fake.name(), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_policy.policy_admin.coverage_details",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_policy.policy_admin.coverage_details (
    coverage_id STRING NOT NULL COMMENT 'Unique identifier for the coverage line item',
    policy_id STRING NOT NULL COMMENT 'Reference to the parent policy',
    coverage_type STRING NOT NULL COMMENT 'Type of coverage',
    coverage_limit DECIMAL(15,2) NOT NULL COMMENT 'Maximum coverage limit in USD',
    deductible DECIMAL(10,2) COMMENT 'Deductible amount in USD',
    premium_portion DECIMAL(10,2) COMMENT 'Portion of total premium for this coverage',
    is_active BOOLEAN NOT NULL COMMENT 'Whether this coverage is currently active',
    effective_date DATE NOT NULL COMMENT 'Date this coverage became effective',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Detailed coverage types, limits, and deductibles for each insurance policy'""",
    "cols": ["coverage_id","policy_id","coverage_type","coverage_limit","deductible","premium_portion","is_active","effective_date","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), POLICY_IDS[i % NUM_RECORDS], random.choice(COVERAGE_TYPES),
        random_money(10000,1000000), random_money(250,5000), random_money(50,5000),
        random.choice([True,True,True,False]), str(random_date()), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_policy.policy_admin.endorsements",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_policy.policy_admin.endorsements (
    endorsement_id STRING NOT NULL COMMENT 'Unique identifier for the endorsement',
    policy_id STRING NOT NULL COMMENT 'Reference to the policy being endorsed',
    endorsement_type STRING NOT NULL COMMENT 'Type of endorsement',
    endorsement_date DATE NOT NULL COMMENT 'Date the endorsement was applied',
    description STRING COMMENT 'Description of changes made',
    premium_change DECIMAL(10,2) COMMENT 'Change in premium amount',
    approved_by STRING COMMENT 'Person who approved the endorsement',
    status STRING NOT NULL COMMENT 'Endorsement status',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Mid-term policy modifications and endorsement records'""",
    "cols": ["endorsement_id","policy_id","endorsement_type","endorsement_date","description","premium_change","approved_by","status","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), POLICY_IDS[i % NUM_RECORDS], random.choice(ENDORSEMENT_TYPES),
        str(random_date()), fake.sentence(), round(random.uniform(-500,2000),2),
        fake.name(), random.choice(["Pending","Approved","Applied","Rejected"]), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_policy.policy_admin.renewals",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_policy.policy_admin.renewals (
    renewal_id STRING NOT NULL COMMENT 'Unique identifier for the renewal record',
    policy_id STRING NOT NULL COMMENT 'Reference to the policy being renewed',
    renewal_date DATE NOT NULL COMMENT 'Date of the renewal',
    previous_premium DECIMAL(12,2) COMMENT 'Premium amount before renewal',
    new_premium DECIMAL(12,2) COMMENT 'Premium amount after renewal',
    renewal_status STRING NOT NULL COMMENT 'Status of the renewal',
    renewal_term_months INT COMMENT 'Renewal term length in months',
    processed_by STRING COMMENT 'Agent or system that processed the renewal',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Policy renewal records tracking premium changes and renewal history'""",
    "cols": ["renewal_id","policy_id","renewal_date","previous_premium","new_premium","renewal_status","renewal_term_months","processed_by","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), POLICY_IDS[i % NUM_RECORDS], str(random_date()),
        random_money(500,25000), random_money(500,25000),
        random.choice(["Auto-Renewed","Manual","Lapsed","Declined"]),
        random.choice([6,12,24]), fake.name(), str(random_datetime())
    ),
})

# ─── ins_claims.claims_processing ─────
TABLE_DEFS.append({
    "name": "ins_claims.claims_processing.claims",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_claims.claims_processing.claims (
    claim_id STRING NOT NULL COMMENT 'Unique identifier for the insurance claim',
    policy_id STRING NOT NULL COMMENT 'Reference to the associated insurance policy',
    customer_id STRING NOT NULL COMMENT 'Reference to the claimant customer',
    claim_number STRING NOT NULL COMMENT 'Human-readable claim reference number',
    claim_type STRING NOT NULL COMMENT 'Type of claim',
    incident_date DATE NOT NULL COMMENT 'Date the incident occurred',
    reported_date DATE NOT NULL COMMENT 'Date the claim was reported (FNOL)',
    estimated_amount DECIMAL(15,2) COMMENT 'Initial estimated claim amount in USD',
    status STRING NOT NULL COMMENT 'Current claim status',
    priority STRING COMMENT 'Claim priority level',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
) COMMENT 'Master claim records for all insurance claims filed against policies'""",
    "cols": ["claim_id","policy_id","customer_id","claim_number","claim_type","incident_date","reported_date","estimated_amount","status","priority","created_at","updated_at"],
    "gen": lambda i: (
        CLAIM_IDS[i], POLICY_IDS[i % NUM_RECORDS], CUSTOMER_IDS[i % NUM_RECORDS],
        f"CLM-{random.randint(100000,999999)}", random.choice(CLAIM_TYPES),
        str(random_date()), str(random_date()), random_money(500,500000),
        random.choice(STATUSES_CLAIM), random.choice(["Low","Medium","High","Critical"]),
        str(random_datetime()), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_claims.claims_processing.claim_incidents",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_claims.claims_processing.claim_incidents (
    incident_id STRING NOT NULL COMMENT 'Unique identifier for the incident record',
    claim_id STRING NOT NULL COMMENT 'Reference to the associated claim',
    incident_date DATE NOT NULL COMMENT 'Date the incident occurred',
    incident_location STRING COMMENT 'Location where the incident took place',
    incident_state STRING COMMENT 'US state where the incident occurred',
    description STRING COMMENT 'Detailed description of the incident',
    police_report_number STRING COMMENT 'Police report number if applicable',
    witnesses_count INT COMMENT 'Number of witnesses',
    weather_conditions STRING COMMENT 'Weather conditions at time of incident',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Detailed incident information for insurance claims including location and circumstances'""",
    "cols": ["incident_id","claim_id","incident_date","incident_location","incident_state","description","police_report_number","witnesses_count","weather_conditions","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), CLAIM_IDS[i % NUM_RECORDS], str(random_date()),
        fake.street_address(), random.choice(US_STATES),
        fake.sentence(), f"PR-{random.randint(100000,999999)}" if random.random()>0.5 else None,
        random.randint(0,5), random.choice(["Clear","Rain","Snow","Fog","Windy","N/A"]),
        str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_claims.claims_processing.claimant_information",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_claims.claims_processing.claimant_information (
    claimant_id STRING NOT NULL COMMENT 'Unique identifier for the claimant record',
    claim_id STRING NOT NULL COMMENT 'Reference to the associated claim',
    first_name STRING NOT NULL COMMENT 'Claimant first name (PII)',
    last_name STRING NOT NULL COMMENT 'Claimant last name (PII)',
    date_of_birth DATE COMMENT 'Claimant date of birth (PII)',
    ssn_last_four STRING COMMENT 'Last four digits of SSN (PII)',
    phone_number STRING COMMENT 'Claimant phone number (PII)',
    email STRING COMMENT 'Claimant email address (PII)',
    relationship_to_insured STRING COMMENT 'Relationship to the policyholder',
    injury_description STRING COMMENT 'Description of injuries (PHI)',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Claimant personal information including PII and injury details - CONTAINS SENSITIVE DATA'""",
    "cols": ["claimant_id","claim_id","first_name","last_name","date_of_birth","ssn_last_four","phone_number","email","relationship_to_insured","injury_description","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), CLAIM_IDS[i % NUM_RECORDS], fake.first_name(), fake.last_name(),
        str(fake.date_of_birth(minimum_age=18,maximum_age=85)), f"{random.randint(1000,9999)}",
        fake.phone_number()[:14], fake.email(),
        random.choice(["Self","Spouse","Child","Parent","Third Party"]),
        random.choice(["None","Minor bruising","Whiplash","Fracture","Concussion","Soft tissue injury"]),
        str(random_datetime())
    ),
})

# ─── ins_claims.adjudication ─────
TABLE_DEFS.append({
    "name": "ins_claims.adjudication.claim_adjudications",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_claims.adjudication.claim_adjudications (
    adjudication_id STRING NOT NULL COMMENT 'Unique identifier for the adjudication record',
    claim_id STRING NOT NULL COMMENT 'Reference to the associated claim',
    adjuster_id STRING NOT NULL COMMENT 'Identifier of the claims adjuster',
    adjuster_name STRING COMMENT 'Name of the claims adjuster',
    review_date DATE NOT NULL COMMENT 'Date the claim was reviewed',
    decision STRING NOT NULL COMMENT 'Adjudication decision',
    approved_amount DECIMAL(15,2) COMMENT 'Approved claim amount in USD',
    denial_reason STRING COMMENT 'Reason for denial or partial approval',
    notes STRING COMMENT 'Adjuster notes and observations',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Claim adjudication decisions with approved amounts and rationale'""",
    "cols": ["adjudication_id","claim_id","adjuster_id","adjuster_name","review_date","decision","approved_amount","denial_reason","notes","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), CLAIM_IDS[i % NUM_RECORDS], str(uuid.uuid4()), fake.name(),
        str(random_date()), random.choice(["Approved","Denied","Partial","Escalated"]),
        random_money(500,500000),
        random.choice(["N/A","Policy exclusion","Insufficient evidence","Pre-existing condition","Filing deadline missed"]),
        fake.sentence(), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_claims.adjudication.evidence_documents",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_claims.adjudication.evidence_documents (
    document_id STRING NOT NULL COMMENT 'Unique identifier for the evidence document',
    claim_id STRING NOT NULL COMMENT 'Reference to the associated claim',
    document_type STRING NOT NULL COMMENT 'Type of evidence document',
    document_name STRING NOT NULL COMMENT 'Name of the uploaded document',
    file_path STRING COMMENT 'Storage path for the document file',
    uploaded_by STRING COMMENT 'Person who uploaded the document',
    upload_date DATE NOT NULL COMMENT 'Date the document was uploaded',
    file_size_bytes BIGINT COMMENT 'File size in bytes',
    is_verified BOOLEAN COMMENT 'Whether the document has been verified',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Supporting evidence and documentation metadata for insurance claims'""",
    "cols": ["document_id","claim_id","document_type","document_name","file_path","uploaded_by","upload_date","file_size_bytes","is_verified","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), CLAIM_IDS[i % NUM_RECORDS], random.choice(EVIDENCE_TYPES),
        f"{random.choice(EVIDENCE_TYPES).lower().replace(' ','_')}_{random.randint(1000,9999)}.pdf",
        f"/documents/claims/{random.randint(2020,2025)}/{uuid.uuid4()}.pdf",
        fake.name(), str(random_date()), random.randint(10000,50000000),
        random.choice([True,False]), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_claims.adjudication.claim_reserves",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_claims.adjudication.claim_reserves (
    reserve_id STRING NOT NULL COMMENT 'Unique identifier for the reserve record',
    claim_id STRING NOT NULL COMMENT 'Reference to the associated claim',
    reserve_type STRING NOT NULL COMMENT 'Type of reserve (Indemnity, Expense, Legal)',
    initial_amount DECIMAL(15,2) NOT NULL COMMENT 'Initial reserve amount in USD',
    current_amount DECIMAL(15,2) NOT NULL COMMENT 'Current reserve amount in USD',
    reserve_date DATE NOT NULL COMMENT 'Date the reserve was established',
    last_reviewed_date DATE COMMENT 'Date of last reserve review',
    set_by STRING COMMENT 'Person who set the reserve',
    status STRING NOT NULL COMMENT 'Reserve status',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Financial reserves allocated for insurance claims'""",
    "cols": ["reserve_id","claim_id","reserve_type","initial_amount","current_amount","reserve_date","last_reviewed_date","set_by","status","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), CLAIM_IDS[i % NUM_RECORDS],
        random.choice(["Indemnity","Expense","Legal"]),
        random_money(1000,500000), random_money(1000,500000),
        str(random_date()), str(random_date()), fake.name(),
        random.choice(["Active","Released","Exhausted"]), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_claims.adjudication.reserve_adjustments",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_claims.adjudication.reserve_adjustments (
    adjustment_id STRING NOT NULL COMMENT 'Unique identifier for the reserve adjustment',
    reserve_id STRING NOT NULL COMMENT 'Reference to the associated reserve',
    claim_id STRING NOT NULL COMMENT 'Reference to the associated claim',
    adjustment_date DATE NOT NULL COMMENT 'Date the adjustment was made',
    previous_amount DECIMAL(15,2) NOT NULL COMMENT 'Reserve amount before adjustment',
    new_amount DECIMAL(15,2) NOT NULL COMMENT 'Reserve amount after adjustment',
    adjustment_reason STRING COMMENT 'Reason for the adjustment',
    adjusted_by STRING COMMENT 'Person who made the adjustment',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'History of reserve amount changes and adjustments'""",
    "cols": ["adjustment_id","reserve_id","claim_id","adjustment_date","previous_amount","new_amount","adjustment_reason","adjusted_by","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), str(uuid.uuid4()), CLAIM_IDS[i % NUM_RECORDS],
        str(random_date()), random_money(1000,500000), random_money(1000,500000),
        random.choice(["New information received","Medical update","Legal development","Settlement negotiation","Periodic review"]),
        fake.name(), str(random_datetime())
    ),
})

# ─── ins_claims.payments ─────
TABLE_DEFS.append({
    "name": "ins_claims.payments.claim_payments",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_claims.payments.claim_payments (
    payment_id STRING NOT NULL COMMENT 'Unique identifier for the claim payment',
    claim_id STRING NOT NULL COMMENT 'Reference to the associated claim',
    payment_date DATE NOT NULL COMMENT 'Date the payment was issued',
    payment_amount DECIMAL(15,2) NOT NULL COMMENT 'Payment amount in USD',
    payment_method STRING NOT NULL COMMENT 'Method of payment',
    payee_name STRING NOT NULL COMMENT 'Name of the payment recipient (PII)',
    bank_account_last_four STRING COMMENT 'Last four digits of bank account (PII)',
    check_number STRING COMMENT 'Check number if paid by check',
    payment_status STRING NOT NULL COMMENT 'Payment status',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Claim payment transactions - CONTAINS SENSITIVE DATA'""",
    "cols": ["payment_id","claim_id","payment_date","payment_amount","payment_method","payee_name","bank_account_last_four","check_number","payment_status","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), CLAIM_IDS[i % NUM_RECORDS], str(random_date()),
        random_money(100,500000), random.choice(PAYMENT_METHODS), fake.name(),
        f"{random.randint(1000,9999)}",
        f"CHK-{random.randint(100000,999999)}" if random.random()>0.5 else None,
        random.choice(["Pending","Processed","Cleared","Voided"]), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_claims.payments.subrogation_cases",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_claims.payments.subrogation_cases (
    subrogation_id STRING NOT NULL COMMENT 'Unique identifier for the subrogation case',
    claim_id STRING NOT NULL COMMENT 'Reference to the associated claim',
    third_party_name STRING COMMENT 'Name of the third party being pursued',
    third_party_insurer STRING COMMENT 'Insurance company of the third party',
    demand_amount DECIMAL(15,2) COMMENT 'Amount demanded in USD',
    recovered_amount DECIMAL(15,2) COMMENT 'Amount actually recovered in USD',
    status STRING NOT NULL COMMENT 'Subrogation status',
    opened_date DATE NOT NULL COMMENT 'Date the case was opened',
    closed_date DATE COMMENT 'Date the case was closed',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Subrogation and recovery cases for pursuing third-party reimbursement'""",
    "cols": ["subrogation_id","claim_id","third_party_name","third_party_insurer","demand_amount","recovered_amount","status","opened_date","closed_date","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), CLAIM_IDS[i % NUM_RECORDS], fake.name(),
        fake.company() + " Insurance", random_money(1000,300000), random_money(0,300000),
        random.choice(["Open","Negotiating","Settled","Closed","Abandoned"]),
        str(random_date()), str(random_date()) if random.random()>0.3 else None,
        str(random_datetime())
    ),
})

# ─── ins_customer.customer_master ─────
TABLE_DEFS.append({
    "name": "ins_customer.customer_master.customers",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_customer.customer_master.customers (
    customer_id STRING NOT NULL COMMENT 'Unique identifier for the customer',
    first_name STRING NOT NULL COMMENT 'Customer first name (PII)',
    last_name STRING NOT NULL COMMENT 'Customer last name (PII)',
    date_of_birth DATE COMMENT 'Customer date of birth (PII)',
    ssn_encrypted STRING COMMENT 'Encrypted Social Security Number (PII)',
    gender STRING COMMENT 'Customer gender',
    marital_status STRING COMMENT 'Customer marital status',
    customer_since DATE COMMENT 'Date the customer relationship began',
    customer_segment STRING COMMENT 'Customer segment',
    status STRING NOT NULL COMMENT 'Customer status',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
) COMMENT 'Master customer records with personal identification - CONTAINS SENSITIVE PII DATA'""",
    "cols": ["customer_id","first_name","last_name","date_of_birth","ssn_encrypted","gender","marital_status","customer_since","customer_segment","status","created_at","updated_at"],
    "gen": lambda i: (
        CUSTOMER_IDS[i], fake.first_name(), fake.last_name(),
        str(fake.date_of_birth(minimum_age=18,maximum_age=85)),
        fake.sha256()[:64],
        random.choice(["Male","Female","Non-binary","Prefer not to say"]),
        random.choice(["Single","Married","Divorced","Widowed"]),
        str(random_date(2010,2024)), random.choice(["Individual","Family","Commercial"]),
        random.choice(["Active","Inactive","Suspended"]),
        str(random_datetime()), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_customer.customer_master.customer_addresses",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_customer.customer_master.customer_addresses (
    address_id STRING NOT NULL COMMENT 'Unique identifier for the address record',
    customer_id STRING NOT NULL COMMENT 'Reference to the customer',
    address_type STRING NOT NULL COMMENT 'Type of address',
    street_address STRING NOT NULL COMMENT 'Street address line (PII)',
    city STRING NOT NULL COMMENT 'City name',
    state STRING NOT NULL COMMENT 'US state code',
    zip_code STRING NOT NULL COMMENT 'ZIP code',
    is_primary BOOLEAN NOT NULL COMMENT 'Whether this is the primary address',
    effective_date DATE COMMENT 'Date this address became effective',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Customer address history including home, mailing, and business addresses'""",
    "cols": ["address_id","customer_id","address_type","street_address","city","state","zip_code","is_primary","effective_date","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), CUSTOMER_IDS[i % NUM_RECORDS],
        random.choice(["Home","Mailing","Business"]),
        fake.street_address(), fake.city(), random.choice(US_STATES), fake.zipcode(),
        random.choice([True,False]), str(random_date()), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_customer.customer_master.customer_contacts",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_customer.customer_master.customer_contacts (
    contact_id STRING NOT NULL COMMENT 'Unique identifier for the contact record',
    customer_id STRING NOT NULL COMMENT 'Reference to the customer',
    contact_type STRING NOT NULL COMMENT 'Type of contact',
    contact_value STRING NOT NULL COMMENT 'Contact value - phone or email (PII)',
    is_primary BOOLEAN NOT NULL COMMENT 'Whether this is the primary contact',
    is_verified BOOLEAN COMMENT 'Whether this contact has been verified',
    opt_in_marketing BOOLEAN COMMENT 'Whether customer opted in to marketing',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Customer contact information - CONTAINS PII'""",
    "cols": ["contact_id","customer_id","contact_type","contact_value","is_primary","is_verified","opt_in_marketing","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), CUSTOMER_IDS[i % NUM_RECORDS],
        random.choice(["Phone","Email","Mobile"]),
        fake.email() if random.random()>0.5 else fake.phone_number()[:14],
        random.choice([True,False]), random.choice([True,True,False]),
        random.choice([True,False]), str(random_datetime())
    ),
})

# ─── ins_customer.agent_management ─────
TABLE_DEFS.append({
    "name": "ins_customer.agent_management.agents",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_customer.agent_management.agents (
    agent_id STRING NOT NULL COMMENT 'Unique identifier for the insurance agent',
    first_name STRING NOT NULL COMMENT 'Agent first name',
    last_name STRING NOT NULL COMMENT 'Agent last name',
    license_number STRING COMMENT 'State insurance license number',
    license_type STRING COMMENT 'Type of insurance license held',
    license_state STRING COMMENT 'State where the license is issued',
    agency_name STRING COMMENT 'Name of the agency',
    hire_date DATE COMMENT 'Date the agent was hired',
    territory STRING COMMENT 'Sales territory assigned',
    status STRING NOT NULL COMMENT 'Agent status',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Insurance agent profiles including licensing and territory information'""",
    "cols": ["agent_id","first_name","last_name","license_number","license_type","license_state","agency_name","hire_date","territory","status","created_at"],
    "gen": lambda i: (
        AGENT_IDS[i] if i < len(AGENT_IDS) else str(uuid.uuid4()),
        fake.first_name(), fake.last_name(), f"LIC-{random.randint(100000,999999)}",
        random.choice(LICENSE_TYPES), random.choice(US_STATES),
        fake.company() + " Insurance Agency", str(random_date(2010,2024)),
        random.choice(["Northeast","Southeast","Midwest","Southwest","West","Northwest"]),
        random.choice(["Active","Inactive","Terminated","On Leave"]),
        str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_customer.agent_management.agent_assignments",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_customer.agent_management.agent_assignments (
    assignment_id STRING NOT NULL COMMENT 'Unique identifier for the agent-policy assignment',
    agent_id STRING NOT NULL COMMENT 'Reference to the assigned agent',
    policy_id STRING NOT NULL COMMENT 'Reference to the assigned policy',
    assignment_date DATE NOT NULL COMMENT 'Date the agent was assigned',
    role STRING NOT NULL COMMENT 'Agent role for this policy',
    is_active BOOLEAN NOT NULL COMMENT 'Whether the assignment is active',
    commission_rate DECIMAL(5,4) COMMENT 'Commission rate for this assignment',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Agent-to-policy assignments tracking which agents service which policies'""",
    "cols": ["assignment_id","agent_id","policy_id","assignment_date","role","is_active","commission_rate","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), AGENT_IDS[i % len(AGENT_IDS)], POLICY_IDS[i % NUM_RECORDS],
        str(random_date()), random.choice(["Primary","Secondary","Servicing"]),
        random.choice([True,True,True,False]), round(random.uniform(0.02,0.15),4),
        str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_customer.agent_management.agent_commissions",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_customer.agent_management.agent_commissions (
    commission_id STRING NOT NULL COMMENT 'Unique identifier for the commission record',
    agent_id STRING NOT NULL COMMENT 'Reference to the agent earning the commission',
    policy_id STRING NOT NULL COMMENT 'Reference to the policy generating the commission',
    commission_date DATE NOT NULL COMMENT 'Date the commission was earned',
    commission_amount DECIMAL(10,2) NOT NULL COMMENT 'Commission amount in USD',
    commission_type STRING NOT NULL COMMENT 'Type of commission',
    commission_rate DECIMAL(5,4) COMMENT 'Commission rate applied',
    premium_basis DECIMAL(12,2) COMMENT 'Premium amount commission was calculated on',
    payment_status STRING NOT NULL COMMENT 'Payment status',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Agent commission records for policy sales and renewals'""",
    "cols": ["commission_id","agent_id","policy_id","commission_date","commission_amount","commission_type","commission_rate","premium_basis","payment_status","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), AGENT_IDS[i % len(AGENT_IDS)], POLICY_IDS[i % NUM_RECORDS],
        str(random_date()), random_money(50,5000),
        random.choice(["New Business","Renewal","Override","Bonus"]),
        round(random.uniform(0.02,0.15),4), random_money(500,25000),
        random.choice(["Pending","Paid","Reversed"]), str(random_datetime())
    ),
})

# ─── ins_customer.crm ─────
TABLE_DEFS.append({
    "name": "ins_customer.crm.customer_interactions",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_customer.crm.customer_interactions (
    interaction_id STRING NOT NULL COMMENT 'Unique identifier for the customer interaction',
    customer_id STRING NOT NULL COMMENT 'Reference to the customer',
    interaction_date TIMESTAMP NOT NULL COMMENT 'Date and time of the interaction',
    interaction_type STRING NOT NULL COMMENT 'Type of interaction',
    channel STRING COMMENT 'Communication channel used',
    subject STRING COMMENT 'Subject or topic of the interaction',
    summary STRING COMMENT 'Summary of the interaction',
    handled_by STRING COMMENT 'Agent or representative who handled it',
    satisfaction_score INT COMMENT 'Customer satisfaction score (1-10)',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Customer interaction and touchpoint log for CRM tracking'""",
    "cols": ["interaction_id","customer_id","interaction_date","interaction_type","channel","subject","summary","handled_by","satisfaction_score","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), CUSTOMER_IDS[i % NUM_RECORDS], str(random_datetime()),
        random.choice(INTERACTION_TYPES),
        random.choice(["Web","Phone","Mobile App","Branch","Partner"]),
        random.choice(["Policy inquiry","Claim status","Billing question","Coverage change","Complaint","General inquiry"]),
        fake.sentence(), fake.name(), random.randint(1,10), str(random_datetime())
    ),
})

TABLE_DEFS.append({
    "name": "ins_customer.crm.customer_risk_scores",
    "ddl": """CREATE TABLE IF NOT EXISTS ins_customer.crm.customer_risk_scores (
    score_id STRING NOT NULL COMMENT 'Unique identifier for the risk score record',
    customer_id STRING NOT NULL COMMENT 'Reference to the customer',
    score_date DATE NOT NULL COMMENT 'Date the risk score was calculated',
    overall_risk_score DECIMAL(5,2) NOT NULL COMMENT 'Overall customer risk score (0-100)',
    claims_frequency_score DECIMAL(5,2) COMMENT 'Score based on claims frequency (0-100)',
    claims_severity_score DECIMAL(5,2) COMMENT 'Score based on claims severity (0-100)',
    payment_history_score DECIMAL(5,2) COMMENT 'Score based on payment history (0-100)',
    fraud_indicator_score DECIMAL(5,2) COMMENT 'Score indicating fraud risk (0-100)',
    model_version STRING COMMENT 'Version of the risk scoring model used',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
) COMMENT 'Customer risk scoring history for underwriting and fraud detection'""",
    "cols": ["score_id","customer_id","score_date","overall_risk_score","claims_frequency_score","claims_severity_score","payment_history_score","fraud_indicator_score","model_version","created_at"],
    "gen": lambda i: (
        str(uuid.uuid4()), CUSTOMER_IDS[i % NUM_RECORDS], str(random_date()),
        round(random.uniform(0,100),2), round(random.uniform(0,100),2),
        round(random.uniform(0,100),2), round(random.uniform(0,100),2),
        round(random.uniform(0,100),2),
        random.choice(["v1.0","v1.1","v2.0","v2.1"]), str(random_datetime())
    ),
})


# ─── GOVERNED TAGS ─────
DATA_PRODUCT_TAGS = {
    "policy_lifecycle": [
        "ins_policy.policy_admin.policies",
        "ins_policy.policy_admin.policy_versions",
        "ins_policy.policy_admin.coverage_details",
        "ins_policy.policy_admin.endorsements",
        "ins_policy.policy_admin.renewals",
    ],
    "underwriting_risk": [
        "ins_policy.underwriting.risk_applications",
        "ins_policy.underwriting.underwriting_decisions",
        "ins_policy.underwriting.risk_assessments",
    ],
    "claims_management": [
        "ins_claims.claims_processing.claims",
        "ins_claims.claims_processing.claim_incidents",
        "ins_claims.claims_processing.claimant_information",
    ],
    "claims_financial": [
        "ins_claims.adjudication.claim_adjudications",
        "ins_claims.adjudication.claim_reserves",
        "ins_claims.adjudication.reserve_adjustments",
        "ins_claims.payments.claim_payments",
        "ins_claims.payments.subrogation_cases",
    ],
    "customer_360": [
        "ins_customer.customer_master.customers",
        "ins_customer.customer_master.customer_addresses",
        "ins_customer.customer_master.customer_contacts",
        "ins_customer.crm.customer_interactions",
        "ins_customer.crm.customer_risk_scores",
    ],
    "agent_network": [
        "ins_customer.agent_management.agents",
        "ins_customer.agent_management.agent_assignments",
        "ins_customer.agent_management.agent_commissions",
    ],
    "evidence_docs": [
        "ins_claims.adjudication.evidence_documents",
    ],
}


def build_insert_sql(table_name, cols, rows):
    """Build a multi-row INSERT VALUES statement."""
    col_str = ", ".join(cols)
    value_rows = []
    for row in rows:
        vals = ", ".join(esc(v) for v in row)
        value_rows.append(f"({vals})")
    values_str = ",\n".join(value_rows)
    return f"INSERT INTO {table_name} ({col_str}) VALUES\n{values_str}"


def main():
    host, token = get_connection_params()
    print(f"Connecting to {host}...")
    db = DatabricksSQL(host, token, WAREHOUSE_ID)
    print("Connected.\n")

    # Step 1: Create catalogs and schemas
    print("=" * 60)
    print("STEP 1: Creating catalogs and schemas")
    print("=" * 60)

    for catalog, info in CATALOGS_SCHEMAS.items():
        comment = info["comment"].replace("'", "''")
        db.execute_quiet(
            f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT '{comment}'",
            f"Catalog: {catalog}"
        )
        for schema, schema_comment in info["schemas"].items():
            sc = schema_comment.replace("'", "''")
            db.execute_quiet(
                f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT '{sc}'",
                f"  Schema: {catalog}.{schema}"
            )

    # Step 2: Create tables
    print("\n" + "=" * 60)
    print("STEP 2: Creating tables")
    print("=" * 60)

    for tdef in TABLE_DEFS:
        db.execute_quiet(tdef["ddl"], f"Table: {tdef['name']}")

    # Step 3: Generate and insert data
    print("\n" + "=" * 60)
    print("STEP 3: Generating and inserting data (~100K records per table)")
    print("=" * 60)

    for tdef in TABLE_DEFS:
        table_name = tdef["name"]
        print(f"\n  Populating {table_name}...")

        # Check existing count
        row = db.fetch_one(f"SELECT COUNT(*) FROM {table_name}")
        count = int(row[0]) if row else 0
        if count >= NUM_RECORDS:
            print(f"    Already has {count:,} records, skipping.")
            continue

        gen_fn = tdef["gen"]
        cols = tdef["cols"]
        batch = []
        inserted = 0

        for i in range(NUM_RECORDS):
            row = gen_fn(i)
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                sql = build_insert_sql(table_name, cols, batch)
                try:
                    db.execute(sql, wait_timeout="50s")
                except Exception as e:
                    print(f"    ERROR at batch {inserted}-{inserted+len(batch)}: {e}")
                    # Try smaller batch
                    half = len(batch) // 2
                    if half > 0:
                        try:
                            sql1 = build_insert_sql(table_name, cols, batch[:half])
                            db.execute(sql1, wait_timeout="50s")
                            sql2 = build_insert_sql(table_name, cols, batch[half:])
                            db.execute(sql2, wait_timeout="50s")
                        except Exception as e2:
                            print(f"    RETRY FAILED: {e2}")

                inserted += len(batch)
                batch = []
                if inserted % 20000 == 0:
                    pct = (inserted / NUM_RECORDS) * 100
                    print(f"    {pct:.0f}% ({inserted:,}/{NUM_RECORDS:,})")

        # Insert remaining
        if batch:
            sql = build_insert_sql(table_name, cols, batch)
            try:
                db.execute(sql, wait_timeout="50s")
            except Exception as e:
                print(f"    ERROR on final batch: {e}")
            inserted += len(batch)

        # Verify
        row = db.fetch_one(f"SELECT COUNT(*) FROM {table_name}")
        final_count = int(row[0]) if row else 0
        print(f"    Done: {final_count:,} records")

    # Step 4: Apply governed tags
    print("\n" + "=" * 60)
    print("STEP 4: Applying governed tags (data_product)")
    print("=" * 60)

    for product_name, tables in DATA_PRODUCT_TAGS.items():
        for table in tables:
            db.execute_quiet(
                f"ALTER TABLE {table} SET TAGS ('data_product' = '{product_name}')",
                f"Tag {table} -> data_product={product_name}"
            )

    print("\n" + "=" * 60)
    print("COMPLETE!")
    print("=" * 60)


if __name__ == "__main__":
    main()
