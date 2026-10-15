# ⚡ Serverless Schema-Aware ETL Pipeline

A production-grade, event-driven Serverless ETL (Extract, Transform, Load) Pipeline engineered in Python. The pipeline dynamically ingests arbitrary datasets (Customer, Employee, Sales, IoT Telemetry, etc.) in CSV and JSON formats, performs automated schema detection or configured schema validation, anomaly detection, deduplication, cleanly partitions accepted and rejected records, and generates an audited reconciliation report.

Built for high reliability, zero data loss, and complete local-to-cloud parity: **run 100% offline on your machine or deploy serverlessly to AWS Lambda and Amazon S3.**

---

## 📑 Table of Contents
- [Architecture & System Design](#-architecture--system-design)
- [Complete Data Flow](#-complete-data-flow)
- [Project Directory Structure](#-project-directory-structure)
- [Core Invariants & Quality Standards](#-core-invariants--quality-standards)
- [Local Quickstart & Execution](#-local-quickstart--execution)
- [Streamlit Operations Dashboard](#-streamlit-operations-dashboard)
- [Automated Testing Suite (Pytest)](#-automated-testing-suite-pytest)
- [AWS Cloud Deployment Guide (Step-by-Step)](#-aws-cloud-deployment-guide-step-by-step)
- [IAM Least-Privilege Policies](#-iam-least-privilege-policies)
- [Recursion Guard & Operational Safeguards](#-recursion-guard--operational-safeguards)
- [Hackathon Demonstration Script](#-hackathon-demonstration-script)
- [Verification Checklist (PASS/FAIL Evidence)](#-verification-checklist)

---

## 🏗 Architecture & System Design

```mermaid
graph TD
    subgraph "Ingestion Sources"
        A1["Local Files / Streams\n(CSV or JSON)"]
        A2["Amazon S3 Bucket\ns3://bucket/input/*"]
    end

    subgraph "ETL Engine (Schema-Aware Core)"
        B["Extractor\n• CSV DictReader & JSON Parser\n• Duplicate Header Detection\n• Empty & Malformed Detection"]
        SI["Schema Inferrer\n• Dynamic Type Inference (majority vote)\n• Null Counts & Identifier Detection\n• Warnings vs Errors Detection"]
        C["Schema Validator\n• Auto Mode (Inferred Rules)\n• Configured Mode (User Rules/Presets)\n• Type Checks, Ranges & Deduplication"]
        D["Transformer\n• Dynamic Whitespace Stripping\n• Domain-Aware Normalization\n• Injects UTC Audit Metadata"]
        E["Reconciler\n• Enforce Invariant: Total = Acc + Rej\n• Quality Score (0-100%)\n• Anomaly Reason Analytics"]
    end

    subgraph "Output Partitions"
        F1["output/accepted/\nClean Records (CSV / JSON)"]
        F2["output/rejected/\nOriginal Data + Rejection Reasons"]
        F3["output/reports/\nAudited Reconciliation JSON"]
    end

    subgraph "Delivery Interfaces"
        G1["Command Line Interface\n(cli.py --mode / --preset)"]
        G2["Streamlit Monitoring Dashboard\n(dashboard/app.py)"]
        G3["AWS Lambda Function\n(lambda_function.py)"]
        G4["Amazon CloudWatch Logs"]
    end

    A1 --> G1
    A1 --> G2
    A2 -->|S3:ObjectCreated Notification| G3
    G3 --> B
    G1 --> B
    G2 --> B
    B --> SI
    SI --> C
    B --> C
    C -->|Valid Records| D
    C -->|Invalid Records| E
    D --> E
    E --> F1
    E --> F2
    E --> F3
    G3 -->|PutObject AES256| A2
    G3 -->|Structured Logs| G4
```

---

## 🌊 Complete Data Flow & Dual Validation Modes

The pipeline operates in two distinct, non-destructive validation modes:
- **Automatic Mode (`auto`)**: Automatically analyzes the incoming records, infers column data types using robust majority-vote heuristics (so occasional anomalies don't ruin type detection), checks null rates, detects candidate unique identifiers, and validates against safe type and non-null constraints.
- **Configured Mode (`configured`)**: Evaluates datasets against an explicitly provided JSON schema or domain preset (`customer`, `employee`, `sales`). Enforces required fields, exact data types, value boundaries, and unique constraints.

### 5-Stage Execution Flow:
1. **Extraction (`src/engine/extractor.py`)**:
   - Accepts CSV or JSON via file path, bytes, or in-memory string.
   - Detects duplicate column headers in CSV files and reports a schema warning.
   - Gracefully handles empty files (returns 0 records without crashing) and invalid JSON/CSV syntax.
2. **Schema Inference & Discovery (`src/engine/schema_inferrer.py`)**:
   - Infers column types (`integer`, `float`, `boolean`, `date`, `email`, `string`) using a 70% confidence threshold to prevent anomalous rows from degrading the inferred type.
   - Detects potential unique identifier columns using naming conventions (`*_id`, `*_key`, `*_uuid`) combined with high uniqueness ratios.
   - Computes missing/empty value counts per column.
3. **Validation (`src/engine/validator.py`)**:
   - **Never hardcodes `customer_id` or customer-specific fields**.
   - In `auto` mode, applies inferred rules; distinguishes warnings (e.g. optional null values) from errors.
   - In `configured` mode, strictly asserts required fields, type conversions, regex formats, and value ranges.
   - Dynamically tracks unique values of any configured or detected identifier column to reject in-batch duplicates.
4. **Transformation (`src/engine/transformer.py`)**:
   - Strips whitespace across all string columns for arbitrary dataset structures.
   - Applies domain-specific transformations when relevant (e.g. ISO-3 country normalization and lowercase emails for customer schemas).
   - Injects a UTC ISO-8601 audit timestamp (`processed_at`).
5. **Reconciliation & Auditing (`src/engine/reconciler.py`)**:
   - Calculates execution duration and quality score: `Quality = (Accepted / Total) * 100`.
   - **Enforces Accounting Invariant**: asserts `Total Input == Accepted Records + Rejected Records`.
   - Records granular rejection causes for every invalid row.
6. **Persistence (`src/engine/pipeline.py` & `src/aws/s3_service.py`)**:
   - Dynamically writes accepted records with accurate field headers to `output/accepted/`.
   - Persists rejected records with original fields and `_rejection_reasons` to `output/rejected/`.
   - Writes reconciliation report including detected schema and warnings to `output/reports/`.

---

## 📁 Project Directory Structure

```text
Cloud_Hackathon/
├── .env.example                 # Non-secret environment variable template
├── .gitignore                   # Excludes secrets, virtual environments, and outputs
├── README.md                    # System architecture and full documentation
├── requirements.txt             # Project dependencies (local & dashboard)
├── requirements-lambda.txt      # Minimal AWS Lambda packaging dependencies
├── lambda_function.py           # AWS Lambda root entrypoint wrapper
├── cli.py                       # Command line interface for local & S3 execution
│
├── config/                      # Configuration management
│   ├── __init__.py
│   └── settings.py              # Environment variable loader with safe defaults
│
├── src/                         # Core application source code
│   ├── __init__.py
│   ├── models/                  # Domain schemas and data structures
│   │   ├── __init__.py
│   │   ├── record.py            # CustomerRecord, DataRecord, ReconciliationReport
│   │   └── schema.py            # FieldDefinition, DatasetSchema, SchemaInferenceResult
│   ├── engine/                  # Pure, decoupled ETL processing engine
│   │   ├── __init__.py
│   │   ├── extractor.py         # CSV & JSON extraction, duplicate header detection
│   │   ├── schema_inferrer.py   # Type inference, null counts, identifier detection
│   │   ├── validator.py         # SchemaValidator & CustomerValidator (backward compatible)
│   │   ├── transformer.py       # Generic & domain-aware record transformations
│   │   ├── reconciler.py        # Invariant enforcement and quality scoring
│   │   └── pipeline.py          # Orchestrates execution, dynamic CSV/JSON outputs
│   ├── aws/                     # AWS Cloud integrations
│   │   ├── __init__.py
│   │   ├── s3_service.py        # Reusable S3 operations (list, download, upload)
│   │   └── lambda_handler.py    # Event parser, URL decoding, recursion guard
│   └── utils/                   # Shared utilities
│       ├── __init__.py
│       └── logger.py            # Centralized logger with credential scrubbing
│
├── dashboard/                   # Monitoring & operations dashboard
│   └── app.py                   # Streamlit web application with Schema Discovery
│
├── sample_data/                 # Multi-domain test and demonstration datasets
│   ├── customers_valid.csv      # Customer domain: 100% clean valid records
│   ├── customers_valid.json     # Customer domain: clean JSON dataset
│   ├── customers_mixed.csv      # Customer domain: mixed (valid, bad emails, duplicates)
│   ├── customers_mixed.json     # Customer domain: mixed JSON dataset
│   ├── customers_empty.csv      # Edge case: 0 records
│   ├── customers_malformed.csv  # Edge case: syntax errors
│   ├── employees_valid.csv      # Employee domain: valid employee_id, name, salary, dept
│   ├── employees_mixed.csv      # Employee domain: mixed with invalid salary and blanks
│   ├── sales_valid.json         # Sales domain: valid order_id, product, quantity, price
│   ├── sales_mixed.csv          # Sales domain: mixed with negative quantity and bad price
│   └── sensor_readings.csv      # IoT domain: arbitrary telemetry (device_id, temp, humidity)
│
├── tests/                       # Automated test suite (48 tests)
│   ├── __init__.py
│   ├── conftest.py              # Shared fixtures
│   ├── test_extractor.py        # CSV/JSON/Empty/Malformed tests
│   ├── test_validator.py        # Missing fields, regex, age bounds, duplicate tests
│   ├── test_transformer.py      # Cleansing and country mapping tests
│   ├── test_pipeline.py         # Invariant checks, file output tests
│   ├── test_s3_service.py       # Mocked S3 listing, downloads, and uploads
│   ├── test_lambda_handler.py   # Event parsing, URL decoding, recursion guards
│   └── test_schema_awareness.py # Multi-domain, schema inference, and config tests
│
├── test_fixtures/               # Test event payloads
│   └── s3_event.json            # Simulated S3 ObjectCreated event
│
├── scripts/                     # Operational automation scripts
│   └── package_lambda.py        # Automated deployment zip builder
│
└── output/                      # Local output partition directories
    ├── accepted/
    ├── rejected/
    └── reports/
```

---

## ⚖️ Core Invariants & Quality Standards

1. **Zero Silent Record Loss**:
   $$\text{Total Input Records} = \text{Accepted Records} + \text{Rejected Records}$$
   If any discrepancy occurs, the engine flags an invariant violation immediately.
2. **Auditability**:
   Every rejected record retains its complete, unmodified raw payload alongside human-readable `_rejection_reasons` and UTC `_rejected_at` timestamp.
3. **Data Quality Score**:
   Every run calculates a quantitative quality metric:
   $$\text{Quality Score} = \left(\frac{\text{Accepted Records}}{\text{Total Input Records}}\right) \times 100\%$$
4. **Cloud Decoupling**:
   The core engine (`src/engine/`) imports standard Python libraries only. It runs on any machine with 0 external cloud dependencies.
5. **Security**:
   - Zero hardcoded credentials.
   - Real-time regex log filter scrubs API keys, tokens, and secrets from all logger outputs.
   - S3 uploads enforce server-side AES256 encryption.

---

## 💻 Local Quickstart & Execution

### 1. Prerequisites
- Python 3.10+ (tested on Python 3.13)
- `pip`

### 2. Setup Environment
```bash
# Clone or navigate to the directory
cd Cloud_Hackathon

# (Optional) Create and activate virtual environment
python -m venv venv
venv\Scripts\activate   # Windows
# source venv/bin/activate # macOS/Linux

# Install dependencies
pip install -r requirements.txt
```

### 3. Run Pipeline via CLI

The CLI provides two flexible validation modes:
- `--mode auto` (default): Automatically detects columns, types, nulls, and identifier keys.
- `--mode configured`: Enforces rules from a preset (`--preset customer|employee|sales`) or a custom JSON schema (`--schema-file path/to/schema.json`).

#### A. Automatic Mode (Arbitrary Datasets)
```bash
# Process Employee dataset (inferred schema, employee_id detected)
python cli.py local --input sample_data/employees_mixed.csv

# Process Sales dataset (inferred schema, order_id detected)
python cli.py local --input sample_data/sales_mixed.csv

# Process IoT Sensor Telemetry (arbitrary columns)
python cli.py local --input sample_data/sensor_readings.csv
```

#### B. Configured-Schema Mode (Presets or Custom Schemas)
```bash
# Strictly validate Employee dataset with employee preset
python cli.py local --input sample_data/employees_valid.csv --mode configured --preset employee

# Strictly validate Sales dataset with sales preset
python cli.py local --input sample_data/sales_valid.json --mode configured --preset sales

# Customer dataset (preserves legacy validation behavior)
python cli.py local --input sample_data/customers_mixed.csv --mode configured --preset customer
```

**Example Console Output (Employee Dataset in Auto Mode):**
```text
======================================================================
 [SERVERLESS ETL PIPELINE] - Core Engine & AWS Integrations
======================================================================
2026-10-15 10:45:00 [INFO] [src.engine.pipeline] Inferred schema 'inferred_employees_mixed.csv' with 5 fields. Potential identifier: employee_id
2026-10-15 10:45:00 [INFO] [src.engine.pipeline] Processing complete in 0.0006s: Total=6, Accepted=4, Rejected=2, Status=PARTIAL_SUCCESS

----------------------------------------------------------------------
 [RECONCILIATION & AUDIT REPORT]
----------------------------------------------------------------------
 Run ID:             run_20261015_051500_1a2b3c
 Source File:        employees_mixed.csv
 Validation Mode:    auto
 Schema Applied:     inferred_employees_mixed.csv
 Primary Key:        employee_id
 Processing Status:  PARTIAL_SUCCESS
 Duration:           0.0006s
 Total Input:        6
 Accepted Records:   4
 Rejected Records:   2
 Duplicate IDs:      0
 Data Quality Score: 66.7%
 Invariant Holds:    [OK] YES (Total = Accepted + Rejected)

 [!] Rejection Breakdown:
   * Invalid salary: cannot convert 'not_a_number' to float: 1
   * Required field 'employee_name' cannot be empty or blank: 1
----------------------------------------------------------------------

 [OUTPUT FILES GENERATED]
   * Accepted: output\accepted\accepted_run_20261015_051500_1a2b3c.json
   * Rejected: output\rejected\rejected_run_20261015_051500_1a2b3c.json
   * Report: output\reports\report_run_20261015_051500_1a2b3c.json
```

---

## 📊 Streamlit Operations Dashboard

Launch the interactive monitoring dashboard:
```bash
streamlit run dashboard/app.py
```

### Dashboard Features:
1. **Schema Discovery Panel**: View inferred or configured column names, detected data types, missing/null counts, and detected unique identifiers *before* and *after* execution.
2. **Dual Validation Mode Controls**: Toggle between **Automatic Mode** (dynamic inference + explainable anomaly checks) and **Configured Mode** (strict preset or JSON schema enforcement).
3. **Multi-Domain Sample Datasets**: Instant 1-click execution for Customers, Employees, Sales Orders, and IoT Sensor Telemetry.
4. **Local vs S3 Cloud Mode Toggle**: Seamlessly switch between 100% offline local processing and live AWS infrastructure.
5. **Executive KPI Cards**: Real-time counters for Total Records, Accepted, Rejected, Duplicate IDs, and Data Quality %.
6. **Interactive Data Tables**: Search, filter, and inspect accepted records and rejected anomalies.
7. **Rejection Analytics**: Visual bar chart breakdown of anomaly categories (type mismatches, missing required fields, duplicates, blank values).
8. **Data Exports**: Download clean data (CSV/JSON), rejected anomalies, or the formal reconciliation report.
9. **Session Audit Trail**: History table tracking all runs executed in the current session.

---

## 🧪 Automated Testing Suite (Pytest)

Run all 48 deterministic unit and integration tests:
```bash
pytest tests/ -v
```

### Test Coverage Highlights:
- **`test_schema_awareness.py` (8 new tests)**: Dynamic schema inference for employees, sales, and arbitrary IoT telemetry; non-rejection of optional nulls; anomaly rejection under inferred types; strict configured schema mode; missing required columns; empty and malformed file handling.
- **`test_extractor.py`**: CSV parsing, JSON parsing, empty files, malformed syntax errors, unsupported extensions, missing files, duplicate column header detection.
- **`test_validator.py`**: Required field presence, blank strings, RFC email validation, age bounds, batch duplicate identifier detection, state reset.
- **`test_transformer.py`**: Whitespace trimming, email lowercasing, country alias mapping (US, UK, IN, CA, DE, FR, AU, JP, SG), integer casting.
- **`test_pipeline.py`**: End-to-end local runs, reconciliation invariant verification (`Total == Accepted + Rejected`), output file generation.
- **`test_s3_service.py`**: Mocked S3 client operations, prefix listing, secure downloading, AES256 server-side encryption enforcement.
- **`test_lambda_handler.py`**: S3 event parsing, URL-decoded object keys (`customers+batch%201.csv`), recursion guard skipping `output/` keys, error payloads.

---

## ☁️ AWS Cloud Deployment Guide (Step-by-Step)

Follow these steps to deploy the ETL pipeline to AWS using the AWS Management Console or AWS CLI.

### Step 1: Select Region & Create S3 Bucket
1. Open the AWS Console and select your region (e.g., `ap-south-1` or `us-east-1`).
2. Navigate to **Amazon S3** > **Create bucket**.
3. Name your bucket (e.g. `serverless-etl-pipeline-<your-name>-2026`).
4. Keep **Block all public access** checked (buckets must remain private).
5. Enable **Bucket Key** and **Amazon S3-managed keys (SSE-S3)** encryption.
6. Create the following folder prefixes:
   - `input/`
   - `output/accepted/`
   - `output/rejected/`
   - `output/reports/`

*AWS CLI alternative:*
```bash
aws s3api create-bucket --bucket serverless-etl-pipeline-hackathon-2026 --region ap-south-1 --create-bucket-configuration LocationConstraint=ap-south-1
aws s3api put-public-access-block --bucket serverless-etl-pipeline-hackathon-2026 --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
```

---

### Step 2: Create IAM Least-Privilege Role for Lambda
1. Navigate to **IAM** > **Roles** > **Create role**.
2. Select **AWS service** > **Lambda**.
3. Create an inline policy named `ServerlessETLPipelinePolicy` with the exact permissions below:

#### Least-Privilege IAM Policy (`iam_policy.json`):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudWatchLogsPermission",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Sid": "S3InputReadPermission",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::serverless-etl-pipeline-hackathon-2026/input/*"
    },
    {
      "Sid": "S3OutputWritePermission",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::serverless-etl-pipeline-hackathon-2026/output/*"
    },
    {
      "Sid": "S3BucketListingPermission",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::serverless-etl-pipeline-hackathon-2026"
    }
  ]
}
```

---

### Step 3: Package the Lambda Function
Run the automated packaging script to build `serverless_etl_pipeline.zip`:
```bash
python scripts/package_lambda.py
```
This generates a compact (~17 KB) zip file excluding unnecessary bloat, tests, and secrets.

---

### Step 4: Create & Configure the Lambda Function
1. Navigate to **AWS Lambda** > **Create function**.
2. Name: `serverless-customer-etl`.
3. Runtime: **Python 3.12** or **Python 3.13**.
4. Architecture: `x86_64` (or `arm64`).
5. Role: Select the existing role created in Step 2.
6. Under **Code Source**, click **Upload from** > **.zip file** and select `serverless_etl_pipeline.zip`.
7. Under **Runtime settings**, verify:
   - **Handler:** `lambda_function.lambda_handler`
8. Under **Configuration** > **General configuration**:
   - Memory: **256 MB**
   - Timeout: **1 minute** (default 3s is too low for S3 downloads)
   - Ephemeral storage: **512 MB** (default is sufficient)
9. Under **Configuration** > **Environment variables**, set:
   - `AWS_REGION` = `ap-south-1`
   - `S3_BUCKET_NAME` = `serverless-etl-pipeline-hackathon-2026`
   - `S3_INPUT_PREFIX` = `input/`
   - `LOG_LEVEL` = `INFO`

---

### Step 5: Configure S3 Event Notification (Recursion Safe)
> [!IMPORTANT]
> To prevent infinite recursive invocation loops, **DO NOT** configure the S3 trigger for the entire bucket!
> Configure the trigger strictly on the `input/` prefix:

1. Navigate to your S3 bucket > **Properties** tab.
2. Scroll to **Event notifications** > **Create event notification**.
3. Name: `TriggerETLOnNewInput`.
4. Prefix: `input/`
5. Event types: Select **All object create events** (`s3:ObjectCreated:*`).
6. Destination: **Lambda function** > `serverless-customer-etl`.
7. Click **Save changes**.

---

### Step 6: Test & Verify End-to-End
Upload a file to trigger the Lambda pipeline:
```bash
aws s3 cp sample_data/customers_mixed.csv s3://serverless-etl-pipeline-hackathon-2026/input/customers_mixed.csv
```

Inspect the output prefixes:
```bash
# Check accepted records
aws s3 ls s3://serverless-etl-pipeline-hackathon-2026/output/accepted/

# Check rejected records
aws s3 ls s3://serverless-etl-pipeline-hackathon-2026/output/rejected/

# Check reconciliation report
aws s3 ls s3://serverless-etl-pipeline-hackathon-2026/output/reports/
```

Inspect CloudWatch Logs:
1. Navigate to **CloudWatch** > **Log groups** > `/aws/lambda/serverless-customer-etl`.
2. Open the latest log stream and confirm:
   - Run ID generated
   - Ingestion counts and duration logged
   - Invariant verified

---

### Step 7: Post-Hackathon Resource Cleanup
When the demonstration is complete, clean up all cloud resources:
```bash
# Empty S3 bucket
aws s3 rm s3://serverless-etl-pipeline-hackathon-2026 --recursive

# Delete S3 bucket
aws s3api delete-bucket --bucket serverless-etl-pipeline-hackathon-2026 --region ap-south-1

# Delete Lambda function
aws lambda delete-function --function-name serverless-customer-etl

# Delete IAM role
aws iam delete-role-policy --role-name ServerlessETLLambdaRole --policy-name ServerlessETLPipelinePolicy
aws iam delete-role --role-name ServerlessETLLambdaRole
```

---

## 🛡 Recursion Guard & Operational Safeguards

A notorious trap in S3-triggered serverless pipelines is the **Recursive Trigger Loop**:
If Lambda is triggered by `ObjectCreated` on the whole bucket, writing output files back into the same bucket triggers Lambda again, creating an exponential billing loop!

### Our Defense-in-Depth Protection:
1. **Architectural Prefix Segregation**:
   - Event notifications are bound strictly to `input/`.
   - All pipeline outputs write exclusively to `output/accepted/`, `output/rejected/`, and `output/reports/`.
2. **Code-Level Recursion Guard (`src/aws/lambda_handler.py`)**:
   ```python
   output_prefixes = ["output/accepted/", "output/rejected/", "output/reports/", "output/"]
   if any(object_key.startswith(p) for p in output_prefixes):
       logger.warning("Skipping key '%s' to prevent recursive loop.", object_key)
       return {"key": object_key, "status": "SKIPPED_RECURSION_GUARD"}
   ```
3. **URL Decoding**:
   S3 event keys containing spaces or special characters (e.g. `customer+batch%201.csv`) are decoded using `urllib.parse.unquote_plus` before fetching.

---

## 🎙 Hackathon Demonstration Script

Use this 3-minute script to demonstrate the project to hackathon judges:

1. **The Hook (30s)**:
   > *"In modern enterprise data platforms, data ingestion often suffers from silent failures, invalid records polluting downstream data lakes, or uncontrolled AWS costs. Today, we built a production-ready Serverless ETL Pipeline designed around accounting invariants, schema validation, and zero silent data loss."*
2. **The Architecture (45s)**:
   > *"Our architecture decouples core ETL logic from cloud providers. The exact same validation rules, country standardization maps, and deduplication engine run locally on a developer's machine or serverlessly inside AWS Lambda triggered by Amazon S3."*
3. **Live Dashboard Demo (60s)**:
   - Open Streamlit dashboard: `streamlit run dashboard/app.py`.
   - Select **Mixed Dataset** and click **Load & Process Sample**.
   - Point out the **Executive KPI cards**:
     > *"Notice out of 10 input records: 3 were accepted, 7 were rejected, 1 duplicate ID was caught, and our Data Quality Score is 30%."*
   - Show the **Reconciliation Invariant**:
     > *"Crucially, our invariant holds: 10 input = 3 accepted + 7 rejected. Zero records dropped."*
   - Click the **Rejected Records** tab:
     > *"Every rejected record captures the original raw data alongside clear reasons, like invalid email syntax or negative age."*
   - Click the **Accepted Records** tab:
     > *"Clean records feature lowercase emails, stripped whitespace, and ISO standardized countries like USA and GBR."*
4. **Cloud & Testing Wrap-up (15s)**:
   - Mention the 48 automated pytest tests passing in < 0.5s.
   - Point out the schema inference engine, recursion guard, and the 24 KB deployment package.

---

## ✅ Verification Checklist

| Requirement | Category | Result | Evidence / Implementation |
|:---|:---:|:---:|:---|
| Multi-Format Ingestion | Ingestion | **PASS** | `Extractor` parses CSV and JSON formats |
| Dynamic Schema Inference | Schema | **PASS** | `SchemaInferrer` detects types, nulls, keys with thresholding |
| Dual Validation Modes | Validation | **PASS** | Automatic mode (`auto`) and Configured mode (`configured`) |
| Domain Presets & Custom Schemas | Validation | **PASS** | Presets for `customer`, `employee`, `sales`, + custom JSON |
| Multi-Domain Support | Validation | **PASS** | Validates Customer, Employee, Sales, IoT datasets |
| Missing Fields & Blanks | Validation | **PASS** | Detected & logged with descriptive reasons |
| RFC Email & Numeric Bounds | Validation | **PASS** | Regex format validation; range & bound checks |
| Dynamic Duplicate ID Detection | Validation | **PASS** | Stateful batch tracking for any detected/configured PK |
| Warning vs Error Distinction | Quality | **PASS** | Optional nulls retained; real errors rejected |
| Empty & Malformed Handling | Resilience | **PASS** | File-level error reported; zero row false-rejections |
| Reconciliation Invariant | Auditing | **PASS** | Asserts `Total == Accepted + Rejected` (100% verified) |
| Unit Test Suite (48 tests) | Testing | **PASS** | 48/48 tests pass in pytest (`tests/`) with 0 regressions |
| AWS S3 Service Module | Cloud | **PASS** | `S3Service` with encryption and credential safety |
| AWS Lambda Handler | Cloud | **PASS** | Schema-aware event parsing, URL decoding, metrics |
| Recursion Guard | Security | **PASS** | Ignores output keys; filters prefix strictly |
| Streamlit Operations UI | Dashboard | **PASS** | Schema discovery panel, mode toggles, visual tables |
| Deployment Packaging | Ops | **PASS** | `scripts/package_lambda.py` builds 24.89 KB ZIP |
| Credential Safety | Security | **PASS** | Redacted logs, no hardcoded keys, `.gitignore` active |
