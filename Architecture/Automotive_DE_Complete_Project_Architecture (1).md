# Automotive Sales & Finance Data Engineering Platform

> **Production-oriented Azure Data Engineering project covering batch ingestion, incremental loading, data quality, medallion architecture, CDC concepts, streaming, orchestration, security, monitoring, and BI.**

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Business Objective](#2-business-objective)
3. [Overall Architecture](#3-overall-architecture)
4. [Architecture Diagram](#4-architecture-diagram)
5. [Source Systems](#5-source-systems)
6. [Three Ingestion Patterns](#6-three-ingestion-patterns)
7. [Azure Data Factory Layer](#7-azure-data-factory-layer)
8. [ADF Component Architecture](#8-adf-component-architecture)
9. [PostgreSQL Ingestion Architecture](#9-postgresql-ingestion-architecture)
10. [Reusable ADF Pipeline](#10-reusable-adf-pipeline)
11. [Incremental Loading Architecture](#11-incremental-loading-architecture)
12. [Watermark Architecture](#12-watermark-architecture)
13. [External CSV Batch Architecture](#13-external-csv-batch-architecture)
14. [ADLS Gen2 Data Lake](#14-adls-gen2-data-lake)
15. [Medallion Architecture](#15-medallion-architecture)
16. [Data Quality Architecture](#16-data-quality-architecture)
17. [Quarantine / Reject Architecture](#17-quarantine--reject-architecture)
18. [Databricks and PySpark Layer](#18-databricks-and-pyspark-layer)
19. [Delta Lake Architecture](#19-delta-lake-architecture)
20. [Streaming Architecture](#20-streaming-architecture)
21. [CDC Architecture](#21-cdc-architecture)
22. [Batch → Incremental → CDC → Streaming Evolution](#22-batch--incremental--cdc--streaming-evolution)
23. [Master Orchestration Architecture](#23-master-orchestration-architecture)
24. [Security Architecture](#24-security-architecture)
25. [Monitoring and Observability](#25-monitoring-and-observability)
26. [Error Handling and Recovery](#26-error-handling-and-recovery)
27. [Data Flow](#27-data-flow)
28. [Automotive Data Domains](#28-automotive-data-domains)
29. [Project Azure Resources](#29-project-azure-resources)
30. [Repository Architecture](#30-repository-architecture)
31. [End-to-End Architecture](#31-end-to-end-architecture)
32. [Production Design Principles](#32-production-design-principles)
33. [Cost Optimization](#33-cost-optimization)
34. [Current Implementation Status](#34-current-implementation-status)
35. [Future Roadmap](#35-future-roadmap)
36. [Interview Summary](#36-interview-summary)

---

# 1. Project Overview

## Project Name

**Automotive Sales & Finance Data Engineering Platform**

## Technology Stack

- Azure Data Factory
- Azure Data Lake Storage Gen2
- PostgreSQL
- Self-hosted Integration Runtime
- Azure Key Vault
- Managed Identity
- Azure RBAC
- Databricks
- PySpark
- Delta Lake
- Event Hubs / Kafka
- Structured Streaming
- Power BI
- GitHub

## Architecture Philosophy

The project is designed around:

- Reusable ingestion
- Parameterization
- Incremental loading
- Data quality
- Medallion architecture
- Separation of ingestion and transformation
- Batch + streaming patterns
- CDC concepts
- Security
- Monitoring
- Error handling
- Production-oriented orchestration

---

# 2. Business Objective

The platform is designed for an automotive organization that needs to integrate:

- Customer information
- Dealer information
- Vehicle models and variants
- Orders
- Order items
- Invoices
- Payments
- Dealer targets
- Dealer commissions
- Finance/accounting information
- Pricing
- Location/reference data
- Real-time sales/payment events

The platform should transform operational and external data into trusted analytical datasets for:

- Sales analytics
- Dealer performance
- Revenue analysis
- Payment analysis
- Finance analytics
- Vehicle performance
- Customer analytics
- Business reporting

---

# 3. Overall Architecture

The high-level architecture is:

```text
                         SOURCE SYSTEMS
                              |
            +-----------------+-----------------+
            |                 |                 |
            ▼                 ▼                 ▼
      PostgreSQL         External CSV      Streaming Events
        OLTP                  Files
            |                 |                 |
            | Batch /         | Batch          | Real-time
            | Incremental     |                |
            ▼                 ▼                 ▼
      +------------------------------------------------+
      |              AZURE DATA FACTORY               |
      |                                                |
      | Ingestion | Orchestration | Scheduling         |
      | Parameters | Validation | Monitoring           |
      +-------------------------+----------------------+
                                |
                                ▼
                      +--------------------+
                      |     ADLS Gen2      |
                      |    DATA LAKE       |
                      +---------+----------+
                                |
              +-----------------+-----------------+
              |                 |                 |
              ▼                 ▼                 ▼
           BRONZE            SILVER             GOLD
              |                 |                 |
              +-----------------+-----------------+
                                |
                                ▼
                    Databricks / PySpark
                         + Delta Lake
                                |
                                ▼
                            POWER BI
```

Streaming path:

```text
Streaming Events
      ↓
Event Hubs / Kafka
      ↓
Databricks Structured Streaming
      ↓
Delta Lake
      ↓
Silver
      ↓
Gold
      ↓
Power BI / Applications
```

---

# 4. Architecture Diagram

## Complete Platform Architecture

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                           SOURCE SYSTEMS                                     │
├───────────────────────┬───────────────────────┬──────────────────────────────┤
│                       │                       │                              │
│ PostgreSQL OLTP       │ External Batch Files  │ Streaming Event Sources     │
│                       │                       │                              │
│ • Customers           │ • Dealer Targets      │ • ORDER_CREATED             │
│ • Dealers             │ • Dealer Commissions  │ • PAYMENT_RECEIVED          │
│ • Vehicle Models      │ • Finance Data        │ • PAYMENT_FAILED             │
│ • Variants            │ • Pricing Data        │ • Future Vehicle Events     │
│ • Orders              │ • Location Data       │                              │
│ • Order Items         │                       │                              │
│ • Invoices            │                       │                              │
│ • Payments            │                       │                              │
└───────────────┬───────┴──────────────┬────────┴──────────────┬───────────────┘
                │                      │                       │
                │ Batch / Incremental  │ Batch                 │ Streaming
                ▼                      ▼                       ▼
┌──────────────────────────────────────────────────────┐   ┌───────────────────┐
│                 AZURE DATA FACTORY                   │   │ Event Hubs / Kafka│
│                                                      │   └─────────┬─────────┘
│ • Ingestion                                          │             │
│ • Parameterized pipelines                            │             │
│ • Scheduling                                         │             │
│ • Orchestration                                      │             │
│ • Lookup / watermark logic                           │             │
│ • Data movement                                      │             │
│ • Validation                                         │             │
│ • Dependency management                              │             │
│ • Monitoring                                         │             │
└──────────────────────────┬───────────────────────────┘             │
                           │                                         │
                           │                                         ▼
                           │                              ┌─────────────────────┐
                           │                              │ Databricks          │
                           │                              │ Structured          │
                           │                              │ Streaming           │
                           │                              └──────────┬──────────┘
                           │                                         │
                           └──────────────────┬──────────────────────┘
                                              ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                             ADLS GEN2                                       │
│                              DATA LAKE                                      │
│                                                                              │
│   ┌──────────────────┐                                                       │
│   │     BRONZE       │  Raw / minimally transformed                         │
│   │                  │                                                       │
│   │ PostgreSQL       │                                                       │
│   │ CSV              │                                                       │
│   │ Streaming events │                                                       │
│   └────────┬─────────┘                                                       │
│            │                                                                 │
│            ▼                                                                 │
│   ┌──────────────────┐                                                       │
│   │     SILVER       │  Cleaned / validated / standardized                   │
│   │                  │                                                       │
│   │ Deduplicated     │                                                       │
│   │ Typed            │                                                       │
│   │ Validated        │                                                       │
│   │ Enriched         │                                                       │
│   └────────┬─────────┘                                                       │
│            │                                                                 │
│            ▼                                                                 │
│   ┌──────────────────┐                                                       │
│   │      GOLD        │  Business-ready                                      │
│   │                  │                                                       │
│   │ Sales KPIs       │                                                       │
│   │ Dealer KPIs      │                                                       │
│   │ Finance KPIs     │                                                       │
│   │ Payment KPIs     │                                                       │
│   └────────┬─────────┘                                                       │
└────────────┼─────────────────────────────────────────────────────────────────┘
             │
             ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       DATABRICKS / PYSPARK / DELTA                           │
│                                                                              │
│ Transformation • Data Quality • Joins • Aggregations • Incremental Loads    │
│ Deduplication • Streaming • Delta Lake                                      │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
                               ▼
                     ┌──────────────────────┐
                     │       POWER BI       │
                     │                      │
                     │ Sales Dashboard      │
                     │ Dealer Dashboard     │
                     │ Finance Dashboard    │
                     │ Payment Dashboard    │
                     └──────────────────────┘
```

---

# 5. Source Systems

The platform uses three source patterns.

## 5.1 PostgreSQL OLTP

PostgreSQL represents operational transactional data.

Main domains:

```text
customer
dealer
product
sales
```

Example tables:

```text
customer.customers
dealer.dealers
product.vehicle_models
product.vehicle_variants
sales.orders
sales.order_items
sales.invoices
sales.payments
```

PostgreSQL is appropriate here because operational systems frequently contain:

- Transactions
- Relationships
- Current-state records
- Frequently changing data
- Relational integrity

---

## 5.2 External CSV Batch Sources

External files represent periodic data received from other teams, systems, partners or business processes.

Examples:

```text
dealer_targets
dealer_commissions
dealer_payouts
gl_accounts
ledger_entries
tax_records
price_master
states
cities
```

These files follow a batch ingestion pattern.

```text
CSV
 ↓
ADF
 ↓
ADLS Bronze
```

---

## 5.3 Streaming Events

The project also includes a streaming source representing continuously generated business events.

Example events:

```text
ORDER_CREATED
PAYMENT_RECEIVED
PAYMENT_FAILED
```

Architecture:

```text
Event Producer
      ↓
Event Hubs / Kafka
      ↓
Databricks Structured Streaming
      ↓
Delta Lake
```

---

# 6. Three Ingestion Patterns

The project deliberately demonstrates three ingestion patterns.

## Pattern 1 — PostgreSQL Batch / Incremental

```text
PostgreSQL
    ↓
Self-hosted IR
    ↓
ADF
    ↓
ADLS Bronze
```

For incremental processing:

```text
PostgreSQL
    ↓
ADF Lookup Watermark
    ↓
Dynamic Query
    ↓
Only New/Changed Rows
    ↓
ADLS Bronze
```

---

## Pattern 2 — External CSV Batch

```text
CSV
 ↓
ADF
 ↓
ADLS Bronze
```

---

## Pattern 3 — Streaming

```text
Event Source
     ↓
Event Hubs / Kafka
     ↓
Databricks Structured Streaming
     ↓
Delta Bronze
     ↓
Silver
     ↓
Gold
```

These patterns can coexist in the same platform.

---

# 7. Azure Data Factory Layer

ADF is the main integration and orchestration layer for batch workloads.

Responsibilities:

```text
                 ADF
                  |
     +------------+------------+
     |            |            |
 Ingestion   Orchestration   Monitoring
     |
     +--> Parameterization
     |
     +--> Incremental loading
     |
     +--> Validation
     |
     +--> Dependencies
     |
     +--> Error handling
```

ADF does not replace:

- ADLS as storage
- Databricks as distributed processing
- Power BI as visualization
- Event Hubs/Kafka as event transport

---

# 8. ADF Component Architecture

```text
                    ADF Factory
                        |
        +---------------+----------------+
        |               |                |
   Linked Services   Datasets        Pipelines
        |               |                |
        |               |           +----+----+
        |               |           |         |
        |               |        Activities  Control Flow
        |               |
        |          Data definitions
        |
     Connections
        |
        v
Integration Runtime
```

Key components:

| Component | Role |
|---|---|
| Pipeline | Workflow |
| Activity | Individual operation |
| Dataset | Data reference |
| Linked Service | Connection |
| Integration Runtime | Execution/network bridge |
| Parameter | Runtime configuration |
| Variable | Runtime state |
| Trigger | Starts pipeline |
| Expression | Dynamic logic |
| Monitor | Operational visibility |

---

# 9. PostgreSQL Ingestion Architecture

The local PostgreSQL setup is connected to Azure Data Factory through a Self-hosted Integration Runtime.

```text
┌─────────────────────┐
│ Windows Laptop      │
│                     │
│ PostgreSQL          │
│ automotive_db       │
└──────────┬──────────┘
           │
           │ Local connectivity
           ▼
┌─────────────────────┐
│ Self-hosted IR      │
│ integrationRuntime  │
│ psql                │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Azure Data Factory  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ ADLS Gen2 Bronze    │
└─────────────────────┘
```

This design allows ADF to access the local PostgreSQL instance.

---

# 10. Reusable ADF Pipeline

Pipeline:

```text
PL_02_PostgreSQL_To_Bronze_Reusable
```

Parameters:

```text
schema_name
table_name
```

Pipeline flow:

```text
Pipeline Parameters
        |
        +---- schema_name
        |
        +---- table_name
        |
        ▼
PostgreSQL Dataset
        |
        ▼
Copy Activity
        |
        ▼
ADLS Bronze Dataset
        |
        ▼
bronze/<schema>/<table>.csv
```

### Parameterized PostgreSQL Dataset

Dataset:

```text
DS_PostgreSQL_Customer
```

Parameters:

```text
schema_name
table_name
```

Expressions:

```text
@dataset().schema_name
@dataset().table_name
```

Pipeline mapping:

```text
@trim(pipeline().parameters.schema_name)
@trim(pipeline().parameters.table_name)
```

### Parameterized ADLS Dataset

Dataset:

```text
DS_ADLS_Bronze_Reusable
```

Directory:

```text
@dataset().schema_name
```

Filename:

```text
@concat(dataset().table_name, '.csv')
```

Destination:

```text
bronze/<schema>/<table>.csv
```

---

# 11. Incremental Loading Architecture

Incremental loading avoids repeatedly processing the complete source.

Full load:

```text
Source
  ↓
ALL records
  ↓
Target
```

Incremental:

```text
Source
  ↓
Last successful watermark
  ↓
New/changed records
  ↓
Target
```

Current project pattern:

```text
PostgreSQL
     ↓
Lookup_Last_Watermark
     ↓
Dynamic Query
     ↓
Copy_PostgreSQL_To_Bronze
     ↓
ADLS Bronze
```

Final intended pattern:

```text
Lookup Watermark
      ↓
Copy New Records
      ↓
SUCCESS?
   /       \
 YES        NO
  ↓          ↓
Update      Keep
Watermark   Old Watermark
```

---

# 12. Watermark Architecture

Control table:

```text
public.etl_watermark
```

Definition:

```sql
CREATE TABLE IF NOT EXISTS public.etl_watermark (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_loaded_timestamp TIMESTAMP
);
```

Initial record:

```sql
INSERT INTO public.etl_watermark
    (pipeline_name, last_loaded_timestamp)
VALUES
    ('PL_02_sales_orders_incremental', '1900-01-01 00:00:00');
```

Lookup:

```sql
SELECT last_loaded_timestamp
FROM public.etl_watermark
WHERE pipeline_name = 'PL_02_sales_orders_incremental';
```

Dynamic query:

```text
@concat(
'SELECT * FROM sales.orders WHERE created_at > ''',
activity('Lookup_Last_Watermark').output.firstRow.last_loaded_timestamp,
''''
)
```

The critical production rule is:

> The watermark should advance only after successful data processing.

---

# 13. External CSV Batch Architecture

External files are ingested using ADF.

```text
External CSV
     ↓
ADF
     ↓
ADLS Gen2
     ↓
Bronze
```

Examples:

```text
dealer_targets
dealer_commissions
dealer_payouts
gl_accounts
ledger_entries
tax_records
price_master
states
cities
```

A reusable CSV pipeline can also be parameterized:

```text
source_path
file_name
target_folder
```

This avoids creating one pipeline for every incoming file type when the ingestion logic is common.

---

# 14. ADLS Gen2 Data Lake

ADLS Gen2 is the central storage layer.

Logical structure:

```text
ADLS Gen2
│
├── bronze/
│
├── silver/
│
├── gold/
│
└── quarantine/
```

Bronze:

```text
Raw / minimally transformed
```

Silver:

```text
Cleaned / standardized / validated
```

Gold:

```text
Business-ready analytical data
```

Quarantine:

```text
Invalid / rejected records
```

---

# 15. Medallion Architecture

## Bronze

Purpose:

- Preserve source data
- Replay ingestion
- Audit
- Debug
- Maintain raw history

```text
Sources
   ↓
Bronze
```

---

## Silver

Purpose:

- Clean
- Standardize
- Validate
- Deduplicate
- Enrich

```text
Bronze
   ↓
Silver
```

Typical transformations:

```text
Null handling
Data type correction
Duplicate removal
Business validation
Joins
Standardization
```

---

## Gold

Purpose:

Create business-ready datasets.

Examples:

```text
Sales Analytics
Dealer Performance
Revenue
Payment Analytics
Finance Analytics
Vehicle Performance
Customer Analytics
```

```text
Silver
   ↓
Gold
```

---

# 16. Data Quality Architecture

Data quality should be a separate logical stage.

```text
                 Bronze
                   |
                   ▼
             Data Validation
              /           \
             /             \
         VALID             INVALID
           |                  |
           ▼                  ▼
        Silver           Quarantine
           |
           ▼
          Gold
```

Potential validations:

```text
Schema validation
Null validation
Data type validation
Referential integrity
Duplicate detection
Business rule validation
Range validation
Mandatory field validation
```

---

# 17. Quarantine / Reject Architecture

Invalid records should not simply be deleted.

Recommended:

```text
ADLS
│
├── bronze/
├── silver/
├── gold/
└── quarantine/
    └── orders/
        ├── invalid_customer/
        ├── invalid_amount/
        ├── invalid_schema/
        └── duplicate/
```

Useful metadata:

```text
source_file
record_id
error_type
error_message
pipeline_run_id
load_timestamp
```

The original record can be retained alongside the error reason.

This supports:

- Investigation
- Replay
- Auditing
- Source correction

---

# 18. Databricks and PySpark Layer

Databricks is used for scalable transformation and processing.

Responsibilities:

```text
ADLS Bronze
      ↓
Databricks / PySpark
      |
      +--> Clean
      +--> Validate
      +--> Deduplicate
      +--> Join
      +--> Aggregate
      +--> Enrich
      +--> Transform
      ↓
Silver
      ↓
Gold
```

ADF can orchestrate Databricks execution, while Databricks performs distributed processing.

---

# 19. Delta Lake Architecture

Delta Lake provides a transactional table layer for lakehouse workloads.

Conceptually:

```text
ADLS
  ↓
Delta Bronze
  ↓
Delta Silver
  ↓
Delta Gold
```

Delta is separate from ADLS Gen2.

```text
ADLS Gen2 = storage/data lake foundation

Delta Lake = table/storage transaction layer on the lake
```

Delta capabilities support production-style lakehouse processing such as:

- ACID-style transactions
- Schema enforcement/evolution
- Versioned data
- Reliable upserts/merges
- Streaming + batch interoperability

---

# 20. Streaming Architecture

Streaming is designed separately from traditional scheduled ADF batch ingestion.

```text
Event Producer
      ↓
Event Hubs / Kafka
      ↓
Databricks Structured Streaming
      ↓
Delta Bronze
      ↓
Validation / Transformation
      ↓
Delta Silver
      ↓
Delta Gold
      ↓
Power BI / Applications
```

Example events:

```text
ORDER_CREATED
PAYMENT_RECEIVED
PAYMENT_FAILED
```

Streaming is appropriate when the business requires lower latency than periodic batch ingestion.

---

# 21. CDC Architecture

CDC means **Change Data Capture**.

Instead of repeatedly scanning for timestamps, CDC captures source changes.

```text
Operational Database
        ↓
CDC / Change Log
        ↓
Change Events
        ↓
Event Transport / Ingestion
        ↓
Lakehouse
```

Typical operations:

```text
INSERT
UPDATE
DELETE
```

CDC is particularly useful when:

- Updates matter
- Deletes matter
- Exact changes need to be propagated
- Full scans are expensive
- Lower latency is required

Important distinction:

```text
Timestamp Incremental
= Query-based change detection

CDC
= Source-level change capture
```

---

# 22. Batch → Incremental → CDC → Streaming Evolution

The platform demonstrates the progression of data ingestion patterns.

## Stage 1 — Full Batch

```text
Database
   ↓
ADF
   ↓
Data Lake
```

All rows are processed.

## Stage 2 — Incremental Batch

```text
Database
   ↓
Watermark
   ↓
ADF
   ↓
Only new/changed records
```

## Stage 3 — CDC

```text
Database
   ↓
CDC
   ↓
Actual changes
   ↓
Data platform
```

## Stage 4 — Streaming

```text
Application
   ↓
Event Hubs / Kafka
   ↓
Streaming Engine
   ↓
Lakehouse
```

## Stage 5 — Modern Lakehouse

```text
Batch ──┐
CDC ────┼──> Lakehouse / Delta
Stream ─┘          ↓
              Silver / Gold
                   ↓
             BI / ML / Apps
```

The mature architecture can combine all four patterns.

---

# 23. Master Orchestration Architecture

As the project grows, a master ADF pipeline can coordinate child pipelines.

```text
                    PL_Master_Automotive_DE
                              |
        +---------------------+---------------------+
        |                     |                     |
        ▼                     ▼                     ▼
PostgreSQL Ingestion     CSV Ingestion        Validation
        |                     |                     |
        +---------------------+---------------------+
                              |
                              ▼
                         ADLS Bronze
                              |
                              ▼
                       Bronze → Silver
                              |
                              ▼
                        Silver → Gold
                              |
                              ▼
                           Power BI
```

Possible child pipelines:

```text
PL_01_PostgreSQL_To_Bronze
PL_02_CSV_To_Bronze
PL_03_Data_Validation
PL_04_Bronze_To_Silver
PL_05_Silver_To_Gold
```

Streaming remains independently continuous:

```text
Event Hubs/Kafka
      ↓
Databricks Streaming
      ↓
Delta
```

ADF can orchestrate surrounding processing where appropriate.

---

# 24. Security Architecture

Security is cross-cutting.

```text
                    Azure Key Vault
                           |
                        Secrets
                           |
                           ▼
                   Managed Identity
                           |
                           ▼
                          ADF
                           |
             +-------------+-------------+
             |                           |
             ▼                           ▼
        PostgreSQL                    ADLS
```

Security controls:

- Azure RBAC
- ADLS ACLs
- Managed Identity
- Key Vault
- Least privilege
- Resource-group isolation
- Secret management

Project Key Vault:

```text
kv-de-learning-2026-isha
```

Secret:

```text
postgres-pass
```

ADF managed identity is used rather than embedding credentials directly in pipeline definitions.

---

# 25. Monitoring and Observability

Monitoring covers:

```text
ADF Pipeline Runs
ADF Activity Runs
Data Quality
Failures
Rows Read
Rows Written
Duration
Watermark
Streaming Health
```

Troubleshooting pattern:

```text
Pipeline Failure
      ↓
Open Run
      ↓
Open Failed Activity
      ↓
Read Error
      ↓
Check:
  • Source
  • Dataset
  • Linked Service
  • Expression
  • IR
      ↓
Fix
      ↓
Retry
```

Production observability should also retain:

```text
pipeline_run_id
source
target
start_time
end_time
status
rows_read
rows_written
error_message
```

---

# 26. Error Handling and Recovery

Recommended workflow:

```text
                    Copy
                     |
               +-----+-----+
               |           |
            SUCCESS       FAILURE
               |           |
               ▼           ▼
        Update State    Error Log
               |           |
               ▼           ▼
           Continue     Quarantine/
                        Notification/
                        Retry
```

For incremental processing:

```text
Copy fails
   ↓
DO NOT advance watermark
   ↓
Retry
```

This prevents records from being skipped.

---

# 27. Data Flow

## PostgreSQL to Gold

```text
PostgreSQL
    ↓
Self-hosted IR
    ↓
ADF
    ↓
ADLS Bronze
    ↓
Databricks / PySpark
    ↓
Silver
    ↓
Gold
    ↓
Power BI
```

## CSV to Gold

```text
CSV
 ↓
ADF
 ↓
ADLS Bronze
 ↓
Databricks
 ↓
Silver
 ↓
Gold
 ↓
Power BI
```

## Streaming to Gold

```text
Events
 ↓
Event Hubs / Kafka
 ↓
Databricks Structured Streaming
 ↓
Delta Bronze
 ↓
Delta Silver
 ↓
Delta Gold
 ↓
Power BI / Applications
```

---

# 28. Automotive Data Domains

The project covers several business domains.

## Customer

```text
customer.customers
```

## Dealer

```text
dealer.dealers
```

## Product

```text
product.vehicle_models
product.vehicle_variants
```

## Sales

```text
sales.orders
sales.order_items
sales.invoices
sales.payments
```

## Finance

```text
dealer payouts
GL accounts
ledger entries
tax records
```

## Pricing

```text
price master
```

## Location

```text
states
cities
```

## Streaming

```text
vehicle/order/payment events
```

---

# 29. Project Azure Resources

Current foundation:

```text
Subscription:
Azure subscription 1

Resource Group:
rg-de-learning

Region:
Central India

ADF:
isha1803adf

Self-hosted IR:
integrationRuntimepsql

Key Vault:
kv-de-learning-2026-isha
```

Budget monitoring is configured separately for cost awareness.

---

# 30. Repository Architecture

Recommended GitHub structure:

```text
automotive-data-engineering-platform/
│
├── README.md
│
├── architecture/
│   ├── architecture.md
│   ├── architecture-diagram.md
│   └── data-flow.md
│
├── sql/
│   ├── schemas/
│   ├── tables/
│   ├── validation/
│   └── incremental/
│
├── adf/
│   ├── pipelines/
│   ├── datasets/
│   ├── linked-services/
│   └── triggers/
│
├── databricks/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── streaming/
│
├── streaming/
│
├── data-quality/
│
├── docs/
│   ├── adf/
│   ├── postgresql/
│   ├── databricks/
│   └── architecture/
│
├── screenshots/
│
└── data/
```

Do not commit:

```text
Passwords
API keys
Certificates
.env files
Large raw datasets
Private credentials
```

---

# 31. End-to-End Architecture

The complete target architecture is:

```text
                              ┌─────────────────────┐
                              │     SOURCES         │
                              └──────────┬──────────┘
                                         │
             ┌───────────────────────────┼───────────────────────────┐
             │                           │                           │
             ▼                           ▼                           ▼
       PostgreSQL                    CSV Files                Event Sources
             │                           │                           │
             │                           │                           ▼
             │                           │                    Event Hubs/Kafka
             │                           │                           │
             ▼                           ▼                           ▼
       Self-hosted IR                  ADF                  Databricks Streaming
             │                           │                           │
             └───────────────┬───────────┘                           │
                             ▼                                       │
                       Azure Data Factory                            │
                             │                                       │
                             ▼                                       │
                        ADLS Bronze ◄────────────────────────────────┘
                             │
                             ▼
                       Data Validation
                         /          \
                        /            \
                       ▼              ▼
                   Valid Data     Invalid Data
                       │              │
                       ▼              ▼
                    Silver        Quarantine
                       │
                       ▼
                      Gold
                       │
                       ▼
                  Power BI / BI
                       │
                       ▼
             Business Decisions
```

Cross-cutting services:

```text
Security:
Key Vault + Managed Identity + RBAC

Observability:
ADF Monitor + Logs + Data Quality Metrics

Orchestration:
Master ADF Pipeline + Child Pipelines

Data State:
Watermark / CDC / Streaming Checkpoints

Storage:
ADLS Gen2 + Delta Lake
```

---

# 32. Production Design Principles

## 1. Reusability

Prefer:

```text
One parameterized pipeline
```

over duplicated pipelines when the workflow is common.

## 2. Separation of concerns

```text
ADF
→ Ingestion + Orchestration

ADLS
→ Storage

Databricks
→ Transformation + Processing

Delta
→ Lakehouse tables

Power BI
→ Analytics
```

## 3. Security

Use:

```text
Key Vault
Managed Identity
RBAC
ACL
Least privilege
```

## 4. Reliability

Design for:

- Retry
- Failure recovery
- Idempotency
- Validation
- Quarantine

## 5. Observability

Track:

```text
run ID
status
duration
rows
errors
source
target
watermark
```

## 6. State management

For incremental loads:

```text
Load succeeds
     ↓
Advance state
```

Not the reverse.

## 7. Scalability

Use the appropriate technology for the workload:

```text
Small batch → ADF
Large transformation → Spark/Databricks
Exact changes → CDC
Real-time events → Streaming
```

---

# 33. Cost Optimization

This project is designed to minimize unnecessary Azure consumption.

Guidelines:

- Use small datasets during testing.
- Avoid unnecessary Databricks clusters.
- Stop billable compute when not in use.
- Avoid repeated large debug runs.
- Monitor Cost Management.
- Maintain budget alerts.
- Build one capability at a time.
- Do not create production-scale infrastructure merely for learning.

Important:

> Resource definitions and orchestration objects are not equivalent to continuously running compute. Actual costs depend on the service, runtime, data movement, and compute used.

---

# 34. Current Implementation Status

## Completed

### Azure Foundation

- [x] Azure subscription
- [x] Resource group
- [x] Budget
- [x] ADLS Gen2
- [x] Bronze/Silver/Gold structure

### PostgreSQL

- [x] PostgreSQL database
- [x] Schemas
- [x] Automotive tables
- [x] CSV loading
- [x] Relationship validation
- [x] Data quality checks

### ADF

- [x] ADF resource
- [x] Self-hosted IR
- [x] Key Vault
- [x] Managed Identity
- [x] PostgreSQL linked service
- [x] ADLS linked service
- [x] First PostgreSQL → Bronze pipeline
- [x] Reusable parameterized pipeline
- [x] Multi-table testing

### Incremental Loading

- [x] Watermark concept
- [x] `etl_watermark`
- [x] Lookup activity
- [x] Dynamic incremental query
- [x] Initial incremental run

## In Progress

- [ ] Update watermark after successful load
- [ ] Second incremental run
- [ ] Verify only new records
- [ ] Failure-safe state update

## Future

- [ ] CSV ingestion pipeline
- [ ] Data validation pipeline
- [ ] Quarantine implementation
- [ ] Bronze → Silver
- [ ] Silver → Gold
- [ ] Databricks
- [ ] PySpark
- [ ] Delta Lake
- [ ] CDC implementation/concepts
- [ ] Event Hubs/Kafka
- [ ] Structured Streaming
- [ ] Master orchestrator
- [ ] Production monitoring
- [ ] CI/CD

---

# 35. Future Roadmap

```text
                    CURRENT
                       |
                       ▼
             Complete Incremental
                  Loading
                       |
                       ▼
              CSV Batch Ingestion
                       |
                       ▼
             Data Quality + Rejects
                       |
                       ▼
                Silver Layer
                       |
                       ▼
                  Gold Layer
                       |
                       ▼
             Databricks + PySpark
                       |
                       ▼
                 Delta Lake
                       |
                       ▼
                    CDC
                       |
                       ▼
             Event Hubs / Kafka
                       |
                       ▼
          Structured Streaming
                       |
                       ▼
             Master Orchestration
                       |
                       ▼
            Production Hardening
                       |
                       ▼
                  Power BI
```

---

# 36. Interview Summary

### One-minute architecture explanation

> I built a production-oriented automotive sales and finance data engineering platform on Azure. PostgreSQL is used as the operational source, while external CSV files represent batch sources and Event Hubs/Kafka represents the streaming path. Azure Data Factory handles batch ingestion and orchestration, including reusable parameterized pipelines and incremental loading using a watermark. Data is landed in ADLS Gen2 using a Bronze/Silver/Gold medallion architecture. Databricks and PySpark handle scalable transformation, data quality and Delta Lake processing. Invalid records are quarantined rather than deleted. For real-time events, Databricks Structured Streaming processes events from Event Hubs or Kafka into Delta. Security is handled using Key Vault, Managed Identity and RBAC, while monitoring and error handling provide production visibility. Power BI consumes Gold-layer business datasets for sales and finance analytics.

### Architecture in one line

```text
PostgreSQL + CSV + Streaming Events
              ↓
ADF / Event Hubs
              ↓
ADLS Gen2 Bronze
              ↓
Databricks + PySpark + Delta
              ↓
Silver
              ↓
Gold
              ↓
Power BI
```

### Core engineering progression

```text
ADF Fundamentals
       ↓
Reusable Pipelines
       ↓
Incremental Loading
       ↓
Data Quality
       ↓
Medallion Architecture
       ↓
Databricks + PySpark
       ↓
Delta Lake
       ↓
CDC
       ↓
Streaming
       ↓
Production Orchestration
       ↓
Modern Lakehouse
```

---

## Final Architecture Principle

The platform is intentionally designed so that each technology has a clear responsibility:

```text
PostgreSQL
    → Operational transactional source

CSV
    → External batch source

Event Hubs / Kafka
    → Event transport

ADF
    → Batch ingestion + orchestration

ADLS Gen2
    → Central data lake storage

Databricks / PySpark
    → Distributed transformation

Delta Lake
    → Reliable lakehouse tables

Bronze / Silver / Gold
    → Data lifecycle and business layers

Key Vault + Managed Identity + RBAC
    → Security

Quarantine
    → Invalid-data handling

Monitoring
    → Observability

Power BI
    → Business analytics
```

The result is an end-to-end architecture that demonstrates not only individual Azure services, but also **how a Data Engineer chooses and connects different technologies based on ingestion pattern, latency, scale, reliability, and business requirements.**
