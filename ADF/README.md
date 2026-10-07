# Azure Data Factory

## Overview

Azure Data Factory (ADF) is used in this project for:

* Batch data ingestion
* Data movement between source and destination systems
* Pipeline orchestration
* Scheduling and triggering
* Error handling
* Monitoring
* Reusable and parameterized pipelines

The first implemented ingestion pattern is:

```text
PostgreSQL OLTP
       ↓
Self-hosted Integration Runtime
       ↓
Azure Data Factory
       ↓
ADLS Gen2
       ↓
Bronze Layer
```

## 1\. ADF Resource

**Data Factory:** `isha1803adf`  
**Resource Group:** `rg-de-learning`  
**Region:** `Central India`

## 2\. Self-hosted Integration Runtime

The PostgreSQL source database is running locally on the development laptop. Because Azure Data Factory cannot directly access the laptop's `localhost`, a Self-hosted Integration Runtime is used.

**Integration Runtime:** `integrationRuntimepsql`

```text
Developer Laptop
│
├── PostgreSQL
│   └── localhost:5432
│
└── Self-hosted Integration Runtime
          │
          ▼
    Azure Data Factory
          │
          ▼
       ADLS Gen2
```

The laptop must be powered on, connected to the internet, awake, with PostgreSQL and the Self-hosted Integration Runtime running when the pipeline needs to access PostgreSQL.

## 3\. Linked Services

### PostgreSQL

**Name:** `LS\\\_PostgreSQL\\\_Automotive`

```text
Server: localhost
Port: 5432
Database: automotive\\\_db
Username: postgres
Integration Runtime: integrationRuntimepsql
```

The PostgreSQL password is retrieved from Azure Key Vault rather than hardcoded in the pipeline.

### Azure Key Vault

**Name:** `LS\\\_AzureKeyVault`

Used by ADF to retrieve the PostgreSQL password securely.

**Secret:** `postgres-pass`

The actual secret value is intentionally not stored in this repository.

### ADLS Gen2

**Name:** `LS\\\_AzureDatalake\\\_Automotive`

Used to connect Azure Data Factory to the project's ADLS Gen2 account.

## 4\. Security

Azure Key Vault is used to avoid storing database credentials directly in pipeline definitions, source code, GitHub, or documentation.

ADF uses its system-assigned managed identity to access the Key Vault secret.

```text
ADF
 │
 │ Managed Identity
 ▼
Azure Key Vault
 │
 │ postgres-pass
 ▼
PostgreSQL Credential
```

The PostgreSQL password must never be committed to GitHub.

## 5\. First ADF Pipeline

**Pipeline:** `PL\\\_01\\\_PostgreSQL\\\_Customer\\\_To\\\_Bronze`

Purpose: extract customer data from PostgreSQL and load it into the Bronze layer of ADLS Gen2.

```text
PostgreSQL
customer.customers
       ↓
ADF Copy Activity
       ↓
ADLS Gen2
bronze/customer/customers.csv
```

## 6\. Copy Data Activity

**Activity:** `Copy\\\_Customer\\\_PostgreSQL\\\_To\\\_Bronze`

The Copy Data activity moves data from PostgreSQL to ADLS Gen2.

## 7\. Source Dataset

**Dataset:** `DS\\\_PostgreSQL\\\_Customer`

**Linked Service:** `LS\\\_PostgreSQL\\\_Automotive`

**Source:** `customer.customers`

The source was configured using a query because the schema/table dropdown was not exposed in the ADF interface during configuration.

```sql
SELECT \\\*
FROM customer.customers;
```

## 8\. Sink Dataset

**Dataset:** `DS\\\_ADLS\\\_Bronze\\\_Customer`

**Linked Service:** `LS\\\_AzureDatalake\\\_Automotive`

**Destination:**

```text
bronze/customer/customers.csv
```

Target structure:

```text
ADLS Gen2
│
└── bronze
    └── customer
        └── customers.csv
```

## 9\. Data Format

The destination uses a delimited text / CSV format.

```text
Column delimiter: ,
Encoding: UTF-8
First row as header: Enabled
```

## 10\. Pipeline Execution

The pipeline was successfully executed using ADF Debug.

```text
Pipeline status: Succeeded
Copy activity status: Succeeded
```

The Bronze file was also verified in ADLS Gen2.

```text
PostgreSQL
    ↓
Self-hosted Integration Runtime
    ↓
Azure Data Factory
    ↓
Copy Data Activity
    ↓
ADLS Gen2
    ↓
bronze/customer/customers.csv
```

## 11\. Current Project Status

Completed:

* \[x] Azure subscription setup
* \[x] Resource Group
* \[x] ADLS Gen2
* \[x] Bronze/Silver/Gold lake structure
* \[x] PostgreSQL database
* \[x] PostgreSQL schemas
* \[x] PostgreSQL source tables
* \[x] Source CSV loading
* \[x] PostgreSQL data validation
* \[x] Azure Data Factory
* \[x] Self-hosted Integration Runtime
* \[x] Azure Key Vault
* \[x] ADF Managed Identity
* \[x] PostgreSQL linked service
* \[x] Key Vault linked service
* \[x] ADLS Gen2 linked service
* \[x] First ADF pipeline
* \[x] PostgreSQL source dataset
* \[x] ADLS Bronze sink dataset
* \[x] Copy Data activity
* \[x] PostgreSQL → ADLS Bronze ingestion
* \[x] Pipeline execution verification
* \[x] Bronze file verification

## 12\. Current Working Pipeline

```text
Source:
DS\\\_PostgreSQL\\\_Customer
        │
        ▼
Copy:
Copy\\\_Customer\\\_PostgreSQL\\\_To\\\_Bronze
        │
        ▼
Sink:
DS\\\_ADLS\\\_Bronze\\\_Customer
```

## 13\. Why Bronze?

The Bronze layer is the raw/minimally transformed landing layer.

At this stage, the goal is primarily to ingest and preserve source data. Major transformations will happen later.

```text
Bronze
   ↓
Cleaning
   ↓
Validation
   ↓
Deduplication
   ↓
Null handling
   ↓
Joins
   ↓
Silver
```

## 14\. Target Architecture

```text
                         ┌─────────────────────┐
                         │   PostgreSQL OLTP   │
                         │  Operational Data   │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Azure Data Factory  │
                         │   Ingestion /       │
                         │   Orchestration     │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │     ADLS Gen2       │
                         │      BRONZE         │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Databricks / PySpark│
                         │ Cleaning / Validation│
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │       SILVER        │
                         │   Trusted Data      │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │        GOLD         │
                         │ Business-ready Data │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │      Power BI       │
                         │     Analytics       │
                         └─────────────────────┘
```

## 15\. Planned Additional Ingestion Patterns

### Pattern 1 — PostgreSQL Batch

Implemented first:

```text
PostgreSQL
    ↓
ADF
    ↓
ADLS Bronze
```

### Pattern 2 — External CSV Batch

Planned:

```text
External CSV Files
        ↓
ADF
        ↓
ADLS Bronze
```

### Pattern 3 — Streaming

Planned:

```text
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

Example events:

```text
ORDER\\\_CREATED
PAYMENT\\\_RECEIVED
PAYMENT\\\_FAILED
```

Streaming will be handled separately from normal ADF batch ingestion because it is a continuous event-processing workload.

## 16\. Planned ADF Improvements

Future ADF improvements include:

* Incremental loading
* Pipeline parameters
* Dynamic datasets
* Reusable pipelines
* Multiple-table ingestion
* Data validation
* Error handling
* Reject/quarantine records
* Logging
* Pipeline monitoring
* Master orchestration pipeline
* Trigger-based execution

Planned orchestration:

```text
PL\\\_Master\\\_Automotive\\\_DE
│
├── PostgreSQL ingestion
├── CSV ingestion
├── Validation
├── Transformation
├── Error handling
└── Monitoring / notification
```

## 17\. Data Quality and Quarantine

Invalid records should not simply be deleted.

```text
                Bronze
                  │
                  ▼
             Validation
              /       \\\\
             /         \\\\
         Valid         Invalid
           │              │
           ▼              ▼
        Silver       Quarantine
           │
           ▼
          Gold
```

Example:

```text
quarantine/
└── orders/
    └── invalid\\\_customer/
```

The quarantine process will preserve information such as:

```text
source\\\_file
record\\\_id
error\\\_type
error\\\_message
pipeline\\\_run\\\_id
load\\\_timestamp
original\\\_record
```

## 18\. Future Databricks Layer

Databricks/PySpark will process the Bronze data.

Planned operations:

* Null handling
* Duplicate removal
* Data type standardization
* Data validation
* Joins
* Business transformations
* Aggregations

```text
Bronze
   ↓
PySpark
   ↓
Silver
   ↓
Business transformations
   ↓
Gold
```

Delta Lake will be introduced as part of the Databricks layer.

## 19\. Future Incremental Loading

The current pipeline performs a basic batch ingestion.

Later, incremental loading will identify and load only new or changed records instead of repeatedly processing the entire source.

```text
First Load
    ↓
Load required records
```

Future runs:

```text
Source
  ↓
Identify new/changed records
  ↓
Load only required data
  ↓
ADLS
```

## 20\. GitHub and Security Rules

The repository should contain project code, configuration documentation, SQL, pipeline documentation, architecture diagrams, screenshots, and project notes.

Do **NOT** commit:

```text
Passwords
API keys
Azure secrets
Key Vault secret values
.env files
Private certificates
Authentication tokens
Large raw datasets
```

## 21\. First Production-Style Milestone

> Successfully extracted customer data from a local PostgreSQL OLTP database using Azure Data Factory and a Self-hosted Integration Runtime, and landed the data in the ADLS Gen2 Bronze layer.

Working flow:

```text
PostgreSQL
customer.customers
        │
        ▼
Self-hosted Integration Runtime
        │
        ▼
Azure Data Factory
        │
        ▼
Copy\\\_Customer\\\_PostgreSQL\\\_To\\\_Bronze
        │
        ▼
ADLS Gen2
        │
        ▼
bronze/customer/customers.csv
```

## 22\. Next Step

The next engineering goal is to make the ingestion design reusable instead of creating one separate pipeline for every table.

The next phase will cover:

```text
Reusable ADF Pipeline
        ↓
Parameters
        ↓
Dynamic Dataset
        ↓
Multiple PostgreSQL Tables
        ↓
Incremental Loading
        ↓
Validation / Quarantine
        ↓
Master Orchestrator
```
<img width="1920" height="1020" alt="Pipeine 1 data from  postgresto bronze " src="https://github.com/user-attachments/assets/7f8cf5d2-661c-4c2f-a60b-4f1111c12644" />

<img width="1920" height="1020" alt="image" src="https://github.com/user-attachments/assets/e9086102-dfd6-458d-898f-aa3a09935667" />




