# Azure Data Factory — Architecture, Working, Components, Incremental Loading, CDC & Modern Data Engineering

**Project:** Automotive Sales & Finance Data Engineering Platform  
**Stack:** Azure Data Factory, PostgreSQL, ADLS Gen2, Databricks/PySpark, Delta Lake, Power BI

## Table of Contents
1. Overview
2. What is ADF?
3. Where ADF fits
4. Architecture
5. How ADF works
6. Core components
7. Pipeline vs Activity vs Dataset vs Linked Service
8. Integration Runtime
9. Parameters, Expressions and Variables
10. Triggers and Dependencies
11. Copy and Lookup Activities
12. Monitoring
13. Security
14. Project Implementation
15. Reusable Parameterized Pipeline
16. Incremental Loading
17. Watermark Pattern
18. Initial Incremental Implementation
19. Incremental Patterns
20. Timestamp Limitations
21. CDC
22. Evolution from Batch to Modern Data Engineering
23. Batch vs Incremental vs CDC vs Streaming
24. Medallion Architecture
25. Data Quality and Quarantine
26. Master Orchestration
27. Production Principles
28. Cost Optimization
29. Common Mistakes
30. Implementation Checklist
31. Interview Answers
32. Current Status and Next Steps

---

# 1. Overview

Azure Data Factory (ADF) is a managed Azure service for **data integration and orchestration**. It connects data sources, moves data, coordinates processing, schedules workflows, manages dependencies, and provides monitoring.

ADF is not the data lake or analytical warehouse itself. It is primarily the **integration and orchestration layer**.

A useful mental model:

```text
Sources → ADF → Storage → Transformation → Analytics
```

For this project:

```text
PostgreSQL ──┐
             ├──> Azure Data Factory ──> ADLS Gen2 ──> Databricks ──> Power BI
CSV ─────────┘                              Bronze       Silver/Gold
```

Streaming is a complementary path:

```text
Events → Event Hubs/Kafka → Databricks Structured Streaming → Delta
```


---

# 2. What is Azure Data Factory?

ADF supports:

- Data ingestion
- Data movement
- Workflow orchestration
- Scheduling
- Parameterization
- Dependency management
- Monitoring
- Integration with processing services

Example:

```text
PostgreSQL
    ↓
ADF Copy Activity
    ↓
ADLS Gen2 Bronze
```

ADF can also orchestrate a larger workflow:

```text
Ingest → Validate → Transform → Load → Notify
```

The important distinction is:

> **ADF coordinates data workflows; other services store and process the data.**


---

# 3. Where ADF Fits in the Architecture

```text
                 SOURCE SYSTEMS
          +----------+-----------+
          |                      |
      PostgreSQL             External CSV
          |                      |
          +----------+-----------+
                     |
                     v
              Azure Data Factory
           Ingestion + Orchestration
                     |
                     v
                 ADLS Gen2
          +----------+----------+
          |          |          |
       Bronze      Silver      Gold
          |          |          |
          +----------+----------+
                     |
                     v
                  Power BI
```

ADF can orchestrate Databricks jobs, but continuous streaming is normally handled by a streaming engine.

```text
Application Events
       ↓
Event Hubs / Kafka
       ↓
Databricks Structured Streaming
       ↓
Delta Lake
```


---

# 4. Project Architecture

## Batch / OLTP ingestion

```text
PostgreSQL
    ↓
Azure Data Factory
    ↓
ADLS Gen2 Bronze
    ↓
Databricks + PySpark
    ↓
Silver
    ↓
Gold
    ↓
Power BI
```

## External file ingestion

```text
CSV Files
    ↓
ADF
    ↓
ADLS Bronze
```

## Streaming extension

```text
Event Producer
    ↓
Event Hubs / Kafka
    ↓
Databricks Structured Streaming
    ↓
Delta
    ↓
Silver / Gold
```

The project deliberately covers three ingestion patterns:

1. PostgreSQL batch/incremental ingestion
2. External CSV batch ingestion
3. Event streaming


---

# 5. How ADF Basically Works

Think in layers:

```text
Linked Service
     ↓
"How do I connect?"
     ↓
Dataset
     ↓
"What data am I accessing?"
     ↓
Activity
     ↓
"What operation do I perform?"
     ↓
Pipeline
     ↓
"In what workflow/order?"
     ↓
Trigger
     ↓
"When should it run?"
     ↓
Monitoring
     ↓
"Did it succeed?"
```

Example:

```text
PostgreSQL Linked Service
        ↓
PostgreSQL Dataset
        ↓
Copy Activity
        ↓
ADF Pipeline
        ↓
Schedule/Manual Trigger
        ↓
Monitor Run
```


---

# 6. Core ADF Components

| Component | Purpose |
|---|---|
| Factory | Top-level ADF resource |
| Pipeline | Workflow containing activities |
| Activity | Individual operation |
| Dataset | Data location/structure reference |
| Linked Service | Connection information |
| Integration Runtime | Execution/network bridge |
| Parameter | Runtime configuration input |
| Variable | Mutable runtime value |
| Trigger | Starts a pipeline |
| Expression | Dynamic logic/value |
| Monitor | Execution visibility |



---

# 7. Pipeline vs Activity vs Dataset vs Linked Service

### Pipeline
A workflow/container.

```text
PL_02_PostgreSQL_To_Bronze_Reusable
```

### Activity
One operation inside a pipeline.

Examples:

```text
Copy Data
Lookup
Execute Pipeline
ForEach
If Condition
Databricks
```

### Dataset
Represents data being accessed.

Examples:

```text
PostgreSQL table
ADLS CSV
ADLS folder/file
```

### Linked Service
Defines how ADF connects to a system.

Examples:

```text
PostgreSQL connection
ADLS connection
Key Vault connection
```

### Mental model

```text
Linked Service = How to connect
Dataset        = What data
Activity       = What to do
Pipeline       = Workflow
Trigger        = When to start
```


---

# 8. Integration Runtime

Integration Runtime (IR) provides the infrastructure used by ADF for data integration operations.

### Azure IR

Used for cloud-based movement/activities.

```text
ADF → Azure IR → ADLS
```

### Self-hosted IR

Useful when ADF must reach a private/local system.

This project uses:

```text
Windows Laptop
    ↓
PostgreSQL
    ↓
Self-hosted IR
    ↓
ADF
    ↓
ADLS
```

If the laptop is sleeping or the SHIR service is unavailable, local PostgreSQL ingestion cannot run.



---

# 9. Parameters, Expressions and Variables

Parameterization makes ADF reusable.

This project uses:

```text
schema_name
table_name
```

Examples:

```text
@pipeline().parameters.schema_name
@pipeline().parameters.table_name
@dataset().schema_name
@dataset().table_name
```

Activity outputs can be referenced:

```text
activity('Lookup_Last_Watermark').output.firstRow.last_loaded_timestamp
```

Example dynamic SQL:

```text
@concat(
'SELECT * FROM sales.orders WHERE created_at > ''',
activity('Lookup_Last_Watermark').output.firstRow.last_loaded_timestamp,
''''
)
```

### Parameters vs Variables

| Parameter | Variable |
|---|---|
| Configuration/input | Runtime state |
| Supplied at execution | Can change during execution |
| Good for schema/table/path | Good for flags/counters/temporary values |


---

# 10. Triggers and Dependencies

Triggers determine when pipelines start.

Common patterns:

- Manual trigger
- Schedule trigger
- Tumbling-window trigger
- Event-based trigger

Dependencies control execution order:

```text
Lookup
  ↓ Success
Copy
  ↓ Success
Update Watermark
```

Possible dependency conditions include:

- Success
- Failure
- Completion
- Skipped

This lets ADF implement robust workflow logic.


---

# 11. Copy Activity and Lookup Activity

## Copy Activity

Moves data from source to sink.

```text
PostgreSQL
    ↓
Copy Activity
    ↓
ADLS Bronze
```

It can support full loads, incremental queries, file ingestion and data movement.

## Lookup Activity

Retrieves values needed by later activities.

This project uses:

```text
Lookup_Last_Watermark
```

Query:

```sql
SELECT last_loaded_timestamp
FROM public.etl_watermark
WHERE pipeline_name = 'PL_02_sales_orders_incremental';
```

Its result is referenced with:

```text
activity('Lookup_Last_Watermark').output.firstRow.last_loaded_timestamp
```


---

# 12. Monitoring

ADF Monitor provides visibility into:

- Pipeline runs
- Activity runs
- Success/failure
- Duration
- Errors
- Input/output
- Trigger executions

Troubleshooting flow:

```text
Pipeline Failed
    ↓
Open Run
    ↓
Open Failed Activity
    ↓
Read Error
    ↓
Check Dataset / Linked Service / Expression / Source
    ↓
Fix and Retry
```


---

# 13. Security and Secrets

Do not hardcode credentials in pipelines.

Recommended pattern:

```text
ADF Managed Identity
       ↓
Azure Key Vault
       ↓
Secret
       ↓
Linked Service
       ↓
Source
```

Project resources include:

```text
Key Vault: kv-de-learning-2026-isha
Secret: postgres-pass
```

ADF's system-assigned managed identity was given Key Vault access.

The Self-hosted IR handles connectivity to local PostgreSQL; Key Vault handles secret storage.


---

# 14. Project Implementation

**Project:** Automotive Sales & Finance Data Engineering Platform

Main flow:

```text
PostgreSQL
    ↓
ADF
    ↓
ADLS Gen2 Bronze
    ↓
Databricks + PySpark
    ↓
Silver
    ↓
Gold
    ↓
Power BI
```

Implemented ADF resources include:

```text
ADF: isha1803adf
Resource Group: rg-de-learning
```

Key linked services:

```text
LS_PostgreSQL_Automotive
LS_AzureDatalake_Automotive
LS_AzureKeyVault
```

Key datasets:

```text
DS_PostgreSQL_Customer
DS_ADLS_Bronze_Customer
DS_ADLS_Bronze_Reusable
```


---

# 15. Reusable Parameterized Pipeline

Pipeline:

```text
PL_02_PostgreSQL_To_Bronze_Reusable
```

Parameters:

```text
schema_name
table_name
```

PostgreSQL dataset parameters:

```text
schema_name
table_name
```

Dataset expressions:

```text
@dataset().schema_name
@dataset().table_name
```

Pipeline-to-dataset mapping:

```text
@trim(pipeline().parameters.schema_name)
@trim(pipeline().parameters.table_name)
```

ADLS reusable dataset:

```text
DS_ADLS_Bronze_Reusable
```

Destination pattern:

```text
bronze/<schema>/<table>.csv
```

Filename:

```text
@concat(dataset().table_name, '.csv')
```

### Successful tests

| Schema | Table | Bronze Output |
|---|---|---|
| customer | customers | bronze/customer/customers.csv |
| sales | orders | bronze/sales/orders.csv |
| dealer | dealers | bronze/dealer/dealers.csv |
| product | vehicle_models | bronze/product/vehicle_models.csv |

A trailing-space issue caused:

```text
42P01: relation "customer.customers " does not exist
```

Using `trim()` on parameter mappings resolved it.

The important design principle is:

> **One reusable pipeline → many tables.**


---

# 16. Incremental Loading

Incremental loading processes only data that is new or changed since the previous successful processing point.

Instead of:

```text
Every run → read entire table
```

we want:

```text
Previous watermark
       ↓
Read only new/changed records
       ↓
Load
       ↓
Advance watermark
```

Benefits:

- Less data scanned
- Less network movement
- Lower processing time
- Better scalability
- Lower operational cost



---

# 17. Watermark Pattern

A watermark is a stored value representing the last successfully processed point.

Typical values:

```text
created_at
updated_at
increasing ID
source change version
```

Architecture:

```text
Watermark Table
      ↓
Lookup
      ↓
Previous watermark
      ↓
Source query
      ↓
New/changed rows
      ↓
Load
      ↓
SUCCESS
      ↓
Update watermark
```

Critical rule:

> **Update the watermark only after the data load succeeds.**

Otherwise, a failed load could advance the watermark and cause data to be skipped on the next run.


---

# 18. Initial Incremental Implementation

Test table:

```text
sales.orders
```

Watermark column:

```text
created_at
```

Verified with:

```sql
SELECT
    MIN(created_at) AS earliest_created_at,
    MAX(created_at) AS latest_created_at,
    COUNT(*) AS total_orders
FROM sales.orders;
```

Verified values:

```text
Earliest: 2026-04-25 07:52:17.910507
Latest:   2026-04-25 07:52:18.037335
Total:    7000
```

### Control table

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

### Lookup

Activity:

```text
Lookup_Last_Watermark
```

Query:

```sql
SELECT last_loaded_timestamp
FROM public.etl_watermark
WHERE pipeline_name = 'PL_02_sales_orders_incremental';
```

Dependency:

```text
Lookup_Last_Watermark
        ↓ Success
Copy_PostgreSQL_To_Bronze
```

### Dynamic query

```text
@concat(
'SELECT * FROM sales.orders WHERE created_at > ''',
activity('Lookup_Last_Watermark').output.firstRow.last_loaded_timestamp,
''''
)
```

Because the first watermark was `1900-01-01`, the first test qualified all existing orders. This established the initial baseline.

### Current state

```text
Lookup Watermark
      ↓
Copy New Records
```

Still pending:

```text
Copy Success
      ↓
Calculate latest processed timestamp
      ↓
Update etl_watermark
```


---

# 19. Incremental Loading Patterns

## 19.1 Timestamp Watermark

```sql
WHERE updated_at > :last_watermark
```

Good when a reliable timestamp exists.

## 19.2 Increasing ID

```sql
WHERE order_id > :last_order_id
```

Good for append-only data with monotonically increasing IDs.

Limitation: it does not automatically detect updates to old IDs.

## 19.3 Updated Timestamp

```sql
WHERE updated_at > :last_watermark
```

Can detect inserts/updates if the source maintains the timestamp reliably.

## 19.4 CDC

Tracks source changes such as:

```text
INSERT
UPDATE
DELETE
```

It is more change-aware than a simple timestamp query.

## 19.5 Streaming

```text
Event → Event Hubs/Kafka → Streaming Engine → Lakehouse
```

Used when low latency is required.


---

# 20. Limitations of Timestamp Watermarks

Timestamp incremental loading is useful but has edge cases.

### Late-arriving record

```text
Current watermark = 10:00
Late record timestamp = 09:55
```

A strict `> 10:00` filter can miss it.

### Equal timestamps

Several rows may have the same timestamp. Boundary handling must avoid missed or duplicate rows.

### Updates

If only `created_at` exists, an update at 12:00 to a row created at 09:00 may not be detected.

### Deletes

A simple timestamp filter does not tell us that a source row was deleted.

Production solutions may use:

- Overlap windows
- Composite watermarks
- Deduplication
- CDC
- Change tracking
- Source transaction logs


---

# 21. Change Data Capture (CDC)

CDC means **Change Data Capture**.

Instead of repeatedly asking:

```text
"What rows have a timestamp after X?"
```

CDC captures actual source changes.

Typical operations:

```text
INSERT
UPDATE
DELETE
```

Conceptually:

```text
Operational Database
       ↓
Change Log / CDC
       ↓
CDC Capture
       ↓
Events / Change Records
       ↓
Lakehouse / Warehouse
```

CDC is especially valuable when:

- Updates matter
- Deletes matter
- Full scans are expensive
- Low latency is required
- Exact source changes must be propagated

CDC and incremental timestamp loading are related but not identical.

```text
Timestamp Incremental = query-based detection
CDC                   = change-based capture
```


---

# 22. From Batch to CDC and Modern Data Engineering

Modern data platforms evolved through several patterns.

## Stage 1 — Full Batch

```text
Database → ADF → Lake
```

All rows are read each run.

## Stage 2 — Incremental Batch

```text
Database
   ↓
Watermark
   ↓
ADF
   ↓
Only new/changed rows
```

More efficient for growing tables.

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

Better handling of inserts, updates and deletes.

## Stage 4 — Streaming

```text
Application
   ↓
Event Hubs / Kafka
   ↓
Streaming engine
   ↓
Lakehouse
```

Designed for low-latency event processing.

## Stage 5 — Modern Lakehouse

```text
Batch ──┐
CDC ────┼──> Lakehouse / Delta
Stream ─┘          ↓
              Silver / Gold
                   ↓
             BI / ML / Apps
```

Modern platforms commonly combine these approaches.


---

# 23. Batch vs Incremental vs CDC vs Streaming

| Pattern | Typical Latency | Complexity | Inserts | Updates | Deletes |
|---|---|---:|---|---|---|
| Full Batch | High | Low | Yes | Yes via reload | Yes via reload |
| Incremental | Medium | Medium | Yes | Depends on watermark | Usually difficult |
| CDC | Low–Medium | High | Yes | Yes | Yes |
| Streaming | Very Low | High | Event-dependent | Event-dependent | Event-dependent |

The correct pattern depends on business requirements rather than technology popularity.

```text
Once/day → Batch may be enough
Minutes → Incremental may be enough
Exact row changes → CDC
Seconds/real-time → Streaming
```


---

# 24. Medallion Architecture

## Bronze

Raw/minimally transformed data.

Purpose:

- Preserve source data
- Replay
- Audit
- Debug

```text
Source → Bronze
```

## Silver

Cleaned and standardized data.

Typical operations:

- Type standardization
- Null handling
- Deduplication
- Data quality rules
- Joins
- Business standardization

```text
Bronze → Silver
```

## Gold

Business-ready data.

Examples:

```text
Dealer Performance
Monthly Sales
Revenue
Payment Analysis
Vehicle Performance
Customer Segments
```

```text
Silver → Gold → BI
```


---

# 25. Data Quality and Quarantine

Invalid data should generally be preserved and quarantined rather than silently deleted.

```text
                 Bronze
                    |
                Validation
                 /                    Valid       Invalid
               |            |
               v            v
            Silver      Quarantine
               |
               v
              Gold
```

Example:

```text
quarantine/
└── orders/
    └── invalid_customer/
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

This supports investigation, replay and auditing.


---

# 26. Master Orchestration

As the platform grows, a master pipeline can coordinate child pipelines.

```text
PL_Master_Automotive_DE
        |
        +--> PostgreSQL ingestion
        |
        +--> CSV ingestion
        |
        +--> Data validation
        |
        +--> Bronze → Silver
        |
        +--> Silver → Gold
        |
        +--> Error handling / notification
```

Child pipelines keep responsibilities modular:

```text
PL_01_PostgreSQL_To_Bronze
PL_02_CSV_To_Bronze
PL_03_Data_Validation
PL_04_Bronze_To_Silver
PL_05_Silver_To_Gold
```

The master can use `Execute Pipeline`.

The master orchestrator should be introduced after individual workflows are understood and tested.


---

# 27. Production Design Principles

### Reuse

Prefer parameterized pipelines when workflow logic is common.

### Separate configuration from logic

Use parameters for values such as:

```text
schema
table
paths
load mode
```

### Secure secrets

Use Key Vault + Managed Identity.

### Preserve raw data

Bronze should support replay, audit and debugging.

### Update state after success

```text
Extract → Load → Success → Update watermark
```

### Design for retry

Transient failures should be recoverable.

### Idempotency

A rerun should not blindly create duplicates.

Useful techniques:

- Business keys
- Deduplication
- Merge/upsert
- Carefully designed watermark boundaries
- Atomic state updates

### Observability

Track:

```text
run_id
source
target
start_time
end_time
rows_read
rows_written
status
error
```


---

# 28. Cost Optimization

For this learning project:

- Use small datasets during testing.
- Avoid unnecessary Databricks clusters.
- Stop billable compute when finished.
- Avoid repeated large debug runs.
- Monitor Cost Management.
- Use budget alerts.
- Keep the architecture simple until each concept is proven.

Remember:

> Pipeline definitions are not the same as continuously running compute. Costs depend on the service, runtime, activity, data movement and compute used.


---

# 29. Common Mistakes and Lessons Learned

### Hardcoding every table

Instead of:

```text
Pipeline_Customer
Pipeline_Order
Pipeline_Dealer
```

use a parameterized pipeline when the logic is common.

### Confusing pipeline and activity

```text
Pipeline = workflow
Activity = operation inside workflow
```

### Updating watermark too early

Bad:

```text
Update watermark → Copy
```

Safer:

```text
Copy → Success → Update watermark
```

### Calling timestamp filtering CDC

They are different:

```text
Timestamp incremental = query-based
CDC = source-change capture
```

### Ignoring late arrivals

A strict timestamp boundary can miss late records. Production systems may use overlap windows, CDC, composite keys and deduplication.


---

# 30. Practical Implementation Checklist

## ADF Fundamentals

- [x] ADF architecture
- [x] Pipelines
- [x] Activities
- [x] Datasets
- [x] Linked Services
- [x] Integration Runtime
- [x] Parameters
- [x] Expressions
- [x] Dependencies
- [x] Monitoring concepts

## Implemented

- [x] Azure foundation
- [x] ADLS Gen2
- [x] PostgreSQL source
- [x] Self-hosted IR
- [x] Key Vault
- [x] PostgreSQL linked service
- [x] ADLS linked service
- [x] PostgreSQL dataset
- [x] ADLS Bronze dataset
- [x] PostgreSQL → Bronze pipeline
- [x] Reusable parameterized pipeline
- [x] Multiple-table testing
- [x] Watermark control table
- [x] Lookup watermark activity
- [x] Dynamic incremental query
- [x] Initial incremental test

## Pending

- [ ] Update watermark after successful Copy
- [ ] Insert new test records
- [ ] Second incremental run
- [ ] Verify only new records
- [ ] Failure/retry test
- [ ] Batch CSV ingestion
- [ ] Data validation
- [ ] Quarantine
- [ ] Bronze → Silver
- [ ] Silver → Gold
- [ ] Databricks/PySpark
- [ ] Delta Lake
- [ ] Streaming
- [ ] Production monitoring
- [ ] Master orchestrator


---

# 31. Interview Answers

### What is ADF?

> Azure Data Factory is a cloud-based data integration and orchestration service used to move data between systems and coordinate data engineering workflows.

### What is a pipeline?

> A pipeline is a logical workflow containing one or more activities.

### What is an activity?

> An activity is an individual operation such as Copy, Lookup, Execute Pipeline, ForEach or an external processing activity.

### Why parameterize?

> Parameterization allows the same pipeline to work with different schemas, tables, paths or runtime configurations without duplicating logic.

### What is incremental loading?

> Incremental loading processes only new or changed records since the previous successful processing point instead of reprocessing the entire source.

### What is a watermark?

> A watermark is a persisted value representing the last successfully processed point, such as a timestamp or increasing ID.

### Why CDC?

> CDC captures actual inserts, updates and deletes from a source, making it more reliable than a simple timestamp watermark when exact changes must be propagated.

### Is ADF a database?

> No. ADF is primarily a data integration and orchestration service. Storage and analytical processing are provided by services such as ADLS, Databricks, Synapse and data warehouses.


---

# 32. Current Project Status and Next Steps

## Current state

```text
Azure Foundation
      ↓
ADLS Gen2
      ↓
PostgreSQL
      ↓
ADF
      ↓
Reusable Parameterized Pipeline
      ↓
Bronze Ingestion
      ↓
Initial Incremental Loading
```

Reusable pipeline tested with:

```text
customer.customers
sales.orders
dealer.dealers
product.vehicle_models
```

Current incremental flow:

```text
Lookup_Last_Watermark
        ↓
Copy_PostgreSQL_To_Bronze
```

## Immediate next step

Complete:

```text
Lookup Watermark
      ↓
Copy New Records
      ↓
Calculate MAX(created_at)
      ↓
Update etl_watermark
```

Then test:

```text
Initial run
    ↓
Insert new orders
    ↓
Second run
    ↓
Only new records loaded
    ↓
Watermark moves forward
```

## Roadmap after incremental loading

```text
Incremental Loading
       ↓
CSV Batch Ingestion
       ↓
Data Quality + Quarantine
       ↓
Bronze → Silver
       ↓
Silver → Gold
       ↓
Databricks + PySpark
       ↓
Delta Lake
       ↓
CDC
       ↓
Event Hubs / Kafka
       ↓
Structured Streaming
       ↓
Production Orchestration
```

## Final mental model

```text
                    SOURCES
                       |
          +------------+------------+
          |            |            |
      PostgreSQL      CSV         Events
          |            |            |
          +------------+            |
                       |            |
                       v            v
                 Azure Data      Event Hubs
                   Factory       / Kafka
                       |            |
                       v            v
                    ADLS        Databricks
                   Bronze       Streaming
                       |            |
                       +------+-----+
                              |
                              v
                        Lakehouse/Delta
                              |
                         +----+----+
                         |         |
                      Silver      Gold
                         |         |
                         +----+----+
                              |
                         Power BI /
                       Analytics / ML
```

The learning progression is:

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
Modern Lakehouse
      ↓
Production Data Engineering
```
