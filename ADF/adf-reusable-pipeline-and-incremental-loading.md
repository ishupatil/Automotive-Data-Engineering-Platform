# ADF Reusable Pipeline & Initial Incremental Loading

**Project:** Automotive Sales & Finance Data Engineering Platform  
**Date:** 2026-10-07  
**Technology:** Azure Data Factory, PostgreSQL, ADLS Gen2

---

## 1. Objective

The goal was to convert the original PostgreSQL → ADLS Bronze pipeline into a reusable parameterized pipeline.

Instead of creating a separate pipeline for every PostgreSQL table, the same pipeline accepts:

- `schema_name`
- `table_name`

The second objective was to start implementing incremental loading using a watermark so future runs can process only newly created records.

---

# Part A — Reusable ADF Pipeline

## 2. Create the Reusable Pipeline

### Step 1 — Create a new pipeline

In Azure Data Factory:

1. Open **Author**.
2. Select **Pipelines**.
3. Create a new pipeline.
4. Name it:

```text
PL_02_PostgreSQL_To_Bronze_Reusable
```

This was kept separate from the original known-good pipeline.

---

## 3. Add Pipeline Parameters

### Step 2 — Create `schema_name`

Open the pipeline **Parameters** section.

Create:

```text
Name: schema_name
Type: String
Default value: customer
```

Purpose:

The parameter determines which PostgreSQL schema should be read.

Example:

```text
customer
sales
dealer
product
```

### Step 3 — Create `table_name`

Create:

```text
Name: table_name
Type: String
Default value: customers
```

Purpose:

The parameter determines which PostgreSQL table should be read.

Example:

```text
customers
orders
dealers
vehicle_models
```

---

# 4. Parameterize the PostgreSQL Dataset

## Step 4 — Open the PostgreSQL Dataset

The reusable pipeline uses:

```text
DS_PostgreSQL_Customer
```

The dataset was converted from a fixed table definition into a parameterized dataset.

---

## Step 5 — Add Dataset Parameters

Add:

```text
schema_name
table_name
```

Both parameters are of type:

```text
String
```

---

## Step 6 — Make the Schema Dynamic

In the PostgreSQL dataset connection/table configuration, replace the fixed schema with:

```text
@dataset().schema_name
```

This means the dataset gets the schema from its dataset parameter.

---

## Step 7 — Make the Table Dynamic

Replace the fixed table with:

```text
@dataset().table_name
```

The same dataset can now point to different PostgreSQL tables.

Conceptually:

```text
Dataset
   |
   ├── schema_name
   └── table_name
```

---

# 5. Pass Pipeline Parameters to the PostgreSQL Dataset

## Step 8 — Open the Copy Activity

The Copy activity is:

```text
Copy_PostgreSQL_To_Bronze
```

Open the **Source Dataset Parameters** section.

---

## Step 9 — Map `schema_name`

Pass the pipeline parameter into the dataset:

```text
@trim(pipeline().parameters.schema_name)
```

`trim()` was used to prevent accidental leading/trailing spaces.

---

## Step 10 — Map `table_name`

Pass:

```text
@trim(pipeline().parameters.table_name)
```

The final flow is:

```text
Pipeline parameter
        ↓
Dataset parameter
        ↓
PostgreSQL schema/table
```

---

# 6. Troubleshooting the PostgreSQL Parameter

During testing, an error occurred:

```text
42P01: relation "customer.customers " does not exist
```

Notice the space after:

```text
customers
```

The issue was a trailing whitespace character in the parameter value.

The parameter mapping was made safer with:

```text
@trim(pipeline().parameters.schema_name)
```

and:

```text
@trim(pipeline().parameters.table_name)
```

After correcting this, the reusable pipeline worked successfully.

---

# 7. Create the Reusable ADLS Bronze Dataset

## Step 11 — Create/Open the ADLS Dataset

Dataset:

```text
DS_ADLS_Bronze_Reusable
```

This dataset is responsible for dynamically creating the Bronze destination.

---

## Step 12 — Add ADLS Dataset Parameters

Create:

```text
schema_name
table_name
```

Both are:

```text
String
```

---

## Step 13 — Configure the Bronze Directory

Container/file system:

```text
bronze
```

Directory:

```text
@dataset().schema_name
```

Therefore:

```text
schema = sales
```

creates:

```text
bronze/sales/
```

---

## Step 14 — Configure the Filename

File name:

```text
@concat(dataset().table_name, '.csv')
```

For:

```text
table_name = orders
```

the resulting file is:

```text
orders.csv
```

---

# 8. Configure the Sink

## Step 15 — Pass Pipeline Parameters to ADLS Dataset

In the Copy Activity sink dataset parameters:

### Schema

```text
@pipeline().parameters.schema_name
```

### Table

```text
@pipeline().parameters.table_name
```

The final destination becomes:

```text
bronze/<schema>/<table>.csv
```

Example:

```text
bronze/sales/orders.csv
```

---

# 9. Reusable Pipeline Architecture

The completed reusable flow is:

```text
Pipeline Parameters
        |
        |-- schema_name
        |
        |-- table_name
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

The important design principle is:

> One pipeline → many tables.

---

# 10. Test the Reusable Pipeline

## Step 16 — Test Customer Table

Pipeline parameters:

```text
schema_name = customer
table_name = customers
```

Expected output:

```text
bronze/customer/customers.csv
```

Result:

```text
SUCCESS
```

---

## Step 17 — Test Orders Table

Parameters:

```text
schema_name = sales
table_name = orders
```

Output:

```text
bronze/sales/orders.csv
```

Result:

```text
SUCCESS
```

---

## Step 18 — Test Dealer Table

Parameters:

```text
schema_name = dealer
table_name = dealers
```

Output:

```text
bronze/dealer/dealers.csv
```

Result:

```text
SUCCESS
```

---

## Step 19 — Test Vehicle Models Table

Parameters:

```text
schema_name = product
table_name = vehicle_models
```

Output:

```text
bronze/product/vehicle_models.csv
```

Result:

```text
SUCCESS
```

---

## 11. Reusability Test Summary

| Schema | Table | Bronze Output | Result |
|---|---|---|---|
| customer | customers | `bronze/customer/customers.csv` | ✅ |
| sales | orders | `bronze/sales/orders.csv` | ✅ |
| dealer | dealers | `bronze/dealer/dealers.csv` | ✅ |
| product | vehicle_models | `bronze/product/vehicle_models.csv` | ✅ |

The pipeline was then published.

---

# Part B — Initial Incremental Loading

# 12. Understand the Incremental Loading Requirement

A full load reads all records:

```text
Source
  ↓
All records
  ↓
Bronze
```

Incremental loading reads records after the last successfully processed point:

```text
Source
  ↓
Read last watermark
  ↓
Read only new records
  ↓
Bronze
  ↓
Update watermark
```

The watermark acts like a bookmark.

---

# 13. Select the Test Table

The selected table was:

```text
sales.orders
```

The selected watermark column was:

```text
created_at
```

Before implementing incremental loading, the timestamp range was checked.

---

# 14. Verify the Watermark Column

Run in PostgreSQL:

```sql
SELECT
    MIN(created_at) AS earliest_created_at,
    MAX(created_at) AS latest_created_at,
    COUNT(*) AS total_orders
FROM sales.orders;
```

Verified values:

```text
Earliest created_at:
2026-04-25 07:52:17.910507

Latest created_at:
2026-04-25 07:52:18.037335

Total orders:
7000
```

This confirmed that `created_at` could be used as the initial watermark column for this exercise.

---

# 15. Create the Watermark Control Table

## Step 20 — Create `etl_watermark`

Run:

```sql
CREATE TABLE IF NOT EXISTS public.etl_watermark (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_loaded_timestamp TIMESTAMP
);
```

Purpose:

The table stores the last successfully processed timestamp for an ingestion process.

---

# 16. Insert the Initial Watermark

## Step 21 — Insert the Starting Timestamp

Run:

```sql
INSERT INTO public.etl_watermark
    (pipeline_name, last_loaded_timestamp)
VALUES
    ('PL_02_sales_orders_incremental', '1900-01-01 00:00:00');
```

The initial value was:

```text
1900-01-01 00:00:00
```

This is intentionally earlier than the existing order timestamps.

---

# 17. Verify the Watermark

Run:

```sql
SELECT *
FROM public.etl_watermark;
```

Expected logical result:

```text
PL_02_sales_orders_incremental
1900-01-01 00:00:00
```

---

# 18. Create the Lookup Activity

## Step 22 — Add Lookup Activity

In ADF:

1. Open `PL_02_PostgreSQL_To_Bronze_Reusable`.
2. Add a **Lookup** activity.
3. Name it:

```text
Lookup_Last_Watermark
```

The purpose is to retrieve the last processed timestamp before the Copy activity starts.

---

# 19. Configure the Lookup Query

## Step 23 — Use PostgreSQL as the Lookup Source

The Lookup uses the PostgreSQL connection/dataset.

Query:

```sql
SELECT last_loaded_timestamp
FROM public.etl_watermark
WHERE pipeline_name = 'PL_02_sales_orders_incremental';
```

Enable:

```text
First row only
```

The result is available through:

```text
activity('Lookup_Last_Watermark').output.firstRow.last_loaded_timestamp
```

---

# 20. Connect Lookup to Copy

## Step 24 — Add Success Dependency

Connect:

```text
Lookup_Last_Watermark
          |
       Success
          ↓
Copy_PostgreSQL_To_Bronze
```

This ensures the Copy activity starts after the watermark has been successfully retrieved.

---

# 21. Change Copy Activity to Query Mode

## Step 25 — Open the Copy Source

Previously, the reusable pipeline could use the table directly.

For incremental loading, the source was changed to **Query** mode because the query needs a dynamic `WHERE` condition.

Conceptual query:

```sql
SELECT *
FROM sales.orders
WHERE created_at > <last_loaded_timestamp>;
```

---

# 22. Build the Dynamic ADF Query

## Step 26 — Add the ADF Expression

The dynamic query used was:

```text
@concat(
'SELECT * FROM sales.orders WHERE created_at > ''',
activity('Lookup_Last_Watermark').output.firstRow.last_loaded_timestamp,
''''
)
```

ADF obtains the timestamp from the Lookup activity and inserts it into the SQL query.

For example, if the Lookup returns:

```text
2026-04-25 07:52:18
```

ADF builds a query equivalent to:

```sql
SELECT *
FROM sales.orders
WHERE created_at > '2026-04-25 07:52:18';
```

---

# 23. Why the Dynamic Query Was Used

The watermark changes after every successful load.

Therefore, the query cannot be hardcoded as:

```sql
WHERE created_at > '2026-04-25 07:52:18'
```

Instead, ADF dynamically retrieves the latest stored watermark.

The pattern is:

```text
Watermark Table
      ↓
Lookup
      ↓
ADF Expression
      ↓
Dynamic SQL
      ↓
Copy New Records
```

---

# 24. Validate the Pipeline

## Step 27 — Validate

Use ADF's **Validate** option.

The pipeline validation succeeded after configuring:

- Pipeline parameters
- Dataset parameters
- Lookup activity
- Lookup dependency
- Dynamic source query

---

# 25. Run the Initial Incremental Test

## Step 28 — Debug the Pipeline

Use parameters:

```text
schema_name = sales
table_name = orders
```

The Lookup retrieved:

```text
1900-01-01 00:00:00
```

The generated condition therefore became:

```sql
WHERE created_at > '1900-01-01 00:00:00'
```

Since all existing records have a later `created_at`, the initial incremental test processed the existing orders.

The test completed successfully.

---

# 26. Why the First Incremental Run Loads Existing Data

This is important.

The first watermark was:

```text
1900-01-01 00:00:00
```

Existing orders have timestamps around:

```text
2026-04-25
```

Therefore:

```text
2026-04-25 > 1900-01-01
```

So all existing records qualify.

This first run effectively establishes the initial baseline.

It is not yet the complete incremental-loading implementation because the watermark has not yet been advanced.

---

# 27. Current Incremental Architecture

Current implementation:

```text
                 PostgreSQL
                     |
                     ▼
             Lookup Watermark
                     |
                     ▼
              Last Timestamp
                     |
                     ▼
              Dynamic SQL Query
                     |
                     ▼
             Copy New Records
                     |
                     ▼
                 ADLS Bronze
```

The intended final flow is:

```text
Lookup Watermark
       ↓
Copy New Records
       ↓
Copy Successful?
    /       \
  YES        NO
   ↓          ↓
Update      Keep old
Watermark   watermark
```

---

# 28. Work Completed

### Reusable Pipeline

- [x] Created `PL_02_PostgreSQL_To_Bronze_Reusable`
- [x] Added `schema_name`
- [x] Added `table_name`
- [x] Parameterized PostgreSQL dataset
- [x] Parameterized ADLS Bronze dataset
- [x] Created dynamic Bronze path
- [x] Added `trim()` to parameter mappings
- [x] Fixed trailing-space PostgreSQL error
- [x] Tested customer table
- [x] Tested orders table
- [x] Tested dealers table
- [x] Tested vehicle models table
- [x] Published the pipeline

### Initial Incremental Loading

- [x] Selected `sales.orders`
- [x] Selected `created_at` as watermark
- [x] Verified watermark column
- [x] Created `public.etl_watermark`
- [x] Inserted initial watermark
- [x] Created `Lookup_Last_Watermark`
- [x] Connected Lookup → Copy with Success dependency
- [x] Changed Copy source to Query mode
- [x] Created dynamic watermark-based SQL
- [x] Validated pipeline
- [x] Completed initial incremental test

---

# 29. Pending Work

The incremental implementation is intentionally not finished yet.

Next steps:

- [ ] Calculate the latest successfully processed `created_at`
- [ ] Update `public.etl_watermark` after successful Copy
- [ ] Make the watermark update failure-safe
- [ ] Insert new orders into PostgreSQL
- [ ] Run the pipeline again
- [ ] Verify that only the new records are processed
- [ ] Confirm the watermark moves forward
- [ ] Test failure/retry behavior

---

# 30. Interview Explanation

> I initially created a PostgreSQL-to-ADLS Bronze pipeline. To avoid creating separate pipelines for every table, I parameterized the schema and table names and made both the PostgreSQL source and ADLS sink dynamic. I tested the same pipeline with customer, orders, dealers, and vehicle models.
>
> I then introduced incremental loading using a watermark control table. ADF first retrieves the last processed timestamp using a Lookup activity, and the Copy activity dynamically queries records where `created_at` is greater than that watermark. The initial implementation successfully retrieved the watermark and performed the first incremental test. The next step is to update the watermark only after a successful load and validate the second incremental run with newly inserted records.

---

# 31. Key Production Concepts Learned

### Parameterization

```text
One pipeline
      ↓
Many tables
```

### Watermark

```text
Last successfully processed point
```

### Incremental Loading

```text
Only process data after the watermark
```

### Important Production Rule

The watermark should advance **only after the data load succeeds**.

If the Copy fails, the previous watermark should remain available so the records can be retried safely.

---

# 32. GitHub Documentation

Recommended file:

```text
docs/adf-reusable-pipeline-and-incremental-loading.md
```

Recommended commit message:

```text
feat: document reusable ADF pipeline and initial incremental loading
```
