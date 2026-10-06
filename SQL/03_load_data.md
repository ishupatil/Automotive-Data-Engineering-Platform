# PostgreSQL Data Loading

## 1\. PostgreSQL Connection

The PostgreSQL database was created and accessed using the `psql` command-line client.

### Check PostgreSQL Installation

```cmd
psql --version
```

### Connect to PostgreSQL

```cmd
psql -U postgres
```

### Create the Project Database

```sql
CREATE DATABASE automotive\\\_db;
```

### Connect to the Project Database

```sql
\\\\c automotive\\\_db
```

\---

## 2\. Create Project Schemas

```sql
CREATE SCHEMA customer;
CREATE SCHEMA dealer;
CREATE SCHEMA product;
CREATE SCHEMA sales;
```

Project schema structure:

```text
automotive\\\_db
├── customer
├── dealer
├── product
└── sales
```

\---

## 3\. Navigate to the CSV Source Directory

The CSV files were extracted into the `postgres\\\_source` directory.

From Windows Command Prompt:

```cmd
cd path\\\\to\\\\postgres\\\_source
```

Check the files:

```cmd
dir
```

Expected source files:

```text
customer\\\_customers.csv
dealer\\\_dealers.csv
product\\\_management\\\_vehicle\\\_models.csv
product\\\_management\\\_vehicle\\\_variants.csv
sales\\\_transaction\\\_orders.csv
sales\\\_transaction\\\_order\\\_items.csv
sales\\\_transaction\\\_invoices.csv
sales\\\_transaction\\\_payments.csv
```

\---

## 4\. Inspect CSV Headers

Before loading the files, the CSV headers were checked.

### Customer

```cmd
powershell -Command "Get-Content .\\\\customer\\\_customers.csv -TotalCount 1"
```

### Dealer

```cmd
powershell -Command "Get-Content .\\\\dealer\\\_dealers.csv -TotalCount 1"
```

### Vehicle Models

```cmd
powershell -Command "Get-Content .\\\\product\\\_management\\\_vehicle\\\_models.csv -TotalCount 1"
```

### Vehicle Variants

```cmd
powershell -Command "Get-Content .\\\\product\\\_management\\\_vehicle\\\_variants.csv -TotalCount 1"
```

### Orders

```cmd
powershell -Command "Get-Content .\\\\sales\\\_transaction\\\_orders.csv -TotalCount 1"
```

### Order Items

```cmd
powershell -Command "Get-Content .\\\\sales\\\_transaction\\\_order\\\_items.csv -TotalCount 1"
```

### Invoices

```cmd
powershell -Command "Get-Content .\\\\sales\\\_transaction\\\_invoices.csv -TotalCount 1"
```

### Payments

```cmd
powershell -Command "Get-Content .\\\\sales\\\_transaction\\\_payments.csv -TotalCount 1"
```

\---

# 5\. Load CSV Data Using `\\\\copy`

The CSV files were loaded using PostgreSQL's `psql` client-side `\\\\copy` command.

`\\\\copy` reads the file from the local/client machine.

## Customer

```sql
\\\\copy customer.customers FROM 'customer\\\_customers.csv' WITH (FORMAT csv, HEADER true);
```

## Dealer

```sql
\\\\copy dealer.dealers FROM 'dealer\\\_dealers.csv' WITH (FORMAT csv, HEADER true);
```

## Vehicle Models

```sql
\\\\copy product.vehicle\\\_models FROM 'product\\\_management\\\_vehicle\\\_models.csv' WITH (FORMAT csv, HEADER true);
```

The vehicle model CSV contained:

```text
26 lines
1 header
25 data records
```

Therefore:

```text
COPY 25
```

was the expected result.

## Vehicle Variants

```sql
\\\\copy product.vehicle\\\_variants FROM 'product\\\_management\\\_vehicle\\\_variants.csv' WITH (FORMAT csv, HEADER true);
```

## Orders

```sql
\\\\copy sales.orders FROM 'sales\\\_transaction\\\_orders.csv' WITH (FORMAT csv, HEADER true);
```

## Order Items

```sql
\\\\copy sales.order\\\_items FROM 'sales\\\_transaction\\\_order\\\_items.csv' WITH (FORMAT csv, HEADER true);
```

## Invoices

```sql
\\\\copy sales.invoices FROM 'sales\\\_transaction\\\_invoices.csv' WITH (FORMAT csv, HEADER true);
```

## Payments

```sql
\\\\copy sales.payments FROM 'sales\\\_transaction\\\_payments.csv' WITH (FORMAT csv, HEADER true);
```

\---

# 6\. Data-Type Issue During Orders Load

The initial `discount\\\_id` column was created as:

```sql
discount\\\_id INTEGER
```

The CSV contained values such as:

```text
1.0
```

This caused:

```text
ERROR: invalid input syntax for type integer
```

The column was changed to:

```sql
ALTER TABLE sales.orders
ALTER COLUMN discount\\\_id TYPE NUMERIC(10,2);
```

The orders data was then loaded successfully:

```sql
\\\\copy sales.orders FROM 'sales\\\_transaction\\\_orders.csv' WITH (FORMAT csv, HEADER true);
```

\---

# 7\. Verify Loaded Data

## Customer

```sql
SELECT COUNT(\\\*)
FROM customer.customers;
```

```sql
SELECT \\\*
FROM customer.customers
LIMIT 5;
```

## Dealer

```sql
SELECT COUNT(\\\*)
FROM dealer.dealers;
```

```sql
SELECT \\\*
FROM dealer.dealers
LIMIT 5;
```

## Vehicle Models

```sql
SELECT COUNT(\\\*)
FROM product.vehicle\\\_models;
```

## Vehicle Variants

```sql
SELECT COUNT(\\\*)
FROM product.vehicle\\\_variants;
```

## Orders

```sql
SELECT COUNT(\\\*)
FROM sales.orders;
```

## Order Items

```sql
SELECT COUNT(\\\*)
FROM sales.order\\\_items;
```

## Invoices

```sql
SELECT COUNT(\\\*)
FROM sales.invoices;
```

## Payments

```sql
SELECT COUNT(\\\*)
FROM sales.payments;
```

\---

# 8\. PostgreSQL Data Flow

```text
CSV Files
    ↓
Local Machine
    ↓
psql / \\\\copy
    ↓
PostgreSQL
    ↓
automotive\\\_db
    ↓
customer / dealer / product / sales schemas
```

\---

# 9\. PostgreSQL Role in the Data Engineering Project

PostgreSQL represents the operational OLTP source system.

The next ingestion stage is:

```text
PostgreSQL
    ↓
Self-hosted Integration Runtime
    ↓
Azure Data Factory
    ↓
ADLS Gen2
    ↓
Bronze
```

\---

# 10\. `\\\\copy` vs `COPY`

## `\\\\copy`

```text
Local/Client Machine
        ↓
PostgreSQL
```

`\\\\copy` is a `psql` client command and reads the file from the machine running the PostgreSQL client.

## `COPY`

```text
PostgreSQL Server Filesystem
        ↓
PostgreSQL
```

`COPY` is a PostgreSQL SQL command and uses the PostgreSQL server's filesystem.

For this project, `\\\\copy` was used because the CSV files were stored on the local development machine.



