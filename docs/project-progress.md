# Automotive Data Engineering Platform — Project Progress

## Project Objective

Build a production-oriented Azure Data Engineering platform for automotive sales and finance data.

## Target Architecture

PostgreSQL / CSV Sources  
↓  
Azure Data Factory  
↓  
ADLS Gen2 — Bronze  
↓  
Databricks + PySpark  
↓  
Silver  
↓  
Gold  
↓  
Power BI

Invalid records are handled separately through a quarantine/reject layer.

## Ingestion Patterns

### 1. PostgreSQL Batch

PostgreSQL → Self-hosted Integration Runtime → ADF → ADLS Gen2

### 2. External CSV Batch

CSV Files → ADF → ADLS Gen2

### 3. Streaming

Event Producer → Event Hubs/Kafka → Databricks Structured Streaming → Delta

## Completed

### Azure Foundation

- Azure subscription configured
- Resource group: `rg-de-learning`
- Region: Central India
- ₹500 budget alert created
- Project tags configured

### ADLS Gen2

- Storage account configured
- Bronze layer created
- Silver layer created
- Gold layer created

### PostgreSQL Source

- Database: `automotive_db`
- Schemas:
  - `customer`
  - `dealer`
  - `product`
  - `sales`
- Automotive source tables created
- CSV data loaded into PostgreSQL
- Data relationships validated

### Azure Data Factory

- ADF: `isha1803adf`
- Self-hosted IR: `integrationRuntimepsql`
- PostgreSQL linked service created
- ADLS Gen2 linked service created

### Security

- Key Vault configured
- PostgreSQL password stored in Key Vault
- ADF managed identity configured
- Key Vault RBAC configured

## In Progress

- PostgreSQL → ADLS Bronze pipeline
- Incremental loading
- Data validation
- Reject/quarantine handling
- Databricks transformations
- Silver and Gold processing
- CSV ingestion pipeline
- Streaming pipeline
- Master ADF orchestration

## Cost Management

The project is being developed with minimal Azure consumption.

### Development Principles

- Use small test datasets
- Avoid unnecessary compute
- Run pipelines only when required
- Stop billable compute when not in use
- Monitor Azure budget and usage
# Project Progress

## 2026-10-07 — ADF Reusable Ingestion & Incremental Loading

### Completed

#### Parameterized ADF Ingestion

Created a reusable Azure Data Factory pipeline:

`PL_02_PostgreSQL_To_Bronze_Reusable`

The pipeline accepts:

- `schema_name`
- `table_name`

These parameters are passed to the PostgreSQL source dataset and ADLS Gen2 Bronze destination dataset.

The reusable pipeline was successfully tested with:

| Schema | Table | Bronze Destination |
|---|---|---|
| customer | customers | bronze/customer/customers.csv |
| sales | orders | bronze/sales/orders.csv |
| dealer | dealers | bronze/dealer/dealers.csv |
| product | vehicle_models | bronze/product/vehicle_models.csv |

#### Incremental Loading — Initial Implementation

Created PostgreSQL control table:

`public.etl_watermark`

The table stores the last successfully processed timestamp.

Initial watermark:

`1900-01-01 00:00:00`

Verified `sales.orders.created_at` as the watermark column.

Created ADF Lookup activity:

`Lookup_Last_Watermark`

The Lookup retrieves the current watermark:

```sql
SELECT last_loaded_timestamp
FROM public.etl_watermark
WHERE pipeline_name = 'PL_02_sales_orders_incremental';

The Lookup is connected to the Copy activity using a Success dependency.
The Copy activity uses a dynamic query based on the retrieved watermark:
SELECT *
FROM sales.orders
WHERE created_at > <last_loaded_timestamp>;

The first incremental-load test completed successfully.