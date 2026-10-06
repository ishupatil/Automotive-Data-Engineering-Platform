\# Automotive Data Engineering Platform — Project Progress



\## Project Objective




Build a production-oriented Azure Data Engineering platform for automotive sales and finance data.



\## Target Architecture



PostgreSQL / CSV Sources

&#x20;       ↓

Azure Data Factory

&#x20;       ↓

ADLS Gen2 — Bronze

&#x20;       ↓

Databricks + PySpark

&#x20;       ↓

Silver

&#x20;       ↓

Gold

&#x20;       ↓

Power BI



Invalid records are handled separately through a quarantine/reject layer.



\## Ingestion Patterns



\### 1. PostgreSQL Batch

PostgreSQL → Self-hosted Integration Runtime → ADF → ADLS Gen2



\### 2. External CSV Batch

CSV Files → ADF → ADLS Gen2



\### 3. Streaming

Event Producer → Event Hubs/Kafka → Databricks Structured Streaming → Delta



\## Completed



\### Azure Foundation

\- Azure subscription configured

\- Resource group: `rg-de-learning`

\- Region: Central India

\- ₹500 budget alert created

\- Project tags configured



\### ADLS Gen2

\- Storage account configured

\- Bronze layer created

\- Silver layer created

\- Gold layer created



\### PostgreSQL Source

\- Database: `automotive\_db`

\- Schemas:

&#x20; - `customer`

&#x20; - `dealer`

&#x20; - `product`

&#x20; - `sales`

\- Automotive source tables created

\- CSV data loaded into PostgreSQL

\- Data relationships validated



\### Azure Data Factory

\- ADF: `isha1803adf`

\- Self-hosted IR: `integrationRuntimepsql`

\- PostgreSQL linked service created

\- ADLS Gen2 linked service created



\### Security

\- Key Vault configured

\- PostgreSQL password stored in Key Vault

\- ADF managed identity configured

\- Key Vault RBAC configured



\## In Progress



\- PostgreSQL → ADLS Bronze pipeline

\- Incremental loading

\- Data validation

\- Reject/quarantine handling

\- Databricks transformations

\- Silver and Gold processing

\- CSV ingestion pipeline

\- Streaming pipeline

\- Master ADF orchestration



\## Cost Management



The project is being developed with minimal Azure consumption.



Development principles:

\- Use small test datasets

\- Avoid unnecessary compute

\- Run pipelines only when required

\- Stop billable compute when not in use

\- Monitor Azure budget and usage



