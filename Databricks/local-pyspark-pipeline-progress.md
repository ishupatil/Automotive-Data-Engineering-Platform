# Project Progress: Local PySpark Data Quality Pipeline

**Project:** Automotive Data Engineering Platform  
**Milestone:** Local PySpark transformation and data-quality validation  
**Status:** Completed locally; Databricks Delta integration is a future step.

## Architecture in progress

The target platform is:

PostgreSQL / CSV sources → Azure Data Factory → ADLS Gen2 Bronze → Databricks + PySpark Silver → dbt Gold → Power BI

The current milestone focuses on developing and validating the PySpark transformation logic locally before moving it to Databricks.

## What I completed

- Built and ran a local PySpark pipeline for the automotive orders dataset.
- Read orders and reference snapshots for customers, dealers, and vehicle models.
- Excluded test orders from normal processing and saved them separately.
- Normalized selected text fields, including order status, order number, payment mode, and finance provider.
- Validated required IDs, order status, customer/dealer references, duplicate order IDs, and positive total amounts.
- Checked financial values and net-amount consistency.
- Classified records as valid, review-required, or rejected according to the current validation rules.
- Kept review-required records in Silver while also producing a separate review output.
- Generated separate CSV outputs for Silver orders, quarantined orders, review-required orders, excluded test records, and a quality report.
- Added reconciliation checks so validated records are accounted for as Silver or quarantined records; review-required records are a subset of Silver.
- Used a Python CSV writer for local Windows output to avoid the local Hadoop native CSV-writer configuration issue.

## Latest local run

| Metric | Result |
|---|---:|
| Source orders | 7,002 |
| Test orders excluded | 2 |
| Orders validated | 7,000 |
| Silver orders | 7,000 |
| Quarantined orders | 0 |
| Review-required orders | 782 |
| Missing required IDs | 0 |
| Invalid order statuses | 0 |
| Orders with unmatched customers | 0 |
| Orders with unmatched dealers | 0 |
| Duplicate order ID records | 0 |
| Non-positive total amounts | 0 |
| Negative financial amount flags | 1 |
| Missing finance provider flags | 781 |
| Net amount mismatches | 0 |

The 782 review-required records consist of 781 records missing a finance provider and 1 record with a negative financial amount. These are flagged for investigation rather than automatically corrected because the appropriate business treatment needs confirmation.

## Local output location

The final-output version of the script is configured to write its outputs under:

`Databricks/final_outputs/`

Expected output folders:

- `silver_orders/data.csv`
- `quarantine_orders/data.csv`
- `review_orders/data.csv`
- `excluded_test_records/data.csv`
- `quality_report/data.csv`

These are local CSV outputs. They are **not yet Delta tables in Databricks**.

## Important scope notes

This milestone demonstrates local PySpark transformation and data-quality logic. The following are still future work:

- Run the transformation as a Databricks job.
- Write Silver and quarantine data to persistent Delta tables.
- Implement and test safe incremental processing and reruns.
- Orchestrate ingestion and transformation through Azure Data Factory.
- Build the Gold layer with dbt and add automated dbt tests.
- Add production monitoring, alerting, and recovery procedures.
- Connect curated Gold data to Power BI.

## Business rules needing confirmation

The current rules are a working baseline. Confirm the following with business stakeholders before treating them as final production policy:

- Whether zero-value orders are valid.
- How duplicate order IDs should be handled.
- Whether a finance provider is required for every EMI, lease, or loan record, including cancelled orders.
- Whether negative discounts can represent valid adjustments.
- The accepted net-amount calculation and tolerance.
- The complete list of permitted order statuses.

## Reproducibility and security

- Keep credentials, passwords, connection strings containing secrets, access keys, and `.env` files out of Git.
- Do not commit local virtual environments, Spark temporary files, or generated data files unless the repository intentionally tracks approved sample data.
- Document dataset assumptions and the exact validation rules used for each run.
