# Automotive dbt project

## Source
Databricks table: sales_transformations.default.silver_orders

## Layers
- Staging: standardizes the Silver source.
- Intermediate: derives a sales-reporting eligibility flag.
- Marts: daily sales, monthly sales, dealer sales, payment analysis, and order quality.

## Safety
Gold models use a separate dbt target schema. Rows marked for review are excluded from sales-reporting marts but remain represented in the quality mart.

## Commands
- `uv run dbt --version`
- `uv run dbt parse`
- `uv run dbt debug`
- `uv run dbt build`

Configure the dbt profile and credentials before connecting or building.
