-- Initialize medallion architecture schemas in PostgreSQL

-- Bronze schema (raw data)
CREATE SCHEMA IF NOT EXISTS bronze;

-- Silver schema (cleaned/processed data)
CREATE SCHEMA IF NOT EXISTS silver;

-- Gold schema (aggregated/business-ready data)
CREATE SCHEMA IF NOT EXISTS gold;
