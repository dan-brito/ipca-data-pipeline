-- Bronze raw tables for IPCA SIDRA extractions.
-- `ipca_1737_raw` stores the national series.
-- `ipca_7060_raw` stores the regional series by locality and classification.
-- Both tables preserve the original payload shape and add lineage metadata.

CREATE TABLE IF NOT EXISTS bronze.ipca_1737_raw (
    -- Shared SIDRA attributes
    territorial_level_code INTEGER,
    territorial_level_name TEXT,
    unit_of_measure_code INTEGER,
    unit_of_measure_name TEXT,
    value NUMERIC(15,6),
    variable_code INTEGER,
    variable_name TEXT,
    month_code INTEGER,
    month_name TEXT,
    location_code INTEGER,
    location_name TEXT,

    -- Lineage metadata
    source_table INTEGER DEFAULT 1737,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.ipca_7060_raw (
    -- Shared SIDRA attributes
    territorial_level_code INTEGER,
    territorial_level_name TEXT,
    unit_of_measure_code INTEGER,
    unit_of_measure_name TEXT,
    value NUMERIC(15,6),
    variable_code INTEGER,
    variable_name TEXT,
    month_code INTEGER,
    month_name TEXT,
    location_code INTEGER,
    location_name TEXT,

    -- Regional-specific fields
    classification_code INTEGER,
    classification_name TEXT,

    -- Lineage metadata
    source_table INTEGER DEFAULT 7060,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
