\set ON_ERROR_STOP on

/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script validates the integrity and structure of the Bronze layer.
    It includes checks for:
    - row counts in the raw Bronze tables
    - source_table lineage values
    - ingested_at presence
    - required Bronze columns populated

Usage Notes:
    - Run these checks after loading Bronze.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

DO $$
DECLARE
    bronze_1737_count INTEGER;
    bronze_7060_count INTEGER;
    bronze_1737_source_table_mismatch INTEGER;
    bronze_7060_source_table_mismatch INTEGER;
    bronze_1737_missing_ingested_at INTEGER;
    bronze_7060_missing_ingested_at INTEGER;
 
BEGIN
    -- Count raw rows first so we can confirm the load produced usable data.
    SELECT COUNT(*)
    INTO bronze_1737_count
    FROM bronze.ipca_1737_raw;

    SELECT COUNT(*)
    INTO bronze_7060_count
    FROM bronze.ipca_7060_raw;

    -- Verify lineage metadata was preserved correctly by the load step.
    SELECT COUNT(*)
    INTO bronze_1737_source_table_mismatch
    FROM bronze.ipca_1737_raw
    WHERE source_table IS DISTINCT FROM 1737;

    SELECT COUNT(*)
    INTO bronze_7060_source_table_mismatch
    FROM bronze.ipca_7060_raw
    WHERE source_table IS DISTINCT FROM 7060;

    -- Ensure every Bronze row has an ingestion timestamp.
    SELECT COUNT(*)
    INTO bronze_1737_missing_ingested_at
    FROM bronze.ipca_1737_raw
    WHERE ingested_at IS NULL;

    SELECT COUNT(*)
    INTO bronze_7060_missing_ingested_at
    FROM bronze.ipca_7060_raw
    WHERE ingested_at IS NULL;

    -- The Bronze tables must not be empty after a successful ingestion.
    IF bronze_1737_count = 0 THEN
        RAISE EXCEPTION
            'Bronze 1737 is empty after load.';
    END IF;

    IF bronze_7060_count = 0 THEN
        RAISE EXCEPTION
            'Bronze 7060 is empty after load.';
    END IF;

    -- Lineage failures break traceability, so we stop immediately when they appear.
    IF bronze_1737_source_table_mismatch <> 0 THEN
        RAISE EXCEPTION
            'Bronze 1737 has % rows with invalid source_table values.',
            bronze_1737_source_table_mismatch;
    END IF;

    IF bronze_7060_source_table_mismatch <> 0 THEN
        RAISE EXCEPTION
            'Bronze 7060 has % rows with invalid source_table values.',
            bronze_7060_source_table_mismatch;
    END IF;

    -- Missing ingestion timestamps usually signal a broken load path or partial write.
    IF bronze_1737_missing_ingested_at <> 0 THEN
        RAISE EXCEPTION
            'Bronze 1737 has % rows with missing ingested_at.',
            bronze_1737_missing_ingested_at;
    END IF;

    IF bronze_7060_missing_ingested_at <> 0 THEN
        RAISE EXCEPTION
            'Bronze 7060 has % rows with missing ingested_at.',
            bronze_7060_missing_ingested_at;
    END IF;

END $$;

SELECT 'bronze_validation_passed' AS status;
