\set ON_ERROR_STOP on

/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script validates the integrity and completeness of the Silver layer.
    It emits a complete report with one line per check in the format:
        check_name -> PASS|FAIL -> detail

Validation Scope:
    1. unexpected nullability
    2. grain duplication
    3. expected referential integrity
    4. dynamic volume/count sanity checks against Bronze

Usage Notes:
    - Run these checks after loading Silver.
    - Review every reported check, even when the script succeeds.
    - On failures, the report is emitted as NOTICE lines before a final EXCEPTION.
===============================================================================
*/

BEGIN;

DROP TABLE IF EXISTS silver_validation_report;

-- Temporary table to hold validation results before final reporting
CREATE TEMP TABLE silver_validation_report (
    check_name   TEXT,
    check_status TEXT,
    check_detail TEXT,
    issue_count  BIGINT
) ON COMMIT DROP;

/*
----------------------------------------------------------------------------
1) Unexpected Nullability
----------------------------------------------------------------------------
*/

INSERT INTO silver_validation_report (
    check_name,
    check_status,
    check_detail,
    issue_count
)
SELECT
    check_name,
    CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('%s lines', issue_count),
    issue_count
FROM (
    SELECT 'null_month_code_dim_month' AS check_name, COUNT(*) AS issue_count
    FROM silver.dim_month
    WHERE month_code IS NULL

    UNION ALL
    SELECT 'null_territorial_level_code_dim_location', COUNT(*)
    FROM silver.dim_location
    WHERE territorial_level_code IS NULL

    UNION ALL
    SELECT 'null_location_code_dim_location', COUNT(*)
    FROM silver.dim_location
    WHERE location_code IS NULL

    UNION ALL
    SELECT 'null_location_id_fact_brazil', COUNT(*)
    FROM silver.fact_ipca_index_number_brazil
    WHERE location_id IS NULL

    UNION ALL 
    SELECT 'null_month_code_fact_brazil', COUNT(*)
    FROM silver.fact_ipca_index_number_brazil
    WHERE month_code IS NULL

    UNION ALL
    SELECT 'null_index_number_fact_brazil', COUNT(*)
    FROM silver.fact_ipca_index_number_brazil
    WHERE index_number IS NULL

    UNION ALL
    SELECT 'null_location_id_fact_regional', COUNT(*)
    FROM silver.fact_ipca_monthly_variation_regional
    WHERE location_id IS NULL

    UNION ALL
    SELECT 'null_month_code_fact_regional', COUNT(*)
    FROM silver.fact_ipca_monthly_variation_regional
    WHERE month_code IS NULL
    
    UNION ALL
    SELECT 'null_monthly_variation_fact_regional', COUNT(*) 
    FROM silver.fact_ipca_monthly_variation_regional
    WHERE monthly_variation IS NULL
) AS nullability_issues;

/*
----------------------------------------------------------------------------
2) Grain Duplication
----------------------------------------------------------------------------
*/

INSERT INTO silver_validation_report (
    check_name,
    check_status,
    check_detail,
    issue_count
)
SELECT
    check_name,
    CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('%s duplicate groups', issue_count),
    issue_count
FROM (
    SELECT 'duplicate_dim_month_month_code' AS check_name, COUNT(*) AS issue_count
    FROM (
        SELECT month_code
        FROM silver.dim_month
        GROUP BY month_code
        HAVING COUNT(*) > 1
    ) AS duplicates_groups
        
    UNION ALL

    SELECT 'duplicate_dim_location_id', COUNT(*)
    FROM (
        SELECT location_id
        FROM silver.dim_location
        GROUP BY location_id
        HAVING COUNT(*) > 1
    ) AS duplicates_groups

    UNION ALL

    SELECT 'duplicate_fact_brazil_month_location_grain', COUNT(*)
    FROM (
        SELECT month_code, location_id
        FROM silver.fact_ipca_index_number_brazil
        GROUP BY month_code, location_id
        HAVING COUNT(*) > 1
    ) AS duplicates_groups

    UNION ALL

    SELECT 'duplicate_fact_regional_month_location_grain', COUNT(*)
    FROM (
        SELECT month_code, location_id
        FROM silver.fact_ipca_monthly_variation_regional
        GROUP BY month_code, location_id
        HAVING COUNT(*) > 1
    ) AS duplicates_groups
) AS duplication_issues;

/*
----------------------------------------------------------------------------
3) Expected Referential Integrity
----------------------------------------------------------------------------
*/

INSERT INTO silver_validation_report (
    check_name,
    check_status,
    check_detail,
    issue_count
)
SELECT
    check_name,
    CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('%s orphan fact rows', issue_count),
    issue_count
FROM (
    SELECT 'orphan_month_code_fact_brazil' AS check_name, COUNT(*) AS issue_count
    FROM silver.fact_ipca_index_number_brazil fact
    LEFT JOIN silver.dim_month dim
        ON dim.month_code = fact.month_code
    WHERE dim.month_code IS NULL

    UNION ALL
    SELECT 'orphan_month_code_fact_regional', COUNT(*)
    FROM silver.fact_ipca_monthly_variation_regional fact
    LEFT JOIN silver.dim_month dim
        ON dim.month_code = fact.month_code
    WHERE dim.month_code IS NULL

    UNION ALL
    SELECT 'orphan_location_id_fact_brazil', COUNT(*)
    FROM silver.fact_ipca_index_number_brazil fact
    LEFT JOIN silver.dim_location dim
        ON dim.location_id = fact.location_id
    WHERE dim.location_id IS NULL

    UNION ALL
    SELECT 'orphan_location_id_fact_regional', COUNT(*)
    FROM silver.fact_ipca_monthly_variation_regional fact
    LEFT JOIN silver.dim_location dim
        ON dim.location_id = fact.location_id
    WHERE dim.location_id IS NULL
) AS referential_issues;

/*
----------------------------------------------------------------------------
4) Dynamic Volume / Count Sanity Checks
----------------------------------------------------------------------------
*/

INSERT INTO silver_validation_report (
    check_name,
    check_status,
    check_detail,
    issue_count
)
SELECT
    'row_count_mismatch_fact_brazil',
    CASE WHEN ABS(expected_count - actual_count) = 0 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT(
        'bronze=%s, silver=%s, mismatch=%s',
        expected_count,
        actual_count,
        ABS(expected_count - actual_count)
    ),
    ABS(expected_count - actual_count)
FROM (
    SELECT
        (SELECT COUNT(*) FROM bronze.ipca_1737_raw) AS expected_count,
        (SELECT COUNT(*) FROM silver.fact_ipca_index_number_brazil) AS actual_count
) AS counts;

INSERT INTO silver_validation_report (
    check_name,
    check_status,
    check_detail,
    issue_count
)
SELECT
    'row_count_mismatch_fact_regional',
    CASE WHEN ABS(expected_count - actual_count) = 0 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT(
        'bronze=%s, silver=%s, mismatch=%s',
        expected_count,
        actual_count,
        ABS(expected_count - actual_count)
    ),
    ABS(expected_count - actual_count)
FROM (
    SELECT
        (SELECT COUNT(*) FROM bronze.ipca_7060_raw) AS expected_count,
        (SELECT COUNT(*) FROM silver.fact_ipca_monthly_variation_regional) AS actual_count
) AS counts;        

DO $$
DECLARE
    rec RECORD;
    failure_count BIGINT;
BEGIN
    FOR rec IN
        SELECT *
        FROM silver_validation_report
    LOOP
        RAISE NOTICE '% -> % -> %',
            rec.check_name,
            rec.check_status,
            rec.check_detail;
    END LOOP;

    SELECT COUNT(*)
    INTO failure_count
    FROM silver_validation_report
    WHERE check_status = 'FAIL';

    IF failure_count > 0 THEN
        RAISE EXCEPTION 'Silver validation failed (% failing checks)', failure_count;
    END IF;
END $$;


SELECT 'silver_validation_passed' AS status;

COMMIT;